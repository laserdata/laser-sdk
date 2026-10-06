use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::errors::to_pyerr;
use laser_sdk::laser::Laser;
use laser_sdk::wire::change::ChangeRecord;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// The change feed: one record per committed projector batch for a binding
    /// that opted into notify, so "query after my data landed" is await-then-query
    /// instead of sleep-and-retry. Narrow to one materialized index with `index=`,
    /// resume from persisted `offsets` with `from_offsets=`. Each record is a
    /// wakeup, not the rows: read the rows with `query()`. Requires the `watch`
    /// capability: opening the reader raises `UnsupportedError` when the
    /// deployment does not publish the feed, as Rust and TypeScript fail when
    /// they open the records reader.
    #[pyo3(signature = (*, index=None, from_offsets=None))]
    fn watch(
        &self,
        index: Option<String>,
        from_offsets: Option<Vec<u64>>,
    ) -> PyResult<PyWatchReader> {
        self.inner.watch().records().map_err(to_pyerr)?;
        Ok(PyWatchReader {
            laser: self.inner.clone(),
            index,
            offsets: Arc::new(Mutex::new(from_offsets.unwrap_or_default())),
            buffered: Arc::new(Mutex::new(VecDeque::new())),
            rewinds: Arc::new(AtomicU64::new(0)),
        })
    }
}

/// A resumable change-feed reader over the changes topic.
#[gen_stub_pyclass]
#[pyclass(name = "WatchReader")]
pub struct PyWatchReader {
    laser: Laser,
    index: Option<String>,
    // Shared so the 'static poll future can read the saved offsets and write back
    // the advanced ones, keeping resumption correct across polls.
    offsets: Arc<Mutex<Vec<u64>>>,
    // Holds one poll's records so `async for` yields them one at a time.
    buffered: Arc<Mutex<VecDeque<ChangeRecord>>>,
    // Counts `from_offsets` calls. A poll that was in flight across one must
    // not write its older offsets back over the rewind.
    rewinds: Arc<AtomicU64>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWatchReader {
    /// The next offset to read on each partition. Persist this to resume later.
    #[getter]
    fn offsets(&self) -> Vec<u64> {
        self.offsets.lock().expect("offsets lock").clone()
    }

    /// Resume from persisted per-partition `offsets` (the value `offsets`
    /// returned earlier). Returns the same reader.
    fn from_offsets(slf: PyRef<'_, Self>, offsets: Vec<u64>) -> PyRef<'_, Self> {
        slf.rewinds.fetch_add(1, Ordering::AcqRel);
        *slf.offsets.lock().expect("offsets lock") = offsets;
        slf.buffered.lock().expect("buffer lock").clear();
        slf
    }

    /// Drain the change records that landed since the last poll, filtered to the
    /// watched index when one was set. Returns an empty list when caught up.
    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let index = self.index.clone();
        let offsets = self.offsets.clone();
        let rewinds = self.rewinds.clone();
        let saved = offsets.lock().expect("offsets lock").clone();
        let rewind = rewinds.load(Ordering::Acquire);
        future_into_py(py, async move {
            let mut watch = laser.watch();
            if let Some(index) = &index {
                watch = watch.index(index.clone());
            }
            let mut reader = watch.records().map_err(to_pyerr)?.from_offsets(saved);
            let records = reader.poll().await.map_err(to_pyerr)?;
            if rewinds.load(Ordering::Acquire) != rewind {
                return Ok(Vec::new());
            }
            *offsets.lock().expect("offsets lock") = reader.offsets().to_vec();
            Ok(records
                .into_iter()
                .map(PyChangeRecord::from)
                .collect::<Vec<_>>())
        })
    }

    /// `async for record in reader` drains what landed since the last poll, one
    /// record per step, and stops (raises `StopAsyncIteration`) when caught up. A
    /// fresh `async for` later resumes from the same offsets.
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let index = self.index.clone();
        let offsets = self.offsets.clone();
        let buffered = self.buffered.clone();
        let rewinds = self.rewinds.clone();
        future_into_py(py, async move {
            if let Some(record) = buffered.lock().expect("buffer lock").pop_front() {
                return Ok(PyChangeRecord::from(record));
            }
            let saved = offsets.lock().expect("offsets lock").clone();
            let rewind = rewinds.load(Ordering::Acquire);
            let mut watch = laser.watch();
            if let Some(index) = &index {
                watch = watch.index(index.clone());
            }
            let mut reader = watch.records().map_err(to_pyerr)?.from_offsets(saved);
            let records = reader.poll().await.map_err(to_pyerr)?;
            if rewinds.load(Ordering::Acquire) != rewind {
                return Err(PyStopAsyncIteration::new_err(()));
            }
            *offsets.lock().expect("offsets lock") = reader.offsets().to_vec();
            let mut queue: VecDeque<ChangeRecord> = records.into_iter().collect();
            match queue.pop_front() {
                Some(record) => {
                    buffered.lock().expect("buffer lock").extend(queue);
                    Ok(PyChangeRecord::from(record))
                }
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }
}

/// One change-feed record: a materialized index advanced past an offset window.
#[gen_stub_pyclass]
#[pyclass(name = "ChangeRecord", frozen, get_all)]
pub struct PyChangeRecord {
    /// The change-feed record version.
    pub v: u32,
    /// The materialized index that advanced.
    pub index: String,
    /// The source partition the batch came from.
    pub partition_id: u32,
    /// First source offset the committed batch covered (inclusive).
    pub from_offset: u64,
    /// Last source offset the committed batch covered (inclusive).
    pub to_offset: u64,
    /// Rows the batch wrote.
    pub rows: u32,
}

impl From<laser_sdk::wire::change::ChangeRecord> for PyChangeRecord {
    fn from(record: laser_sdk::wire::change::ChangeRecord) -> Self {
        Self {
            v: record.v,
            index: record.index,
            partition_id: record.partition_id,
            from_offset: record.from_offset,
            to_offset: record.to_offset,
            rows: record.rows,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyChangeRecord {
    fn __repr__(&self) -> String {
        format!(
            "ChangeRecord(index={}, partition={}, offsets={}..={}, rows={})",
            self.index, self.partition_id, self.from_offset, self.to_offset, self.rows
        )
    }
}
