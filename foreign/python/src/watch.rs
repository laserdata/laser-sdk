use crate::async_bridge::{Awaitable, awaitable};
use crate::client::PyLaser;
use crate::convert::duration_ms;
use crate::errors::{InvalidError, to_pyerr};
use crate::ids::PyMessageId;
use laser_sdk::watch::{WatchReader, WatchedChange};
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// The change feed: one record per committed projector batch for a binding
    /// that opted into notify, so "query after my data landed" is await-then-query
    /// instead of sleep-and-retry. Narrow to one materialized index with `index=`,
    /// start at the feed's current end with `from_end=True`, or resume from
    /// persisted `offsets` with `from_offsets=`. Each change is a wakeup, not the
    /// rows: read the rows with `query()`. Opening the reader reads the
    /// capabilities again and raises `UnsupportedError` when the deployment does
    /// not publish the feed, as Rust and TypeScript fail when they open the
    /// records reader.
    #[pyo3(signature = (*, index=None, from_offsets=None, from_end=false))]
    fn watch<'py>(
        &self,
        py: Python<'py>,
        index: Option<String>,
        from_offsets: Option<Vec<u64>>,
        from_end: bool,
    ) -> PyResult<Awaitable<'py, PyWatchReader>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
            let mut watch = laser.watch();
            if let Some(index) = index {
                watch = watch.index(index);
            }
            if from_end {
                watch = watch.from_end();
            }
            let mut reader = watch.records().await.map_err(to_pyerr)?;
            if let Some(offsets) = from_offsets {
                reader = reader.from_offsets(offsets);
            }
            Ok(PyWatchReader::new(reader))
        })
    }
}

/// A resumable change-feed reader over the changes topic.
#[gen_stub_pyclass]
#[pyclass(name = "WatchReader")]
pub struct PyWatchReader {
    // Taken out only while `from_offsets` rebuilds it, never across an await.
    reader: Arc<tokio::sync::Mutex<Option<WatchReader>>>,
    // What the last finished poll left, read by the synchronous getters.
    state: Arc<Mutex<ReaderState>>,
    // The offsets `from_offsets` asked for, applied before the next poll.
    rewind: Arc<Mutex<Rewind>>,
    // Holds one poll's changes so `async for` yields them one at a time.
    buffered: Arc<Mutex<VecDeque<WatchedChange>>>,
}

impl PyWatchReader {
    fn new(reader: WatchReader) -> Self {
        let state = ReaderState::from(&reader);
        Self {
            reader: Arc::new(tokio::sync::Mutex::new(Some(reader))),
            state: Arc::new(Mutex::new(state)),
            rewind: Arc::new(Mutex::new(Rewind::default())),
            buffered: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn state(&self) -> ReaderState {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    // One read of the feed, waiting up to `wait` for the first change. A
    // rewind that landed while it ran wins: the read then returns nothing.
    fn read(&self, wait: Option<std::time::Duration>) -> ReadTask {
        ReadTask {
            reader: Arc::clone(&self.reader),
            state: Arc::clone(&self.state),
            rewind: Arc::clone(&self.rewind),
            wait,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWatchReader {
    /// The next offset to read on each partition. Persist this to resume later.
    #[getter]
    fn offsets(&self) -> Vec<u64> {
        self.state().offsets
    }

    /// Feed records retention removed before this reader reached them, counted
    /// from the offsets it started or resumed at. Non-zero means changes were
    /// missed: read the view again instead of trusting the feed.
    #[getter]
    fn missed(&self) -> u64 {
        self.state().missed
    }

    /// Feed records that were not change records and were skipped, for
    /// example other traffic on an overridden changes topic.
    #[getter]
    fn skipped(&self) -> u64 {
        self.state().skipped
    }

    /// Resume from persisted per-partition `offsets` (the value `offsets`
    /// returned earlier). Returns the same reader.
    fn from_offsets(slf: PyRef<'_, Self>, offsets: Vec<u64>) -> PyRef<'_, Self> {
        {
            let mut rewind = slf
                .rewind
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            rewind.generation += 1;
            rewind.offsets = Some(offsets.clone());
        }
        slf.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .offsets = offsets;
        slf.buffered
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        slf
    }

    /// Drain the changes that landed since the last poll, filtered to the
    /// watched index when one was set. Returns an empty list when caught up. A
    /// change record of another version stops its partition there: the changes
    /// before it are returned, and a poll with nothing else to return raises
    /// `CommandError` with code `VersionSkew`.
    fn poll<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<PyWatchedChange>>> {
        let read = self.read(None);
        awaitable(py, async move {
            let changes = read.run().await?;
            Ok(changes
                .into_iter()
                .map(PyWatchedChange::from)
                .collect::<Vec<_>>())
        })
    }

    /// `poll`, waiting up to `timeout_ms` milliseconds for the first change to
    /// land. Returns an empty list when none landed in time.
    fn poll_within<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: f64,
    ) -> PyResult<Awaitable<'py, Vec<PyWatchedChange>>> {
        let read = self.read(Some(duration_ms(timeout_ms, "timeout_ms")?));
        awaitable(py, async move {
            let changes = read.run().await?;
            Ok(changes
                .into_iter()
                .map(PyWatchedChange::from)
                .collect::<Vec<_>>())
        })
    }

    /// `async for change in reader` drains what landed since the last poll, one
    /// change per step, and stops (raises `StopAsyncIteration`) when caught up. A
    /// fresh `async for` later resumes from the same offsets.
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyWatchedChange>> {
        let buffered = Arc::clone(&self.buffered);
        let read = self.read(None);
        awaitable(py, async move {
            let next = buffered
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front();
            if let Some(change) = next {
                return Ok(PyWatchedChange::from(change));
            }
            let mut queue: VecDeque<WatchedChange> = read.run().await?.into_iter().collect();
            match queue.pop_front() {
                Some(change) => {
                    buffered
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .extend(queue);
                    Ok(PyWatchedChange::from(change))
                }
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }
}

/// One change-feed record with its place on the feed.
#[gen_stub_pyclass]
#[pyclass(name = "WatchedChange", frozen, get_all)]
pub struct PyWatchedChange {
    /// The materialized-view advancement.
    pub change: PyChangeRecord,
    /// The feed record's message id. A retried batch publishes a change again
    /// under the same id, so a reader drops a copy it has seen.
    pub message_id: u128,
    /// Where the record sits on the change feed.
    pub position: PyMessageId,
}

impl From<WatchedChange> for PyWatchedChange {
    fn from(watched: WatchedChange) -> Self {
        Self {
            change: PyChangeRecord::from(watched.change),
            message_id: watched.message_id,
            position: PyMessageId::from(watched.position),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWatchedChange {
    fn __repr__(&self) -> String {
        format!(
            "WatchedChange(index={}, message_id={}, position={})",
            self.change.index, self.message_id, self.position.inner
        )
    }
}

/// One change-feed record: a materialized index advanced past an offset window.
#[gen_stub_pyclass]
#[pyclass(name = "ChangeRecord", frozen, get_all, skip_from_py_object)]
#[derive(Clone)]
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
    /// The stream whose source the batch read, or None when the deployment
    /// publishes one feed for every stream.
    pub stream: Option<String>,
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
            stream: record.stream,
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

#[derive(Clone, Default)]
struct ReaderState {
    offsets: Vec<u64>,
    missed: u64,
    skipped: u64,
}

impl From<&WatchReader> for ReaderState {
    fn from(reader: &WatchReader) -> Self {
        Self {
            offsets: reader.offsets().to_vec(),
            missed: reader.missed(),
            skipped: reader.skipped(),
        }
    }
}

// The offsets `from_offsets` asked for, and how many rewinds there were, so a
// poll in flight across one knows its changes came from before it.
#[derive(Default)]
struct Rewind {
    generation: u64,
    offsets: Option<Vec<u64>>,
}

// The shared handles one poll needs, moved into its future.
struct ReadTask {
    reader: Arc<tokio::sync::Mutex<Option<WatchReader>>>,
    state: Arc<Mutex<ReaderState>>,
    rewind: Arc<Mutex<Rewind>>,
    wait: Option<std::time::Duration>,
}

impl ReadTask {
    async fn run(self) -> PyResult<Vec<WatchedChange>> {
        let mut guard = self.reader.lock().await;
        let (generation, pending) = {
            let mut rewind = self
                .rewind
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (rewind.generation, rewind.offsets.take())
        };
        if let Some(offsets) = pending {
            *guard = guard.take().map(|reader| reader.from_offsets(offsets));
        }
        let reader = guard
            .as_mut()
            .ok_or_else(|| InvalidError::new_err("the watch reader is no longer open"))?;
        let changes = match self.wait {
            Some(timeout) => reader.poll_within(timeout).await,
            None => reader.poll().await,
        };
        let rewound = self
            .rewind
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .generation
            != generation;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let offsets = std::mem::take(&mut state.offsets);
        *state = ReaderState::from(&*reader);
        if rewound {
            state.offsets = offsets;
        }
        drop(state);
        let changes = changes.map_err(to_pyerr)?;
        Ok(if rewound { Vec::new() } else { changes })
    }
}
