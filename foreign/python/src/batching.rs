use crate::async_bridge::future_into_py;
use crate::convert::payload_bytes;
use crate::errors::to_pyerr;
use laser_sdk::batching::{BatchingProducer, BatchingProducerBuilder};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A size-and-time batching publisher, built by `Topic.batching`. `send`
/// queues a payload and flushes inline when a size bound trips, `flush` sends
/// what is queued, and `close` flushes and stops the linger timer.
#[gen_stub_pyclass]
#[pyclass(name = "BatchingProducer", frozen)]
pub struct PyBatchingProducer {
    inner: Arc<Mutex<State>>,
}

enum State {
    Pending(Box<BatchingProducerBuilder>),
    Ready(BatchingProducer),
    Closed,
}

impl State {
    fn producer(&mut self) -> PyResult<&BatchingProducer> {
        if matches!(self, Self::Pending(_)) {
            let Self::Pending(builder) = std::mem::replace(self, Self::Closed) else {
                unreachable!("pending producer state was checked");
            };
            *self = Self::Ready(builder.build());
        }
        match self {
            Self::Ready(producer) => Ok(producer),
            Self::Pending(_) | Self::Closed => Err(closed()),
        }
    }
}

impl PyBatchingProducer {
    pub(crate) fn new(builder: BatchingProducerBuilder) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State::Pending(Box::new(builder)))),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBatchingProducer {
    /// Queue `payload` with optional `headers`. Flushes inline when a size
    /// bound trips, so backpressure lands on the sender.
    #[pyo3(signature = (payload, *, headers=None))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload = payload_bytes(payload)?;
        let headers = crate::transport::headers(headers)?;
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let producer = guard.producer()?;
            producer.send(payload, headers).await.map_err(to_pyerr)
        })
    }

    /// Flush everything queued as one batch append. A no-op on an empty queue.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard.producer()?.flush().await.map_err(to_pyerr)
        })
    }

    /// Flush and stop the linger timer. Later sends raise `InvalidError`.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            match std::mem::replace(&mut *guard, State::Closed) {
                State::Ready(producer) => producer.close().await.map_err(to_pyerr),
                State::Pending(_) | State::Closed => Ok(()),
            }
        })
    }
}

fn closed() -> PyErr {
    crate::errors::InvalidError::new_err("the batching producer is closed")
}
