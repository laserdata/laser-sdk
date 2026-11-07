use crate::async_bridge::{Awaitable, awaitable};
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
/// what is queued, and `close` flushes and stops the linger timer. A failed
/// linger flush never stops the timer. Its failure is kept and raised by the
/// next `send`, `flush`, or `close`, whichever comes first.
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

    // The work `close` and an `async with` exit share.
    fn closing(&self) -> impl Future<Output = PyResult<()>> + Send + 'static {
        let inner = self.inner.clone();
        async move {
            let mut guard = inner.lock().await;
            match std::mem::replace(&mut *guard, State::Closed) {
                State::Ready(producer) => producer.close().await.map_err(to_pyerr),
                State::Pending(_) | State::Closed => Ok(()),
            }
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBatchingProducer {
    /// Queue `payload` with optional `headers`. Flushes inline when a size
    /// bound trips, so backpressure lands on the sender, and raises that
    /// flush's failure with this record unconfirmed. When a linger flush
    /// failed before this call, the record is not queued: it raises that kept
    /// failure with this record added to its unconfirmed records, once.
    #[pyo3(signature = (payload, *, headers=None))]
    fn send<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Awaitable<'py, ()>> {
        let payload = payload_bytes(payload)?;
        let headers = crate::transport::headers(headers)?;
        let inner = self.inner.clone();
        awaitable(py, async move {
            let mut guard = inner.lock().await;
            let producer = guard.producer()?;
            producer.send(payload, headers).await.map_err(to_pyerr)
        })
    }

    /// Flush everything queued as one batch append, then raise the failure an
    /// earlier linger flush kept, if any. A no-op on an empty queue.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, ()>> {
        let inner = self.inner.clone();
        awaitable(py, async move {
            let mut guard = inner.lock().await;
            guard.producer()?.flush().await.map_err(to_pyerr)
        })
    }

    /// Flush and stop the linger timer, then raise a kept linger failure.
    /// Later sends raise `InvalidError`.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, ()>> {
        awaitable(py, self.closing())
    }

    /// Enter `async with`: yield the producer. Exiting the block closes it.
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Awaitable<'py, Py<Self>>> {
        awaitable(py, async move { Ok(slf) })
    }

    /// Exit `async with`: `close`, then return `False` so an exception in the
    /// body is not suppressed.
    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, bool>> {
        let closing = self.closing();
        awaitable(py, async move {
            closing.await?;
            Ok(false)
        })
    }
}

fn closed() -> PyErr {
    crate::errors::InvalidError::new_err("the batching producer is closed")
}
