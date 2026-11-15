use crate::async_bridge::{Awaitable, awaitable};
use crate::client::PyLaser;
use laser_sdk::connection::ConnectionEvents;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::get_runtime;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Watch this connection get connected, disconnected, signed in, signed
    /// out, and shut down, from now on. `async for event in
    /// laser.connection_events()` yields each event as `"connected"`,
    /// `"disconnected"`, `"signed_in"`, `"signed_out"`, or `"shutdown"`, and
    /// stops after `"shutdown"`. A leader change that moves the client to
    /// another node is not a disconnect.
    fn connection_events(&self) -> PyConnectionEvents {
        let laser = self.inner.clone();
        let (subscribed, subscription) = oneshot::channel();
        get_runtime().spawn(async move {
            let _ = subscribed.send(laser.connection_events().await);
        });
        PyConnectionEvents {
            state: Arc::new(Mutex::new(Subscription::Pending(subscription))),
        }
    }
}

enum Subscription {
    Pending(oneshot::Receiver<ConnectionEvents>),
    Ready(ConnectionEvents),
    Ended,
}

/// `Laser.connection_events`.
#[gen_stub_pyclass]
#[pyclass(name = "ConnectionEvents")]
pub struct PyConnectionEvents {
    state: Arc<Mutex<Subscription>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConnectionEvents {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[typing.Literal['connected', 'disconnected', 'signed_in', 'signed_out', 'shutdown']]",
        imports = ("typing",)
    ))]
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, String>> {
        let state = Arc::clone(&self.state);
        awaitable(py, async move {
            let mut state = state.lock().await;
            if let Subscription::Pending(subscription) = &mut *state {
                *state = match subscription.await {
                    Ok(events) => Subscription::Ready(events),
                    Err(_) => Subscription::Ended,
                };
            }
            let event = match &mut *state {
                Subscription::Ready(events) => events.next().await,
                _ => None,
            };
            match event {
                Some(event) => Ok(event.to_string()),
                None => {
                    *state = Subscription::Ended;
                    Err(PyStopAsyncIteration::new_err(()))
                }
            }
        })
    }
}
