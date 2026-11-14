use crate::async_bridge::{Awaitable, PyHook, awaitable};
use crate::errors::InvalidError;
use axum::Router;
use axum::extract::Request;
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use axum::response::IntoResponse;
use laser_sdk::a2a::A2aCallerPrincipal;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString, PyTuple};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::net::SocketAddr;
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

// The shutdown sender and the task of a serving router.
type Running = (oneshot::Sender<()>, JoinHandle<std::io::Result<()>>);

/// A running bridge HTTP server from `A2aBridge.serve` or `McpBridge.serve`.
/// `port` is the bound port, the one the system picked when `serve` asked
/// for port 0. `close` stops accepting connections and waits for the requests
/// in flight.
#[gen_stub_pyclass]
#[pyclass(name = "BridgeServer", frozen)]
pub struct PyBridgeServer {
    address: SocketAddr,
    running: Mutex<Option<Running>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyBridgeServer {
    #[getter]
    fn host(&self) -> String {
        self.address.ip().to_string()
    }

    #[getter]
    const fn port(&self) -> u16 {
        self.address.port()
    }

    /// The `http://host:port` base URL of the JSON-RPC endpoint.
    #[getter]
    fn url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Stop the server. A second call does nothing.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, ()>> {
        let running = self
            .running
            .lock()
            .expect("the bridge server lock is never poisoned")
            .take();
        awaitable(py, async move {
            if let Some((stop, task)) = running {
                let _ = stop.send(());
                let _ = task.await;
            }
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        format!("BridgeServer(url=http://{})", self.address)
    }
}

pub(crate) fn serve<'py>(
    py: Python<'py>,
    router: Router,
    host: String,
    port: u16,
) -> PyResult<Awaitable<'py, PyBridgeServer>> {
    awaitable(py, async move {
        let listener = TcpListener::bind((host.as_str(), port))
            .await
            .map_err(|error| {
                InvalidError::new_err(format!("cannot listen on {host}:{port}: {error}"))
            })?;
        let address = listener.local_addr().map_err(|error| {
            InvalidError::new_err(format!("cannot read the bound address: {error}"))
        })?;
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
        });
        Ok(PyBridgeServer {
            address,
            running: Mutex::new(Some((stop, task))),
        })
    })
}

// Run the host's `caller` hook on each JSON-RPC request, `POST /`, and hand
// the principal it names to the A2A router. A hook that raises or returns
// anything but a string or `None` refuses the request with 401.
pub(crate) fn with_caller(router: Router, caller: Option<PyHook>) -> Router {
    let Some(caller) = caller else {
        return router;
    };
    router.layer(axum::middleware::from_fn(
        move |mut request: Request, next: Next| {
            let caller = caller.clone();
            async move {
                if request.method() != Method::POST || request.uri().path() != "/" {
                    return next.run(request).await;
                }
                let headers: Vec<(String, String)> = request
                    .headers()
                    .keys()
                    .map(|name| {
                        let values: Vec<String> = request
                            .headers()
                            .get_all(name)
                            .iter()
                            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
                            .collect();
                        (name.as_str().to_owned(), values.join(", "))
                    })
                    .collect();
                let principal = caller
                    .call(|py| {
                        let dict = PyDict::new(py);
                        for (name, value) in &headers {
                            dict.set_item(name, value)?;
                        }
                        PyTuple::new(py, [dict])
                    })
                    .await
                    .and_then(|value| {
                        Python::attach(|py| {
                            let value = value.bind(py);
                            if value.is_none() {
                                Ok(None)
                            } else {
                                Ok(Some(value.cast::<PyString>()?.to_str()?.to_owned()))
                            }
                        })
                    });
                match principal {
                    Ok(Some(principal)) => {
                        request
                            .extensions_mut()
                            .insert(A2aCallerPrincipal(principal));
                        next.run(request).await
                    }
                    Ok(None) => next.run(request).await,
                    Err(_) => StatusCode::UNAUTHORIZED.into_response(),
                }
            }
        },
    ))
}
