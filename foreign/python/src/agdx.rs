use crate::agent_runtime::static_topic;
use crate::client::PyLaser;
use crate::convert::payload_bytes;
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::LaserError;
use laser_sdk::agent::{Agdx, AgdxStream};
use laser_sdk::wire::agent::{AgentId, ConversationId, CorrelationId};
use laser_sdk::wire::content::ContentType;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

fn wire_agent(value: &str) -> PyResult<AgentId> {
    value.parse().map_err(|e| to_pyerr(LaserError::from(e)))
}

fn wire_conversation(value: &str) -> PyResult<ConversationId> {
    ConversationId::from_str(value)
        .map_err(|e| InvalidError::new_err(format!("invalid conversation id: {e}")))
}

fn wire_correlation(value: &str) -> PyResult<CorrelationId> {
    CorrelationId::from_str(value)
        .map_err(|e| InvalidError::new_err(format!("invalid correlation id: {e}")))
}

fn parse_content_type(value: Option<String>) -> PyResult<Option<ContentType>> {
    match value {
        Some(value) => ContentType::from_str(&value)
            .map(Some)
            .map_err(|_| InvalidError::new_err(format!("unknown content type '{value}'"))),
        None => Ok(None),
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A typed Agent Data Exchange Protocol producer publishing as `source`
    /// within `conversation_id` on `topic`. Every send is a validated AGDX
    /// envelope (`command` / `respond` / `emit` / `stream`).
    fn agdx(&self, topic: String, source: String, conversation_id: String) -> PyResult<PyAgdx> {
        let source = wire_agent(&source)?;
        let conversation = wire_conversation(&conversation_id)?;
        Ok(PyAgdx {
            inner: self.inner.agdx(static_topic(topic), source, conversation),
        })
    }
}

/// The typed AGDX producer over one topic and conversation.
#[gen_stub_pyclass]
#[pyclass(name = "Agdx", frozen)]
pub struct PyAgdx {
    inner: Agdx,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgdx {
    /// Publish a `command` (expects a reply or effect under `correlation`).
    /// Returns the minted record id.
    #[pyo3(signature = (correlation, body, *, operation=None, content_type=None, target=None))]
    fn command<'py>(
        &self,
        py: Python<'py>,
        correlation: String,
        body: &Bound<'_, PyAny>,
        operation: Option<String>,
        content_type: Option<String>,
        target: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let correlation = wire_correlation(&correlation)?;
        let body = payload_bytes(body)?;
        let content_type = parse_content_type(content_type)?;
        let target = target.map(|t| wire_agent(&t)).transpose()?;
        future_into_py(py, async move {
            let mut send = agdx.command(correlation, body);
            if let Some(operation) = operation {
                send = send.with_operation(operation);
            }
            if let Some(content_type) = content_type {
                send = send.content_type(content_type);
            }
            if let Some(target) = target {
                send = send.with_target(target);
            }
            let record = send.send().await.map_err(to_pyerr)?;
            Ok(record.map(|id| id.to_string()))
        })
    }

    /// Publish a `response` (the paired answer to a command, same `correlation`).
    #[pyo3(signature = (correlation, body, *, operation=None, content_type=None, target=None))]
    fn respond<'py>(
        &self,
        py: Python<'py>,
        correlation: String,
        body: &Bound<'_, PyAny>,
        operation: Option<String>,
        content_type: Option<String>,
        target: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let correlation = wire_correlation(&correlation)?;
        let body = payload_bytes(body)?;
        let content_type = parse_content_type(content_type)?;
        let target = target.map(|t| wire_agent(&t)).transpose()?;
        future_into_py(py, async move {
            let mut send = agdx.respond(correlation, body);
            if let Some(operation) = operation {
                send = send.with_operation(operation);
            }
            if let Some(content_type) = content_type {
                send = send.content_type(content_type);
            }
            if let Some(target) = target {
                send = send.with_target(target);
            }
            let record = send.send().await.map_err(to_pyerr)?;
            Ok(record.map(|id| id.to_string()))
        })
    }

    /// Publish an `event` (expects nothing back).
    #[pyo3(signature = (body, *, operation=None, content_type=None, target=None))]
    fn emit<'py>(
        &self,
        py: Python<'py>,
        body: &Bound<'_, PyAny>,
        operation: Option<String>,
        content_type: Option<String>,
        target: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let body = payload_bytes(body)?;
        let content_type = parse_content_type(content_type)?;
        let target = target.map(|t| wire_agent(&t)).transpose()?;
        future_into_py(py, async move {
            let mut send = agdx.emit(body);
            if let Some(operation) = operation {
                send = send.with_operation(operation);
            }
            if let Some(content_type) = content_type {
                send = send.content_type(content_type);
            }
            if let Some(target) = target {
                send = send.with_target(target);
            }
            let record = send.send().await.map_err(to_pyerr)?;
            Ok(record.map(|id| id.to_string()))
        })
    }

    /// Open a chunk-stream writer under `correlation`. `purpose` is the
    /// chunk-stream vocabulary ('chat', 'reasoning', or 'tool_args').
    fn stream(&self, correlation: String, purpose: String) -> PyResult<PyAgdxStream> {
        let correlation = wire_correlation(&correlation)?;
        Ok(PyAgdxStream {
            inner: Arc::new(Mutex::new(Some(self.inner.stream(correlation, purpose)))),
        })
    }

    /// Human-in-the-loop interrupt/resume: publish a prompt `command` under a
    /// fresh correlation on this producer's topic, then await the human's
    /// correlated `response` on `reply_topic` up to `timeout_secs` and return
    /// its body bytes. A responder answers with `AgentCtx.respond_input`, or
    /// rejects with an error which raises here. Blocks the caller until the
    /// response lands or the timeout elapses, which is the point: the task is
    /// genuinely paused on a human.
    #[pyo3(signature = (reply_topic, prompt, *, timeout_secs=30.0))]
    fn request_input<'py>(
        &self,
        py: Python<'py>,
        reply_topic: String,
        prompt: &Bound<'_, PyAny>,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let prompt = payload_bytes(prompt)?;
        future_into_py(py, async move {
            let body = agdx
                .request_input(
                    static_topic(reply_topic),
                    prompt,
                    Duration::from_secs_f64(timeout_secs),
                )
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &body).into_any().unbind()))
        })
    }
}

/// A chunk-stream writer: `write` each chunk, then one terminal (`finish`). The
/// opening chunk carries the purpose.
#[gen_stub_pyclass]
#[pyclass(name = "AgdxStream")]
pub struct PyAgdxStream {
    inner: Arc<Mutex<Option<AgdxStream>>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgdxStream {
    /// Publish the next chunk (str, bytes, or bytearray).
    fn write<'py>(&self, py: Python<'py>, body: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let cell = self.inner.clone();
        let body = payload_bytes(body)?;
        future_into_py(py, async move {
            let mut guard = cell.lock().await;
            let stream = guard.as_mut().ok_or_else(|| {
                to_pyerr(LaserError::Handler(
                    "the stream is already finished".to_owned(),
                ))
            })?;
            stream.write(body).await.map_err(to_pyerr)
        })
    }

    /// Publish the terminal chunk with the reason the stream ended (default 'stop').
    #[pyo3(signature = (*, finish_reason="stop".to_owned()))]
    fn finish<'py>(&self, py: Python<'py>, finish_reason: String) -> PyResult<Bound<'py, PyAny>> {
        let cell = self.inner.clone();
        future_into_py(py, async move {
            let stream = cell.lock().await.take().ok_or_else(|| {
                to_pyerr(LaserError::Handler(
                    "the stream is already finished".to_owned(),
                ))
            })?;
            stream.finish(finish_reason, None).await.map_err(to_pyerr)
        })
    }
}
