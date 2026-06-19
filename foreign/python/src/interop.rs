use crate::agent_runtime::static_topic;
use crate::client::PyLaser;
use crate::convert::{json_to_py, payload_bytes, py_to_de, py_to_json, ser_to_py};
use crate::errors::to_pyerr;
use laser_sdk::LaserError;
use laser_sdk::a2a::A2aBridge;
use laser_sdk::mcp::{McpBridge, McpPrompt};
use laser_sdk::types::ConversationId;
use laser_sdk::wire::agent::AgentId;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

// The bridges publish AGDX, which uses the wire agent id (a validated name string).
fn agent_id(value: &str) -> PyResult<AgentId> {
    value.parse().map_err(|e| to_pyerr(LaserError::from(e)))
}

fn parse_conversation(value: &str) -> PyResult<ConversationId> {
    ConversationId::from_str(value).map_err(|e| to_pyerr(e.into()))
}

// A JSON-ish argument accepts `str` / `bytes` (used verbatim) or any other value
// (encoded as JSON), so callers can pass either a raw body or a Python dict.
fn json_arg(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = payload_bytes(obj) {
        return Ok(bytes);
    }
    let value = py_to_json(obj)?;
    serde_json::to_vec(&value).map_err(|e| crate::errors::CodecError::new_err(e.to_string()))
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// An A2A bridge mapping JSON-RPC methods onto agent topics, publishing as
    /// `source`. Use it to drive an agent as an A2A task source from Python.
    fn a2a_bridge(
        &self,
        source: String,
        request_topic: String,
        reply_topic: String,
    ) -> PyResult<PyA2aBridge> {
        Ok(PyA2aBridge {
            inner: Arc::new(A2aBridge::new(
                self.inner.clone(),
                agent_id(&source)?,
                static_topic(request_topic),
                static_topic(reply_topic),
            )),
        })
    }

    /// An MCP bridge serving tools / resources / prompts over the log, publishing
    /// tool calls as `source`. `tools` / `resources` / `prompts` are lists of
    /// dicts (a tool is `{name, description?, input_schema}`, a prompt is
    /// `{prompt: {name, title?, description?, arguments?}, messages: [[role, text]]}`).
    #[pyo3(signature = (source, tool_topic, reply_topic, server_name, *, tools=None, resources=None, prompts=None, timeout_secs=None))]
    #[allow(clippy::too_many_arguments)]
    fn mcp_bridge(
        &self,
        source: String,
        tool_topic: String,
        reply_topic: String,
        server_name: String,
        tools: Option<&Bound<'_, PyAny>>,
        resources: Option<&Bound<'_, PyAny>>,
        prompts: Option<&Bound<'_, PyAny>>,
        timeout_secs: Option<f64>,
    ) -> PyResult<PyMcpBridge> {
        let tools: Vec<ToolSpec> = tools.map(py_to_de).transpose()?.unwrap_or_default();
        let resources: Vec<ResourceSpec> = resources.map(py_to_de).transpose()?.unwrap_or_default();
        let prompts: Vec<PromptSpec> = prompts.map(py_to_de).transpose()?.unwrap_or_default();

        let mut bridge = McpBridge::new(
            self.inner.clone(),
            agent_id(&source)?,
            static_topic(tool_topic),
            static_topic(reply_topic),
            server_name,
        );
        for tool in tools {
            bridge = bridge.with_tool(tool.name, tool.description, tool.input_schema);
        }
        for resource in resources {
            bridge = bridge.with_resource(
                resource.uri,
                resource.name,
                resource.mime_type,
                resource.text,
            );
        }
        for prompt in prompts {
            bridge = bridge.with_prompt(prompt.prompt, prompt.messages);
        }
        if let Some(secs) = timeout_secs {
            bridge = bridge.with_timeout(Duration::from_secs_f64(secs));
        }
        Ok(PyMcpBridge {
            inner: Arc::new(bridge),
        })
    }

    /// Publish an AG-UI state snapshot (the full shared state as JSON) on `topic`.
    fn publish_state_snapshot<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        source: String,
        conversation_id: String,
        state: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = agent_id(&source)?;
        let conversation = parse_conversation(&conversation_id)?;
        let state = py_to_json(state)?;
        future_into_py(py, async move {
            laser
                .publish_state_snapshot(static_topic(topic), source, conversation, &state)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Publish an AG-UI state delta (an RFC 6902 JSON Patch document) on `topic`.
    fn publish_state_delta<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        source: String,
        conversation_id: String,
        patch: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = agent_id(&source)?;
        let conversation = parse_conversation(&conversation_id)?;
        let patch = py_to_json(patch)?;
        future_into_py(py, async move {
            laser
                .publish_state_delta(static_topic(topic), source, conversation, &patch)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Reconstruct AG-UI shared state by replaying the conversation's snapshot
    /// and deltas on `topic`, or `None` until a snapshot exists.
    fn reconstruct_state<'py>(
        &self,
        py: Python<'py>,
        conversation_id: String,
        topic: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let conversation = parse_conversation(&conversation_id)?;
        future_into_py(py, async move {
            let state = laser
                .reconstruct_state(conversation, static_topic(topic))
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| match state {
                Some(state) => json_to_py(py, &state),
                None => Ok(py.None()),
            })
        })
    }

    /// Render a conversation on `topic` as AG-UI events by reading the log.
    fn agui_events<'py>(
        &self,
        py: Python<'py>,
        conversation_id: String,
        topic: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let conversation = parse_conversation(&conversation_id)?;
        future_into_py(py, async move {
            let events = laser
                .agui_events(conversation, static_topic(topic))
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &events))
        })
    }
}

#[derive(Deserialize)]
struct ToolSpec {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    input_schema: serde_json::Value,
}

#[derive(Deserialize)]
struct ResourceSpec {
    uri: String,
    name: String,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    text: String,
}

#[derive(Deserialize)]
struct PromptSpec {
    prompt: McpPrompt,
    #[serde(default)]
    messages: Vec<(String, String)>,
}

/// An A2A bridge: drive an agent as an A2A task source (`message/send` ->
/// `tasks/get` / `tasks/cancel`).
#[gen_stub_pyclass]
#[pyclass(name = "A2aBridge", frozen)]
pub struct PyA2aBridge {
    inner: Arc<A2aBridge>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyA2aBridge {
    /// `message/send`: publish the params (a dict, or raw JSON str / bytes) as a
    /// task and return the submitted task as a dict.
    fn submit<'py>(
        &self,
        py: Python<'py>,
        params: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.clone();
        let params = json_arg(params)?;
        future_into_py(py, async move {
            let task = bridge.submit(params).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &task))
        })
    }

    /// `tasks/get`: the task's current state (Working until an answer lands).
    fn task<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.clone();
        future_into_py(py, async move {
            let task = bridge.task(&id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &task))
        })
    }

    /// `tasks/cancel`: cancel the task and return it canceled.
    fn cancel<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.clone();
        future_into_py(py, async move {
            let task = bridge.cancel(&id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &task))
        })
    }

    /// The bridge's A2A Agent Card, for discovery.
    fn card(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.card())
    }
}

/// An MCP bridge: serve tools / resources / prompts over the log and route
/// `tools/call` to an agent.
#[gen_stub_pyclass]
#[pyclass(name = "McpBridge", frozen)]
pub struct PyMcpBridge {
    inner: Arc<McpBridge>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMcpBridge {
    /// `initialize`: the protocol version and advertised capabilities.
    #[pyo3(signature = (protocol_version=None))]
    fn initialize(&self, py: Python<'_>, protocol_version: Option<String>) -> PyResult<Py<PyAny>> {
        let value = self.inner.initialize(protocol_version.as_deref());
        json_to_py(py, &value)
    }

    /// `tools/list`.
    fn list_tools(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &self.inner.list_tools())
    }

    /// `resources/list`.
    fn list_resources(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &self.inner.list_resources())
    }

    /// `resources/read`: the contents advertised for `uri`.
    fn read_resource(&self, py: Python<'_>, uri: String) -> PyResult<Py<PyAny>> {
        let value = self.inner.read_resource(&uri).map_err(to_pyerr)?;
        json_to_py(py, &value)
    }

    /// `prompts/list`.
    fn list_prompts(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &self.inner.list_prompts())
    }

    /// `prompts/get`: the rendered messages for the prompt `name`.
    fn get_prompt(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        let value = self.inner.get_prompt(&name).map_err(to_pyerr)?;
        json_to_py(py, &value)
    }

    /// `tools/call`: route the call to the agent and return the tool result as a
    /// dict. `arguments` is a dict, or raw JSON str / bytes.
    fn call_tool<'py>(
        &self,
        py: Python<'py>,
        name: String,
        arguments: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.clone();
        let arguments = json_arg(arguments)?;
        future_into_py(py, async move {
            let result = bridge.call_tool(&name, arguments).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }
}
