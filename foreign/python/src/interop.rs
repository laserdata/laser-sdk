use crate::agent_runtime::static_topic;
use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{
    duration_seconds, json_to_py, payload_bytes, py_to_de, py_to_json, ser_to_py,
};
use crate::errors::to_pyerr;
use laser_sdk::LaserError;
use laser_sdk::a2a::A2aBridge;
use laser_sdk::mcp::{McpBridge, McpPrompt};
use laser_sdk::types::ConversationId;
use laser_sdk::wire::agent::AgentId;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};

/// Append a bridge id to `bridge_hops`, rejecting a repeated hop.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn enter_bridge(bridge: String, previous: Vec<String>) -> PyResult<Vec<String>> {
    laser_sdk::a2a::enter_bridge(&bridge, &previous).map_err(to_pyerr)
}
use serde::Deserialize;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

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
    if let Ok(payload) = payload_bytes(obj) {
        return Ok(payload);
    }
    let value = py_to_json(obj)?;
    serde_json::to_vec(&value).map_err(|e| crate::errors::CodecError::new_err(e.to_string()))
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
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
                .publish_state_snapshot(static_topic(topic)?, source, conversation, &state)
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
                .publish_state_delta(static_topic(topic)?, source, conversation, &patch)
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
                .reconstruct_state(conversation, static_topic(topic)?)
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
                .agui_events(conversation, static_topic(topic)?)
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

/// An A2A bridge: drive an agent as an A2A task source (`SendMessage` ->
/// `GetTask` / `CancelTask`).
#[gen_stub_pyclass]
#[pyclass(name = "A2aBridge", frozen)]
pub struct PyA2aBridge {
    inner: Held<A2aBridge>,
    source: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyA2aBridge {
    /// An A2A bridge mapping JSON-RPC methods onto agent topics over `laser`,
    /// publishing as `source`. Use it to drive an agent as an A2A task source
    /// from Python. `capabilities` (skill ids, or capability descriptor dicts)
    /// become the card's skills. `signing_key` signs the published task
    /// envelopes. Every record the bridge publishes carries its `bridge_hops`
    /// loop-guard path, starting at `source`.
    #[new]
    #[pyo3(signature = (laser, source, request_topic, reply_topic, *, capabilities=None, signing_key=None))]
    fn new(
        laser: PyRef<'_, PyLaser>,
        source: String,
        request_topic: String,
        reply_topic: String,
        capabilities: Option<Vec<Bound<'_, PyAny>>>,
        signing_key: Option<PyRef<'_, crate::sign::PySigningKey>>,
    ) -> PyResult<Self> {
        let mut bridge = A2aBridge::new(
            laser.inner.clone(),
            agent_id(&source)?,
            static_topic(request_topic)?,
            static_topic(reply_topic)?,
        );
        if let Some(capabilities) = capabilities {
            bridge = bridge.with_capabilities(
                capabilities
                    .iter()
                    .map(capability_descriptor)
                    .collect::<PyResult<Vec<_>>>()?,
            );
        }
        if let Some(key) = signing_key {
            bridge = bridge.with_signing_key(key.inner.as_ref().clone());
        }
        Ok(Self {
            inner: Held::new(bridge),
            source,
        })
    }

    fn handle_rpc<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.get();
        let request = py_to_json(request)?;
        future_into_py(py, async move {
            let response = bridge.handle_rpc(request).await;
            Python::attach(|py| ser_to_py(py, &response))
        })
    }

    /// `SendMessage`: publish the params (a dict, or raw JSON str / bytes) as a
    /// task and return the submitted task as a dict.
    fn submit<'py>(
        &self,
        py: Python<'py>,
        params_json: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.get();
        let params = json_arg(params_json)?;
        future_into_py(py, async move {
            let task = bridge.submit(params).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &task))
        })
    }

    /// `GetTask`: the task's current state (Working until an answer lands).
    fn task<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.get();
        future_into_py(py, async move {
            let task = bridge.task(&id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &task))
        })
    }

    /// `CancelTask`: cancel the task and return it canceled.
    fn cancel<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.get();
        future_into_py(py, async move {
            let task = bridge.cancel(&id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &task))
        })
    }

    /// The bridge's A2A Agent Card, for discovery.
    fn card(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.get().card())
    }

    /// The Agent Card signed with `key`, so a client can verify who published
    /// it.
    fn signed_card(
        &self,
        py: Python<'_>,
        key: PyRef<'_, crate::sign::PySigningKey>,
    ) -> PyResult<Py<PyAny>> {
        let card = self.inner.get().signed_card(&key.inner).map_err(to_pyerr)?;
        ser_to_py(py, &card)
    }
    /// Continue the loop-guard path of work that already crossed other
    /// bridges: `previous` is its `bridge_hops`, and this bridge's id is
    /// appended to it. Every record the bridge publishes afterwards carries
    /// the path. Raises `InvalidError` when `previous` already holds this
    /// bridge, or while a call is in flight. Returns the bridge.
    fn with_bridge_hops(slf: PyRef<'_, Self>, previous: Vec<String>) -> PyResult<PyRef<'_, Self>> {
        laser_sdk::a2a::enter_bridge(&slf.source, &previous).map_err(to_pyerr)?;
        slf.inner.rebuild(|bridge| {
            bridge
                .with_bridge_hops(&previous)
                .expect("the path was checked against this bridge")
        })?;
        Ok(slf)
    }
}

/// An MCP bridge: serve tools / resources / prompts over the log and route
/// `tools/call` to an agent.
#[gen_stub_pyclass]
#[pyclass(name = "McpBridge", frozen)]
pub struct PyMcpBridge {
    inner: Held<McpBridge>,
    source: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMcpBridge {
    /// An MCP bridge over `laser` serving tools / resources / prompts over the
    /// log, publishing tool calls as `source`. `tools` / `resources` /
    /// `prompts` are lists of dicts (a tool is `{name, description?,
    /// input_schema}`, a prompt is `{prompt: {name, title?, description?,
    /// arguments?}, messages: [[role, text]]}`). `memory_tools=True` adds the
    /// conventional `remember` and `recall` tools. `timeout_secs` bounds each
    /// tool call (default 30). Every tool call carries the `bridge_hops`
    /// loop-guard path, starting at `source`.
    #[new]
    #[pyo3(signature = (laser, source, tool_topic, reply_topic, server_name, *, tools=None, resources=None, prompts=None, timeout_secs=None, memory_tools=false))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        laser: PyRef<'_, PyLaser>,
        source: String,
        tool_topic: String,
        reply_topic: String,
        server_name: String,
        tools: Option<&Bound<'_, PyAny>>,
        resources: Option<&Bound<'_, PyAny>>,
        prompts: Option<&Bound<'_, PyAny>>,
        timeout_secs: Option<f64>,
        memory_tools: bool,
    ) -> PyResult<Self> {
        let tools: Vec<ToolSpec> = tools.map(py_to_de).transpose()?.unwrap_or_default();
        let resources: Vec<ResourceSpec> = resources.map(py_to_de).transpose()?.unwrap_or_default();
        let prompts: Vec<PromptSpec> = prompts.map(py_to_de).transpose()?.unwrap_or_default();

        let mut bridge = McpBridge::new(
            laser.inner.clone(),
            agent_id(&source)?,
            static_topic(tool_topic)?,
            static_topic(reply_topic)?,
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
            bridge = bridge.with_timeout(duration_seconds(secs, "timeout_secs")?);
        }
        if memory_tools {
            bridge = bridge.with_memory_tools();
        }
        Ok(Self {
            inner: Held::new(bridge),
            source,
        })
    }

    fn handle_rpc<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.get();
        let request = py_to_json(request)?;
        future_into_py(py, async move {
            let response = bridge.handle_rpc(request).await;
            Python::attach(|py| ser_to_py(py, &response))
        })
    }

    /// `initialize`: the protocol version and advertised capabilities.
    #[pyo3(signature = (protocol_version=None))]
    fn initialize(&self, py: Python<'_>, protocol_version: Option<String>) -> PyResult<Py<PyAny>> {
        let value = self.inner.get().initialize(protocol_version.as_deref());
        json_to_py(py, &value)
    }

    /// `tools/list`.
    fn list_tools(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &self.inner.get().list_tools())
    }

    /// `resources/list`.
    fn list_resources(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &self.inner.get().list_resources())
    }

    /// `resources/read`: the contents advertised for `uri`.
    fn read_resource(&self, py: Python<'_>, uri: String) -> PyResult<Py<PyAny>> {
        let value = self.inner.get().read_resource(&uri).map_err(to_pyerr)?;
        json_to_py(py, &value)
    }

    /// `prompts/list`.
    fn list_prompts(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        json_to_py(py, &self.inner.get().list_prompts())
    }

    /// `prompts/get`: the rendered messages for the prompt `name`.
    fn get_prompt(&self, py: Python<'_>, name: String) -> PyResult<Py<PyAny>> {
        let value = self.inner.get().get_prompt(&name).map_err(to_pyerr)?;
        json_to_py(py, &value)
    }

    /// `tools/call`: route the call to the agent and return the tool result as a
    /// dict. `params_json` is the JSON the call carries to the agent, a dict or
    /// raw JSON str / bytes.
    fn call_tool<'py>(
        &self,
        py: Python<'py>,
        name: String,
        params_json: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bridge = self.inner.get();
        let arguments = json_arg(params_json)?;
        future_into_py(py, async move {
            let result = bridge.call_tool(&name, arguments).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }
    /// Continue the loop-guard path of a call that already crossed other
    /// bridges: `previous` is its `bridge_hops`, and this bridge's id is
    /// appended to it. Every tool call the bridge publishes afterwards
    /// carries the path. Raises `InvalidError` when `previous` already holds
    /// this bridge, or while a call is in flight. Returns the bridge.
    fn with_bridge_hops(slf: PyRef<'_, Self>, previous: Vec<String>) -> PyResult<PyRef<'_, Self>> {
        laser_sdk::a2a::enter_bridge(&slf.source, &previous).map_err(to_pyerr)?;
        slf.inner.rebuild(|bridge| {
            bridge
                .with_bridge_hops(&previous)
                .expect("the path was checked against this bridge")
        })?;
        Ok(slf)
    }
}

// A bridge shared with its in-flight calls. `with_bridge_hops` rebuilds it in
// place, which needs the only reference, so it waits for no call.
struct Held<T>(RwLock<Option<Arc<T>>>);

impl<T> Held<T> {
    fn new(bridge: T) -> Self {
        Self(RwLock::new(Some(Arc::new(bridge))))
    }

    fn get(&self) -> Arc<T> {
        self.0
            .read()
            .expect("the bridge lock is never poisoned")
            .clone()
            .expect("the bridge is held between calls")
    }

    fn rebuild(&self, rebuild: impl FnOnce(T) -> T) -> PyResult<()> {
        let mut slot = self.0.write().expect("the bridge lock is never poisoned");
        let held = slot.take().expect("the bridge is held between calls");
        match Arc::try_unwrap(held) {
            Ok(bridge) => {
                *slot = Some(Arc::new(rebuild(bridge)));
                Ok(())
            }
            Err(held) => {
                *slot = Some(held);
                Err(crate::errors::InvalidError::new_err(
                    "cannot change the bridge hops while a call is in flight",
                ))
            }
        }
    }
}

// A capability descriptor from a skill id string or a descriptor dict.
fn capability_descriptor(
    value: &Bound<'_, PyAny>,
) -> PyResult<laser_sdk::wire::agent::CapabilityDescriptor> {
    if let Ok(skill_id) = value.extract::<String>() {
        return serde_json::from_value(serde_json::json!({ "skill_id": skill_id }))
            .map_err(|e| crate::errors::CodecError::new_err(e.to_string()));
    }
    py_to_de(value)
}
