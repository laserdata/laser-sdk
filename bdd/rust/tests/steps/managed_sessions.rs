use crate::common::test_iggy::fresh_connected_laser;
use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::a2a::A2aBridge;
use laser_sdk::agent::{
    AgentHandle, SessionChange, SessionConfig, SessionLayout, SessionLease, SessionWatch, Sessions,
    TopicRetention,
};
use laser_sdk::mcp::McpBridge;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    AgentEnvelope, AgentKind, Budget, CorrelationId, RecordId, SessionEnd, SessionStatus,
    TaskState, TokenUsage,
};
use laser_sdk::wire::graph::SourceRef;
use laser_sdk::wire::session::{LinkRelation, LinkSurface, SessionInfo, StateOutcome};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

// Folds run on their own schedule, so every index read waits for its answer.
const CONVERGE: Duration = Duration::from_secs(30);

/// The state of one managed-session scenario.
#[derive(Default)]
pub struct Managed {
    laser: Option<Laser>,
    sessions: Option<Sessions>,
    layout: SessionLayout,
    second: Option<Sessions>,
    second_session: Option<ConversationId>,
    labels: BTreeMap<String, ConversationId>,
    children: Vec<ConversationId>,
    workflow_run: Option<ConversationId>,
    linked_write: Option<Result<(), String>>,
    remembered: Option<laser_sdk::memory::MemoryItem>,
    worker: Option<AgentHandle>,
    lease: Option<SessionLease>,
    watch: Option<oneshot::Receiver<Result<SessionChange, String>>>,
    handled: Arc<Mutex<Vec<ConversationId>>>,
    retained: Option<laser_sdk::agent::Session>,
    retained_state: Option<laser_sdk::agent::SessionState>,
    retained_source: Option<SourceRef>,
    writer: Option<Laser>,
}

impl Managed {
    fn sessions(&self) -> &Sessions {
        self.sessions.as_ref().expect("a managed session stream")
    }

    fn second(&self) -> &Sessions {
        self.second
            .as_ref()
            .expect("a second managed session stream")
    }

    fn laser(&self) -> &Laser {
        self.laser.as_ref().expect("a managed session stream")
    }

    /// Whether this scenario runs on a managed session stream, so the shared
    /// session steps write through it.
    pub fn active(&self) -> bool {
        self.sessions.is_some()
    }

    fn id(&self, label: &str) -> ConversationId {
        *self
            .labels
            .get(label)
            .unwrap_or_else(|| panic!("no session labeled {label}"))
    }
}

fn agent(name: &str) -> laser_sdk::wire::agent::AgentId {
    name.parse().expect("a valid agent id")
}

fn named(name: &str) -> AgentId {
    name.parse().expect("a valid agent id")
}

async fn eventually<T>(what: &str, read: impl AsyncFnMut() -> Option<T>) -> T {
    eventually_showing(what, read, async || String::new()).await
}

// `eventually`, naming what the index held when it gave up.
async fn eventually_showing<T>(
    what: &str,
    mut read: impl AsyncFnMut() -> Option<T>,
    mut show: impl AsyncFnMut() -> String,
) -> T {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        if let Some(value) = read().await {
            return value;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "{what} did not converge within {CONVERGE:?}: {}",
                show().await
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// A stream bootstrapped for sessions in `layout` on a connection-string
// client, registered as a session source.
async fn managed_stream(config: SessionConfig) -> (Laser, Sessions) {
    let fresh = fresh_connected_laser().await;
    let sessions = fresh.laser.sessions_with(config);
    let bootstrap = sessions
        .bootstrap(4, TopicRetention::expire_after(Duration::from_secs(86_400)))
        .await
        .expect("bootstrap the session stream");
    assert!(
        bootstrap.registered,
        "a deployment that serves sessions registers the stream"
    );
    (fresh.laser, sessions)
}

fn layout(name: &str) -> SessionLayout {
    match name {
        "shared" => SessionLayout::Shared,
        "per-agent topic" => SessionLayout::PerAgentTopic(BTreeMap::from([
            (agent("planner"), "planner.inbox".to_owned()),
            (agent("worker"), "worker.inbox".to_owned()),
        ])),
        "declared partitions" => SessionLayout::PerAgentPartition(BTreeMap::from([
            (agent("planner"), 0),
            (agent("worker"), 2),
        ])),
        "single partition" => SessionLayout::SinglePartition,
        other => panic!("unknown layout {other}"),
    }
}

async fn indexed(sessions: &Sessions, id: ConversationId) -> Option<SessionInfo> {
    sessions.get(id).await.ok()
}

#[given(regex = r"^a managed session stream in the (.+) layout$")]
async fn managed_layout(world: &mut LaserWorld, name: String) {
    let layout = layout(&name);
    let (laser, sessions) = managed_stream(SessionConfig::new().layout(layout.clone())).await;
    world.managed.layout = layout;
    world.managed.laser = Some(laser);
    world.managed.sessions = Some(sessions);
}

#[given(regex = r"^a managed session stream whose agents heartbeat every (\d+) seconds?$")]
async fn managed_heartbeat(world: &mut LaserWorld, seconds: u64) {
    let config = SessionConfig::new().heartbeat(Duration::from_secs(seconds));
    let (laser, sessions) = managed_stream(config).await;
    world.managed.laser = Some(laser);
    world.managed.sessions = Some(sessions);
}

#[given("a second managed session stream")]
async fn second_stream(world: &mut LaserWorld) {
    world.managed.second = Some(managed_stream(SessionConfig::new()).await.1);
}

#[when(
    regex = r#"^agent "([^"]+)" runs the session "([^"]+)" with one command answered by "([^"]+)"$"#
)]
async fn run_exchange(world: &mut LaserWorld, owner: String, label: String, worker: String) {
    let managed = &mut world.managed;
    let (session, lease) = managed
        .sessions()
        .create(label.clone())
        .agent(agent(&owner))
        .begin()
        .await
        .expect("the session starts");
    // Every layout sends on the lane. A per-agent topic layout moves the
    // addressed command and reply onto the declared topics.
    let (work, reply) = (AgentTopic::Sessions, AgentTopic::Sessions);
    let laser = managed.laser().clone();
    let conversation = session.conversation();
    let correlation = CorrelationId::from_u128(conversation.as_u128());
    laser
        .agdx(work, agent(&owner), conversation.into())
        .command(correlation, b"{}".to_vec())
        .with_target(agent(&worker))
        .send()
        .await
        .expect("the command is sent");
    laser
        .agdx(reply, agent(&worker), conversation.into())
        .respond(correlation, b"{}".to_vec())
        .with_target(agent(&owner))
        .send()
        .await
        .expect("the reply is sent");
    session.end().await.expect("the session ends");
    drop(lease);
    managed.labels.insert(label, conversation);
}

#[then(regex = r#"^the indexed session "([^"]+)" is completed by "([^"]+)" with (\d+) events$"#)]
async fn indexed_completed(world: &mut LaserWorld, label: String, owner: String, events: u64) {
    let sessions = world.managed.sessions().clone();
    let id = world.managed.id(&label);
    let info = eventually("the completed session row", async || {
        let info = indexed(&sessions, id).await?;
        (info.status == SessionStatus::Completed && info.events >= events).then_some(info)
    })
    .await;
    assert_eq!(info.events, events, "{info:?}");
    assert_eq!(info.label.as_deref(), Some(label.as_str()));
    assert_eq!(
        info.agent
            .as_ref()
            .map(laser_sdk::wire::agent::AgentId::as_str),
        Some(owner.as_str())
    );
    assert!(!info.flags.lane_conflict, "{info:?}");
}

#[then(regex = r"^the stream counts (\d+) completed sessions?$")]
async fn counts_completed(world: &mut LaserWorld, count: u64) {
    let sessions = world.managed.sessions().clone();
    let page = eventually("the completed count", async || {
        let page = sessions
            .list()
            .status(SessionStatus::Completed)
            .total()
            .fetch()
            .await
            .ok()?;
        (page.total == Some(count)).then_some(page)
    })
    .await;
    assert_eq!(page.items.len() as u64, count);
}

struct Answers;

impl AgentHandler for Answers {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        ctx.respond(bytes::Bytes::copy_from_slice(message.body()))
            .await
    }
}

#[given(regex = r#"^agent "([^"]+)" answers every command it receives$"#)]
async fn answering_worker(world: &mut LaserWorld, name: String) {
    let mut worker = Agent::builder()
        .id(named(&name))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .handler(Answers)
        .build()
        .spawn(world.managed.laser().clone());
    if let Err(error) = worker.ready().await {
        panic!(
            "the worker did not start: {error}, {:?}",
            worker.shutdown().await
        );
    }
    world.managed.worker = Some(worker);
}

/// `agent "<owner>" starts the session "<label>"` on a managed stream. The
/// shared session step calls it when the scenario has one.
pub async fn start_managed(world: &mut LaserWorld, owner: String, label: String) {
    let (session, lease) = world
        .managed
        .sessions()
        .create(label.clone())
        .agent(agent(&owner))
        .begin()
        .await
        .expect("the session starts");
    world.managed.labels.insert(label, session.conversation());
    world.session = Some(session);
    world.managed.lease = Some(lease);
}

#[when(regex = r#"^agent "([^"]+)" starts the session "([^"]+)" on the second stream$"#)]
async fn start_on_second(world: &mut LaserWorld, owner: String, label: String) {
    let (session, lease) = world
        .managed
        .second()
        .create(label)
        .agent(agent(&owner))
        .begin()
        .await
        .expect("the session starts");
    world.managed.second_session = Some(session.conversation());
    session.end().await.expect("the session ends");
    drop(lease);
}

#[when(
    regex = r#"^agent "([^"]+)" starts the session "([^"]+)" with an idle timeout of (\d+) seconds$"#
)]
async fn start_with_idle(world: &mut LaserWorld, owner: String, label: String, seconds: u64) {
    let (session, lease) = world
        .managed
        .sessions()
        .create(label.clone())
        .agent(agent(&owner))
        .idle_timeout(Duration::from_secs(seconds))
        .begin()
        .await
        .expect("the session starts");
    world.managed.labels.insert(label, session.conversation());
    world.session = Some(session);
    world.managed.lease = Some(lease);
}

#[when(regex = r#"^the session fans out one branch to "([^"]+)"$"#)]
async fn fan_out_branch(world: &mut LaserWorld, target: String) {
    let session = world.session().clone();
    let laser = world.managed.laser().clone();
    let author = named(session.agent().expect("the session has an agent").as_str());
    let parent = Provenance::builder()
        .conversation_id(session.conversation())
        .agent(author.clone())
        .build();
    // One fan-out branch: a subconversation of the session addressed to its
    // target, the request the gather awaits.
    let mut branch = laser.spawn_subconversation(&parent, &author);
    branch.target_agent_id = Some(named(&target));
    branch.correlation_id = Some(ulid_like(branch.conversation_id));
    laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            b"branch".to_vec(),
            &branch,
            Duration::from_secs(20),
        )
        .await
        .expect("the branch is answered");
    world.managed.children.push(branch.conversation_id);
}

fn ulid_like(conversation: ConversationId) -> String {
    format!("branch-{conversation}")
}

#[given(regex = r#"^agent "([^"]+)" has a bound addressee filter$"#)]
async fn bound_addressee(world: &mut LaserWorld, name: String) {
    let laser = world.managed.laser();
    assert!(laser.capabilities().await.filters.group_policy_reads);
    let binding = laser
        .topic(AgentTopic::Sessions.topic_string())
        .consumer_group(name)
        .filter()
        .get()
        .await
        .expect("read the worker's addressee filter");
    assert!(binding.is_some(), "the worker's group has a filter");
}

#[when(
    regex = r#"^agent "([^"]+)" sends untargeted low-altitude work with send_agent and request$"#
)]
async fn untargeted_work(world: &mut LaserWorld, owner: String) {
    let laser = world.managed.laser();
    let provenance = Provenance::builder()
        .conversation_id(world.session().conversation())
        .agent(named(&owner))
        .build();
    assert!(provenance.target_agent_id.is_none());
    laser
        .send_agent(AgentTopic::Sessions, b"send".to_vec(), &provenance)
        .await
        .expect("untargeted send_agent publishes");
    let reply = laser
        .request(
            AgentTopic::Sessions,
            AgentTopic::Sessions,
            b"request".to_vec(),
            &provenance,
            Duration::from_secs(20),
        )
        .await
        .expect("untargeted request is answered");
    assert_eq!(reply.body(), b"{}");
}

#[then(regex = r#"^agent "([^"]+)" handles both untargeted records for the session "([^"]+)"$"#)]
async fn untargeted_delivered(world: &mut LaserWorld, _name: String, label: String) {
    let expected = world.managed.id(&label);
    eventually("both untargeted records are handled", async || {
        let handled = world.managed.handled.lock().expect("the handled list");
        (handled.len() >= 2).then_some(())
    })
    .await;
    assert_eq!(
        *world.managed.handled.lock().expect("the handled list"),
        vec![expected, expected]
    );
}

#[when("the session submits an A2A task as a child")]
async fn a2a_child(world: &mut LaserWorld) {
    let session = world.session().conversation();
    let bridge = A2aBridge::new(
        world.managed.laser().clone(),
        agent("a2a-gateway"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    );
    let task = bridge
        .submit_in(
            session,
            session,
            br#"{"message":{"role":"user","text":"look"}}"#.to_vec(),
        )
        .await
        .expect("the task is submitted");
    world
        .managed
        .children
        .push(task.id.parse().expect("the task id is a session id"));
}

#[when(regex = r#"^the session calls the MCP tool "([^"]+)" as a child$"#)]
async fn mcp_child(world: &mut LaserWorld, tool: String) {
    let session = world.session().conversation();
    let bridge = McpBridge::new(
        world.managed.laser().clone(),
        agent("mcp-gateway"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
        "bdd",
    )
    .with_timeout(Duration::from_secs(20));
    bridge
        .call_tool_in(session, session, &tool, br#"{"q":"auth"}"#.to_vec())
        .await
        .expect("the tool call is answered");
}

async fn tree(sessions: &Sessions, root: ConversationId) -> Vec<SessionInfo> {
    let Ok(page) = sessions.list().root(root).fetch().await else {
        return Vec::new();
    };
    page.items
        .into_iter()
        .filter(|item| ConversationId::from(item.id) != root)
        .collect()
}

#[then(regex = r#"^the session tree of "([^"]+)" holds (\d+) child(?:ren)?$"#)]
async fn tree_holds(world: &mut LaserWorld, label: String, count: usize) {
    let sessions = world.managed.sessions().clone();
    let root = world.managed.id(&label);
    let children = eventually_showing(
        "the session tree",
        async || {
            let children = tree(&sessions, root).await;
            (children.len() >= count).then_some(children)
        },
        async || {
            format!(
                "tree {:?}, stream {:?}",
                sessions.list().root(root).fetch().await,
                sessions.list().fetch().await
            )
        },
    )
    .await;
    assert_eq!(children.len(), count, "{children:?}");
    let indexed: Vec<ConversationId> = children.iter().map(|child| child.id.into()).collect();
    for child in &world.managed.children {
        assert!(indexed.contains(child), "child {child} is in the tree");
    }
}

#[then(regex = r#"^every child names "([^"]+)" as its parent and root$"#)]
async fn children_name_root(world: &mut LaserWorld, label: String) {
    let sessions = world.managed.sessions().clone();
    let root = world.managed.id(&label);
    for child in tree(&sessions, root).await {
        assert_eq!(child.parent.map(ConversationId::from), Some(root));
        assert_eq!(child.root.map(ConversationId::from), Some(root));
    }
}

async fn run_workflow(
    laser: &Laser,
    name: &str,
    worker: &str,
    run: Option<ConversationId>,
) -> ConversationId {
    let mut workflow = laser
        .workflow(name)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions));
    if let Some(run) = run {
        workflow = workflow.run_id(run);
    }
    workflow
        .step(
            "check",
            Router::to(named(worker)),
            |_ctx: &laser_sdk::agent::StepContext<'_>| b"incident".to_vec(),
        )
        .run()
        .await
        .expect("the workflow completes")
        .run_id
}

#[when(regex = r#"^the workflow "([^"]+)" runs its one step on "([^"]+)"$"#)]
async fn workflow_runs(world: &mut LaserWorld, name: String, worker: String) {
    let laser = world.managed.laser().clone();
    world.managed.workflow_run = Some(run_workflow(&laser, &name, &worker, None).await);
}

#[when(regex = r#"^the workflow "([^"]+)" resumes the same run$"#)]
async fn workflow_resumes(world: &mut LaserWorld, name: String) {
    let laser = world.managed.laser().clone();
    let run = world.managed.workflow_run;
    run_workflow(&laser, &name, "worker", run).await;
}

#[then(regex = r"^the session tree of the workflow run holds (\d+) child(?:ren)?$")]
async fn workflow_tree(world: &mut LaserWorld, count: usize) {
    let sessions = world.managed.sessions().clone();
    let run = world.managed.workflow_run.expect("a workflow ran");
    let children = eventually("the workflow tree", async || {
        let children = tree(&sessions, run).await;
        (children.len() >= count).then_some(children)
    })
    .await;
    // A phantom child would land late, so read the tree once more after the
    // run's records have folded.
    let run_info = eventually("the workflow run row", async || {
        let info = indexed(&sessions, run).await?;
        matches!(
            info.status,
            SessionStatus::Completed | SessionStatus::Failed | SessionStatus::Canceled
        )
        .then_some(info)
    })
    .await;
    assert_eq!(run_info.status, SessionStatus::Completed, "{run_info:?}");
    let children = {
        let again = tree(&sessions, run).await;
        if again.is_empty() { children } else { again }
    };
    assert_eq!(children.len(), count, "{children:?}");
}

#[when("the session records that it recalled the remembered item")]
async fn record_recall(world: &mut LaserWorld) {
    let session = world.session().clone();
    let memory = session.memory();
    let items = eventually("the remembered item", async || {
        let items = memory.recall().recent().folded().fetch().await.ok()?;
        (!items.is_empty()).then_some(items)
    })
    .await;
    session
        .record_retrieval(Some("deploy key".to_owned()), &items[..1])
        .await
        .expect("the retrieval is recorded");
    world.managed.remembered = Some(items[0].clone());
}

#[then("the session links the remembered item as written and recalled")]
async fn memory_links(world: &mut LaserWorld) {
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    let item = world
        .managed
        .remembered
        .as_ref()
        .expect("an item was recalled")
        .id
        .to_string();
    let links = eventually("the memory links", async || {
        let view = sessions.links(id, Some(LinkSurface::Memory)).await.ok()?;
        let relations: Vec<LinkRelation> = view
            .links
            .iter()
            .filter(|link| link.item == item)
            .map(|link| link.relation)
            .collect();
        (relations.contains(&LinkRelation::Wrote) && relations.contains(&LinkRelation::Recalled))
            .then_some(view)
    })
    .await;
    assert!(
        links
            .links
            .iter()
            .all(|link| link.surface == LinkSurface::Memory)
    );
}

#[when(regex = r"^the session state is replaced by (.+)$")]
async fn state_replace(world: &mut LaserWorld, document: String) {
    let document: serde_json::Value = serde_json::from_str(&document).expect("a JSON document");
    world
        .session()
        .state()
        .replace(document)
        .await
        .expect("the state is replaced");
}

#[when("the session writes a state snapshot")]
async fn state_snapshot(world: &mut LaserWorld) {
    world
        .session()
        .state()
        .snapshot()
        .await
        .expect("the snapshot is written");
}

#[then(regex = r"^the indexed state of the session is (.+)$")]
async fn indexed_state(world: &mut LaserWorld, document: String) {
    let expected: serde_json::Value = serde_json::from_str(&document).expect("a JSON document");
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    let view = eventually("the indexed state", async || {
        let view = sessions.state(id, 0).await.ok()?;
        (view.document == expected).then_some(view)
    })
    .await;
    assert!(view.complete, "{view:?}");
}

#[then(
    regex = r"^the indexed state history chains its digests over (\d+) deltas and the snapshot$"
)]
async fn state_history(world: &mut LaserWorld, deltas: usize) {
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    // The snapshot folds after the document already matched, so wait for it.
    let view = eventually("the indexed state history", async || {
        let view = sessions.state(id, 0).await.ok()?;
        (view.history.len() == deltas + 1).then_some(view)
    })
    .await;
    let history = &view.history;
    assert!(
        history
            .iter()
            .all(|change| change.outcome == StateOutcome::Applied
                && change.old_digest.is_some()
                && change.new_digest.is_some()),
        "{history:?}"
    );
    for pair in history.windows(2) {
        assert_eq!(pair[1].old_digest, pair[0].new_digest, "{pair:?}");
    }
    for (index, delta) in history[..deltas].iter().enumerate() {
        assert!(delta.op_id.is_some(), "{delta:?}");
        assert_eq!(delta.revision, index as u64 + 1, "{delta:?}");
        assert_ne!(delta.old_digest, delta.new_digest, "{delta:?}");
    }
    // A snapshot of the document the deltas built keeps the revision and the
    // digest.
    let snapshot = &history[deltas];
    assert!(snapshot.op_id.is_none(), "{snapshot:?}");
    assert_eq!(snapshot.revision, deltas as u64, "{snapshot:?}");
    assert_eq!(snapshot.old_digest, snapshot.new_digest, "{snapshot:?}");
}

#[then(regex = r#"^the indexed session "([^"]+)" is active$"#)]
async fn indexed_active(world: &mut LaserWorld, label: String) {
    let sessions = world.managed.sessions().clone();
    let id = world.managed.id(&label);
    eventually("the active session row", async || {
        let info = indexed(&sessions, id).await?;
        (info.status == SessionStatus::Active).then_some(info)
    })
    .await;
}

#[when(regex = r#"^the session links "([^"]+)" to "([^"]+)" in graph "([^"]+)"$"#)]
async fn graph_link(world: &mut LaserWorld, from: String, to: String, graph: String) {
    world
        .session()
        .linked_graph(graph)
        .expect("a linked graph")
        .link(from, "depends_on", to)
        .await
        .expect("the graph link is written");
}

#[then(regex = r#"^the sessions "([^"]+)" and "([^"]+)" both link the graph node "([^"]+)"$"#)]
async fn both_link_node(world: &mut LaserWorld, first: String, second: String, node: String) {
    let sessions = world.managed.sessions().clone();
    let (kind, value) = node.split_once(':').expect("a kind:value node");
    let node_id = laser_sdk::wire::graph::GraphNode::entity(kind, value)
        .id
        .to_string();
    for label in [first, second] {
        let id = world.managed.id(&label);
        eventually_showing(
            "the graph node link",
            async || {
                let view = sessions
                    .links(id, Some(LinkSurface::GraphNode))
                    .await
                    .ok()?;
                view.links
                    .iter()
                    .any(|link| link.item == node_id)
                    .then_some(())
            },
            async || format!("node {node_id}, links {:?}", sessions.links(id, None).await),
        )
        .await;
    }
}

#[when(regex = r#"^the session sets key "([^"]+)" to "([^"]+)" in namespace "([^"]+)"$"#)]
async fn session_kv_set(world: &mut LaserWorld, key: String, value: String, namespace: String) {
    world
        .session()
        .kv(namespace)
        .expect("a linked namespace")
        .set(key)
        .bytes(value)
        .send()
        .await
        .expect("the key is set");
}

#[then(regex = r#"^the session links key "([^"]+)" in namespace "([^"]+)"$"#)]
async fn kv_link(world: &mut LaserWorld, key: String, namespace: String) {
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    eventually("the key-value link", async || {
        let view = sessions.links(id, Some(LinkSurface::Kv)).await.ok()?;
        view.links
            .iter()
            .any(|link| link.item == key && link.resource.ends_with(&namespace))
            .then_some(())
    })
    .await;
}

#[then(regex = r#"^the key-value lens of the session in namespace "([^"]+)" lists "([^"]+)"$"#)]
async fn kv_lens(world: &mut LaserWorld, namespace: String, key: String) {
    let laser = world.managed.laser().clone();
    let id = world.session().conversation();
    let keys = eventually("the key-value lens", async || {
        let entries = laser
            .kv(namespace.clone())
            .scan()
            .conversation(id)
            .entries()
            .await
            .ok()?;
        let keys: Vec<String> = entries
            .iter()
            .map(|entry| String::from_utf8_lossy(&entry.key).into_owned())
            .collect();
        (!keys.is_empty()).then_some(keys)
    })
    .await;
    assert_eq!(keys, vec![key]);
}

#[when(
    regex = r#"^I set key "([^"]+)" in namespace "([^"]+)" linked to the second stream's session$"#
)]
async fn forged_write(world: &mut LaserWorld, key: String, namespace: String) {
    let victim = world
        .managed
        .second()
        .open(
            world
                .managed
                .second_session
                .expect("a session on the second stream"),
        )
        .reference()
        .expect("a session reference");
    let result = world
        .managed
        .laser()
        .kv(namespace)
        .in_session(victim)
        .set(key)
        .bytes("stolen")
        .send()
        .await;
    world.managed.linked_write = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[then("the linked write is refused")]
async fn write_refused(world: &mut LaserWorld) {
    let outcome = world.managed.linked_write.clone().expect("a linked write");
    assert!(
        outcome.is_err(),
        "a write linked to another stream's session is refused"
    );
}

#[then("the second stream's session links no key-value item")]
async fn victim_unlinked(world: &mut LaserWorld) {
    let second = world.managed.second().clone();
    let id = world
        .managed
        .second_session
        .expect("a session on the second stream");
    eventually("the second session row", async || {
        indexed(&second, id).await
    })
    .await;
    let view = second
        .links(id, Some(LinkSurface::Kv))
        .await
        .expect("the links read");
    assert!(view.links.is_empty(), "{view:?}");
}

#[when(regex = r"^the session stays quiet for (\d+) seconds$")]
async fn stay_quiet(_world: &mut LaserWorld, seconds: u64) {
    tokio::time::sleep(Duration::from_secs(seconds)).await;
}

#[then("the indexed session is live with a recent heartbeat")]
async fn live_with_heartbeat(world: &mut LaserWorld) {
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    let info = eventually("the heartbeat liveness", async || {
        let info = indexed(&sessions, id).await?;
        (!info.flags.liveness_unknown && info.last_heartbeat_at.is_some()).then_some(info)
    })
    .await;
    assert_eq!(info.status, SessionStatus::Active, "{info:?}");
    assert!(!info.idle, "{info:?}");
    assert!(
        info.last_heartbeat_at > info.last_event_at,
        "a heartbeat after the last event keeps the session live: {info:?}"
    );
}

#[then("no indexed event of the session is a heartbeat")]
async fn no_heartbeat_events(world: &mut LaserWorld) {
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    let page = sessions.events(id).fetch().await.expect("the events read");
    assert!(!page.items.is_empty());
    for event in &page.items {
        assert!(
            !event.display.contains("heartbeat") && !event.kind.contains("heartbeat"),
            "{event:?}"
        );
    }
}

#[when("the session lease is released")]
async fn release_lease(world: &mut LaserWorld) {
    world.managed.lease.take().expect("a held lease").release();
}

#[then("the indexed session becomes idle")]
async fn becomes_idle(world: &mut LaserWorld) {
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    let info = eventually("the idle session", async || {
        let info = indexed(&sessions, id).await?;
        info.idle.then_some(info)
    })
    .await;
    assert_eq!(
        info.status,
        SessionStatus::Active,
        "idle never overwrites a status"
    );
}

#[when(regex = r"^the session streams (\d+) chunks in one batch$")]
async fn stream_chunks(world: &mut LaserWorld, chunks: usize) {
    let session = world.session().clone();
    let author = session.agent().cloned().expect("the session has an agent");
    let conversation = session.conversation();
    let mut stream = world
        .managed
        .laser()
        .agdx(AgentTopic::Sessions, author, conversation.into())
        .stream(CorrelationId::from_u128(conversation.as_u128()), "chat")
        .buffered(chunks, Duration::from_secs(60));
    for index in 0..chunks {
        stream
            .write(format!("chunk-{index}").into_bytes())
            .await
            .expect("the chunk is buffered");
    }
    stream.flush().await.expect("the batch is sent");
}

#[then(regex = r"^the index counts (\d+) records for the session$")]
async fn counts_records(world: &mut LaserWorld, records: u64) {
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    let info = eventually("the session counters", async || {
        let info = indexed(&sessions, id).await?;
        (info.events >= records).then_some(info)
    })
    .await;
    assert_eq!(info.events, records, "{info:?}");
}

#[then(regex = r"^the change feed names the session in at most (\d+) rows$")]
async fn feed_rows(world: &mut LaserWorld, rows: usize) {
    let sessions = world.managed.sessions().clone();
    let id: laser_sdk::wire::agent::ConversationId = world.session().conversation().into();
    let changes = sessions.changes(0, 0).await.expect("the change feed reads");
    let naming = changes
        .rows
        .iter()
        .filter(|row| row.sessions.contains(&id))
        .count();
    assert!(naming >= 1, "{changes:?}");
    assert!(
        naming <= rows,
        "one row per fold batch, not per record: {changes:?}"
    );
}

#[then("the change feed never names the second stream's session")]
async fn feed_isolated(world: &mut LaserWorld) {
    let second = world.managed.second().clone();
    let other: laser_sdk::wire::agent::ConversationId = world
        .managed
        .second_session
        .expect("a session on the second stream")
        .into();
    // The second stream's own feed names it, so both folds have run.
    eventually("the second stream's feed", async || {
        let changes = second.changes(0, 0).await.ok()?;
        changes
            .rows
            .iter()
            .any(|row| row.sessions.contains(&other))
            .then_some(())
    })
    .await;
    let changes = world
        .managed
        .sessions()
        .changes(0, 0)
        .await
        .expect("the change feed reads");
    assert!(
        changes
            .rows
            .iter()
            .all(|row| !row.sessions.contains(&other)),
        "{changes:?}"
    );
}

#[when("a watch follows the stream's session changes")]
async fn watch_changes(world: &mut LaserWorld) {
    let mut watch: SessionWatch = world
        .managed
        .sessions()
        .watch(Duration::from_millis(250))
        .await
        .expect("the watch starts");
    let (sender, receiver) = oneshot::channel();
    // A follower awaits its next change from the start, as a console would.
    tokio::spawn(async move {
        let _ = sender.send(watch.next().await.map_err(|error| format!("{error:?}")));
    });
    world.managed.watch = Some(receiver);
}

#[then(regex = r#"^listing the stream finds "([^"]+)" as completed$"#)]
async fn listing_finds(world: &mut LaserWorld, label: String) {
    let sessions = world.managed.sessions().clone();
    let id = world.managed.id(&label);
    let page = eventually("the listed session", async || {
        let page = sessions.list().fetch().await.ok()?;
        page.items
            .iter()
            .any(|item| {
                ConversationId::from(item.id) == id && item.status == SessionStatus::Completed
            })
            .then_some(page)
    })
    .await;
    assert_eq!(page.items.len(), 1, "{page:?}");
}

#[then(regex = r#"^the indexed events of the session are (".+")$"#)]
async fn indexed_events(world: &mut LaserWorld, list: String) {
    let expected: Vec<String> = list
        .split(", ")
        .map(|name| name.trim_matches('"').to_owned())
        .collect();
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    let displays = eventually("the indexed events", async || {
        let page = sessions.events(id).fetch().await.ok()?;
        let displays: Vec<String> = page
            .items
            .iter()
            .map(|event| event.display.clone())
            .collect();
        (displays.len() >= expected.len()).then_some(displays)
    })
    .await;
    assert_eq!(displays, expected);
}

#[then("the session sources show folded offsets within their heads")]
async fn sources_cover(world: &mut LaserWorld) {
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    let sources = sessions.sources(id).await.expect("the sources read");
    assert!(
        sources.sources.iter().any(|source| source.folded.is_some()),
        "{sources:?}"
    );
    for source in &sources.sources {
        if let (Some(folded), Some(head)) = (source.folded, source.head) {
            assert!(folded <= head, "{source:?}");
        }
    }
}

#[then("the session has no links")]
async fn no_links(world: &mut LaserWorld) {
    let sessions = world.managed.sessions().clone();
    let id = world.session().conversation();
    let view = sessions.links(id, None).await.expect("the links read");
    assert!(view.links.is_empty(), "{view:?}");
}

#[then("the watch reports the session")]
async fn watch_reports(world: &mut LaserWorld) {
    let id: laser_sdk::wire::agent::ConversationId = world.session().conversation().into();
    let receiver = world.managed.watch.take().expect("a watch");
    let change = tokio::time::timeout(CONVERGE, receiver)
        .await
        .expect("the watch reports within the deadline")
        .expect("the watch task finishes")
        .expect("the watch reads");
    match change {
        SessionChange::Changed(sessions) => assert!(sessions.contains(&id), "{sessions:?}"),
        SessionChange::Resync => panic!("a fresh watch never resyncs"),
    }
}

// Records the session of every work record it is handed.
struct Counts {
    handled: Arc<Mutex<Vec<ConversationId>>>,
}

impl AgentHandler for Counts {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.handled
            .lock()
            .expect("the handled list")
            .push(message.provenance.conversation_id);
        ctx.respond(b"{}".to_vec()).await
    }
}

#[given(regex = r#"^agent "([^"]+)" counts the work it handles$"#)]
async fn counting_worker(world: &mut LaserWorld, name: String) {
    let mut worker = Agent::builder()
        .id(named(&name))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .handler(Counts {
            handled: Arc::clone(&world.managed.handled),
        })
        .build()
        .spawn(world.managed.laser().clone());
    if let Err(error) = worker.ready().await {
        panic!(
            "the worker did not start: {error}, {:?}",
            worker.shutdown().await
        );
    }
    world.managed.worker = Some(worker);
}

#[when(regex = r#"^agent "([^"]+)" starts the session "([^"]+)" with a budget of (\d+) tokens$"#)]
async fn start_with_budget(world: &mut LaserWorld, owner: String, label: String, tokens: u64) {
    let (session, lease) = world
        .managed
        .sessions()
        .create(label.clone())
        .agent(agent(&owner))
        .budget(Budget {
            tokens: Some(tokens),
            cost_micros: None,
        })
        .begin()
        .await
        .expect("the session starts");
    world.managed.labels.insert(label, session.conversation());
    world.session = Some(session);
    world.managed.lease = Some(lease);
}

#[when(regex = r"^the session records a model call that used (\d+) tokens$")]
async fn record_spend(world: &mut LaserWorld, tokens: u64) {
    world
        .session()
        .record_model_call(
            laser_sdk::agent::ModelRequest::new("gpt-test", b"q".to_vec()),
            laser_sdk::agent::ModelResponse {
                body: b"a".to_vec(),
                usage: Some(TokenUsage {
                    input_tokens: tokens,
                    ..TokenUsage::default()
                }),
                ..laser_sdk::agent::ModelResponse::default()
            },
            None,
        )
        .await
        .expect("the model call is recorded");
}

#[then(regex = r#"^the indexed session "([^"]+)" is over its budget$"#)]
async fn indexed_over_budget(world: &mut LaserWorld, label: String) {
    let sessions = world.managed.sessions().clone();
    let id = world.managed.id(&label);
    eventually("the over-budget flag", async || {
        indexed(&sessions, id).await?.over_budget.then_some(())
    })
    .await;
    assert!(
        sessions
            .open(id)
            .over_budget()
            .await
            .expect("the budget reads"),
        "the session lens reads the flag from the index"
    );
}

#[when(regex = r#"^agent "([^"]+)" sends the session (\d+) work records? for "([^"]+)"$"#)]
async fn send_work(world: &mut LaserWorld, owner: String, count: usize, target: String) {
    let laser = world.managed.laser().clone();
    let conversation = world.session().conversation();
    for _ in 0..count {
        laser
            .agdx(AgentTopic::Sessions, agent(&owner), conversation.into())
            .command(
                <CorrelationId as laser_sdk::types::MintUlid>::mint(),
                b"{}".to_vec(),
            )
            .with_target(agent(&target))
            .send()
            .await
            .expect("the work is sent");
    }
}

#[then(regex = r#"^agent "([^"]+)" handles only the work of the session "([^"]+)"$"#)]
async fn handles_only(world: &mut LaserWorld, _name: String, label: String) {
    let id = world.managed.id(&label);
    let handled = Arc::clone(&world.managed.handled);
    let seen = eventually("the handled work", async || {
        let seen = handled.lock().expect("the handled list").clone();
        seen.contains(&id).then_some(seen)
    })
    .await;
    assert!(seen.iter().all(|session| *session == id), "{seen:?}");
}

#[then(regex = r#"^the session "([^"]+)" ends failed once with reason "([^"]+)"$"#)]
async fn ends_failed_once(world: &mut LaserWorld, label: String, reason: String) {
    let id = world.managed.id(&label);
    let lens = world.managed.sessions().open(id);
    let ends = eventually("the failed terminal", async || {
        let turns = lens.context().await.ok()?;
        let ends: Vec<SessionEnd> = turns
            .into_iter()
            .filter_map(|turn| {
                let envelope = turn.message.envelope?;
                (envelope.kind == AgentKind::Status
                    && envelope.operation.as_deref() == Some("session")
                    && envelope.task_state == Some(TaskState::Failed))
                .then(|| laser_sdk::wire::framing::decode_named(&envelope.body).ok())
                .flatten()
            })
            .collect();
        (!ends.is_empty()).then_some(ends)
    })
    .await;
    assert_eq!(ends.len(), 1, "{ends:?}");
    assert_eq!(ends[0].reason.as_deref(), Some(reason.as_str()));
    assert!(
        ends[0]
            .error
            .as_ref()
            .and_then(|error| error.message.as_deref())
            .is_some_and(|message| message.contains("budget")),
        "{ends:?}"
    );
}

#[when("I retain the session and state handles for lane identity checks")]
async fn retain_lane_handles(world: &mut LaserWorld) {
    let session = world.session.as_ref().expect("a started session");
    session
        .state()
        .set("before", serde_json::json!(1))
        .await
        .expect("seed the retained state cursor");
    world.managed.retained = Some(session.clone().as_agent(agent("planner")));
    world.managed.retained_state = Some(session.state());
    world.managed.lease = None;
    world.session_lease = None;
}

async fn first_source_reference(
    sessions: &Sessions,
    session: &laser_sdk::agent::Session,
) -> SourceRef {
    let first = eventually("the session's first source record", async || {
        session
            .context()
            .await
            .ok()?
            .into_iter()
            .next()
            .map(|turn| turn.message)
    })
    .await;
    let (topic, generation, _) = eventually("the registered lane identity", async || {
        sessions.sources(session.conversation()).await.ok()?.lane
    })
    .await;
    assert_eq!(topic, first.topic_id);
    SourceRef::Message {
        stream: first.stream_id,
        topic,
        partition: first.id.partition_id,
        offset: first.id.offset,
        generation: Some(generation),
        conversation: None,
    }
}

#[when("I retain a generation-bearing reference to its first record")]
async fn retain_source_reference(world: &mut LaserWorld) {
    let at = first_source_reference(world.managed.sessions(), world.session()).await;
    assert!(
        world
            .managed
            .laser()
            .read_at(&at)
            .await
            .expect("read the original source")
            .is_some(),
        "the retained reference initially names an existing record"
    );
    world.managed.retained_source = Some(at);
    world.managed.lease = None;
    world.session_lease = None;
}

#[when("a replacement session records its start at the retained source address")]
async fn replacement_source_record(world: &mut LaserWorld) {
    let sessions = world.managed.second();
    let (replacement, lease) = sessions
        .start()
        .with_id(world.session().conversation())
        .agent(agent("planner"))
        .begin()
        .await
        .expect("start a replacement session on the recovered source");
    drop(lease);
    let at = first_source_reference(sessions, &replacement).await;
    let original = world
        .managed
        .retained_source
        .as_ref()
        .expect("the retained reference");
    let address = |at: &SourceRef| match at {
        SourceRef::Message {
            stream,
            topic,
            partition,
            offset,
            generation,
            ..
        } => ((*stream, *topic, *partition, *offset), *generation),
        _ => panic!("the retained reference names a message"),
    };
    assert_eq!(
        address(original).0,
        address(&at).0,
        "the recreated source reuses the record address"
    );
    assert_ne!(
        address(original).1,
        address(&at).1,
        "the replacement has a new source generation"
    );
    assert!(
        world
            .managed
            .laser()
            .read_at(&at)
            .await
            .expect("read the replacement source")
            .is_some(),
        "the replacement address contains a readable record"
    );
}

#[then("reading the retained source reference returns no replacement record")]
async fn refuse_replacement_source_record(world: &mut LaserWorld) {
    let at = world
        .managed
        .retained_source
        .as_ref()
        .expect("the retained reference");
    assert!(
        world
            .managed
            .laser()
            .read_at(at)
            .await
            .expect("refuse the old source generation")
            .is_none(),
        "an old source reference must not return the replacement record"
    );
}

#[when(regex = r"^the session source changes its (.+)$")]
async fn change_lane_source(world: &mut LaserWorld, change: String) {
    let stream = world.managed.sessions().stream().expect("a stream");
    let count = if world.managed.layout == SessionLayout::SinglePartition {
        1
    } else {
        4
    };
    let endpoint = format!(
        "iggy:iggy@{}",
        std::env::var("LASER_BDD_ADDR").expect("managed address")
    );
    let helper = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../session-lane-admin.mjs");
    let output = tokio::process::Command::new("rtk")
        .args(["proxy", "node"])
        .arg(helper)
        .arg(endpoint)
        .arg(stream)
        .arg(change)
        .arg(count.to_string())
        .output()
        .await
        .expect("run the native administration fixture");
    assert!(
        output.status.success(),
        "lane fixture: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_stale_lane<T>(result: Result<T, LaserError>) {
    assert!(
        matches!(
            result,
            Err(LaserError::Session(
                laser_sdk::wire::session::SessionError::Stale(_)
            ))
        ),
        "the SDK refuses the changed source before appending"
    );
}

#[then("retained session handles refuse lifecycle and state writes as stale")]
async fn refuse_retained_lane(world: &mut LaserWorld) {
    let session = world.session.as_ref().expect("the original session");
    let clone = world.managed.retained.as_ref().expect("a retained clone");
    let state = world
        .managed
        .retained_state
        .as_ref()
        .expect("a retained state handle");
    let before = session
        .context()
        .await
        .expect("read the current lane")
        .len();
    assert_stale_lane(session.cancel().await);
    assert_stale_lane(clone.cancel().await);
    assert_stale_lane(state.set("after", serde_json::json!(2)).await);
    assert_stale_lane(state.snapshot().await);
    let envelope = AgentEnvelope::event(
        RecordId::from_u128(1),
        session.conversation().into(),
        agent("planner"),
        vec![1],
    );
    assert_stale_lane(session.append(envelope).await);
    assert_stale_lane(
        world
            .managed
            .sessions()
            .start()
            .agent(agent("planner"))
            .begin()
            .await,
    );
    assert_stale_lane(
        world
            .managed
            .sessions()
            .submit(agent("worker"), vec![1])
            .from(agent("planner"))
            .send()
            .await,
    );
    assert_eq!(
        session
            .context()
            .await
            .expect("read the lane after refusal")
            .len(),
        before,
        "refused writes append no records"
    );
}

#[then("a fresh handle refuses the stale lane registration")]
async fn refuse_stale_registration(world: &mut LaserWorld) {
    let laser = world.managed.laser();
    let id = world
        .session
        .as_ref()
        .expect("the original session")
        .conversation();
    let before = world
        .session
        .as_ref()
        .expect("the session")
        .context()
        .await
        .expect("read the lane")
        .len();
    assert_stale_lane(
        laser
            .sessions()
            .open(id)
            .as_agent(agent("planner"))
            .cancel()
            .await,
    );
    assert_eq!(
        world
            .session
            .as_ref()
            .expect("the session")
            .context()
            .await
            .expect("read the lane")
            .len(),
        before
    );
}

#[when("the session source is explicitly removed and registered again")]
async fn recover_lane_registration(world: &mut LaserWorld) {
    let laser = world.managed.laser().clone();
    let stream = world
        .managed
        .sessions()
        .stream()
        .expect("a stream")
        .to_owned();
    let id = world
        .session
        .as_ref()
        .expect("a retained session")
        .conversation();
    let command = laser_sdk::wire::control::ControlEnvelope {
        v: laser_sdk::wire::codes::CONTROL_OP_VERSION,
        timestamp_micros: u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_micros(),
        )
        .expect("timestamp fits u64"),
        command: laser_sdk::wire::control::ControlCommand::RemoveSessionSource { stream },
        stream: None,
    };
    laser
        .stream(laser.ops_stream())
        .topic(laser.control_topic())
        .send(
            laser_sdk::wire::framing::encode_named(&command).expect("encode removal"),
            BTreeMap::new(),
            Some("control"),
        )
        .await
        .expect("publish source removal");
    eventually("source registration removal", async || {
        matches!(
            laser.sessions().sources(id).await,
            Err(LaserError::Session(
                laser_sdk::wire::session::SessionError::NotRegistered(_)
            ))
        )
        .then_some(())
    })
    .await;
    let sessions = laser.sessions_with(SessionConfig::new().layout(world.managed.layout.clone()));
    assert!(
        sessions
            .bootstrap(4, TopicRetention::expire_after(Duration::from_secs(86400)))
            .await
            .expect("explicit new registration")
            .registered
    );
    world.managed.second = Some(sessions);
}

#[then("a fresh session can write lifecycle and state on the recovered lane")]
async fn recovered_lane_writes(world: &mut LaserWorld) {
    let sessions = world.managed.second();
    let (session, lease) = sessions
        .start()
        .agent(agent("planner"))
        .begin()
        .await
        .expect("start on the recovered lane");
    session
        .state()
        .set("after", serde_json::json!(2))
        .await
        .expect("state on the recovered lane");
    session.end().await.expect("end on the recovered lane");
    drop(lease);
    let id = session.conversation();
    eventually("the recovered session state", async || {
        let view = sessions.state(id, 10).await.ok()?;
        (view.document == serde_json::json!({"after": 2})).then_some(())
    })
    .await;
    let info = eventually("the recovered session completion", async || {
        let info = sessions.get(id).await.ok()?;
        (info.status == SessionStatus::Completed).then_some(info)
    })
    .await;
    assert!(!info.flags.lane_conflict);
}

#[then("retained session handles still refuse the recovered lane")]
async fn old_lane_stays_stale(world: &mut LaserWorld) {
    assert_stale_lane(
        world
            .managed
            .retained
            .as_ref()
            .expect("the retained clone")
            .cancel()
            .await,
    );
    assert_stale_lane(
        world
            .managed
            .retained_state
            .as_ref()
            .expect("the retained state")
            .set("after", serde_json::json!(3))
            .await,
    );
}

#[then("the old lease publishes no heartbeat into the recreated stream")]
async fn old_generation_never_heartbeats(world: &mut LaserWorld) {
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut cursor = world
        .managed
        .laser()
        .topic(laser_sdk::wire::topics::AGENT_HEARTBEATS)
        .replay()
        .expect("a heartbeat reader");
    assert!(
        cursor
            .poll()
            .await
            .expect("read the recreated heartbeat topic")
            .is_empty(),
        "an old-generation lease must publish no new-generation heartbeat"
    );
    world.managed.lease = None;
    world.session_lease = None;
}

#[when("a native lane writer without session read grants writes lifecycle and state")]
async fn native_lane_writer(world: &mut LaserWorld) {
    let stream = world
        .managed
        .sessions()
        .stream()
        .expect("a stream")
        .to_owned();
    let username = format!("writer-{stream}");
    let address = std::env::var("LASER_BDD_ADDR").expect("managed address");
    let helper = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../session-lane-admin.mjs");
    let output = tokio::process::Command::new("rtk")
        .args(["proxy", "node"])
        .arg(helper)
        .arg(format!("iggy:iggy@{address}"))
        .arg(&stream)
        .arg("writer credentials")
        .arg("4")
        .arg(&username)
        .output()
        .await
        .expect("create native writer credentials");
    assert!(
        output.status.success(),
        "writer fixture: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let writer =
        Laser::connect_with_stream(&format!("{username}:lane-writer-test@{address}"), &stream)
            .await
            .expect("connect with native writer permissions");
    let (session, lease) = writer
        .sessions()
        .create("writer")
        .agent(agent("planner"))
        .begin()
        .await
        .expect("a native writer may start a session");
    session
        .state()
        .set("step", serde_json::json!(1))
        .await
        .expect("a native writer may write session state");
    session
        .end()
        .await
        .expect("a native writer may end the session");
    drop(lease);
    world
        .managed
        .labels
        .insert("writer".to_owned(), session.conversation());
    world.managed.writer = Some(writer);
}

#[then("aggregate session reads remain refused for that writer")]
async fn native_writer_cannot_read_aggregate(world: &mut LaserWorld) {
    let writer = world.managed.writer.take().expect("a native-only writer");
    let id = world.managed.id("writer");
    assert!(
        writer
            .sessions()
            .sources(id)
            .await
            .expect_err("aggregate sources require session read authority")
            .is_permission_denied()
    );
    assert!(
        writer
            .sessions()
            .get(id)
            .await
            .expect_err("session history requires session read authority")
            .is_permission_denied()
    );
    writer.close().await.expect("close the native writer");
}
