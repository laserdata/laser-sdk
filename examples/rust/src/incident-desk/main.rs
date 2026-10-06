use async_trait::async_trait;
use futures::future::join_all;
use laser_examples::{
    LlmClient, PARTITIONS, batch, default_llm, ensure_view, env_bool, fresh_run, index_for,
    init_tracing, laser, managed_feature_ready, messages, phase, stream_for,
};
use laser_sdk::iggy::prelude::IggyTimestamp;
use laser_sdk::prelude::full::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

// THE agentic example: an AI incident desk operating a live incident, end to
// end, with every agent coordinating only through the log. One realistic
// story, each platform feature doing the job it exists for:
//
//   1. WORLD       a ticket firehose bulk-ingests into a queryable index
//                  (the desk's world model), every record carrying the
//                  `message_type` + `ts` convention fields.
//   2. MEMORY      past resolution notes are remembered semantically, the
//                  desk recalls the closest ones when the incident arrives.
//   3. THE DESK    four agents on the agent topics:
//                    triage     (Commands -> fan-out -> Responses) queries
//                               the index as a tool, fans one diagnostic
//                               angle per specialist call under a deadline,
//                               and synthesizes a diagnosis with the LLM
//                    specialist (ToolCalls -> ToolResults) answers each
//                               angle from recalled memory plus the LLM
//                    resolver   (Commands, KV-deduplicated) executes
//                               capacity grants effectively once, large
//                               ones gated behind a durable approval
//                    approver   (HumanInput -> Responses) stands in for the
//                               human behind that gate
//   4. SPECULATION the diagnosis proposes bulk-resolving the matching
//                  backlog. The desk tries it in a copy-on-write fork,
//                  compares the forked backlog against the trunk, and
//                  leaves the fork open with the verdict logged so
//                  LaserData Cloud shows it. Set LASER_APPLY_PLAN=1 to act on
//                  the verdict (promote when it clears the criticals,
//                  squash when it does not).
//   5. AUDIT       the whole incident is one conversation on the log. A
//                  crashed coordinator rebuilds it by folding the
//                  conversation, which the run demonstrates at the end.
//
// The LLM seam is `default_llm()`: deterministic MockLlm by default, a real
// model with `--features llm-anthropic` (ANTHROPIC_API_KEY) or
// `--features llm-openai` (OPENAI_API_KEY). Scale the world with the shared
// volume knobs:
//
//   # quick: 2k tickets
//   cargo run --release --example incident-desk
//
//   # heavy: a million tickets, bigger batches
//   LASER_MESSAGES=1000000 LASER_BATCH=1000 cargo run --release --example incident-desk
//
// The desk's queryable world model, KV, approvals, and forks are LaserData
// Cloud features. On an open server the example prints how to point at a
// deployment and exits green before provisioning them.

const TICKETS_TOPIC: &str = "support_tickets";
const TRIAGE_FORK: &str = "bulk-resolve-plan";

const EMBEDDING_DIMS: usize = 64;
const TOP_K: usize = 3;
const PROJECTOR_TIMEOUT: Duration = Duration::from_secs(120);
const PROJECTION_POLL: Duration = Duration::from_millis(150);
// Roomy enough for cold consumer-group joins on a remote deployment.
const TOOL_TIMEOUT: Duration = Duration::from_secs(60);
const DESK_TIMEOUT: Duration = Duration::from_secs(150);
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(60);
// A rate-limited deployment (a free-tier plan's bandwidth cap, say) can take
// minutes to settle a handful of KV writes. LASER_DESK_GRANT_TIMEOUT_SECS
// overrides the default for a deployment slower than even this margin.
const GRANT_DEADLINE_DEFAULT_SECS: u64 = 180;
const GRANT_POLL: Duration = Duration::from_millis(250);
// Dedup keys self-expire, long enough to outlive a redelivery.
const DEDUP_TTL: Duration = Duration::from_secs(3600);
// Grants at or above this many capacity units hold for a durable approval first.
const APPROVAL_UNITS: u64 = 100;

const CLUSTERS: &[&str] = &["east-1", "west-2", "eu-1", "ap-1", "lab"];
const COMPONENTS: &[&str] = &["auth", "config", "storage", "metrics", "gateway"];
const SEVERITIES: &[&str] = &["low", "medium", "high", "critical"];

// What the desk has learned resolving past incidents, recalled semantically
// when a similar one arrives.
const RESOLUTION_NOTES: &[&str] = &[
    "auth latency spikes are usually database connection pool exhaustion",
    "duplicate config pushes trace back to retries without an idempotency key",
    "metrics returning stale results means the nightly index rebuild failed",
    "auth token errors after a deploy come from the rotated signing key",
    "storage write failures over 10 MB are the proxy body-size limit, not the bucket",
    "critical auth pages resolve fastest by failing over the read replica",
];

const INCIDENT: &str = "auth is slow for several clusters";

// The diagnostic angles triage fans out, one specialist call each.
const ANGLES: &[&str] = &[
    "most likely root cause",
    "fastest mitigation",
    "blast radius to check",
];

// (idempotency key, cluster, capacity units) the diagnosis remediates with.
// The list is sent twice to prove the resolver is effectively once: the
// redelivery must not grant any cluster twice.
const GRANTS: &[(&str, &str, u64)] = &[
    ("gr-1", "east-1", 150),
    ("gr-2", "west-2", 50),
    ("gr-3", "eu-1", 80),
];
const GRANT_TOTALS: &[(&str, u64)] = &[("east-1", 150), ("west-2", 50), ("eu-1", 80)];

#[derive(Serialize)]
struct Ticket {
    ticket_id: String,
    message_type: String,
    cluster: String,
    component: String,
    severity: String,
    status: String,
    ts: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Grant {
    cluster: String,
    units: u64,
}

// One incident step appended to the conversation by an agent. Folding these
// back together is the audit and recovery path at the end of the run.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct IncidentLog {
    diagnosis: String,
    findings: Vec<String>,
}

impl IncidentLog {
    fn absorb(&mut self, other: IncidentLog) {
        if !other.diagnosis.is_empty() {
            self.diagnosis = other.diagnosis;
        }
        self.findings.extend(other.findings);
    }
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    phase("warming up");
    let laser = laser(&stream_for("incident-desk"), Capabilities::OPEN).await?;
    fresh_run(&laser, &stream_for("incident-desk"), async {
        laser.bootstrap(PARTITIONS).await?;
        laser.topic(TICKETS_TOPIC).ensure(PARTITIONS).await?;
        let capabilities = laser.capabilities().await;
        if !managed_feature_ready(capabilities.managed, "the agentic desk", "incident-desk") {
            return Ok(());
        }
        // The index carries this run's token, so a rerun or another language's
        // desk on the same deployment never shares its rows.
        let index = index_for(TICKETS_TOPIC);
        ensure_view(
            &laser,
            TICKETS_TOPIC,
            &index,
            ContentType::Json,
            &[
                "ticket_id",
                "message_type",
                "cluster",
                "component",
                "severity",
                "status",
                "ts",
            ],
        )
        .await?;

        let total = messages(2_000);
        let chunk = batch(200);
        phase("ingesting the ticket firehose (the desk's world model)");
        ingest_tickets(&laser, total, chunk).await?;
        wait_for_index(&laser, &index, total as usize).await?;
        backlog_snapshot(&laser).await?;

        phase("seeding semantic memory with past resolutions");
        // One shared in-process semantic index for the whole desk: seeded here, read
        // by every specialist call, and appended to as incidents resolve.
        let semantic = Arc::new(VectorMemory::new(HashEmbedder));
        seed_memory(&semantic).await?;

        phase("spawning the desk: triage, specialist, resolver, approver");
        let llm = default_llm();
        let mut triage = Agent::builder()
            .id("triage".parse()?)
            .listen_on(AgentTopic::Commands)
            .respond_on(AgentTopic::Responses)
            .handler(Triage {
                llm: llm.clone(),
                index: index.clone(),
            })
            .build()
            .spawn(laser.clone());
        let mut specialist = Agent::builder()
            .id("specialist".parse()?)
            .listen_on(AgentTopic::ToolCalls)
            .respond_on(AgentTopic::ToolResults)
            .handler(Specialist {
                llm: llm.clone(),
                semantic: semantic.clone(),
            })
            .build()
            .spawn(laser.clone());
        let mut approver = Agent::builder()
            .id("approver".parse()?)
            .listen_on(AgentTopic::HumanInput)
            .respond_on(AgentTopic::Responses)
            .handler(Approver)
            .build()
            .spawn(laser.clone());
        // Run-scoped namespaces so reruns never read each other's state.
        let run = ConversationId::new();
        let dedup_namespace = format!("desk-dedup-{run}");
        let grants_namespace = format!("desk-grants-{run}");
        let mut resolver = Agent::builder()
            .id("resolver".parse()?)
            .listen_on(AgentTopic::Commands)
            .handler(Resolver {
                grants: grants_namespace.clone(),
            })
            .deduplicator(Box::new(KvDeduplicator {
                laser: laser.clone(),
                namespace: dedup_namespace.clone(),
                ttl: DEDUP_TTL,
            }))
            .build()
            .spawn(laser.clone());
        for agent in [&mut triage, &mut specialist, &mut approver, &mut resolver] {
            agent.ready().await?;
        }

        phase("triaging the incident through the desk");
        let incident = ConversationId::new();
        let task = Provenance::builder().conversation_id(incident).build();
        info!("incident on conversation {incident}: {INCIDENT}");
        let diagnosed: IncidentLog = serde_json::from_slice(
            &laser
                .agent("orchestrator".parse()?)
                .ask(
                    AgentTopic::Commands,
                    AgentTopic::Responses,
                    INCIDENT.as_bytes().to_vec(),
                    &task,
                    DESK_TIMEOUT,
                )
                .await?
                .payload,
        )
        .map_err(|error| LaserError::Codec(error.to_string()))?;
        info!("diagnosis: {}", diagnosed.diagnosis);

        phase("executing capacity grants effectively once");
        // Send the grant list twice. The KV deduplicator keyed on each grant's
        // idempotency key makes the redelivery a no-op, so the totals stay exact.
        send_grants(&laser, incident, GRANTS).await?;
        send_grants(&laser, incident, GRANTS).await?;
        wait_for_grants(&laser, &grants_namespace, GRANT_TOTALS).await?;
        for &(cluster, expected) in GRANT_TOTALS {
            let actual = read_u64(&laser.kv(&grants_namespace), cluster).await?;
            if actual != expected {
                return Err(LaserError::Invalid(format!(
                    "grants were not effectively once: {cluster}={actual}, want {expected}"
                )));
            }
        }
        info!(
            "grants applied exactly once despite the redelivery, inspect them in LaserData Cloud under \
             KV namespace {grants_namespace}"
        );

        phase("optimistic concurrency, read-your-writes, and the unified result space");
        coordination_demo(&laser).await?;

        if capabilities.forks {
            phase("speculating a bulk-resolve plan in a fork");
            speculative_bulk_resolve(&laser).await?;
        } else {
            info!("read-model forks unavailable here, skipping the speculative plan");
        }

        phase("remembering this resolution for the next incident");
        remember_resolution(&semantic, &diagnosed.diagnosis).await?;

        phase("rebuilding the incident from the log alone (the audit trail)");
        let recovered = recover_incident(&laser, incident).await?;
        info!(
            "recovered from the log: {} findings, diagnosis intact: {}",
            recovered.findings.len(),
            !recovered.diagnosis.is_empty(),
        );

        for agent in [triage, specialist, approver, resolver] {
            agent.shutdown().await?;
        }
        phase("done");
        info!(
            "inspect the run in LaserData Cloud: index `{index}`, \
             KV namespaces `{grants_namespace}` and `{dedup_namespace}`"
        );
        Ok(())
    })
    .await
}

// A small xorshift64* pseudo random generator seeded by a constant, so the
// whole run replays identically with no extra crate.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut state = self.0;
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        self.0 = state;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound.max(1)
    }
}

// Publish `total` tickets in `chunk`-sized batches. Every field rides as an
// indexed header and the JSON body is inlined, so LaserData Cloud materializes a
// fully queryable ticket table while the log keeps the raw bytes. Tickets
// carry the `message_type` and `ts` convention fields, so the reserved
// columns fill and the `message_type` / `time_range` query sugar works.
async fn ingest_tickets(laser: &Laser, total: u64, chunk: usize) -> Result<(), LaserError> {
    let mut rng = Rng::new(7);
    let mut ts = 1_900_000_000_000_000u64;
    let mut published = 0u64;
    while published < total {
        let size = chunk.min((total - published) as usize);
        let tickets = laser.topic(TICKETS_TOPIC);
        let mut request = tickets.publish_batch();
        for index in 0..size {
            let ticket_number = published + index as u64;
            ts += rng.below(60_000_000);
            let ticket = Ticket {
                ticket_id: format!("t-{ticket_number:08}"),
                message_type: "ticket_opened".to_owned(),
                cluster: CLUSTERS[rng.below(CLUSTERS.len() as u64) as usize].to_owned(),
                component: COMPONENTS[rng.below(COMPONENTS.len() as u64) as usize].to_owned(),
                severity: SEVERITIES[rng.below(SEVERITIES.len() as u64) as usize].to_owned(),
                status: if rng.below(100) < 80 {
                    "open"
                } else {
                    "resolved"
                }
                .to_owned(),
                ts,
            };
            // The body alone drives indexing: the projection's pointers
            // extract every column out of the decoded JSON, typed. No
            // `agdx.idx.*` headers, those exist for codecs a projector cannot
            // decode (raw, arrow), not as a copy of the payload.
            let body = serde_json::to_vec(&ticket)
                .map_err(|error| LaserError::Codec(error.to_string()))?;
            let record = Record::builder()
                .content_type(ContentType::Json)
                .inline_payload()
                .build();
            request = request.add_record(body, record);
        }
        request.send().await?;
        published += size as u64;
        if published % 100_000 < size as u64 {
            info!("ingested {published}/{total} tickets");
        }
    }
    info!("ingested {total} tickets in batches of {chunk}");
    Ok(())
}

// Poll until `expected` rows are materialized in `index`, tolerant of a
// not-yet-created table while LaserData Cloud applies the projection.
async fn wait_for_index(laser: &Laser, index: &str, expected: usize) -> Result<(), LaserError> {
    let deadline = Instant::now() + PROJECTOR_TIMEOUT;
    let mut last = usize::MAX;
    loop {
        let total = laser
            .query(index)
            .with_total()
            .fetch()
            .await
            .map(|result| result.page.total.unwrap_or(0) as usize)
            .unwrap_or(0);
        if total != last {
            info!("projector materialized {total}/{expected} rows in `{index}`");
            last = total;
        }
        if total >= expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(format!(
                "projector indexed only {total}/{expected} rows in `{index}` before the deadline"
            )));
        }
        tokio::time::sleep(PROJECTION_POLL).await;
    }
}

// The questions an on-call asks first, straight off the materialized index.
async fn backlog_snapshot(laser: &Laser) -> Result<(), LaserError> {
    let index = index_for(TICKETS_TOPIC);
    let by_severity = laser
        .query(&index)
        .filter_eq("status", "open")
        .count()
        .group_by(["severity"])
        .fetch()
        .await?;
    for row in &by_severity.rows {
        let severity = by_severity
            .value_text(row, "severity")
            .unwrap_or_else(|| "?".to_owned());
        let count = by_severity
            .value_text(row, "count")
            .unwrap_or_else(|| "?".to_owned());
        info!("  open {} tickets: {}", severity, count,);
    }
    Ok(())
}

// The deterministic bag-of-words embedder: hash tokens into buckets and
// L2-normalize. Swap a real model in behind the same `Embedder` seam.
struct HashEmbedder;

impl Embedder for HashEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        let mut vector = vec![0.0f32; EMBEDDING_DIMS];
        for token in text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| !t.is_empty())
        {
            vector[fnv1a(&token.to_ascii_lowercase()) as usize % EMBEDDING_DIMS] += 1.0;
        }
        let norm = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            vector.iter_mut().for_each(|x| *x /= norm);
        }
        Ok(vector)
    }
}

// 32-bit FNV-1a, enough to spread tokens across embedding buckets.
fn fnv1a(text: &str) -> u32 {
    let mut hash = 0x811c_9dc5u32;
    for byte in text.bytes() {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

// Remember every past resolution note in the shared semantic index, embedded for
// similarity recall. Cross-incident knowledge, so each note is unscoped.
async fn seed_memory(memory: &VectorMemory<HashEmbedder>) -> Result<(), LaserError> {
    for note in RESOLUTION_NOTES {
        Memory::remember(memory, &MemoryScope::default(), note.as_bytes().to_vec()).await?;
    }
    Ok(())
}

// First aggregate value of a single-aggregate result, as text.
fn scalar(result: &QueryResult) -> String {
    result
        .rows
        .first()
        .and_then(|row| result.value_text(row, "count"))
        .unwrap_or_else(|| "0".to_owned())
}

// The orchestrator. Reads the live backlog off the index (the index as a
// tool), fans one diagnostic angle per specialist call under a deadline, and
// synthesizes the findings into a diagnosis with the LLM. The diagnosis and
// the findings ride back on the conversation, durable on the log.
struct Triage {
    llm: Arc<dyn LlmClient>,
    index: String,
}

impl AgentHandler for Triage {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        // The resolver shares the Commands topic. Grants are its traffic,
        // free text is ours. Never fail on foreign messages.
        if serde_json::from_slice::<Grant>(&message.payload).is_ok() {
            return Ok(());
        }
        let incident = String::from_utf8_lossy(&message.payload).into_owned();
        let triage_id: AgentId = "triage".parse()?;

        // Tool 1: the materialized index. The desk reads the live blast
        // radius the same way an on-call would.
        let open_criticals = scalar(
            &ctx.laser()
                .query(&self.index)
                .filter_eq("severity", "critical")
                .filter_eq("status", "open")
                .filter_eq("component", "auth")
                .count()
                .fetch()
                .await?,
        );
        info!(
            agent = "triage",
            open_criticals, "queried the index for the blast radius"
        );

        // Tool 2: the specialist, one deadline-bounded call per angle, each
        // on its own correlation conversation so the replies never cross.
        let deadline =
            IggyTimestamp::from(IggyTimestamp::now().as_micros() + TOOL_TIMEOUT.as_micros() as u64);
        let calls = ANGLES.iter().map(|angle| {
            let correlation = Provenance::builder()
                .conversation_id(ConversationId::new())
                .agent(triage_id.clone())
                .deadline(deadline)
                .build();
            let query = Vec::<u8>::from(format!("{angle} for: {incident}"));
            async move {
                ctx.request(
                    AgentTopic::ToolCalls,
                    AgentTopic::ToolResults,
                    query,
                    &correlation,
                    TOOL_TIMEOUT,
                )
                .await
            }
        });
        let findings: Vec<String> = join_all(calls)
            .await
            .into_iter()
            .filter_map(Result::ok)
            .map(|result| String::from_utf8_lossy(&result.payload).into_owned())
            .collect();
        info!(agent = "triage", "gathered {} findings", findings.len());

        let prompt = format!(
            "Diagnose this incident and recommend one mitigation.\nIncident: {incident}\n\
             Open critical auth tickets: {open_criticals}\nFindings:\n{}",
            findings.join("\n"),
        );
        let diagnosis = self.llm.complete(&prompt).await;
        let log = IncidentLog {
            diagnosis,
            findings,
        };
        ctx.respond(
            serde_json::to_vec(&log).map_err(|error| LaserError::Handler(error.to_string()))?,
        )
        .await
    }
}

// The tool agent. Answers one diagnostic angle from what the desk remembers
// (semantic recall over past resolutions) plus the LLM. It recalls from the
// shared in-process `VectorMemory` the desk seeds at startup. The whole desk
// runs in one process, so every specialist call reads the one index.
struct Specialist {
    llm: Arc<dyn LlmClient>,
    semantic: Arc<VectorMemory<HashEmbedder>>,
}

impl AgentHandler for Specialist {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let query = String::from_utf8_lossy(&message.payload).into_owned();
        // Past resolutions are cross-incident knowledge, so recall is unscoped
        // (no conversation): the closest notes to this query, whatever incident
        // first learned them.
        let scope = MemoryScope::default();
        let recall = MemoryQuery::builder()
            .semantic(query.clone())
            .limit(TOP_K)
            .build();
        let remembered: Vec<String> = Memory::recall(self.semantic.as_ref(), &scope, &recall)
            .await?
            .into_iter()
            .map(|hit| String::from_utf8_lossy(&hit.payload).into_owned())
            .collect();
        info!(
            agent = "specialist",
            "recalled {} past resolutions for: {query}",
            remembered.len(),
        );
        let answer = self
            .llm
            .complete(&format!(
                "Answer briefly: {query}\nWhat past incidents taught us:\n{}",
                remembered.join("\n"),
            ))
            .await;
        ctx.respond(Vec::<u8>::from(answer)).await
    }
}

// Stands in for the approval UI. In production a person clicks a button,
// here an agent keeps the run deterministic. The approval is durable: it
// rides the log like everything else and survives a restart.
struct Approver;

impl AgentHandler for Approver {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        info!(agent = "approver", "approved a held grant");
        ctx.respond(b"approved".to_vec()).await
    }
}

// Applies a capacity grant to a cluster's quota in KV. The effect is a
// read-modify-write, which is exactly why the dedup gate in front of it
// matters. Grants at or above the threshold hold for a durable approval.
struct Resolver {
    grants: String,
}

impl AgentHandler for Resolver {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        // Triage shares the Commands topic. Free text is its traffic.
        let Ok(grant) = serde_json::from_slice::<Grant>(&message.payload) else {
            return Ok(());
        };
        let key = message.provenance.idempotency_key.as_deref().unwrap_or("?");
        if grant.units >= APPROVAL_UNITS {
            info!(
                key,
                cluster = grant.cluster,
                units = grant.units,
                "large grant, requesting approval"
            );
            if !approved(ctx, &grant).await? {
                warn!(key, cluster = grant.cluster, "grant declined");
                return Ok(());
            }
        }
        let store = ctx.laser().kv(&self.grants);
        let quota = read_u64(&store, &grant.cluster).await? + grant.units;
        store
            .set(&grant.cluster)
            .bytes(quota.to_string())
            .send()
            .await?;
        info!(
            key,
            cluster = grant.cluster,
            units = grant.units,
            quota,
            "applied grant"
        );
        Ok(())
    }
}

// Hold a large grant for approval: ask on the human-input topic and block
// on the decision. Returns whether to apply it.
async fn approved(ctx: &AgentCtx<'_>, grant: &Grant) -> Result<bool, LaserError> {
    let request = Provenance::builder()
        .conversation_id(ConversationId::new())
        .agent("resolver".parse()?)
        .build();
    let prompt = format!(
        "approve a {} unit capacity grant to {}?",
        grant.units, grant.cluster
    );
    let decision = ctx
        .request(
            AgentTopic::HumanInput,
            AgentTopic::Responses,
            prompt.into_bytes(),
            &request,
            APPROVAL_TIMEOUT,
        )
        .await?;
    Ok(decision.payload == b"approved")
}

// A durable, self-expiring dedup backend over the managed KV store, plugged
// into the runtime via `Agent::builder().deduplicator(..)`. The runtime
// calls `observe` before the handler and skips the handler when it returns
// false. The default is an in-memory window, KV is the durable drop-in that
// survives a restart.
struct KvDeduplicator {
    laser: Laser,
    namespace: String,
    ttl: Duration,
}

#[async_trait]
impl Deduplicator for KvDeduplicator {
    async fn observe(&self, key: &str) -> bool {
        let store = self.laser.kv(&self.namespace);
        match store.get(key).await {
            Ok(Some(_)) => {
                info!(key, "duplicate grant, skipping (dedup)");
                false
            }
            Ok(None) => {
                if let Err(error) = store.set(key).bytes(b"1").ttl(self.ttl).send().await {
                    warn!(%error, key, "dedup write failed, processing anyway (at-least-once)");
                }
                true
            }
            // Fail open: on a store error, process rather than silently drop.
            Err(error) => {
                warn!(%error, key, "dedup read failed, processing anyway (at-least-once)");
                true
            }
        }
    }
}

async fn send_grants(
    laser: &Laser,
    conversation: ConversationId,
    grants: &[(&str, &str, u64)],
) -> Result<(), LaserError> {
    for &(key, cluster, units) in grants {
        let grant = Grant {
            cluster: cluster.to_owned(),
            units,
        };
        let provenance = Provenance::builder()
            .conversation_id(conversation)
            .idempotency_key(key.to_owned())
            .build();
        laser
            .agent("orchestrator".parse()?)
            .send(
                AgentTopic::Commands,
                serde_json::to_vec(&grant).map_err(|error| LaserError::Codec(error.to_string()))?,
                &provenance,
            )
            .await?;
    }
    Ok(())
}

async fn wait_for_grants(
    laser: &Laser,
    namespace: &str,
    targets: &[(&str, u64)],
) -> Result<(), LaserError> {
    let store = laser.kv(namespace);
    let timeout = Duration::from_secs(laser_examples::env_u64(
        "LASER_DESK_GRANT_TIMEOUT_SECS",
        GRANT_DEADLINE_DEFAULT_SECS,
    ));
    let deadline = Instant::now() + timeout;
    loop {
        let mut pending = Vec::new();
        for &(cluster, target) in targets {
            if read_u64(&store, cluster).await? < target {
                pending.push(cluster);
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LaserError::Invalid(format!(
                "timed out applying grants, still short on: {}",
                pending.join(", ")
            )));
        }
        tokio::time::sleep(GRANT_POLL).await;
    }
}

async fn read_u64(store: &Kv, key: &str) -> Result<u64, LaserError> {
    Ok(store
        .get(key)
        .await?
        .and_then(|payload| String::from_utf8(payload).ok())
        .and_then(|text| text.parse().ok())
        .unwrap_or(0))
}

// Three coordination primitives on the same connection: optimistic concurrency
// (compare-and-swap), read-your-writes consistency, and the unified result-code
// space that classifies any outcome. Each runs against the managed backend.
// Where a backend does not serve one, `LaserError::code()` classifies the
// outcome and we log it rather than failing, the exact branch a real client
// uses to adapt to a deployment's capabilities, never a silent fallback.
async fn coordination_demo(laser: &Laser) -> Result<(), LaserError> {
    let index = index_for(TICKETS_TOPIC);
    let ledger = laser.kv("desk_quota_ledger");
    let account = "pool:demo";

    // Seed the quota create-if-absent: the compare-and-swap refuses if a
    // racing writer already created it. `Committed { version }` hands back the
    // new version to chain the next conditional write without a re-read.
    match ledger
        .set(account)
        .bytes(b"0")
        .expect_absent()
        .commit()
        .await
    {
        Ok(version) => info!(version, "seeded the quota ledger (compare-and-swap)"),
        Err(error) if error.is_version_conflict() => {
            info!("ledger already seeded by a concurrent writer")
        }
        Err(error) => {
            info!(code = ?error.code(), %error, "compare-and-swap not served here, skipping the demo");
            return Ok(());
        }
    }

    // A read-modify-CAS loop: the race-safe way two agents apply grants to the
    // same quota. On a version conflict we re-read and retry, and anything else is
    // a real error. This is the compare-and-swap primitive doing the job a bare get-then-set
    // cannot (a lost update under contention). Bounded retries, and exhausting
    // them is a failure we surface, never a silently dropped write.
    const MAX_CAS_ATTEMPTS: u32 = 5;
    let mut applied = false;
    for attempt in 0..MAX_CAS_ATTEMPTS {
        // The ledger was created just above (or by a concurrent writer), so the
        // entry must exist. Its absence here is a real anomaly, not a default.
        let entry = ledger.get_entry(account).await?.ok_or_else(|| {
            LaserError::Invalid("ledger entry vanished after it was seeded".to_owned())
        })?;
        let quota: u64 = std::str::from_utf8(&entry.value)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| LaserError::Invalid("ledger value is not a base-10 quota".to_owned()))?;
        let next = quota + 25;
        match ledger
            .set(account)
            .bytes(next.to_string())
            .expect_version(entry.version)
            .commit()
            .await
        {
            Ok(version) => {
                info!(
                    quota = next,
                    version, "applied a grant via compare-and-swap"
                );
                applied = true;
                break;
            }
            Err(error) if error.is_version_conflict() => {
                warn!(
                    attempt,
                    "lost the compare-and-swap race, re-reading and retrying"
                );
            }
            Err(error) => return Err(error),
        }
    }
    if !applied {
        return Err(LaserError::Invalid(format!(
            "grant not applied after {MAX_CAS_ATTEMPTS} compare-and-swap attempts"
        )));
    }

    // A read-your-writes query: after the writes above, read at a level that
    // waits for the projector to catch up instead of racing it. A `Stale`
    // outcome (the projector could not catch up within the deadline) is
    // retryable, distinct from an unsupported level, and the unified result
    // space tells them apart.
    match laser
        .query(&index)
        .read_your_writes()
        .limit(1)
        .fetch()
        .await
    {
        Ok(result) => info!(
            rows = result.rows.len(),
            "read-your-writes query served fresh"
        ),
        Err(error) if error.is_stale() => {
            warn!("projector still catching up (stale): a real client retries")
        }
        Err(error) => {
            info!(code = ?error.code(), %error, "read-your-writes not served here")
        }
    }
    Ok(())
}

// What-if remediation without touching the trunk: fork the read model, mark
// the open critical auth tickets resolved in the overlay, compare the
// backlogs, then log the verdict. The fork stays open by default so it shows
// up in LaserData Cloud. Set LASER_APPLY_PLAN=1 to act on the verdict instead:
// promote when the plan clears the backlog, squash when it does not.
async fn speculative_bulk_resolve(laser: &Laser) -> Result<(), LaserError> {
    let index = index_for(TICKETS_TOPIC);
    let criticals = laser
        .query(&index)
        .filter_eq("severity", "critical")
        .filter_eq("status", "open")
        .filter_eq("component", "auth")
        .limit(10)
        .fetch()
        .await?;
    if criticals.rows.is_empty() {
        info!("no open critical auth tickets to plan against");
        return Ok(());
    }

    let fork = laser.fork(TRIAGE_FORK);
    // A previous run may have left the fork open for inspection. Clear it so
    // this run plans against a fresh overlay.
    let _ = fork.squash().await;
    fork.create().continuous().send().await?;
    for row in &criticals.rows {
        let partition = criticals
            .value_u64(row, laser_sdk::wire::schema::PARTITION_ID_FIELD_NAME)
            .and_then(|value| u32::try_from(value).ok());
        let offset = criticals.value_u64(row, laser_sdk::wire::schema::OFFSET_FIELD_NAME);
        let (Some(partition), Some(offset)) = (partition, offset) else {
            continue;
        };
        fork.put_row(&index, partition, offset)
            .field("status", "resolved")
            .send()
            .await?;
    }

    let forked_open = laser
        .query(&index)
        .fork(TRIAGE_FORK)
        .filter_eq("severity", "critical")
        .filter_eq("status", "open")
        .filter_eq("component", "auth")
        .count()
        .fetch()
        .await?;
    let cleared = scalar(&forked_open) == "0";

    if !env_bool("LASER_APPLY_PLAN", false) {
        info!(
            "plan staged in fork `{TRIAGE_FORK}` (clears the critical auth backlog: \
             {cleared}), left open so LaserData Cloud shows it. Apply the verdict with \
             LASER_APPLY_PLAN=1"
        );
        return Ok(());
    }
    if cleared {
        let applied = fork.promote().await?;
        info!("plan clears the critical auth backlog, promoted {applied} row(s) to the trunk");
    } else {
        fork.squash().await?;
        info!("plan does not clear the backlog, squashed `{TRIAGE_FORK}`, the trunk never changed");
    }
    Ok(())
}

// Close the memory loop: what this incident taught the desk becomes a note the
// next incident recalls.
async fn remember_resolution(
    memory: &VectorMemory<HashEmbedder>,
    diagnosis: &str,
) -> Result<(), LaserError> {
    let note = format!("auth slowdowns: {diagnosis}");
    Memory::remember(memory, &MemoryScope::default(), note.into_bytes()).await?;
    info!("remembered the resolution for the next incident");
    Ok(())
}

// Rebuild the incident by folding every step of its conversation off the
// log. This is the recovery and audit path: state lives in the stream, so
// any agent can reconstruct it with no side database.
async fn recover_incident(
    laser: &Laser,
    conversation: ConversationId,
) -> Result<IncidentLog, LaserError> {
    ConversationState::load(
        laser,
        conversation,
        vec![AgentTopic::Responses],
        // An incident conversation is a few dozen steps, so the full walk is
        // the honest bound here, written out as the law requires.
        ReplayBound::Full,
        IncidentLog::default(),
        |mut state, message| {
            if let Ok(step) = serde_json::from_slice::<IncidentLog>(&message.payload) {
                state.absorb(step);
            }
            state
        },
    )
    .await
}
