use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;
use iggy::prelude::{Identifier, StreamClient};
use laser_wire::agent::{AgentId, OPERATION_PROGRESS};
use laser_wire::content::ContentType;
use laser_wire::framing::encode_named;
use laser_wire::limits::MAX_HEARTBEAT_SESSIONS;
use laser_wire::session::SessionHeartbeat;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::warn;

/// Ownership of one session's liveness. While any lease on a session is held,
/// the process lists the session in its heartbeat on `agent.heartbeats`. Drop
/// the lease, or call [`release`](Self::release), when the process stops
/// working on the session. A terminal verb on the session does not release
/// leases held elsewhere in the process.
pub struct SessionLease {
    registry: Arc<LeaseRegistry>,
    key: LeaseKey,
}

impl SessionLease {
    /// The leased session.
    pub fn session(&self) -> ConversationId {
        self.key.session
    }

    /// The stream the leased session lives in.
    pub fn stream(&self) -> &str {
        &self.key.stream
    }

    /// Release the lease now.
    pub fn release(self) {}
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        self.registry.release(&self.key);
    }
}

impl std::fmt::Debug for SessionLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionLease")
            .field("stream", &self.key.stream)
            .field("session", &self.key.session)
            .finish()
    }
}

// One lease identity: the stream by name and creation time, and the session.
// A recreated stream is a different lease scope.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LeaseKey {
    pub(crate) stream: String,
    pub(crate) stream_generation: u64,
    pub(crate) session: ConversationId,
}

struct LeaseEntry {
    holders: usize,
    agent: AgentId,
    interval: Duration,
}

#[derive(Default)]
struct RegistryState {
    leases: HashMap<LeaseKey, LeaseEntry>,
    running: bool,
}

/// The per-connection lease map read by the heartbeat task every tick.
pub(crate) struct LeaseRegistry {
    process: String,
    state: Mutex<RegistryState>,
}

impl Default for LeaseRegistry {
    fn default() -> Self {
        Self {
            process: ulid::Ulid::generate().to_string(),
            state: Mutex::new(RegistryState::default()),
        }
    }
}

impl LeaseRegistry {
    /// Take a lease on `key` for `agent`, starting the heartbeat task when it is
    /// not already running.
    pub(crate) fn acquire(
        self: &Arc<Self>,
        laser: &Laser,
        key: LeaseKey,
        agent: AgentId,
        interval: Duration,
    ) -> SessionLease {
        let start = {
            let mut state = self.state.lock().expect("lease registry lock");
            state
                .leases
                .entry(key.clone())
                .and_modify(|entry| {
                    entry.holders += 1;
                    entry.interval = entry.interval.min(interval);
                })
                .or_insert(LeaseEntry {
                    holders: 1,
                    agent,
                    interval,
                });
            let start = !state.running;
            state.running = true;
            start
        };
        if start {
            let registry = Arc::clone(self);
            let laser = laser.clone();
            tokio::spawn(async move { registry.heartbeat_loop(laser).await });
        }
        SessionLease {
            registry: Arc::clone(self),
            key,
        }
    }

    fn release(&self, key: &LeaseKey) {
        let mut state = self.state.lock().expect("lease registry lock");
        if let Some(entry) = state.leases.get_mut(key) {
            entry.holders -= 1;
            if entry.holders == 0 {
                state.leases.remove(key);
            }
        }
    }

    // One heartbeat per stream per tick, at the shortest interval among the
    // held leases. The task ends when the last lease is released, so a process
    // that holds no lease publishes nothing. A failed publish is retried on the
    // next tick and never fails a session.
    async fn heartbeat_loop(self: Arc<Self>, laser: Laser) {
        loop {
            let Some(interval) = self.interval() else {
                return;
            };
            tokio::time::sleep(interval).await;
            for (stream, generation, agent, sessions) in self.snapshot() {
                if let Err(error) = self
                    .beat(&laser, &stream, generation, agent, sessions)
                    .await
                {
                    warn!(stream = %stream, %error, "session heartbeat failed, retrying next tick");
                }
            }
        }
    }

    fn interval(&self) -> Option<Duration> {
        let mut state = self.state.lock().expect("lease registry lock");
        let interval = state.leases.values().map(|entry| entry.interval).min();
        if interval.is_none() {
            state.running = false;
        }
        interval
    }

    // The held sessions grouped by stream, so one stream's ids never ride
    // another stream's heartbeat.
    fn snapshot(&self) -> Vec<(String, u64, AgentId, Vec<ConversationId>)> {
        let state = self.state.lock().expect("lease registry lock");
        let mut by_stream: BTreeMap<(String, u64), (AgentId, Vec<ConversationId>)> =
            BTreeMap::new();
        for (key, entry) in &state.leases {
            by_stream
                .entry((key.stream.clone(), key.stream_generation))
                .or_insert_with(|| (entry.agent.clone(), Vec::new()))
                .1
                .push(key.session);
        }
        by_stream
            .into_iter()
            .map(|((stream, generation), (agent, mut sessions))| {
                sessions.sort_by_key(ToString::to_string);
                sessions.dedup();
                (stream, generation, agent, sessions)
            })
            .collect()
    }

    async fn beat(
        &self,
        laser: &Laser,
        stream: &str,
        generation: u64,
        agent: AgentId,
        sessions: Vec<ConversationId>,
    ) -> Result<(), LaserError> {
        let details = laser
            .client()
            .get_stream(&Identifier::named(stream)?)
            .await?;
        if details.is_none_or(|details| details.created_at.as_micros() != generation) {
            return Ok(());
        }
        let laser = laser.with_default_stream(stream);
        let conversation = ConversationId::derive(&format!("heartbeat/{}", self.process));
        let producer = laser
            .agdx(AgentTopic::Heartbeats, agent, conversation.into())
            .with_stream_generation(generation);
        for chunk in sessions.chunks(MAX_HEARTBEAT_SESSIONS) {
            let body = encode_named(&SessionHeartbeat {
                process: self.process.clone(),
                stream: stream.to_owned(),
                sessions: chunk.iter().copied().map(Into::into).collect(),
            })?;
            producer
                .status(OPERATION_PROGRESS)
                .body(body)
                .content_type(ContentType::Cbor)
                .send()
                .await?;
        }
        Ok(())
    }
}
