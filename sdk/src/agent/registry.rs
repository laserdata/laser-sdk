#[cfg(feature = "query")]
use crate::agent::clock::{Clock, SystemClock};
use crate::cursor::Cursor;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::{AgentId, MintUlid, PrincipalId};
use laser_wire::agent::{
    AgentCard, AgentEnvelope, AgentPresence, ConversationId, Health, OPERATION_CARD,
    OPERATION_QUARANTINE, OPERATION_UNQUARANTINE, RecordId,
};
use laser_wire::content::ContentType;
use laser_wire::framing::decode_named;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// How long a paged presence read is reused before the next refresh pages the
/// connection table again. Presence (an agent's live inbox) changes rarely, so a
/// short reuse window turns the per-send full-table page into an occasional one
/// without routing meaningfully stale inboxes.
#[cfg(feature = "query")]
const PRESENCE_TTL_MICROS: u64 = 2_000_000;

/// The registry read model's persisted state, shared per stream on the connection
/// so a fresh [`AgentRegistry`] resumes the fold instead of re-reading the whole
/// registry topic from offset 0 on every send. Folded cards, the quarantine set,
/// the cursor's resume offsets, and the last paged presence with its read time.
#[derive(Default)]
pub(crate) struct RegistryCache {
    cards: HashMap<AgentId, RegisteredCard>,
    quarantined: HashSet<AgentId>,
    offsets: Vec<u64>,
    presence: HashMap<AgentId, PresenceEntry>,
    presence_read_at_micros: Option<u64>,
    /// Record ids of privileged facts already folded, so a captured fact
    /// republished verbatim (same signed `record` id, new log offset) is dropped
    /// rather than re-applied. The replay-resistance backstop.
    applied_facts: HashSet<RecordId>,
}

/// A folded presence and the authenticated principal of the connection that
/// advertised it. The principal is the identity the substrate authenticated (the
/// trustworthy half). The agent id and inbox inside the presence are the
/// connection's own claim, so an inbox is only safe to trust as that principal's.
#[derive(Clone, Debug)]
pub struct PresenceEntry {
    pub presence: AgentPresence,
    pub principal: Option<PrincipalId>,
}

// The client-metadata discovery read rides the managed binary bridge
// (`send_raw_with_response`), which is gated on `query`, so its SDK surface is too.
#[cfg(feature = "query")]
use laser_wire::clients::{ClientMetadata, ClientMetadataList, ClientMetadataQuery};
#[cfg(feature = "query")]
use laser_wire::codes::{
    AGDX_GET_CLIENTS_METADATA_CODE, AGDX_SET_CLIENT_METADATA_CODE, CLIENT_METADATA_OP_VERSION,
};
#[cfg(feature = "query")]
use laser_wire::framing::encode_named;

/// The shared page cap, so one client-metadata page can never pull an unbounded
/// list. Mirrors the server clamp.
#[cfg(feature = "query")]
const MAX_PAGE_SIZE: usize = laser_wire::limits::MAX_PAGE_SIZE;

/// One agent's latest card, with the time the registry folded it in. A card
/// older than its [`AgentCard::ttl_micros`] is treated as a dead agent.
#[derive(Clone, Debug)]
pub struct RegisteredCard {
    pub agent: AgentId,
    pub card: AgentCard,
    /// When the registry observed this card (epoch micros). The registry's own
    /// fold time, so a live registry refreshed faster than the ttl keeps a
    /// healthy agent fresh.
    pub observed_at_micros: u64,
}

impl RegisteredCard {
    /// Whether the card is still fresh at `now_micros`: a card with no ttl never
    /// expires, otherwise it is fresh while `observed_at + ttl >= now`.
    pub fn is_fresh(&self, now_micros: u64) -> bool {
        match self.card.ttl_micros {
            None => true,
            Some(ttl) => self.observed_at_micros.saturating_add(ttl) >= now_micros,
        }
    }

    /// Whether the card advertises `skill_id`.
    pub fn serves(&self, skill_id: &str) -> bool {
        self.card
            .capabilities
            .iter()
            .any(|capability| capability.skill_id == skill_id)
    }

    /// Whether the agent's advertised health for `skill_id` permits routing: an
    /// `Unavailable` skill is skipped, everything else (healthy, degraded, or
    /// unspecified) stays routable, and a skill the card does not advertise is not
    /// available. The health-aware routing gate, so a self-declared unavailable
    /// agent is not handed work.
    pub fn available_for(&self, skill_id: &str) -> bool {
        self.card
            .capabilities
            .iter()
            .find(|capability| capability.skill_id == skill_id)
            .is_some_and(|capability| capability.health != Some(Health::Unavailable))
    }
}

/// The fused agent registry. Two distinct sources kept apart on purpose: the
/// durable, replayable **log card registry** (a fold over the
/// registry topic to the latest card per agent, capability that outlives a brief
/// disconnect), and the live **connection presence** (what an agent advertises
/// right now over `AGDX_SET_CLIENT_METADATA`, including the inbox topic routing
/// resolves to, gone on disconnect). Build it with [`Laser::agent_registry`],
/// [`refresh`](Self::refresh) to fold new cards, [`refresh_presence`](Self::refresh_presence)
/// to pull live presence, then resolve by capability and inbox.
pub struct AgentRegistry<'a> {
    // Only the `query`-gated presence read uses this. The card fold rides `cursor`.
    #[cfg_attr(not(feature = "query"), allow(dead_code))]
    laser: &'a Laser,
    cursor: Cursor,
    cards: HashMap<AgentId, RegisteredCard>,
    /// Live presence keyed by the agent id the connection claims. Seeded from the
    /// per-stream cache, refreshed by a `query`-gated read no more often than the
    /// presence ttl, so an [`InboxRoute::Advertised`](crate::agent::InboxRoute::Advertised)
    /// resolution without a prior refresh simply finds no inbox.
    presence: HashMap<AgentId, PresenceEntry>,
    /// Agents an operator has quarantined (folded from `quarantine` records on the
    /// registry topic). Excluded from capability resolution, so a quarantined agent
    /// is never routed work even while its card is fresh.
    quarantined: HashSet<AgentId>,
    /// Record ids of privileged facts already folded (replay-resistance backstop).
    applied_facts: HashSet<RecordId>,
    /// The shared per-stream state this registry seeded from and writes its fold
    /// back to, so the next registry resumes instead of re-reading from offset 0.
    cache: Arc<Mutex<RegistryCache>>,
    /// When the seeded presence was last paged (epoch micros), so a refresh within
    /// the ttl reuses it instead of paging the connection table again.
    #[cfg_attr(not(feature = "query"), allow(dead_code))]
    presence_read_at_micros: Option<u64>,
    /// The enrolled-key verifier privileged facts are checked against. When set, a
    /// quarantine or un-quarantine record is folded only if it carries a signature
    /// that verifies. Otherwise the registry topic's write access control is the
    /// sole gate.
    #[cfg(feature = "sign")]
    verifier: Option<Arc<crate::sign::KeyRegistry>>,
}

impl<'a> AgentRegistry<'a> {
    /// Fold every card published since the last refresh into the registry,
    /// stamping `now_micros` as the observation time, and write the result back to
    /// the shared per-stream cache (evicting cards stale at `now_micros`, so the
    /// cached set stays bounded by live agents). Returns the number of records
    /// folded. A record that is not a `status`/`card`/`quarantine` envelope, or
    /// whose body does not decode, is skipped (the registry topic is single-purpose,
    /// but a stray record must not poison the fold).
    pub async fn refresh(&mut self, now_micros: u64) -> Result<usize, LaserError> {
        let messages = self.cursor.poll().await?;
        let mut folded = 0;
        for message in &messages {
            let Ok(envelope) = decode_named::<AgentEnvelope>(&message.payload) else {
                continue;
            };
            let operation = envelope.operation.as_deref();
            let applied = if operation == Some(OPERATION_QUARANTINE)
                || operation == Some(OPERATION_UNQUARANTINE)
            {
                // A quarantine fact evicts an agent from routing, so it is the one
                // privileged write here. Three gates, in order. A fact whose signed
                // `record` id was already folded is a verbatim replay and dropped
                // (G1). An enrolled verifier must confirm an OPERATOR key valid at
                // fold time (G2, so an enrolled agent cannot quarantine another).
                // Without a verifier the registry topic's write access control is
                // the gate. An authorized fact's id is remembered so its replay is
                // caught even when it did not change state.
                if envelope
                    .record
                    .is_some_and(|id| self.applied_facts.contains(&id))
                    || !self.fact_authorized(&envelope, now_micros)
                {
                    false
                } else {
                    if let Some(id) = envelope.record {
                        self.applied_facts.insert(id);
                    }
                    if operation == Some(OPERATION_QUARANTINE) {
                        apply_quarantine(&mut self.quarantined, &envelope)
                    } else {
                        lift_quarantine(&mut self.quarantined, &envelope)
                    }
                }
            } else {
                apply_card(&mut self.cards, &envelope, now_micros)
            };
            if applied {
                folded += 1;
            }
        }
        // A card past its ttl is a dead agent. Drop it so the cached set is bounded
        // by live agents, not every agent ever seen. A ttl-less card never expires.
        self.cards.retain(|_, card| card.is_fresh(now_micros));
        if let Ok(mut cache) = self.cache.lock() {
            cache.cards = self.cards.clone();
            cache.quarantined = self.quarantined.clone();
            cache.applied_facts = self.applied_facts.clone();
            cache.offsets = self.cursor.offsets().to_vec();
        }
        Ok(folded)
    }

    /// Whether a privileged registry fact (quarantine, un-quarantine) is
    /// authorized to fold. With an enrolled verifier it must carry a signature
    /// that verifies against an enrolled OPERATOR key valid at `now_micros`, so an
    /// enrolled agent key cannot quarantine another agent. Without a verifier the
    /// registry topic's write access control is trusted as the gate.
    #[cfg(feature = "sign")]
    fn fact_authorized(&self, envelope: &AgentEnvelope, now_micros: u64) -> bool {
        match &self.verifier {
            Some(registry) => registry
                .verify_at(envelope, now_micros)
                .is_ok_and(|verified| verified.kind == crate::sign::KeyKind::Operator),
            None => true,
        }
    }

    #[cfg(not(feature = "sign"))]
    fn fact_authorized(&self, _envelope: &AgentEnvelope, _now_micros: u64) -> bool {
        true
    }

    /// Every agent with a folded card, newest observation kept.
    pub fn agents(&self) -> impl Iterator<Item = &RegisteredCard> {
        self.cards.values()
    }

    /// The latest card for `agent`, if any.
    pub fn lookup(&self, agent: &AgentId) -> Option<&RegisteredCard> {
        self.cards.get(agent)
    }

    /// Every agent that advertises `skill_id`, is still fresh at `now_micros`, and
    /// is not advertising itself `Unavailable` for it. The capability-resolution
    /// primitive the router builds on, so a stale or self-declared-unavailable
    /// agent is never routed to.
    pub fn resolve(&self, skill_id: &str, now_micros: u64) -> Vec<&RegisteredCard> {
        self.cards
            .values()
            .filter(|card| {
                card.available_for(skill_id)
                    && card.is_fresh(now_micros)
                    && !self.quarantined.contains(&card.agent)
            })
            .collect()
    }

    /// Whether `agent` has been quarantined by an operator.
    pub fn is_quarantined(&self, agent: &AgentId) -> bool {
        self.quarantined.contains(agent)
    }

    /// The inbox topic `agent` advertised in its live presence, or `None` when it
    /// advertised none (or [`refresh_presence`](Self::refresh_presence) has not
    /// run). The destination an [`InboxRoute::Advertised`](crate::agent::InboxRoute::Advertised)
    /// route resolves to, so routing never assumes a shared topic name.
    pub fn inbox_for(&self, agent: &AgentId) -> Option<&str> {
        self.presence
            .get(agent)
            .and_then(|entry| entry.presence.inbox.as_deref())
    }

    /// The inbox `agent` advertised, but only if the connection that advertised it
    /// authenticated as `user_id`. The principal-bound resolve a deployment with an
    /// enrolled agent-to-principal mapping uses, so a connection cannot route work
    /// to itself by claiming another principal's agent id. [`inbox_for`](Self::inbox_for)
    /// is the claim-only resolve for deployments without such a mapping.
    pub fn inbox_for_principal(&self, agent: &AgentId, principal: PrincipalId) -> Option<&str> {
        self.presence.get(agent).and_then(|entry| {
            (entry.principal == Some(principal))
                .then_some(entry.presence.inbox.as_deref())
                .flatten()
        })
    }

    /// The authenticated principal of the live connection claiming `agent`.
    /// `None` means no live presence or a substrate that supplied no principal.
    pub fn principal_for(&self, agent: &AgentId) -> Option<PrincipalId> {
        self.presence.get(agent).and_then(|entry| entry.principal)
    }

    /// Pull live connection presence and fold it in, keyed by the agent id each
    /// connection claims, so [`inbox_for`](Self::inbox_for) can resolve a target
    /// to where it is consuming right now. Reads every page of the client-metadata
    /// discovery command and decodes each connection's metadata as an
    /// [`AgentPresence`], skipping connections whose metadata is absent or is not a
    /// presence body (the channel is opaque, a non-agent client advertises any
    /// blob). A later connection claiming the same agent id wins, matching
    /// last-write presence. Returns the number of presences folded.
    ///
    /// Served by the LaserData fork, so it returns a transport error against a
    /// stock Iggy server that lacks the command.
    #[cfg(feature = "query")]
    pub async fn refresh_presence(&mut self) -> Result<usize, LaserError> {
        let now = SystemClock.now_micros();
        // Reuse the seeded presence while it is within the ttl: paging the whole
        // connection table on every send is the dominant per-send cost, and an
        // agent's inbox changes rarely.
        if let Some(read_at) = self.presence_read_at_micros
            && now.saturating_sub(read_at) < PRESENCE_TTL_MICROS
        {
            return Ok(self.presence.len());
        }
        let connections = self
            .laser
            .client_metadata()
            .with_metadata_only(true)
            .all()
            .await?;
        let mut fresh = HashMap::new();
        for connection in connections {
            if let Some(payload) = connection.metadata.as_deref() {
                // Bind the presence to the connection's authenticated `user_id`
                // (not the self-claimed agent string alone), so a connection
                // cannot advertise an inbox under another principal's agent id.
                apply_presence(&mut fresh, payload, connection.user_id);
            }
        }
        let folded = fresh.len();
        self.presence = fresh;
        self.presence_read_at_micros = Some(now);
        if let Ok(mut cache) = self.cache.lock() {
            cache.presence = self.presence.clone();
            cache.presence_read_at_micros = Some(now);
        }
        Ok(folded)
    }
}

/// Apply one connection's advertised metadata to the presence map: when it
/// decodes as an [`AgentPresence`] claiming a valid agent id, store it keyed by
/// that agent (last write wins). Returns whether a presence was applied. A blob
/// that is not a presence body is skipped (the metadata channel is opaque). Pure,
/// so the decode-and-key fold is unit-testable without a live connection.
#[cfg(feature = "query")]
fn apply_presence(
    presence: &mut HashMap<AgentId, PresenceEntry>,
    metadata: &[u8],
    user_id: Option<u32>,
) -> bool {
    let Ok(body) = decode_named::<AgentPresence>(metadata) else {
        return false;
    };
    let Ok(agent) = AgentId::try_from(body.agent.as_str()) else {
        return false;
    };
    presence.insert(
        agent,
        PresenceEntry {
            presence: body,
            principal: user_id.map(PrincipalId::new),
        },
    );
    true
}

impl Laser {
    /// Open a read model over the agent card registry topic, seeded from the
    /// per-stream cache so it resumes the fold instead of re-reading the topic from
    /// offset 0. Each [`refresh`](AgentRegistry::refresh) folds only what is new and
    /// writes the result back to the cache.
    pub fn agent_registry(&self) -> Result<AgentRegistry<'_>, LaserError> {
        let cache = self.registry_cache()?;
        let (cards, quarantined, applied_facts, presence, offsets, presence_read_at_micros) = {
            let guard = cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (
                guard.cards.clone(),
                guard.quarantined.clone(),
                guard.applied_facts.clone(),
                guard.presence.clone(),
                guard.offsets.clone(),
                guard.presence_read_at_micros,
            )
        };
        let cursor = self
            .reader(AgentTopic::Registry.topic_string())?
            .from_offsets(offsets);
        Ok(AgentRegistry {
            laser: self,
            cursor,
            cards,
            presence,
            quarantined,
            applied_facts,
            cache,
            presence_read_at_micros,
            #[cfg(feature = "sign")]
            verifier: self.registry_verifier(),
        })
    }

    /// Publish `source`'s capability card to the registry topic, the write side of
    /// the log card registry: a `status` record with operation `card` carrying the
    /// encoded [`AgentCard`]. A registry read model folds the latest card per
    /// agent, so capability routing can resolve `source`. Re-publish on an interval
    /// shorter than the card's `ttl_micros` to keep the card fresh, since a card
    /// older than its ttl reads as a dead agent.
    pub async fn publish_card(&self, source: AgentId, card: &AgentCard) -> Result<(), LaserError> {
        let body = laser_wire::framing::encode_named(card)
            .map_err(|error| LaserError::Codec(format!("encode card: {error}")))?;
        self.agdx(
            AgentTopic::Registry,
            source.wire_id(),
            ConversationId::mint(),
        )
        .status(OPERATION_CARD)
        .body(body)
        .content_type(ContentType::Cbor)
        .send()
        .await?;
        Ok(())
    }

    /// Quarantine `agent`: append a `quarantine` fact to the registry topic as
    /// `operator`, so every fused registry folds it and excludes the agent from
    /// capability resolution. Authorized by the registry topic's write access
    /// control (only an operator may append here, the baseline gate). Where a
    /// deployment enrolls an operator-key verifier
    /// ([`LaserBuilder::verifier`](crate::laser::LaserBuilder::verifier)), use
    /// [`quarantine_signed`](Self::quarantine_signed) so the fact also carries a
    /// signature, since a verifying registry drops an unsigned one.
    pub async fn quarantine(&self, operator: AgentId, agent: &AgentId) -> Result<(), LaserError> {
        self.publish_registry_fact(OPERATION_QUARANTINE, operator, agent)
            .await
    }

    /// Lift a prior quarantine on `agent` (an `unquarantine` fact), returning it to
    /// routing. The counterpart to [`quarantine`](Self::quarantine), so a
    /// quarantine is not a one-way door only retention expiry can undo. Same
    /// authorization.
    pub async fn unquarantine(&self, operator: AgentId, agent: &AgentId) -> Result<(), LaserError> {
        self.publish_registry_fact(OPERATION_UNQUARANTINE, operator, agent)
            .await
    }

    async fn publish_registry_fact(
        &self,
        operation: &'static str,
        operator: AgentId,
        agent: &AgentId,
    ) -> Result<(), LaserError> {
        self.agdx(
            AgentTopic::Registry,
            operator.wire_id(),
            ConversationId::mint(),
        )
        .status(operation)
        .body(agent.as_str().as_bytes().to_vec())
        .send()
        .await?;
        Ok(())
    }

    /// Quarantine `agent` with a signed fact, signed by the operator's `key`, so a
    /// registry that enrolls the matching verifying key folds it (and rejects an
    /// unsigned one). The signed counterpart to [`quarantine`](Self::quarantine).
    #[cfg(feature = "sign")]
    pub async fn quarantine_signed(
        &self,
        operator: AgentId,
        agent: &AgentId,
        key: &crate::sign::SigningKey,
    ) -> Result<(), LaserError> {
        self.publish_registry_fact_signed(OPERATION_QUARANTINE, operator, agent, key)
            .await
    }

    /// Lift a quarantine with a signed fact. The signed counterpart to
    /// [`unquarantine`](Self::unquarantine).
    #[cfg(feature = "sign")]
    pub async fn unquarantine_signed(
        &self,
        operator: AgentId,
        agent: &AgentId,
        key: &crate::sign::SigningKey,
    ) -> Result<(), LaserError> {
        self.publish_registry_fact_signed(OPERATION_UNQUARANTINE, operator, agent, key)
            .await
    }

    #[cfg(feature = "sign")]
    async fn publish_registry_fact_signed(
        &self,
        operation: &'static str,
        operator: AgentId,
        agent: &AgentId,
        key: &crate::sign::SigningKey,
    ) -> Result<(), LaserError> {
        self.agdx(
            AgentTopic::Registry,
            operator.wire_id(),
            ConversationId::mint(),
        )
        .status(operation)
        .body(agent.as_str().as_bytes().to_vec())
        .signed_by(key)
        .send()
        .await?;
        Ok(())
    }

    /// Advertise this connection's live presence (`AGDX_SET_CLIENT_METADATA`): the
    /// agent id this connection is and the inbox topic it currently consumes, so a
    /// fused registry can route work to it without any shared topic-name
    /// convention. Mutable mid-session (re-advertise to move the inbox to a
    /// per-workflow topic) and garbage-collected for free on disconnect. Use
    /// [`clear_presence`](Self::clear_presence) to withdraw it.
    ///
    /// Served by the LaserData fork, so it returns a transport error against a
    /// stock Iggy server that lacks the command.
    #[cfg(feature = "query")]
    pub async fn advertise_presence(&self, presence: &AgentPresence) -> Result<(), LaserError> {
        let requested = AgentId::try_from(presence.agent.as_str())?;
        self.claim_presence(requested)?;
        let payload = encode_named(presence)
            .map_err(|error| LaserError::Codec(format!("encode presence: {error}")))?;
        self.send_raw_with_response(AGDX_SET_CLIENT_METADATA_CODE, payload)
            .await?;
        Ok(())
    }

    /// Withdraw this connection's advertised presence (an empty
    /// `AGDX_SET_CLIENT_METADATA` clears it). Disconnecting clears it too, this is
    /// the explicit early withdrawal.
    #[cfg(feature = "query")]
    pub async fn clear_presence(&self) -> Result<(), LaserError> {
        self.send_raw_with_response(AGDX_SET_CLIENT_METADATA_CODE, Vec::new())
            .await?;
        self.release_presence();
        Ok(())
    }

    /// Start a filtered, paginated discovery read of live connections and their
    /// advertised metadata (`AGDX_GET_CLIENTS_METADATA`). Distinct from
    /// [`agent_registry`](Self::agent_registry): this is live connections from the
    /// server's table, not folded cards from a topic. Refine with the builder,
    /// then `.page().await` for one page or `.all().await` to walk every page.
    /// Served by the LaserData fork, so it returns a transport error against a
    /// stock Iggy server that lacks the command.
    #[cfg(feature = "query")]
    pub fn client_metadata(&self) -> ClientMetadataRequest<'_> {
        ClientMetadataRequest {
            laser: self,
            query: ClientMetadataQuery {
                v: CLIENT_METADATA_OP_VERSION,
                with_metadata_only: false,
                user_id: None,
                after_client_id: None,
                limit: MAX_PAGE_SIZE as u32,
            },
        }
    }
}

/// One page of connections plus the cursor to fetch the next, returned by
/// [`ClientMetadataRequest::page`].
#[cfg(feature = "query")]
pub struct ClientMetadataPage {
    pub clients: Vec<ClientMetadata>,
    /// The `after_client_id` for the next page, or `None` on the last page.
    pub next_cursor: Option<u32>,
}

/// Fluent builder for the client-metadata discovery read.
#[cfg(feature = "query")]
pub struct ClientMetadataRequest<'a> {
    laser: &'a Laser,
    query: ClientMetadataQuery,
}

#[cfg(feature = "query")]
impl ClientMetadataRequest<'_> {
    /// Only return connections that advertised metadata.
    pub fn with_metadata_only(mut self, value: bool) -> Self {
        self.query.with_metadata_only = value;
        self
    }

    /// Only return connections authenticated as `principal`.
    pub fn principal(mut self, principal: PrincipalId) -> Self {
        self.query.user_id = Some(principal.get());
        self
    }

    /// Cap the page size (clamped server-side to the page cap).
    pub fn limit(mut self, limit: u32) -> Self {
        self.query.limit = limit;
        self
    }

    /// Start the page after this `client_id` (the previous page's `next_cursor`).
    pub fn after(mut self, client_id: u32) -> Self {
        self.query.after_client_id = Some(client_id);
        self
    }

    /// Fetch one page.
    pub async fn page(&self) -> Result<ClientMetadataPage, LaserError> {
        let payload = encode_named(&self.query)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        let payload = self
            .laser
            .send_raw_with_response(AGDX_GET_CLIENTS_METADATA_CODE, payload)
            .await?;
        let list: ClientMetadataList = decode_named(&payload)?;
        Ok(ClientMetadataPage {
            clients: list.clients,
            next_cursor: list.next_cursor,
        })
    }

    /// Walk every page, following the cursor, and collect all matching
    /// connections. Use [`page`](Self::page) instead when the set may be large.
    pub async fn all(self) -> Result<Vec<ClientMetadata>, LaserError> {
        let mut all = Vec::new();
        let mut cursor = self.query.after_client_id;
        loop {
            let mut request = ClientMetadataRequest {
                laser: self.laser,
                query: self.query.clone(),
            };
            request.query.after_client_id = cursor;
            let page = request.page().await?;
            all.extend(page.clients);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(all)
    }
}

/// Apply one envelope to the card map: when it is a `status`/`card` record from a
/// resolvable agent carrying a decodable [`AgentCard`] body, store it as that
/// agent's latest card. Returns whether a card was applied. Pure (the clock is
/// passed in), so the fold is unit-testable without a live log.
fn apply_card(
    cards: &mut HashMap<AgentId, RegisteredCard>,
    envelope: &AgentEnvelope,
    observed_at_micros: u64,
) -> bool {
    if envelope.operation.as_deref() != Some(OPERATION_CARD) {
        return false;
    }
    let Ok(agent) = AgentId::try_from(envelope.source.as_str()) else {
        return false;
    };
    let Ok(card) = decode_named::<AgentCard>(&envelope.body) else {
        return false;
    };
    cards.insert(
        agent.clone(),
        RegisteredCard {
            agent,
            card,
            observed_at_micros,
        },
    );
    true
}

/// Apply one envelope to the quarantine set: a `status`/`quarantine` record whose
/// body is a valid agent id marks that agent quarantined. Returns whether one was
/// applied. Pure, so the fold is unit-testable without a live log.
fn apply_quarantine(quarantined: &mut HashSet<AgentId>, envelope: &AgentEnvelope) -> bool {
    let Ok(body) = std::str::from_utf8(&envelope.body) else {
        return false;
    };
    let Ok(agent) = AgentId::try_from(body) else {
        return false;
    };
    quarantined.insert(agent)
}

/// Apply one envelope to the quarantine set as a lift: an `unquarantine` record
/// whose body is a valid agent id returns that agent to routing. Returns whether a
/// quarantine was actually lifted. Pure, so the fold is unit-testable without a
/// live log.
fn lift_quarantine(quarantined: &mut HashSet<AgentId>, envelope: &AgentEnvelope) -> bool {
    let Ok(body) = std::str::from_utf8(&envelope.body) else {
        return false;
    };
    let Ok(agent) = AgentId::try_from(body) else {
        return false;
    };
    quarantined.remove(&agent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::agent::{CapabilityDescriptor, ConversationId, Health, RecordId};
    use laser_wire::framing::encode_named;

    fn descriptor(skill_id: &str) -> CapabilityDescriptor {
        CapabilityDescriptor {
            skill_id: skill_id.to_owned(),
            input: None,
            output: None,
            cost_class: None,
            latency_class: None,
            max_concurrency: None,
            health: Some(Health::Healthy),
            load: None,
        }
    }

    fn card_envelope(source: &str, skills: &[&str], ttl_micros: Option<u64>) -> AgentEnvelope {
        let card = AgentCard {
            name: None,
            version: None,
            capabilities: skills.iter().map(|skill| descriptor(skill)).collect(),
            ttl_micros,
        };
        let mut envelope = AgentEnvelope::status(
            RecordId::from_u128(1),
            ConversationId::from_u128(2),
            source.parse().expect("valid agent id"),
            OPERATION_CARD,
        );
        envelope.body = encode_named(&card).expect("card encodes");
        envelope
    }

    #[test]
    fn given_cards_when_folded_then_should_keep_the_latest_per_agent_and_resolve_by_capability() {
        let mut cards = HashMap::new();
        let planner: AgentId = "planner".parse().unwrap();

        // Two agents publish cards.
        assert!(apply_card(
            &mut cards,
            &card_envelope("planner", &["plan"], None),
            100
        ));
        assert!(apply_card(
            &mut cards,
            &card_envelope("worker", &["diagnose"], Some(50)),
            100
        ));
        assert_eq!(cards.len(), 2);

        // A re-publish replaces the agent's card (latest wins).
        assert!(apply_card(
            &mut cards,
            &card_envelope("planner", &["plan", "summarize"], None),
            200
        ));
        assert_eq!(cards.len(), 2);
        assert_eq!(cards[&planner].observed_at_micros, 200);
        assert!(cards[&planner].serves("summarize"));

        // A non-card envelope is skipped.
        let not_a_card = AgentEnvelope::event(
            RecordId::from_u128(9),
            ConversationId::from_u128(9),
            "x".parse().unwrap(),
            b"{}".to_vec(),
        );
        assert!(!apply_card(&mut cards, &not_a_card, 300));

        // The worker's card (ttl 50, observed at 100) is stale past 150.
        let worker = cards
            .values()
            .find(|c| c.agent.as_str() == "worker")
            .unwrap();
        assert!(worker.is_fresh(150));
        assert!(!worker.is_fresh(151));
    }

    #[test]
    fn given_a_quarantine_record_when_folded_then_should_mark_the_named_agent() {
        let mut quarantined = HashSet::new();
        let worker: AgentId = "worker".parse().unwrap();

        let mut envelope = AgentEnvelope::status(
            RecordId::from_u128(1),
            ConversationId::from_u128(2),
            "operator".parse().expect("valid agent id"),
            OPERATION_QUARANTINE,
        );
        envelope.body = b"worker".to_vec();
        assert!(apply_quarantine(&mut quarantined, &envelope));
        assert!(quarantined.contains(&worker));

        // A second identical record is a no-op (already quarantined).
        assert!(!apply_quarantine(&mut quarantined, &envelope));

        // A body that is not a valid agent id is skipped, not poisoned in.
        let mut bad = envelope.clone();
        bad.body = vec![0xff, 0xfe];
        assert!(!apply_quarantine(&mut quarantined, &bad));

        // An un-quarantine record lifts it, returning the agent to routing.
        let mut lift = AgentEnvelope::status(
            RecordId::from_u128(3),
            ConversationId::from_u128(4),
            "operator".parse().expect("valid agent id"),
            OPERATION_UNQUARANTINE,
        );
        lift.body = b"worker".to_vec();
        assert!(lift_quarantine(&mut quarantined, &lift));
        assert!(!quarantined.contains(&worker));
        // Lifting an agent that is not quarantined is a no-op.
        assert!(!lift_quarantine(&mut quarantined, &lift));
    }

    #[test]
    fn given_advertised_health_when_checking_availability_then_should_skip_only_unavailable() {
        let with_health = |health| RegisteredCard {
            agent: "a".parse().expect("valid agent id"),
            card: AgentCard {
                name: None,
                version: None,
                capabilities: vec![CapabilityDescriptor {
                    health,
                    ..descriptor("diagnose")
                }],
                ttl_micros: None,
            },
            observed_at_micros: 0,
        };
        assert!(with_health(Some(Health::Healthy)).available_for("diagnose"));
        assert!(with_health(Some(Health::Degraded)).available_for("diagnose"));
        assert!(with_health(None).available_for("diagnose"));
        assert!(
            !with_health(Some(Health::Unavailable)).available_for("diagnose"),
            "an agent declaring itself unavailable must not be routed to",
        );
        // A skill the card never advertises is not available.
        assert!(!with_health(Some(Health::Healthy)).available_for("other"));
    }

    #[cfg(feature = "query")]
    #[test]
    fn given_connection_metadata_when_folded_as_presence_then_should_key_by_agent_and_skip_non_presence()
     {
        use laser_wire::agent::AgentPresence;

        let mut presence = HashMap::new();
        let worker: AgentId = "worker".parse().unwrap();

        // A presence body declaring an inbox is folded, keyed by its agent id,
        // carrying the connection's authenticated user_id.
        let body =
            encode_named(&AgentPresence::new("worker".parse().unwrap()).with_inbox("worker.work"))
                .expect("presence encodes");
        assert!(apply_presence(&mut presence, &body, Some(7)));
        assert_eq!(
            presence[&worker].presence.inbox.as_deref(),
            Some("worker.work")
        );
        assert_eq!(presence[&worker].principal, Some(PrincipalId::new(7)));

        // Re-advertising moves the inbox (last write wins), the per-workflow case.
        let moved = encode_named(
            &AgentPresence::new("worker".parse().unwrap()).with_inbox("worker.work.v2"),
        )
        .expect("presence encodes");
        assert!(apply_presence(&mut presence, &moved, Some(7)));
        assert_eq!(
            presence[&worker].presence.inbox.as_deref(),
            Some("worker.work.v2")
        );

        // An opaque non-presence blob (a regular app's metadata) is skipped, not
        // poisoned into the map.
        assert!(!apply_presence(&mut presence, b"not a presence body", None));
        assert_eq!(presence.len(), 1);
    }
}
