use crate::error::{driver_wrote, is_transient_iggy_io_error};
use crate::iggy::prelude::{ClusterClient, Identifier, IggyClient, IggyError};
use crate::retry::{MANAGED_DEADLINE, MANAGED_RETRY};
use bytes::Bytes;
use iggy_binary_protocol::codes::GET_PARTITION_ROUTE_CODE;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_common::with_binary_deadline;
use laser_wire::codes::{
    AGDX_AUTHZ_BIND_ROLES_CODE, AGDX_AUTHZ_DEFINE_ROLE_CODE, AGDX_AUTHZ_DELETE_ROLE_CODE,
    AGDX_FILTER_OPERATION_CODE, AGDX_GET_FILTER_BINDING_CODE, AGDX_GET_FILTER_CODE,
    AGDX_LIST_FILTER_BINDINGS_CODE, AGDX_LIST_FILTER_REVISIONS_CODE, AGDX_LIST_FILTERS_CODE,
};
use laser_wire::filter::{FilterError, FilterErrorReason};
use laser_wire::framing::decode_named;
use laser_wire::mutation::{AppliedFrontier, ManagedReplyEnvelope};
use laser_wire::result::CommandError;
use serde::Deserialize;
use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// The node a managed request goes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Target {
    /// The metadata primary, the only node that submits the replicated
    /// authorization writes.
    MetadataPrimary,
    /// The node this client spreads its requests to.
    Caller,
    /// One replica, by the id cluster metadata names it with.
    Replica(u8),
}

impl Target {
    /// Where `code` goes when nothing more specific names its node.
    pub(crate) const fn of_code(code: u32) -> Self {
        match code {
            AGDX_AUTHZ_DEFINE_ROLE_CODE
            | AGDX_AUTHZ_DELETE_ROLE_CODE
            | AGDX_AUTHZ_BIND_ROLES_CODE => Self::MetadataPrimary,
            _ => Self::Caller,
        }
    }
}

/// How a request to the caller's node answers a roster whose every node
/// refused it for want of its managed backend or with a lost link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Walk {
    /// Walk the roster once and return the last refusal, for a caller that
    /// answers a refusal itself.
    Once,
    /// Wait on the managed backoff and walk the roster again until the
    /// request is served or [`MANAGED_DEADLINE`] passes, then return the last
    /// refusal.
    UntilServed,
    /// [`UntilServed`](Self::UntilServed) with this deadline, the call's own.
    #[cfg_attr(not(feature = "query"), allow(dead_code))]
    Until(Instant),
}

/// Picks the node of every managed request. Every node answers every managed
/// request from shared state, so a client keeps to one node chosen by a key
/// of its own, which spreads clients over the cluster, and moves to the next
/// node after a failed link.
pub(crate) struct Nodes {
    caller_key: u64,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    // Ascending replica ids, empty until the first request reads them.
    roster: Vec<u8>,
    // Failed links so far, each one moves the caller to the next node.
    moved: u64,
}

/// The reply of a managed request and the replica that sent it, `None` when
/// the metadata primary answered.
pub(crate) struct Answered {
    /// The reply, opened from the positioned envelope a managed node may wrap
    /// it in.
    pub(crate) reply: Bytes,
    /// The applied frontier the positioned envelope carried.
    pub(crate) frontier: Option<AppliedFrontier>,
    pub(crate) replica: Option<u8>,
}

impl Answered {
    /// Open a positioned reply once, keeping its frontier beside the reply.
    pub(crate) fn open(reply: Bytes, replica: Option<u8>) -> Self {
        match decode_named::<ManagedReplyEnvelope>(&reply) {
            Ok(envelope) => Self {
                reply: Bytes::from(envelope.reply),
                frontier: Some(envelope.frontier),
                replica,
            },
            Err(_) => Self {
                reply,
                frontier: None,
                replica,
            },
        }
    }
}

impl Nodes {
    pub(crate) fn new(caller_key: u64) -> Self {
        Self {
            caller_key,
            state: Mutex::new(State::default()),
        }
    }

    /// Send `code` to `target`. A request to the caller's node moves on to the
    /// next node after a failed link, at most once per node, reads the roster
    /// again once when a replica id is no longer in it, and returns the last
    /// refusal when every node refused it. A request to a named replica is
    /// sent there only.
    pub(crate) async fn send(
        &self,
        client: &IggyClient,
        code: u32,
        payload: Bytes,
        target: Target,
    ) -> Result<Answered, IggyError> {
        self.send_until(client, code, payload, target, None, Walk::Once)
            .await
    }

    /// [`send`](Self::send) for a managed read, which walks the roster again
    /// as `walk` allows while every node refuses it.
    pub(crate) async fn read(
        &self,
        client: &IggyClient,
        code: u32,
        payload: Bytes,
        target: Target,
        walk: Walk,
    ) -> Result<Answered, IggyError> {
        self.send_until(client, code, payload, target, None, walk)
            .await
    }

    /// [`send`](Self::send) for a request the server may hold, bounded by
    /// `deadline` instead of the client's default operation timeout.
    pub(crate) async fn send_held(
        &self,
        client: &IggyClient,
        code: u32,
        payload: Bytes,
        target: Target,
        deadline: Instant,
    ) -> Result<Answered, IggyError> {
        with_binary_deadline(
            deadline.into(),
            self.send_until(client, code, payload, target, Some(deadline), Walk::Once),
        )
        .await
    }

    async fn send_until(
        &self,
        client: &IggyClient,
        code: u32,
        payload: Bytes,
        target: Target,
        deadline: Option<Instant>,
        walk: Walk,
    ) -> Result<Answered, IggyError> {
        match target {
            Target::MetadataPrimary => Ok(Answered::open(
                match deadline {
                    Some(deadline) => {
                        client
                            .send_binary_request_with_deadline(code, payload, deadline)
                            .await?
                    }
                    None => client.send_binary_request(code, payload).await?,
                },
                None,
            )),
            Target::Replica(replica) => Ok(Answered::open(
                send_to_replica(client, code, payload, replica, deadline).await?,
                Some(replica),
            )),
            Target::Caller => {
                self.walk_caller_until(client, code, deadline, walk, |replica| {
                    send_to_replica(client, code, payload.clone(), replica, deadline)
                })
                .await
            }
        }
    }

    /// A request to the caller's node that marks `written` once an attempt
    /// wrote its frame, so a later attempt's failure cannot hide that the
    /// outcome is unknown.
    #[cfg(feature = "kv")]
    pub(crate) async fn send_to_caller_marking(
        &self,
        client: &IggyClient,
        code: u32,
        payload: Bytes,
        written: &AtomicBool,
    ) -> Result<Answered, IggyError> {
        self.walk_caller(client, code, None, written, |replica| {
            send_to_replica(client, code, payload.clone(), replica, None)
        })
        .await
    }

    // Walk the roster as `walk` allows, each walk after the first behind the
    // next jittered step of the managed backoff. A wait that would end past
    // the deadline returns the last refusal instead.
    async fn walk_caller_until<Attempt, Sent>(
        &self,
        client: &IggyClient,
        code: u32,
        deadline: Option<Instant>,
        walk: Walk,
        mut attempt: Attempt,
    ) -> Result<Answered, IggyError>
    where
        Attempt: FnMut(u8) -> Sent,
        Sent: Future<Output = Result<Bytes, IggyError>>,
    {
        let until = match walk {
            Walk::Once => None,
            Walk::UntilServed => Some(Instant::now() + MANAGED_DEADLINE),
            Walk::Until(until) => Some(until),
        };
        let mut walks = 0;
        loop {
            let walked = self
                .walk_caller(
                    client,
                    code,
                    deadline,
                    &AtomicBool::new(false),
                    &mut attempt,
                )
                .await;
            walks += 1;
            let wait = MANAGED_RETRY.delay(walks);
            if !walks_again(code, &walked, until, wait, Instant::now()) {
                return walked;
            }
            sleep(wait).await;
            if until.is_some_and(|until| Instant::now() >= until) {
                return walked;
            }
        }
    }

    async fn walk_caller<Attempt, Sent>(
        &self,
        client: &IggyClient,
        code: u32,
        deadline: Option<Instant>,
        written: &AtomicBool,
        mut attempt: Attempt,
    ) -> Result<Answered, IggyError>
    where
        Attempt: FnMut(u8) -> Sent,
        Sent: Future<Output = Result<Bytes, IggyError>>,
    {
        let mut roster_read = false;
        let mut failed_links = 0;
        loop {
            let (replica, nodes) = self.caller_node(client).await?;
            let sent = attempt(replica).await;
            if sent.as_ref().map_or_else(driver_wrote, |_| true) {
                written.store(true, Ordering::Release);
            }
            match sent {
                Ok(reply) => {
                    let answered = Answered::open(reply, Some(replica));
                    if backend_refused(code, &answered.reply) {
                        self.failed(replica);
                        failed_links += 1;
                        if walks_on(failed_links, nodes, deadline, Instant::now()) {
                            continue;
                        }
                    }
                    return Ok(answered);
                }
                Err(IggyError::InvalidIdentifier) if !roster_read => {
                    roster_read = true;
                    self.forget_roster();
                }
                Err(error) if is_transient_iggy_io_error(&error) => {
                    self.failed(replica);
                    failed_links += 1;
                    if !walks_on(failed_links, nodes, deadline, Instant::now()) {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// The caller's node and the roster size, reading the roster when none is
    /// known.
    pub(crate) async fn caller_node(&self, client: &IggyClient) -> Result<(u8, usize), IggyError> {
        if let Some(picked) = self.pick() {
            return Ok(picked);
        }
        let metadata = client.get_cluster_metadata().await?;
        let mut roster: Vec<u8> = metadata.nodes.iter().map(|node| node.replica_id).collect();
        roster.sort_unstable();
        roster.dedup();
        self.lock().roster = roster;
        self.pick().ok_or(IggyError::InvalidIdentifier)
    }

    /// Record a failed link to `replica`. The caller moves on only when
    /// `replica` is still its node, so concurrent failures move it once.
    pub(crate) fn failed(&self, replica: u8) {
        let mut state = self.lock();
        if pick(&state.roster, self.caller_key, state.moved) == Some(replica) {
            state.moved = state.moved.wrapping_add(1);
        }
    }

    pub(crate) fn forget_roster(&self) {
        self.lock().roster.clear();
    }

    fn pick(&self) -> Option<(u8, usize)> {
        let state = self.lock();
        pick(&state.roster, self.caller_key, state.moved)
            .map(|replica| (replica, state.roster.len()))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

// The error arm of a filter catalog reply, decoded without the outcome a
// served reply carries.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CatalogRefusal {
    Err(FilterError),
}

/// The replica id of the primary of one partition, as any bound replica
/// answers it from its applied metadata.
pub(crate) async fn partition_primary(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    partition_id: u32,
) -> Result<u8, IggyError> {
    let request = GetPartitionRouteRequest {
        stream_id: identifier_to_wire(stream)?,
        topic_id: identifier_to_wire(topic)?,
        partition_id,
    };
    let reply = client
        .send_binary_request(GET_PARTITION_ROUTE_CODE, request.to_bytes())
        .await?;
    PartitionRouteResponse::decode_from(&reply)
        .map(|route| route.primary_replica)
        .map_err(|_| IggyError::InvalidFormat)
}

async fn send_to_replica(
    client: &IggyClient,
    code: u32,
    payload: Bytes,
    replica: u8,
    deadline: Option<Instant>,
) -> Result<Bytes, IggyError> {
    match deadline {
        Some(deadline) => {
            client
                .send_binary_request_to_replica_with_deadline(code, payload, replica, deadline)
                .await
        }
        None => {
            client
                .send_binary_request_to_replica(code, payload, replica)
                .await
        }
    }
}

/// Whether the node a partition request went to no longer leads the
/// partition or cannot be reached, so its route is looked up again.
pub(crate) fn is_route_stale(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::NotPrimary | IggyError::InvalidIdentifier | IggyError::PartitionNotFound(..)
    ) || is_transient_iggy_io_error(error)
}

/// The command error an opened reply carries. A recovering node's positioned
/// refusal is opened first by [`Answered::open`], so both route the same way.
fn reply_error(reply: &[u8]) -> Option<CommandError> {
    decode_named::<CommandError>(reply).ok()
}

/// Whether a node refused `code` for want of its managed backend:
/// `BackendUnavailable`, or for a filter catalog read `catalog_unavailable`,
/// the form the streaming server gives that refusal there.
fn backend_refused(code: u32, reply: &[u8]) -> bool {
    reply_error(reply).is_some_and(|error| error.code.is_backend_unavailable())
        || (is_filter_catalog_read(code)
            && matches!(
                decode_named::<CatalogRefusal>(reply),
                Ok(CatalogRefusal::Err(FilterError {
                    reason: FilterErrorReason::CatalogUnavailable,
                    ..
                }))
            ))
}

const fn is_filter_catalog_read(code: u32) -> bool {
    matches!(
        code,
        AGDX_GET_FILTER_CODE
            | AGDX_LIST_FILTERS_CODE
            | AGDX_LIST_FILTER_REVISIONS_CODE
            | AGDX_GET_FILTER_BINDING_CODE
            | AGDX_LIST_FILTER_BINDINGS_CODE
            | AGDX_FILTER_OPERATION_CODE
    )
}

/// Whether a caller request whose link failed on `failed_links` nodes moves to
/// the next one. An expired attempt deadline ends the walk, so the next attempt
/// starts on the next node with a fresh deadline.
fn walks_on(failed_links: usize, nodes: usize, deadline: Option<Instant>, now: Instant) -> bool {
    failed_links < nodes && deadline.is_none_or(|deadline| now < deadline)
}

/// Whether a read of `code` whose walk ended `walked` walks the roster again
/// after `wait`: every node refused it for want of its managed backend or with
/// a lost link, and the wait ends before `until`. `None` walks once.
fn walks_again(
    code: u32,
    walked: &Result<Answered, IggyError>,
    until: Option<Instant>,
    wait: Duration,
    now: Instant,
) -> bool {
    let refused = match walked {
        Ok(answered) => backend_refused(code, &answered.reply),
        Err(error) => is_transient_iggy_io_error(error),
    };
    refused && until.is_some_and(|until| now + wait < until)
}

fn pick(roster: &[u8], caller_key: u64, moved: u64) -> Option<u8> {
    let nodes = u64::try_from(roster.len())
        .ok()
        .filter(|nodes| *nodes > 0)?;
    let index = usize::try_from(caller_key.wrapping_add(moved) % nodes).ok()?;
    roster.get(index).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::poll;
    use iggy_common::locking::IggyRwLockFn;
    use laser_wire::codes::{
        AGDX_FILTER_MUTATE_CODE, AGDX_FILTERED_POLL_CODE, AGDX_KV_GET_CODE, AGDX_KV_SET_CODE,
    };
    use laser_wire::filter::FilterCatalogReply;
    use laser_wire::framing::encode_named;
    use laser_wire::mutation::AppliedFrontier;
    use laser_wire::result::ResultCode;

    #[tokio::test]
    async fn given_a_blocked_roster_read_when_a_managed_attempt_expires_then_should_end_at_its_deadline()
     {
        let client = IggyClient::default();
        let handle = client.client();
        let _guard = handle.write().await;
        let nodes = Nodes::new(0);
        let result = tokio::time::timeout(
            Duration::from_millis(300),
            nodes.send_held(
                &client,
                AGDX_KV_SET_CODE,
                Bytes::new(),
                Target::Caller,
                Instant::now() + Duration::from_millis(20),
            ),
        )
        .await;
        assert!(
            matches!(result, Ok(Err(IggyError::TransientNotAccepted))),
            "the attempt deadline must cover the roster read"
        );
    }

    #[tokio::test]
    async fn given_a_driver_that_wrote_the_frame_when_the_next_node_stops_before_writing_then_should_mark_the_frame_written()
     {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0, 1];
        let written = AtomicBool::new(false);
        let mut attempts = vec![
            Err(IggyError::ClientShutdown),
            Err(IggyError::TransientNotCommitted),
        ];

        let outcome = nodes
            .walk_caller(&client, AGDX_KV_SET_CODE, None, &written, |_replica| {
                std::future::ready(attempts.pop().expect("an attempt per node"))
            })
            .await;

        assert!(matches!(outcome, Err(IggyError::ClientShutdown)));
        assert!(
            written.load(Ordering::SeqCst),
            "the first node wrote the frame before the client shut down"
        );
    }

    #[tokio::test]
    async fn given_a_driver_that_stops_before_writing_when_walked_then_should_leave_the_frame_unwritten()
     {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0, 1];
        let written = AtomicBool::new(false);

        let outcome = nodes
            .walk_caller(&client, AGDX_KV_SET_CODE, None, &written, |_replica| {
                std::future::ready(Err(IggyError::ClientShutdown))
            })
            .await;

        assert!(matches!(outcome, Err(IggyError::ClientShutdown)));
        assert!(!written.load(Ordering::SeqCst));
    }

    #[test]
    fn given_a_backend_unavailable_reply_with_a_frontier_when_routed_then_should_read_like_a_plain_one()
     {
        let refusal = encode_named(&CommandError::new(
            ResultCode::BackendUnavailable,
            "the plane is recovering",
        ))
        .expect("encoded");
        let positioned = encode_named(&ManagedReplyEnvelope {
            v: 1,
            frontier: AppliedFrontier::default(),
            reply: refusal.clone(),
        })
        .expect("encoded");

        for reply in [refusal, positioned] {
            let answered = Answered::open(Bytes::from(reply), Some(1));
            assert!(
                reply_error(&answered.reply)
                    .is_some_and(|error| error.code.is_backend_unavailable())
            );
        }
    }

    #[test]
    fn given_an_expired_attempt_deadline_when_a_caller_link_fails_then_should_stop_walking_the_roster()
     {
        let now = Instant::now();
        let ahead = now + Duration::from_secs(1);
        assert!(walks_on(1, 3, None, now));
        assert!(walks_on(1, 3, Some(ahead), now));
        assert!(!walks_on(3, 3, Some(ahead), now), "every node failed");
        assert!(
            !walks_on(1, 3, Some(now), now),
            "an expired attempt leaves the next node to the next attempt"
        );
    }

    #[tokio::test]
    async fn given_one_node_that_refuses_then_serves_when_reading_then_should_return_the_served_reply()
     {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0];
        let served = Bytes::from_static(b"served");
        let mut replies = vec![Ok(served.clone()), Ok(refusal("replaying its logs"))];

        let answered = nodes
            .walk_caller_until(
                &client,
                AGDX_KV_GET_CODE,
                None,
                Walk::Until(Instant::now() + Duration::from_secs(5)),
                |_replica| std::future::ready(replies.pop().expect("an attempt per reply")),
            )
            .await
            .expect("the read is served");

        assert_eq!(answered.reply, served);
        assert!(replies.is_empty(), "the refused node is asked again");
    }

    #[tokio::test]
    async fn given_every_node_refusing_past_the_deadline_when_reading_then_should_return_the_last_refusal()
     {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0, 1];
        let started = Instant::now();
        let mut attempts = 0;

        let answered = nodes
            .walk_caller_until(
                &client,
                AGDX_KV_GET_CODE,
                None,
                Walk::Until(started + Duration::from_millis(300)),
                |_replica| {
                    attempts += 1;
                    std::future::ready(Ok(refusal(&format!("refusal {attempts}"))))
                },
            )
            .await
            .expect("a refusal is a reply");

        let error = reply_error(&answered.reply).expect("a command error");
        assert!(error.code.is_backend_unavailable());
        assert!(
            attempts > 2,
            "the roster is walked again before the deadline"
        );
        assert_eq!(
            error.message,
            format!("refusal {attempts}"),
            "the last refusal is returned"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the deadline ends the wait"
        );
    }

    #[tokio::test]
    async fn given_every_node_losing_its_link_when_reading_then_should_walk_again_until_served() {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0, 1];
        let served = Bytes::from_static(b"served");
        let mut replies = vec![
            Ok(served.clone()),
            Err(IggyError::TransientNotAccepted),
            Err(IggyError::TransientNotAccepted),
        ];

        let answered = nodes
            .walk_caller_until(
                &client,
                AGDX_KV_GET_CODE,
                None,
                Walk::Until(Instant::now() + Duration::from_secs(5)),
                |_replica| std::future::ready(replies.pop().expect("an attempt per reply")),
            )
            .await
            .expect("the read is served");

        assert_eq!(answered.reply, served);
    }

    #[tokio::test]
    async fn given_a_filter_catalog_read_every_node_refuses_as_catalog_unavailable_when_reading_then_should_walk_again_until_served()
     {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0, 1];
        let served = Bytes::from_static(b"served");
        let mut replies = vec![
            Ok(served.clone()),
            Ok(catalog_refusal()),
            Ok(catalog_refusal()),
        ];

        let answered = nodes
            .walk_caller_until(
                &client,
                AGDX_GET_FILTER_BINDING_CODE,
                None,
                Walk::Until(Instant::now() + Duration::from_secs(5)),
                |_replica| std::future::ready(replies.pop().expect("an attempt per reply")),
            )
            .await
            .expect("the read is served");

        assert_eq!(answered.reply, served);
        assert!(replies.is_empty(), "both nodes are asked, then asked again");
    }

    #[tokio::test]
    async fn given_catalog_unavailable_for_a_mutation_or_another_surface_when_walked_then_should_return_it_after_one_attempt()
     {
        for code in [AGDX_FILTER_MUTATE_CODE, AGDX_FILTERED_POLL_CODE] {
            let client = IggyClient::default();
            let nodes = Nodes::new(0);
            nodes.lock().roster = vec![0, 1, 2];
            let mut attempts = 0;

            let answered = nodes
                .walk_caller_until(
                    &client,
                    code,
                    None,
                    Walk::Until(Instant::now() + Duration::from_secs(5)),
                    |_replica| {
                        attempts += 1;
                        std::future::ready(Ok(catalog_refusal()))
                    },
                )
                .await
                .expect("a refusal is a reply");

            assert_eq!(answered.reply, catalog_refusal());
            assert_eq!(attempts, 1, "command {code} is not moved or repeated");
        }
    }

    #[tokio::test]
    async fn given_a_mutation_attempt_when_every_node_refuses_then_should_walk_once() {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0, 1, 2];
        let mut attempts = 0;

        let answered = nodes
            .walk_caller_until(
                &client,
                AGDX_KV_SET_CODE,
                Some(Instant::now() + Duration::from_secs(5)),
                Walk::Once,
                |_replica| {
                    attempts += 1;
                    std::future::ready(Ok(refusal("down")))
                },
            )
            .await
            .expect("a refusal is a reply");

        assert!(
            reply_error(&answered.reply).is_some_and(|error| error.code.is_backend_unavailable())
        );
        assert_eq!(
            attempts, 3,
            "one attempt per node, the mutation loop repeats"
        );
    }

    #[tokio::test]
    async fn given_a_read_without_its_own_deadline_when_its_node_refuses_once_then_should_walk_again_until_served()
     {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0];
        let served = Bytes::from_static(b"served");
        let mut replies = vec![Ok(served.clone()), Ok(refusal("replaying its logs"))];

        let answered = nodes
            .walk_caller_until(
                &client,
                AGDX_KV_GET_CODE,
                None,
                Walk::UntilServed,
                |_replica| std::future::ready(replies.pop().expect("an attempt per reply")),
            )
            .await
            .expect("the read is served");

        assert_eq!(answered.reply, served);
    }

    #[tokio::test]
    async fn given_every_node_refusing_when_reading_then_should_back_off_longer_before_each_walk() {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0];
        let mut attempts = 0;

        nodes
            .walk_caller_until(
                &client,
                AGDX_KV_GET_CODE,
                None,
                Walk::Until(Instant::now() + Duration::from_secs(2)),
                |_replica| {
                    attempts += 1;
                    std::future::ready(Ok(refusal("down")))
                },
            )
            .await
            .expect("a refusal is a reply");

        // Waits of at least 50, 100, 200, 400 and 800 ms leave room for six walks in two seconds.
        assert!(attempts <= 6, "{attempts} walks in two seconds");
    }

    #[tokio::test]
    async fn given_a_retry_timer_that_wakes_after_the_read_deadline_when_resumed_then_should_return_the_last_refusal()
     {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0];
        let refused = refusal("replaying its logs");
        let mut replies = vec![Bytes::from_static(b"served"), refused.clone()];
        let deadline = Instant::now() + Duration::from_millis(250);
        let mut reading = Box::pin(nodes.walk_caller_until(
            &client,
            AGDX_KV_GET_CODE,
            None,
            Walk::Until(deadline),
            |_| std::future::ready(Ok(replies.pop().expect("one reply per attempt"))),
        ));

        assert!(poll!(&mut reading).is_pending());
        tokio::time::sleep_until(deadline.into()).await;
        let answer = reading.await.expect("the refusal is returned");

        assert_eq!(answer.reply, refused);
        assert_eq!(replies.len(), 1);
    }

    #[tokio::test]
    async fn given_a_read_waiting_out_refusals_when_its_future_is_dropped_then_should_stop_at_once()
    {
        let client = IggyClient::default();
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0];
        let mut attempts = 0;
        let mut reading = Box::pin(nodes.walk_caller_until(
            &client,
            AGDX_KV_GET_CODE,
            None,
            Walk::Until(Instant::now() + Duration::from_secs(5)),
            |_| {
                attempts += 1;
                std::future::ready(Ok(refusal("replaying its logs")))
            },
        ));

        assert!(poll!(&mut reading).is_pending());
        drop(reading);
        sleep(Duration::from_millis(250)).await;

        assert_eq!(attempts, 1);
    }

    #[test]
    fn given_a_wait_that_would_end_at_the_read_deadline_when_deciding_then_should_not_walk_again() {
        let now = Instant::now();
        let wait = Duration::from_millis(100);
        let refused: Result<Answered, IggyError> = Ok(Answered::open(refusal("down"), Some(0)));

        assert_eq!(
            [now + wait + Duration::from_millis(1), now + wait].map(|until| walks_again(
                AGDX_KV_GET_CODE,
                &refused,
                Some(until),
                wait,
                now
            )),
            [true, false]
        );
    }

    #[test]
    fn given_a_missing_partition_on_a_routed_node_when_classified_then_should_look_the_route_up_again()
     {
        assert!(is_route_stale(&IggyError::PartitionNotFound(
            7,
            Identifier::default(),
            Identifier::default(),
        )));
    }

    #[test]
    fn given_a_roster_when_picking_then_should_keep_one_node_per_caller_key() {
        let roster = [0, 1, 2];
        assert_eq!(pick(&roster, 4, 0), Some(1));
        assert_eq!(pick(&roster, 4, 0), Some(1), "the same key keeps its node");
        assert_eq!(
            pick(&roster, 5, 0),
            Some(2),
            "another key spreads to another node"
        );
        assert_eq!(pick(&[], 4, 0), None);
    }

    #[test]
    fn given_failed_links_when_picking_then_should_move_to_the_next_node_and_wrap() {
        let roster = [3, 5, 9];
        assert_eq!(pick(&roster, 2, 0), Some(9));
        assert_eq!(pick(&roster, 2, 1), Some(3));
        assert_eq!(
            pick(&roster, u64::MAX, 1),
            Some(3),
            "the key wraps instead of overflowing"
        );
    }

    #[test]
    fn given_a_failure_on_a_node_the_caller_already_left_when_recorded_then_should_not_move_again()
    {
        let nodes = Nodes::new(0);
        nodes.lock().roster = vec![0, 1, 2];
        nodes.failed(0);
        assert_eq!(nodes.pick(), Some((1, 3)));
        nodes.failed(0);
        assert_eq!(
            nodes.pick(),
            Some((1, 3)),
            "a late failure of the old node keeps the new one"
        );
    }

    #[test]
    fn given_authorization_writes_when_targeted_then_should_reach_the_metadata_primary() {
        assert_eq!(
            Target::of_code(AGDX_AUTHZ_BIND_ROLES_CODE),
            Target::MetadataPrimary
        );
        assert_eq!(
            Target::of_code(AGDX_AUTHZ_DEFINE_ROLE_CODE),
            Target::MetadataPrimary
        );
        assert_eq!(
            Target::of_code(AGDX_AUTHZ_DELETE_ROLE_CODE),
            Target::MetadataPrimary
        );
        assert_eq!(
            Target::of_code(laser_wire::codes::AGDX_AUTHZ_WHOAMI_CODE),
            Target::Caller
        );
        assert_eq!(
            Target::of_code(laser_wire::codes::AGDX_KV_GET_CODE),
            Target::Caller
        );
    }

    fn refusal(message: &str) -> Bytes {
        Bytes::from(
            encode_named(&CommandError::new(ResultCode::BackendUnavailable, message))
                .expect("encoded"),
        )
    }

    // The refusal the streaming server gives a filter catalog read when it
    // cannot reach its managed backend.
    fn catalog_refusal() -> Bytes {
        Bytes::from(
            encode_named(&FilterCatalogReply::Err(FilterError::new(
                FilterErrorReason::CatalogUnavailable,
                "the managed backend did not answer",
            )))
            .expect("encoded"),
        )
    }
}
