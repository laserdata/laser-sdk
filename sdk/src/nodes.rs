use crate::error::{driver_wrote, is_transient_iggy_io_error};
use crate::iggy::prelude::{ClusterClient, Identifier, IggyClient, IggyError};
use bytes::Bytes;
use iggy_binary_protocol::codes::GET_PARTITION_ROUTE_CODE;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_common::with_binary_deadline;
use laser_wire::codes::{
    AGDX_AUTHZ_BIND_ROLES_CODE, AGDX_AUTHZ_DEFINE_ROLE_CODE, AGDX_AUTHZ_DELETE_ROLE_CODE,
};
use laser_wire::framing::decode_named;
use laser_wire::mutation::{AppliedFrontier, ManagedReplyEnvelope};
use laser_wire::result::CommandError;
use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

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
    /// next node after a failed link, at most once per node, and reads the
    /// roster again once when a replica id is no longer in it. A request to a
    /// named replica is sent there only.
    pub(crate) async fn send(
        &self,
        client: &IggyClient,
        code: u32,
        payload: Bytes,
        target: Target,
    ) -> Result<Answered, IggyError> {
        self.send_until(client, code, payload, target, None).await
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
            self.send_until(client, code, payload, target, Some(deadline)),
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
            Target::Caller => self.send_to_caller(client, code, payload, deadline).await,
        }
    }

    async fn send_to_caller(
        &self,
        client: &IggyClient,
        code: u32,
        payload: Bytes,
        deadline: Option<Instant>,
    ) -> Result<Answered, IggyError> {
        self.walk_caller(client, deadline, &AtomicBool::new(false), |replica| {
            send_to_replica(client, code, payload.clone(), replica, deadline)
        })
        .await
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
        self.walk_caller(client, None, written, |replica| {
            send_to_replica(client, code, payload.clone(), replica, None)
        })
        .await
    }

    async fn walk_caller<Attempt, Sent>(
        &self,
        client: &IggyClient,
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
                    if reply_error(&answered.reply)
                        .is_some_and(|error| error.code.is_backend_unavailable())
                    {
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

/// Whether a caller request whose link failed on `failed_links` nodes moves to
/// the next one. An expired attempt deadline ends the walk, so the next attempt
/// starts on the next node with a fresh deadline.
fn walks_on(failed_links: usize, nodes: usize, deadline: Option<Instant>, now: Instant) -> bool {
    failed_links < nodes && deadline.is_none_or(|deadline| now < deadline)
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
    use iggy_common::locking::IggyRwLockFn;
    use laser_wire::codes::AGDX_KV_SET_CODE;
    use laser_wire::framing::encode_named;
    use laser_wire::mutation::AppliedFrontier;
    use laser_wire::result::ResultCode;
    use std::time::Duration;

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
            .walk_caller(&client, None, &written, |_replica| {
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
            .walk_caller(&client, None, &written, |_replica| {
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
}
