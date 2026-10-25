use crate::error::is_transient_iggy_io_error;
use crate::iggy::prelude::{ClusterClient, Identifier, IggyClient, IggyError};
use bytes::Bytes;
use iggy_binary_protocol::codes::GET_PARTITION_ROUTE_CODE;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::wire_conversions::identifier_to_wire;
use laser_wire::codes::{
    AGDX_AUTHZ_BIND_ROLES_CODE, AGDX_AUTHZ_DEFINE_ROLE_CODE, AGDX_AUTHZ_DELETE_ROLE_CODE,
};
use laser_wire::framing::decode_named;
#[cfg(feature = "query")]
use laser_wire::query::QueryExecutionId;
use laser_wire::result::CommandError;
#[cfg(feature = "query")]
use std::collections::VecDeque;
use std::sync::Mutex;

#[cfg(feature = "query")]
const PINNED_QUERIES: usize = 1024;

/// The node a managed request goes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    // The node each recent query ran on, newest last.
    #[cfg(feature = "query")]
    queries: VecDeque<(QueryExecutionId, u8)>,
}

/// The reply of a managed request and the replica that sent it, `None` when
/// the metadata primary answered.
pub(crate) struct Answered {
    pub(crate) reply: Bytes,
    pub(crate) replica: Option<u8>,
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
        match target {
            Target::MetadataPrimary => Ok(Answered {
                reply: client.send_binary_request(code, payload).await?,
                replica: None,
            }),
            Target::Replica(replica) => Ok(Answered {
                reply: client
                    .send_binary_request_to_replica(code, payload, replica)
                    .await?,
                replica: Some(replica),
            }),
            Target::Caller => self.send_to_caller(client, code, payload).await,
        }
    }

    async fn send_to_caller(
        &self,
        client: &IggyClient,
        code: u32,
        payload: Bytes,
    ) -> Result<Answered, IggyError> {
        let mut roster_read = false;
        let mut failed_links = 0;
        loop {
            let (replica, nodes) = self.caller_node(client).await?;
            match client
                .send_binary_request_to_replica(code, payload.clone(), replica)
                .await
            {
                Ok(reply) => {
                    if decode_named::<CommandError>(&reply)
                        .is_ok_and(|error| error.code.is_backend_unavailable())
                    {
                        self.failed(replica);
                        failed_links += 1;
                        if failed_links < nodes {
                            continue;
                        }
                    }
                    return Ok(Answered {
                        reply,
                        replica: Some(replica),
                    });
                }
                Err(IggyError::InvalidIdentifier) if !roster_read => {
                    roster_read = true;
                    self.forget_roster();
                }
                Err(error) if is_transient_iggy_io_error(&error) => {
                    self.failed(replica);
                    failed_links += 1;
                    if failed_links >= nodes {
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

    #[cfg(feature = "query")]
    /// Remember the node a query ran on, so its pages, status and
    /// cancellation reach the plane that holds its state.
    pub(crate) fn pin_query(&self, execution_id: QueryExecutionId, replica: u8) {
        let mut state = self.lock();
        state.queries.retain(|(pinned, _)| *pinned != execution_id);
        if state.queries.len() == PINNED_QUERIES {
            state.queries.pop_front();
        }
        state.queries.push_back((execution_id, replica));
    }

    #[cfg(feature = "query")]
    /// The node of a pinned query, or the caller's node.
    pub(crate) fn query_target(&self, execution_id: QueryExecutionId) -> Target {
        self.lock()
            .queries
            .iter()
            .find(|(pinned, _)| *pinned == execution_id)
            .map_or(Target::Caller, |(_, replica)| Target::Replica(*replica))
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

/// Whether the node a partition request went to no longer leads the
/// partition or cannot be reached, so its route is looked up again.
pub(crate) fn is_route_stale(error: &IggyError) -> bool {
    matches!(error, IggyError::NotPrimary | IggyError::InvalidIdentifier)
        || is_transient_iggy_io_error(error)
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
    #[cfg(feature = "query")]
    use laser_wire::query::QueryExecutionId;

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

    #[cfg(feature = "query")]
    #[test]
    fn given_pinned_queries_when_the_list_is_full_then_should_drop_the_oldest() {
        let nodes = Nodes::new(0);
        let first = QueryExecutionId::from(u128::MAX);
        nodes.pin_query(first, 2);
        assert_eq!(nodes.query_target(first), Target::Replica(2));
        for id in (0_u128..).take(PINNED_QUERIES) {
            nodes.pin_query(QueryExecutionId::from(id), 1);
        }
        assert_eq!(nodes.query_target(first), Target::Caller);
        assert_eq!(
            nodes.query_target(QueryExecutionId::from(0_u128)),
            Target::Replica(1)
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
