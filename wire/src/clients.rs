use serde::{Deserialize, Serialize};

/// The discovery read request (`AGDX_GET_CLIENTS_METADATA`). Filtered and
/// paginated, because a busy server may hold thousands of connections and a
/// caller must not have to pull them all at once. The server orders connections
/// by `client_id`, applies the filters, skips past `after_client_id`, and returns
/// up to `limit` entries plus a cursor when more remain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetadataQuery {
    pub v: u32,
    /// Only return connections that advertised metadata. The common case for
    /// discovery, where unannounced connections are noise.
    #[serde(default, skip_serializing_if = "is_false")]
    pub with_metadata_only: bool,
    /// Only return connections authenticated as this principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<u32>,
    /// Pagination cursor: return only connections whose `client_id` is strictly
    /// greater than this. `None` starts from the beginning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_client_id: Option<u32>,
    /// Max entries to return. Clamped server-side to the page cap.
    pub limit: u32,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// One connection's discovery record: the connection identity the streaming
/// server holds plus the opaque metadata the client advertised. A LaserData-owned
/// type, deliberately distinct from the upstream Apache Iggy `ClientInfo` (which
/// stays byte-identical so a stock Iggy SDK keeps working against LaserData
/// Cloud). The metadata is opaque: an agent advertises its card, a regular app
/// sets any blob the consumer interprets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetadata {
    pub client_id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<u32>,
    /// Transport code (1 Tcp, 2 Quic, 3 Http, 4 WebSocket), the same dictionary
    /// the upstream binding uses.
    pub transport: u8,
    pub address: String,
    pub consumer_groups_count: u32,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub metadata: Option<Vec<u8>>,
}

/// The reply to `AGDX_GET_CLIENTS_METADATA`: one page of connections with their
/// advertised metadata, plus `next_cursor` (the last `client_id` in the page) when
/// more connections remain, so the caller pages by passing it as the next
/// `after_client_id`. `None` means the last page.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientMetadataList {
    pub clients: Vec<ClientMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<u32>,
}
