// A typed client for the `/agdx/*` HTTP surface (feature `http-client`). Every
// browser UI and native tool otherwise re-implements the same glue: route
// strings, base64url of binary KV keys, query-string composition, and unwrapping
// the bare-`Ok`-or-`ErrorBody` reply contract. This owns all of it once, so a
// consumer writes `client.kv_get(ns, key).await?` and gets a typed value or a
// typed [`ClientError`] carrying the [`ResultCode`].
//
// The crate stays runtime-free: the caller injects the actual IO by
// implementing [`Transport`] over `gloo-net` (wasm) or `reqwest` (native). This
// is the crate's one async surface, and it is runtime-agnostic - an `async fn`
// is just a `Future`, with no executor dependency - so it still compiles for
// `wasm32-unknown-unknown`.

use crate::browse::{ProjectionInfo, SchemaInfo};
use crate::control::{Projection, ProjectionBinding, SchemaSource, SourceSelector};
use crate::fork::ForkInfo;
use crate::graph::GraphQuery;
use crate::http::{
    self, Capabilities, CasCommittedView, ClientMetadataListView, ClientsQuery, DecodeRecordBody,
    DeletedManyView, ErrorBody, ForkCreateBody, ForkPutBody, GraphNeighborsQuery, GraphResultView,
    KvCasQuery, KvPageView, KvPutQuery, KvScanQuery, ProjectionListQuery, PromotedView,
    RemoveBindingBody, SchemaListQuery,
};
use crate::kv::{CasExpect, KvNamespaceInfo};
use crate::query::{Query, QueryResult};
use crate::result::ResultCode;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// The HTTP verb a [`Transport`] must perform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}

impl Method {
    /// The uppercase method token (`"GET"`, ...).
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
        }
    }
}

/// A request the [`Transport`] performs. `path` is already the full path plus
/// query string (e.g. `/agdx/kv/sessions/dXNlcjox?expires_at_micros=10`). The
/// transport prepends its own base URL. A JSON `body` is present only on
/// `POST`/`PUT`, and when present the transport sends `Content-Type:
/// application/json`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: Method,
    pub path: String,
    pub body: Option<Vec<u8>>,
}

/// What the [`Transport`] returns: the numeric status, the response headers (for
/// the routes that carry metadata out of band, like the KV get expiry), and the
/// raw body. A transport that does not surface headers leaves `headers` empty,
/// in which case header-carried metadata reads as absent.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// A response with no headers.
    pub fn new(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body,
        }
    }

    /// Builder helper: attach a header (used by transports and tests).
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// The first value of header `name`, matched case-insensitively (HTTP header
    /// names are case-insensitive). `None` if absent.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// The result of a single-key KV read: the raw value bytes plus the optional
/// absolute expiry (epoch microseconds). A `kv_get` builds it from the raw
/// response body and the [`KV_EXPIRES_AT_MICROS_HEADER`](http::KV_EXPIRES_AT_MICROS_HEADER)
/// header. A scan page carries the base64url [`KvEntryView`](http::KvEntryView)
/// shape instead, since a JSON array cannot hold raw bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvValue {
    pub value: Vec<u8>,
    pub expires_at_micros: Option<u64>,
}

/// The IO seam. A wasm consumer implements it over `gloo-net`, a native one over
/// `reqwest`. The client never constructs URLs beyond the path: the transport
/// owns the base URL, credentials, and headers.
///
/// Authentication and 401 refresh-retry live here, in the transport, not in the
/// client. A transport attaches the credential to every request, and on a 401 it
/// may refresh the token and retry once before returning the response. This is
/// the injection point a real UI needs, and it keeps the client itself
/// auth-agnostic.
///
/// The trait uses an `async fn` in a trait, so the returned future is not bound
/// `Send`. A browser (single-threaded wasm) needs nothing more. A native caller
/// that drives the client on a multi-threaded executor and needs a `Send` future
/// (for example to `tokio::spawn` it) should run the client on a current-thread
/// runtime or wrap the call in a `Send`-producing adapter.
#[allow(async_fn_in_trait)]
pub trait Transport {
    /// The transport's own failure type (a network error, a timeout). Surfaced
    /// verbatim through [`ClientError::Transport`].
    type Error: core::fmt::Display;

    /// Perform one request. A non-2xx status is **not** an error here: the
    /// client inspects the status and decodes the [`ErrorBody`]. Return `Err`
    /// only when the request never produced a response.
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, Self::Error>;
}

/// Why a typed call failed.
#[derive(Debug)]
pub enum ClientError<E> {
    /// The transport never got a response (network down, timeout).
    Transport(E),
    /// A response arrived but its body did not decode as the expected type.
    Decode(String),
    /// The server returned a non-2xx status with a classified [`ErrorBody`].
    Api(ErrorBody),
}

impl<E: core::fmt::Display> core::fmt::Display for ClientError<E> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ClientError::Transport(error) => write!(f, "transport error: {error}"),
            ClientError::Decode(detail) => write!(f, "decode error: {detail}"),
            ClientError::Api(body) => write!(f, "api error ({:?}): {}", body.code, body.message),
        }
    }
}

impl<E: core::fmt::Display + core::fmt::Debug> std::error::Error for ClientError<E> {}

impl<E> ClientError<E> {
    /// The classified [`ResultCode`] for an [`ClientError::Api`], so a caller
    /// branches on the kind (`NotFound`, `Conflict`, ...) without matching the
    /// message text. `None` for a transport or decode failure.
    pub fn code(&self) -> Option<ResultCode> {
        match self {
            ClientError::Api(body) => Some(body.code),
            _ => None,
        }
    }
}

type ClientResult<T, E> = Result<T, ClientError<E>>;

/// A typed `/agdx/*` client over an injected [`Transport`].
///
/// Binary KV keys are base64url-encoded into the path by the client, so raw user
/// bytes never enter a path segment. A `namespace`, `fork id`, or `projection
/// id` argument, by contrast, is placed in the path verbatim, so the caller must
/// pass a path-safe identifier (no `/`, `?`, or `#`). Fork ids are already
/// constrained by [`validate_fork_id`](crate::fork::validate_fork_id).
#[derive(Clone, Debug)]
pub struct HttpClient<T> {
    transport: T,
}

impl<T: Transport> HttpClient<T> {
    /// Wrap a transport. The transport owns the base URL and auth.
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    /// The underlying transport, for a caller that needs an escape hatch.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// `GET /agdx/capabilities`: feature-detect before showing a view.
    pub async fn capabilities(&self) -> ClientResult<Capabilities, T::Error> {
        self.get(http::CAPABILITIES_PATH.to_owned()).await
    }

    /// `POST /agdx/query`: run a query, get the result page back.
    pub async fn query(&self, query: &Query) -> ClientResult<QueryResult, T::Error> {
        self.send_json(Method::Post, http::QUERY_PATH.to_owned(), query)
            .await
    }

    /// `GET /agdx/projections`: list projections and their bindings, optionally
    /// filtered.
    pub async fn list_projections(
        &self,
        filter: &ProjectionListQuery,
    ) -> ClientResult<Vec<ProjectionInfo>, T::Error> {
        self.get(with_query(http::PROJECTIONS_PATH, filter)?).await
    }

    /// `GET /agdx/schemas`: list registered schemas, optionally filtered.
    pub async fn list_schemas(
        &self,
        filter: &SchemaListQuery,
    ) -> ClientResult<Vec<SchemaInfo>, T::Error> {
        self.get(with_query(http::SCHEMAS_PATH, filter)?).await
    }

    /// `POST /agdx/schemas`: register a schema, get its allocated id.
    pub async fn register_schema(
        &self,
        source: SchemaSource,
        name: Option<String>,
        version: Option<u32>,
    ) -> ClientResult<u32, T::Error> {
        let body = http::RegisterSchemaBody {
            source,
            name,
            version,
        };
        self.send_json(Method::Post, http::SCHEMAS_PATH.to_owned(), &body)
            .await
    }

    /// `GET /agdx/kv/{namespace}/{key}`: fetch one entry, or `None` on a 404.
    /// The value rides the raw response body (no base64 inflation, symmetric with
    /// `kv_set`), and the optional expiry rides the
    /// [`KV_EXPIRES_AT_MICROS_HEADER`](http::KV_EXPIRES_AT_MICROS_HEADER) response
    /// header.
    pub async fn kv_get(
        &self,
        namespace: &str,
        key: &[u8],
    ) -> ClientResult<Option<KvValue>, T::Error> {
        let path = http::kv_entry_path(namespace, &base64url_encode(key));
        let response = self.dispatch(Method::Get, path, None).await?;
        if response.status == 404 {
            return Ok(None);
        }
        if !(200..300).contains(&response.status) {
            return Err(api_error(&response));
        }
        let expires_at_micros = response
            .header(http::KV_EXPIRES_AT_MICROS_HEADER)
            .and_then(|value| value.parse::<u64>().ok());
        Ok(Some(KvValue {
            value: response.body,
            expires_at_micros,
        }))
    }

    /// `PUT /agdx/kv/{namespace}/{key}`: set a value, with an optional absolute
    /// expiry (epoch microseconds).
    pub async fn kv_set(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        expires_at_micros: Option<u64>,
    ) -> ClientResult<(), T::Error> {
        let path = with_query(
            &http::kv_entry_path(namespace, &base64url_encode(key)),
            &KvPutQuery { expires_at_micros },
        )?;
        // The key is base64url in the path, but the value rides the raw request
        // body, so a large value carries no base64 inflation.
        self.expect_ok(Method::Put, path, Some(value.to_vec()))
            .await
    }

    /// `PUT /agdx/kv/{namespace}/{key}/cas`: a conditional write
    /// (compare-and-swap). Applies `value` only if `expect` holds, returning the
    /// new version on commit. A precondition miss surfaces as
    /// `ClientError::Api` with `ResultCode::Conflict` (the response's
    /// `ErrorBody.detail` carries the current version). Backend-gated by the
    /// `kv_cas` capability: a deployment that does not serve it answers
    /// unsupported.
    pub async fn kv_cas(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        expect: CasExpect,
        expires_at_micros: Option<u64>,
    ) -> ClientResult<u64, T::Error> {
        let (expect_version, expect_absent) = match expect {
            CasExpect::Match(version) => (Some(version), None),
            CasExpect::Absent => (None, Some(true)),
        };
        let path = with_query(
            &http::kv_cas_path(namespace, &base64url_encode(key)),
            &KvCasQuery {
                expect_version,
                expect_absent,
                expires_at_micros,
            },
        )?;
        // The value rides the raw body like the plain PUT.
        let response = self
            .dispatch(Method::Put, path, Some(value.to_vec()))
            .await?;
        let view: CasCommittedView = decode_ok(&response)?;
        Ok(view.version)
    }

    /// `DELETE /agdx/kv/{namespace}/{key}`: delete one entry, returning `true` if it existed.
    pub async fn kv_delete(&self, namespace: &str, key: &[u8]) -> ClientResult<bool, T::Error> {
        let path = http::kv_entry_path(namespace, &base64url_encode(key));
        self.send_empty(Method::Delete, path).await
    }

    /// `GET /agdx/kv/{namespace}`: scan a page of entries.
    pub async fn kv_scan(
        &self,
        namespace: &str,
        filter: &KvScanQuery,
    ) -> ClientResult<KvPageView, T::Error> {
        self.get(with_query(&http::kv_namespace_path(namespace), filter)?)
            .await
    }

    /// `POST /agdx/forks`: create a fork.
    pub async fn create_fork(&self, body: &ForkCreateBody) -> ClientResult<ForkInfo, T::Error> {
        self.send_json(Method::Post, http::FORKS_PATH.to_owned(), body)
            .await
    }

    /// `GET /agdx/forks`: list the caller's forks.
    pub async fn list_forks(&self) -> ClientResult<Vec<ForkInfo>, T::Error> {
        self.get(http::FORKS_PATH.to_owned()).await
    }

    /// `GET /agdx/clients`: one page of live connections with their advertised
    /// metadata, filtered and paginated per `query`. Follow `next_cursor` as the
    /// next `after` to page.
    pub async fn clients(
        &self,
        query: &ClientsQuery,
    ) -> ClientResult<ClientMetadataListView, T::Error> {
        self.get(with_query(http::CLIENTS_PATH, query)?).await
    }

    /// `GET /agdx/projections/{id}`: read one projection and its bindings, or
    /// `None` on a 404.
    pub async fn get_projection(&self, id: &str) -> ClientResult<Option<ProjectionInfo>, T::Error> {
        self.get_optional(http::projection_path(id)).await
    }

    /// `POST /agdx/projections`: register or replace a projection (a control
    /// command, durable on the control topic).
    pub async fn register_projection(&self, projection: &Projection) -> ClientResult<(), T::Error> {
        self.send_json_ok(Method::Post, http::PROJECTIONS_PATH.to_owned(), projection)
            .await
    }

    /// `DELETE /agdx/projections/{id}`: drop a projection.
    pub async fn drop_projection(&self, id: &str) -> ClientResult<(), T::Error> {
        self.expect_ok(Method::Delete, http::projection_path(id), None)
            .await
    }

    /// `POST /agdx/bindings`: apply (add or update) a binding.
    pub async fn apply_binding(&self, binding: &ProjectionBinding) -> ClientResult<(), T::Error> {
        self.send_json_ok(Method::Post, http::BINDINGS_PATH.to_owned(), binding)
            .await
    }

    /// `DELETE /agdx/bindings`: remove a binding for a source, or one projection
    /// from it when `projection_ref` is set.
    pub async fn remove_binding(
        &self,
        source: &SourceSelector,
        projection_ref: Option<String>,
    ) -> ClientResult<(), T::Error> {
        let body = RemoveBindingBody {
            stream: source.stream.clone(),
            topic: source.topic.clone(),
            projection_ref,
        };
        self.send_json_ok(Method::Delete, http::BINDINGS_PATH.to_owned(), &body)
            .await
    }

    /// `GET /agdx/schemas/{id}`: read one writer schema, or `None` on a 404.
    pub async fn get_schema(&self, id: u32) -> ClientResult<Option<SchemaInfo>, T::Error> {
        self.get_optional(http::schema_path(id)).await
    }

    /// `DELETE /agdx/schemas/{id}`: drop (tombstone) a schema.
    pub async fn drop_schema(&self, id: u32) -> ClientResult<(), T::Error> {
        self.expect_ok(Method::Delete, http::schema_path(id), None)
            .await
    }

    /// `POST /agdx/schemas/{id}/decode`: decode a record body under the schema,
    /// returning its JSON form, or `None` when the body does not decode under it.
    pub async fn decode_record(
        &self,
        id: u32,
        payload: &[u8],
    ) -> ClientResult<Option<serde_json::Value>, T::Error> {
        let body = DecodeRecordBody {
            payload: base64url_encode(payload),
        };
        self.send_json(Method::Post, http::schema_decode_path(id), &body)
            .await
    }

    /// `GET /agdx/kv`: list the caller's namespaces and their entry counts.
    pub async fn kv_namespaces(&self) -> ClientResult<Vec<KvNamespaceInfo>, T::Error> {
        self.get(http::KV_PATH.to_owned()).await
    }

    /// `DELETE /agdx/kv/{namespace}`: bulk-delete entries matching the bounds
    /// (no bounds clears the namespace). Returns the number removed.
    pub async fn kv_delete_many(
        &self,
        namespace: &str,
        filter: &KvScanQuery,
    ) -> ClientResult<usize, T::Error> {
        let path = with_query(&http::kv_namespace_path(namespace), filter)?;
        let view: DeletedManyView = self.send_empty(Method::Delete, path).await?;
        Ok(view.deleted)
    }

    /// `POST /agdx/forks/{id}/promote`: promote a fork's rows onto the trunk.
    /// Returns the number of rows applied.
    pub async fn promote_fork(&self, id: &str) -> ClientResult<usize, T::Error> {
        let view: PromotedView = self
            .send_empty(Method::Post, http::fork_promote_path(id))
            .await?;
        Ok(view.rows)
    }

    /// `DELETE /agdx/forks/{id}`: squash (discard) a fork.
    pub async fn delete_fork(&self, id: &str) -> ClientResult<(), T::Error> {
        self.expect_ok(Method::Delete, http::fork_path(id), None)
            .await
    }

    /// `PUT /agdx/forks/{id}/rows`: write one speculative row into a fork.
    pub async fn put_fork_row(&self, id: &str, body: &ForkPutBody) -> ClientResult<(), T::Error> {
        self.send_json_ok(Method::Put, http::fork_rows_path(id), body)
            .await
    }

    /// `POST /agdx/graph/{name}/query`: run a traversal over a named graph, get
    /// back the reachable nodes and traversed edges. Backend-gated by the `graph`
    /// capability: a deployment without a graph backend answers unsupported.
    pub async fn graph_query(
        &self,
        name: &str,
        query: &GraphQuery,
    ) -> ClientResult<GraphResultView, T::Error> {
        self.send_json(Method::Post, http::graph_query_path(name), query)
            .await
    }

    /// `GET /agdx/graph/{name}/neighbors/{node}`: the neighbor read, the cheap
    /// common traversal. `node` is the Crockford-base32 node id. `query` carries
    /// the direction, an optional edge-type filter, the hop depth, and a limit (a
    /// default `query` reads one hop outward with no filter).
    pub async fn graph_neighbors(
        &self,
        name: &str,
        node: &str,
        query: &GraphNeighborsQuery,
    ) -> ClientResult<GraphResultView, T::Error> {
        self.get(with_query(&http::graph_neighbors_path(name, node), query)?)
            .await
    }

    /// `GET /agdx/graphs`: list the registered graph projections, the discovery
    /// surface a graph explorer reads to offer the available graphs. Reuses the
    /// projection-list filter, narrowed to graph-kind projections server-side.
    pub async fn list_graphs(
        &self,
        filter: &ProjectionListQuery,
    ) -> ClientResult<Vec<ProjectionInfo>, T::Error> {
        self.get(with_query(http::GRAPHS_PATH, filter)?).await
    }

    /// `POST /agdx/graphs`: register a graph projection (a [`Projection`] with
    /// `kind = Graph` and an entity schema). Applied asynchronously, like every
    /// control command.
    pub async fn register_graph(&self, projection: &Projection) -> ClientResult<(), T::Error> {
        self.send_json_ok(Method::Post, http::GRAPHS_PATH.to_owned(), projection)
            .await
    }

    /// `GET /agdx/graphs/{id}`: read one graph projection by id, or `None` when no
    /// graph projection has it.
    pub async fn get_graph(&self, id: &str) -> ClientResult<Option<ProjectionInfo>, T::Error> {
        self.get_optional(http::graph_path(id)).await
    }

    /// `DELETE /agdx/graphs/{id}`: drop the graph projection registered under
    /// `id`. The materialized nodes and edges are left untouched.
    pub async fn drop_graph(&self, id: &str) -> ClientResult<(), T::Error> {
        self.expect_ok(Method::Delete, http::graph_path(id), None)
            .await
    }

    async fn get<R: DeserializeOwned>(&self, path: String) -> ClientResult<R, T::Error> {
        let response = self.dispatch(Method::Get, path, None).await?;
        decode_ok(&response)
    }

    /// A `GET` whose 404 means "absent" rather than an error.
    async fn get_optional<R: DeserializeOwned>(
        &self,
        path: String,
    ) -> ClientResult<Option<R>, T::Error> {
        let response = self.dispatch(Method::Get, path, None).await?;
        if response.status == 404 {
            return Ok(None);
        }
        decode_ok(&response).map(Some)
    }

    async fn send_json<B: Serialize, R: DeserializeOwned>(
        &self,
        method: Method,
        path: String,
        body: &B,
    ) -> ClientResult<R, T::Error> {
        let bytes = serde_json::to_vec(body)
            .map_err(|error| ClientError::Decode(format!("request body: {error}")))?;
        let response = self.dispatch(method, path, Some(bytes)).await?;
        decode_ok(&response)
    }

    async fn send_empty<R: DeserializeOwned>(
        &self,
        method: Method,
        path: String,
    ) -> ClientResult<R, T::Error> {
        let response = self.dispatch(method, path, None).await?;
        decode_ok(&response)
    }

    /// Send a JSON body and only check the status, for a control route whose 2xx
    /// body is empty.
    async fn send_json_ok<B: Serialize>(
        &self,
        method: Method,
        path: String,
        body: &B,
    ) -> ClientResult<(), T::Error> {
        let bytes = serde_json::to_vec(body)
            .map_err(|error| ClientError::Decode(format!("request body: {error}")))?;
        let response = self.dispatch(method, path, Some(bytes)).await?;
        check_status(&response)
    }

    async fn expect_ok(
        &self,
        method: Method,
        path: String,
        body: Option<Vec<u8>>,
    ) -> ClientResult<(), T::Error> {
        let response = self.dispatch(method, path, body).await?;
        check_status(&response)
    }

    async fn dispatch(
        &self,
        method: Method,
        path: String,
        body: Option<Vec<u8>>,
    ) -> ClientResult<HttpResponse, T::Error> {
        self.transport
            .send(HttpRequest { method, path, body })
            .await
            .map_err(ClientError::Transport)
    }
}

/// Append `?<urlencoded>` to a path when `params` serializes to a non-empty
/// query string, else return the path unchanged.
fn with_query<E, P: Serialize>(path: &str, params: &P) -> Result<String, ClientError<E>> {
    let query = serde_urlencoded::to_string(params)
        .map_err(|error| ClientError::Decode(format!("query params: {error}")))?;
    if query.is_empty() {
        Ok(path.to_owned())
    } else {
        Ok(format!("{path}?{query}"))
    }
}

/// On a 2xx, decode the bare `Ok` payload, otherwise turn the body into a typed
/// [`ClientError::Api`] (falling back to a synthetic body if the error body
/// itself does not decode, so a malformed 500 still classifies).
fn decode_ok<E, R: DeserializeOwned>(response: &HttpResponse) -> Result<R, ClientError<E>> {
    if (200..300).contains(&response.status) {
        serde_json::from_slice(&response.body)
            .map_err(|error| ClientError::Decode(format!("response body: {error}")))
    } else {
        Err(api_error(response))
    }
}

/// Like [`decode_ok`] but for a route whose 2xx body is empty.
fn check_status<E>(response: &HttpResponse) -> Result<(), ClientError<E>> {
    if (200..300).contains(&response.status) {
        Ok(())
    } else {
        Err(api_error(response))
    }
}

fn api_error<E>(response: &HttpResponse) -> ClientError<E> {
    let body = serde_json::from_slice::<ErrorBody>(&response.body).unwrap_or_else(|_| {
        ErrorBody::new(
            code_for_status(response.status),
            String::from_utf8_lossy(&response.body).into_owned(),
        )
    });
    ClientError::Api(body)
}

/// A best-effort classification for an `ErrorBody`-less failure response,
/// inferred from the HTTP status alone. Used only as the fallback when the body
/// did not carry a [`ResultCode`].
fn code_for_status(status: u16) -> ResultCode {
    match status {
        404 => ResultCode::NotFound,
        400 => ResultCode::InvalidArgument,
        401 => ResultCode::Unauthorized,
        409 => ResultCode::Conflict,
        413 => ResultCode::TooLarge,
        501 => ResultCode::Unsupported,
        503 => ResultCode::Stale,
        _ => ResultCode::Backend,
    }
}

// base64url (unpadded, RFC 4648 section 5), the encoding this surface uses for
// every binary value. Hand-rolled to keep the portable graph dependency-free.

const B64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Encode bytes as URL-safe unpadded base64 (RFC 4648 §5, no `=`).
pub fn base64url_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as usize;
        out.push(B64URL[b0 >> 2] as char);
        match chunk.len() {
            1 => out.push(B64URL[(b0 & 0b11) << 4] as char),
            2 => {
                let b1 = chunk[1] as usize;
                out.push(B64URL[((b0 & 0b11) << 4) | (b1 >> 4)] as char);
                out.push(B64URL[(b1 & 0b1111) << 2] as char);
            }
            _ => {
                let b1 = chunk[1] as usize;
                let b2 = chunk[2] as usize;
                out.push(B64URL[((b0 & 0b11) << 4) | (b1 >> 4)] as char);
                out.push(B64URL[((b1 & 0b1111) << 2) | (b2 >> 6)] as char);
                out.push(B64URL[b2 & 0b111111] as char);
            }
        }
    }
    out
}

/// Decode URL-safe unpadded base64. `None` on any non-alphabet byte or an
/// impossible length (a single trailing char carries no whole byte).
pub fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    fn val(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let bytes = input.as_bytes();
    if bytes.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let mut acc = 0u32;
        for &byte in chunk {
            acc = (acc << 6) | u32::from(val(byte)?);
        }
        // Left-align the accumulated bits for a short final chunk.
        acc <<= 6 * (4 - chunk.len());
        match chunk.len() {
            2 => out.push((acc >> 16) as u8),
            3 => {
                out.push((acc >> 16) as u8);
                out.push((acc >> 8) as u8);
            }
            _ => {
                out.push((acc >> 16) as u8);
                out.push((acc >> 8) as u8);
                out.push(acc as u8);
            }
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_bytes_when_base64url_round_tripped_then_should_preserve_them() {
        for case in [
            &b""[..],
            &b"f"[..],
            &b"fo"[..],
            &b"foo"[..],
            &b"foob"[..],
            &b"fooba"[..],
            &b"foobar"[..],
            &[0x00, 0xff, 0x10, 0x80][..],
        ] {
            let encoded = base64url_encode(case);
            assert!(
                !encoded.contains('=') && !encoded.contains('+') && !encoded.contains('/'),
                "url-safe unpadded: {encoded}"
            );
            assert_eq!(base64url_decode(&encoded).as_deref(), Some(case));
        }
    }

    #[test]
    fn given_known_vectors_when_encoded_then_should_match_rfc_url_alphabet() {
        assert_eq!(base64url_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64url_encode(&[0xfb, 0xff]), "-_8");
    }

    #[test]
    fn given_a_bad_base64_string_when_decoded_then_should_reject() {
        assert!(
            base64url_decode("====").is_none(),
            "padding is not alphabet"
        );
        assert!(
            base64url_decode("A").is_none(),
            "a lone char carries no byte"
        );
        assert!(base64url_decode("a b").is_none(), "space is not alphabet");
    }

    // A transport that replays a canned response, so the typed methods are
    // testable without any IO.
    struct CannedTransport {
        response: HttpResponse,
    }

    impl Transport for CannedTransport {
        type Error = std::convert::Infallible;
        async fn send(&self, _request: HttpRequest) -> Result<HttpResponse, Self::Error> {
            Ok(self.response.clone())
        }
    }

    fn block_on<F: core::future::Future>(future: F) -> F::Output {
        // A minimal executor: these futures never yield (the canned transport
        // is ready immediately), so a busy poll with the no-op waker resolves
        // them. No `unsafe`, so the crate's `forbid(unsafe_code)` holds.
        use core::task::{Context, Poll, Waker};
        let mut context = Context::from_waker(Waker::noop());
        let mut future = core::pin::pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
        }
    }

    #[test]
    fn given_an_ok_capabilities_response_when_fetched_then_should_decode() {
        let body = serde_json::to_vec(&Capabilities::new(
            true,
            crate::hello::OpVersions::new(1, 1, 1, 1),
        ))
        .unwrap();
        let client = HttpClient::new(CannedTransport {
            response: HttpResponse::new(200, body),
        });
        let caps = block_on(client.capabilities()).expect("decodes");
        assert!(caps.managed && !caps.kv.cas);
    }

    #[test]
    fn given_an_error_status_when_called_then_should_surface_the_typed_code() {
        let body =
            serde_json::to_vec(&ErrorBody::new(ResultCode::NotFound, "no such fork")).unwrap();
        let client = HttpClient::new(CannedTransport {
            response: HttpResponse::new(404, body),
        });
        let error = block_on(client.list_forks()).expect_err("a 404 is an error");
        assert_eq!(error.code(), Some(ResultCode::NotFound));
    }

    #[test]
    fn given_a_missing_kv_entry_when_fetched_then_should_be_none() {
        let body = serde_json::to_vec(&ErrorBody::new(ResultCode::NotFound, "absent")).unwrap();
        let client = HttpClient::new(CannedTransport {
            response: HttpResponse::new(404, body),
        });
        let entry = block_on(client.kv_get("sessions", b"user:1")).expect("404 maps to None");
        assert!(entry.is_none());
    }

    #[test]
    fn given_a_present_kv_entry_when_fetched_then_should_read_raw_body_and_expiry_header() {
        let response = HttpResponse::new(200, b"world".to_vec())
            .with_header(http::KV_EXPIRES_AT_MICROS_HEADER, "1700000000000000");
        let client = HttpClient::new(CannedTransport { response });
        let entry = block_on(client.kv_get("sessions", b"user:1"))
            .expect("decodes")
            .expect("present");
        assert_eq!(entry.value, b"world");
        assert_eq!(entry.expires_at_micros, Some(1_700_000_000_000_000));
    }

    #[test]
    fn given_a_missing_projection_when_fetched_then_should_be_none() {
        let body = serde_json::to_vec(&ErrorBody::new(ResultCode::NotFound, "absent")).unwrap();
        let client = HttpClient::new(CannedTransport {
            response: HttpResponse::new(404, body),
        });
        let info = block_on(client.get_projection("order.v1")).expect("404 maps to None");
        assert!(info.is_none());
    }

    #[test]
    fn given_a_delete_many_reply_when_received_then_should_return_the_count() {
        let body = serde_json::to_vec(&DeletedManyView { deleted: 7 }).unwrap();
        let client = HttpClient::new(CannedTransport {
            response: HttpResponse::new(200, body),
        });
        let removed = block_on(client.kv_delete_many("sessions", &KvScanQuery::default()))
            .expect("decodes the count");
        assert_eq!(removed, 7);
    }

    #[test]
    fn given_an_empty_2xx_when_dropping_a_projection_then_should_succeed() {
        let client = HttpClient::new(CannedTransport {
            response: HttpResponse::new(204, Vec::new()),
        });
        block_on(client.drop_projection("order.v1")).expect("a 204 is a success");
    }

    #[test]
    fn given_a_cas_commit_when_received_then_should_return_the_new_version() {
        let body = serde_json::to_vec(&CasCommittedView { version: 4 }).unwrap();
        let client = HttpClient::new(CannedTransport {
            response: HttpResponse::new(200, body),
        });
        let version = block_on(client.kv_cas("locks", b"job", b"held", CasExpect::Match(3), None))
            .expect("a commit returns the new version");
        assert_eq!(version, 4);
    }

    #[test]
    fn given_a_cas_conflict_when_received_then_should_surface_a_typed_conflict() {
        let body = serde_json::to_vec(
            &ErrorBody::new(ResultCode::Conflict, "version conflict")
                .with_detail(serde_json::json!({ "current": 3 })),
        )
        .unwrap();
        let client = HttpClient::new(CannedTransport {
            response: HttpResponse::new(409, body),
        });
        let error = block_on(client.kv_cas("locks", b"job", b"steal", CasExpect::Absent, None))
            .expect_err("a precondition miss is an error");
        assert_eq!(error.code(), Some(ResultCode::Conflict));
    }
}
