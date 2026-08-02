// The managed key-value surface. Wire types, codes, and caps live in
// laser-wire and are re-exported here unconditionally. The `Kv` handle and
// its fluent builders stay in this crate behind the `kv` feature.

pub use laser_wire::codes::{
    AGDX_KV_BASE, AGDX_KV_CAS_CODE, AGDX_KV_CAS_FENCED_CODE, AGDX_KV_COPY_CODE,
    AGDX_KV_DELETE_CODE, AGDX_KV_DELETE_MANY_CODE, AGDX_KV_EXISTS_CODE, AGDX_KV_EXPIRE_CODE,
    AGDX_KV_GET_CODE, AGDX_KV_LEASE_CODE, AGDX_KV_MOVE_CODE, AGDX_KV_NAMESPACES_CODE,
    AGDX_KV_PATCH_CODE, AGDX_KV_RELEASE_CODE, AGDX_KV_SCAN_CODE, AGDX_KV_SET_CODE, KV_OP_VERSION,
};
pub use laser_wire::kv::{
    CasExpect, KvCas, KvCasFenced, KvCopy, KvDelete, KvDeleteMany, KvEntry, KvError, KvExists,
    KvExpire, KvGet, KvLease, KvMetadata, KvMove, KvNamespaceInfo, KvNamespaces, KvOutcome, KvPage,
    KvPatch, KvRelease, KvReply, KvScan, KvSet,
};
pub use laser_wire::limits::{
    DEFAULT_NAMESPACE, DEFAULT_SCAN_LIMIT, MAX_KEY_BYTES, MAX_SCAN_LIMIT, MAX_VALUE_BYTES,
};

#[cfg(feature = "kv")]
mod client;
#[cfg(feature = "kv")]
pub use client::{Kv, KvCopyRequest, KvDeleteManyRequest, KvScanRequest, KvSetRequest, Lease};
