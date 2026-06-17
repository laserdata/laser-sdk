// One trait pairing each managed command code with its request and reply wire
// types. Not a multi-implementor abstraction (there is one wire contract and one established
// band): a compile-time assertion that the code/type pairings match, giving
// the SDK one generic send path and LaserData Cloud's dispatch table a fixture test
// against the same pairings.

use crate::browse::{
    BrowseReply, DecodeRecord, GetProjection, GetSchema, ListProjections, ListSchemas,
    RegisterSchema,
};
use crate::codes::*;
use crate::fork::{ForkCreate, ForkDelete, ForkList, ForkPromote, ForkPut, ForkReply};
use crate::kv::{KvDelete, KvDeleteMany, KvGet, KvNamespaces, KvReply, KvScan, KvSet};
use crate::query::{QueryEnvelope, QueryReply};

/// A managed command: the request type, its wire code, and the reply type the
/// other end answers with.
pub trait Command {
    /// The managed command code this request rides.
    const CODE: u32;
    /// The reply wire type.
    type Reply;
}

macro_rules! command {
    ($request:ty, $code:expr, $reply:ty) => {
        impl Command for $request {
            const CODE: u32 = $code;
            type Reply = $reply;
        }
    };
}

command!(QueryEnvelope, AGDX_QUERY_CODE, QueryReply);
command!(GetProjection, AGDX_GET_PROJECTION_CODE, BrowseReply);
command!(ListProjections, AGDX_LIST_PROJECTIONS_CODE, BrowseReply);
command!(GetSchema, AGDX_GET_SCHEMA_CODE, BrowseReply);
command!(ListSchemas, AGDX_LIST_SCHEMAS_CODE, BrowseReply);
command!(RegisterSchema, AGDX_REGISTER_SCHEMA_CODE, BrowseReply);
command!(DecodeRecord, AGDX_DECODE_RECORD_CODE, BrowseReply);
command!(KvGet, AGDX_KV_GET_CODE, KvReply);
command!(KvSet, AGDX_KV_SET_CODE, KvReply);
command!(KvScan, AGDX_KV_SCAN_CODE, KvReply);
command!(KvDelete, AGDX_KV_DELETE_CODE, KvReply);
command!(KvDeleteMany, AGDX_KV_DELETE_MANY_CODE, KvReply);
command!(KvNamespaces, AGDX_KV_NAMESPACES_CODE, KvReply);
command!(ForkCreate, AGDX_FORK_CREATE_CODE, ForkReply);
command!(ForkDelete, AGDX_FORK_DELETE_CODE, ForkReply);
command!(ForkPromote, AGDX_FORK_PROMOTE_CODE, ForkReply);
command!(ForkList, AGDX_FORK_LIST_CODE, ForkReply);
command!(ForkPut, AGDX_FORK_PUT_CODE, ForkReply);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_command_impls_when_compared_then_codes_should_match_the_dictionary() {
        assert_eq!(<QueryEnvelope as Command>::CODE, 1_000_100);
        assert_eq!(<GetProjection as Command>::CODE, 1_000_110);
        assert_eq!(<ListProjections as Command>::CODE, 1_000_111);
        assert_eq!(<GetSchema as Command>::CODE, 1_000_120);
        assert_eq!(<ListSchemas as Command>::CODE, 1_000_121);
        assert_eq!(<RegisterSchema as Command>::CODE, 1_000_122);
        assert_eq!(<DecodeRecord as Command>::CODE, 1_000_123);
        assert_eq!(<KvGet as Command>::CODE, 1_000_200);
        assert_eq!(<KvSet as Command>::CODE, 1_000_201);
        assert_eq!(<KvScan as Command>::CODE, 1_000_202);
        assert_eq!(<KvDelete as Command>::CODE, 1_000_203);
        assert_eq!(<KvDeleteMany as Command>::CODE, 1_000_204);
        assert_eq!(<KvNamespaces as Command>::CODE, 1_000_205);
        assert_eq!(<ForkCreate as Command>::CODE, 1_000_300);
        assert_eq!(<ForkDelete as Command>::CODE, 1_000_301);
        assert_eq!(<ForkPromote as Command>::CODE, 1_000_302);
        assert_eq!(<ForkList as Command>::CODE, 1_000_303);
        assert_eq!(<ForkPut as Command>::CODE, 1_000_304);
    }
}
