// One trait pairing each managed command code with its request and reply wire
// types. Not a multi-implementor abstraction (there is one wire contract and one established
// band): a compile-time assertion that the code/type pairings match, giving
// the SDK one generic send path and LaserData Cloud's dispatch table a fixture test
// against the same pairings.

use crate::agent_workflow::{AgentCancel, AgentList, AgentReply, AgentStatusReq, AgentSubmit};
use crate::authz::{
    AuthzReply, BindRolesReq, DefineRoleReq, DeleteRoleReq, GetBindingsReq, GetRoleReq,
    ListRolesReq, WhoamiReq,
};
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

command!(WhoamiReq, AGDX_AUTHZ_WHOAMI_CODE, AuthzReply);
command!(ListRolesReq, AGDX_AUTHZ_LIST_ROLES_CODE, AuthzReply);
command!(GetRoleReq, AGDX_AUTHZ_GET_ROLE_CODE, AuthzReply);
command!(GetBindingsReq, AGDX_AUTHZ_GET_BINDINGS_CODE, AuthzReply);
command!(DefineRoleReq, AGDX_AUTHZ_DEFINE_ROLE_CODE, AuthzReply);
command!(DeleteRoleReq, AGDX_AUTHZ_DELETE_ROLE_CODE, AuthzReply);
command!(BindRolesReq, AGDX_AUTHZ_BIND_ROLES_CODE, AuthzReply);
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
command!(AgentSubmit, AGDX_AGENT_SUBMIT_CODE, AgentReply);
command!(AgentCancel, AGDX_AGENT_CANCEL_CODE, AgentReply);
command!(AgentStatusReq, AGDX_AGENT_STATUS_CODE, AgentReply);
command!(AgentList, AGDX_AGENT_LIST_CODE, AgentReply);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_command_impls_when_compared_then_codes_should_match_the_dictionary() {
        assert_eq!(<WhoamiReq as Command>::CODE, 1_000_100);
        assert_eq!(<ListRolesReq as Command>::CODE, 1_000_101);
        assert_eq!(<GetRoleReq as Command>::CODE, 1_000_102);
        assert_eq!(<GetBindingsReq as Command>::CODE, 1_000_103);
        assert_eq!(<DefineRoleReq as Command>::CODE, 1_000_104);
        assert_eq!(<DeleteRoleReq as Command>::CODE, 1_000_105);
        assert_eq!(<BindRolesReq as Command>::CODE, 1_000_106);
        assert_eq!(<QueryEnvelope as Command>::CODE, 1_000_200);
        assert_eq!(<GetProjection as Command>::CODE, 1_000_210);
        assert_eq!(<ListProjections as Command>::CODE, 1_000_211);
        assert_eq!(<GetSchema as Command>::CODE, 1_000_220);
        assert_eq!(<ListSchemas as Command>::CODE, 1_000_221);
        assert_eq!(<RegisterSchema as Command>::CODE, 1_000_222);
        assert_eq!(<DecodeRecord as Command>::CODE, 1_000_223);
        assert_eq!(<KvGet as Command>::CODE, 1_000_300);
        assert_eq!(<KvSet as Command>::CODE, 1_000_301);
        assert_eq!(<KvScan as Command>::CODE, 1_000_302);
        assert_eq!(<KvDelete as Command>::CODE, 1_000_303);
        assert_eq!(<KvDeleteMany as Command>::CODE, 1_000_304);
        assert_eq!(<KvNamespaces as Command>::CODE, 1_000_305);
        assert_eq!(<ForkCreate as Command>::CODE, 1_000_400);
        assert_eq!(<ForkDelete as Command>::CODE, 1_000_401);
        assert_eq!(<ForkPromote as Command>::CODE, 1_000_402);
        assert_eq!(<ForkList as Command>::CODE, 1_000_403);
        assert_eq!(<ForkPut as Command>::CODE, 1_000_404);
        assert_eq!(<AgentSubmit as Command>::CODE, 1_000_700);
        assert_eq!(<AgentCancel as Command>::CODE, 1_000_701);
        assert_eq!(<AgentStatusReq as Command>::CODE, 1_000_702);
        assert_eq!(<AgentList as Command>::CODE, 1_000_703);
    }
}
