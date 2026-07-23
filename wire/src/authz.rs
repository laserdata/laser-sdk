use crate::codes::*;
use crate::error::InvalidError;
use crate::limits::MAX_ROLE_NAME_BYTES;
use serde::{Deserialize, Serialize};

/// Whether a grant permits or forbids. `Deny` always wins over `Allow`.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Effect {
    #[default]
    Allow,
    Deny,
}

/// The managed surface a grant applies to. Maps to the command bands, so a grant
/// on `Kv` is orthogonal to one on `Projection`.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Feature {
    Kv,
    Memory,
    Projection,
    Fork,
    Graph,
    Query,
    Agent,
    Workflow,
    /// Administration of the authorization layer itself (defining roles and
    /// binding them). Gated by `authz:admin`, never derived from a command code.
    Authz,
    /// A feature name a newer peer used that this build does not know. An unknown
    /// `feature` string decodes here instead of failing the whole grant set, and
    /// it matches no request (requests only ever carry a known feature), so an
    /// unrecognized capability is inert: default-deny, never a silent allow.
    /// Displays as `unrecognized` (a Display that panicked would let one foreign
    /// grant crash any UI that renders a grant set), and the string parses back
    /// into this same deny sink.
    #[serde(other)]
    Unrecognized,
}

/// The verb a grant permits, derived from the command code by [`feature_action`].
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Action {
    Read,
    Write,
    Delete,
    Admin,
    /// An action name a newer peer used that this build does not know. Decodes
    /// here rather than failing the grant set, and matches no request, so an
    /// unrecognized action is inert: default-deny. Displays as `unrecognized`
    /// and parses back into this same deny sink, never a panic.
    #[serde(other)]
    Unrecognized,
}

/// How a [`ResourcePattern`] matches a request's resource selector.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResourceKind {
    /// The whole feature, ignoring `value` (the absent-pattern default).
    #[default]
    All,
    /// One exact resource name.
    Literal,
    /// Every resource name under a prefix.
    Prefix,
}

/// A resource selector on a grant: literal, prefixed, or the whole feature.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourcePattern {
    #[serde(default)]
    pub kind: ResourceKind,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
}

impl ResourcePattern {
    /// The whole-feature pattern.
    pub fn all() -> Self {
        Self::default()
    }

    /// An exact-name pattern.
    pub fn literal(value: impl Into<String>) -> Self {
        Self {
            kind: ResourceKind::Literal,
            value: value.into(),
        }
    }

    /// A prefix pattern (every name under `value`).
    pub fn prefix(value: impl Into<String>) -> Self {
        Self {
            kind: ResourceKind::Prefix,
            value: value.into(),
        }
    }

    /// Whether `resource` (the selector decoded from a request) matches. An
    /// unkeyed request (`None`) matches only a whole-feature pattern.
    pub fn matches(&self, resource: Option<&str>) -> bool {
        match (self.kind, resource) {
            (ResourceKind::All, _) => true,
            (ResourceKind::Literal, Some(r)) => r == self.value,
            (ResourceKind::Prefix, Some(r)) => r.starts_with(&self.value),
            (_, None) => false,
        }
    }
}

/// One capability grant: an effect on a `feature:action`, optionally scoped to a
/// resource pattern.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub effect: Effect,
    pub feature: Feature,
    pub action: Action,
    #[serde(default)]
    pub resource: ResourcePattern,
}

/// A named set of grants, bound to users. A user's effective capability is the
/// union of the grants of every bound role, minus any matching deny.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub grants: Vec<Grant>,
}

/// The roles bound to one user (by the server-stamped `user_id`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleBinding {
    pub user_id: u32,
    pub roles: Vec<String>,
}

/// The canonical role-name rule, shared by the SDK, the server, and the
/// console so a name accepted by one tier is never rejected by the next. A
/// valid name is non-empty, at most [`MAX_ROLE_NAME_BYTES`] bytes, and made
/// only of ASCII letters, digits, `-`, `_`, and `.`. Enforced on define and
/// bind, never on replay: a journaled role loads regardless, so tightening the
/// rule cannot strand existing state.
pub fn validate_role_name(name: &str) -> Result<(), InvalidError> {
    crate::validate::validate_safelisted_name("role name", name, MAX_ROLE_NAME_BYTES)
}

/// The `(feature, action)` a managed command code authorizes against. `None` for
/// a code with no capability semantics (hello, backend hello, client metadata,
/// batch, and the authz band itself), which is gated another way.
pub fn feature_action(code: u32) -> Option<(Feature, Action)> {
    let pair = match code {
        AGDX_QUERY_CODE => (Feature::Query, Action::Read),
        AGDX_GET_PROJECTION_CODE
        | AGDX_LIST_PROJECTIONS_CODE
        | AGDX_GET_SCHEMA_CODE
        | AGDX_LIST_SCHEMAS_CODE
        | AGDX_DECODE_RECORD_CODE => (Feature::Projection, Action::Read),
        AGDX_REGISTER_SCHEMA_CODE => (Feature::Projection, Action::Admin),
        AGDX_KV_GET_CODE | AGDX_KV_SCAN_CODE | AGDX_KV_NAMESPACES_CODE | AGDX_KV_EXISTS_CODE => {
            (Feature::Kv, Action::Read)
        }
        AGDX_KV_SET_CODE
        | AGDX_KV_CAS_CODE
        | AGDX_KV_CAS_FENCED_CODE
        | AGDX_KV_PATCH_CODE
        | AGDX_KV_EXPIRE_CODE
        | AGDX_KV_COPY_CODE
        | AGDX_KV_MOVE_CODE
        | AGDX_KV_LEASE_CODE
        | AGDX_KV_RELEASE_CODE => (Feature::Kv, Action::Write),
        AGDX_KV_DELETE_CODE | AGDX_KV_DELETE_MANY_CODE => (Feature::Kv, Action::Delete),
        AGDX_FORK_LIST_CODE => (Feature::Fork, Action::Read),
        AGDX_FORK_CREATE_CODE | AGDX_FORK_PUT_CODE => (Feature::Fork, Action::Write),
        AGDX_FORK_PROMOTE_CODE => (Feature::Fork, Action::Admin),
        AGDX_FORK_DELETE_CODE => (Feature::Fork, Action::Delete),
        AGDX_GRAPH_QUERY_CODE | AGDX_GRAPH_NEIGHBORS_CODE => (Feature::Graph, Action::Read),
        AGDX_GRAPH_UPSERT_CODE => (Feature::Graph, Action::Write),
        AGDX_AGENT_STATUS_CODE | AGDX_AGENT_LIST_CODE => (Feature::Agent, Action::Read),
        AGDX_AGENT_SUBMIT_CODE => (Feature::Agent, Action::Write),
        AGDX_AGENT_CANCEL_CODE => (Feature::Agent, Action::Delete),
        _ => return None,
    };
    Some(pair)
}

/// The number of [`Action`] variants (including the `Unrecognized` catch-all):
/// the stride of the shared coarse-capability bitmask layout ([`action_index`]).
/// The stride must cover every action so one feature's last action bit never
/// collides with the next feature's first.
pub const ACTION_COUNT: usize = 5;

/// The bit index of a `(feature, action)` in the coarse-capability bitmask, a
/// pure function shared by every enforcer so the fork and the plane cannot drift.
/// `Feature`/`Action` are `VariantArray` enums, so the ordinal is stable per wire
/// revision.
pub fn action_index(feature: Feature, action: Action) -> usize {
    feature as usize * ACTION_COUNT + action as usize
}

// The coarse-capability bitmask every enforcer shares is a `u64`, so every
// `(feature, action)` bit index must fit in 64 bits. Adding features or actions
// past that ceiling is a compile error here, not a silent runtime aliasing of two
// distinct capabilities onto one bit. `ACTION_COUNT` must also stay the true
// `Action` variant count, or `action_index`'s stride would skip or overlap rows.
const _: () = {
    assert!(
        <Action as strum::VariantArray>::VARIANTS.len() == ACTION_COUNT,
        "ACTION_COUNT must equal the number of Action variants"
    );
    assert!(
        <Feature as strum::VariantArray>::VARIANTS.len() * ACTION_COUNT <= 64,
        "authz coarse-capability bitmask overflow: Feature count * ACTION_COUNT exceeds 64 bits"
    );
};

/// Whether `grants` permit `(feature, action)` on `resource`, deny-wins. An
/// empty set permits nothing (there is no allow to match). `resource` is the
/// selector decoded from a request, or `None` for an unkeyed op.
pub fn grants_allow(
    grants: &[Grant],
    feature: Feature,
    action: Action,
    resource: Option<&str>,
) -> bool {
    let mut allowed = false;
    for grant in grants {
        if grant.feature == feature && grant.action == action && grant.resource.matches(resource) {
            match grant.effect {
                Effect::Deny => return false,
                Effect::Allow => allowed = true,
            }
        }
    }
    allowed
}

/// The on-behalf-of check: an agent acting for a user is permitted an op only
/// when both its own grants and the invoking user's grants permit it. The agent
/// can never exceed the user who invoked it (permission intersection).
pub fn delegated_allow(
    agent: &[Grant],
    user: &[Grant],
    feature: Feature,
    action: Action,
    resource: Option<&str>,
) -> bool {
    grants_allow(agent, feature, action, resource) && grants_allow(user, feature, action, resource)
}

/// Request the caller's own effective capabilities.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WhoamiReq {
    pub v: u32,
}

/// The caller's bound roles and their flattened grants.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhoamiReply {
    pub v: u32,
    pub roles: Vec<String>,
    pub grants: Vec<Grant>,
}

/// Request to list roles, optionally filtered. Absent filters list every role,
/// the same bounded-registry browse as `ListProjections`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListRolesReq {
    pub v: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
}

/// Every matching role with its full grant set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListRolesReply {
    pub v: u32,
    pub roles: Vec<Role>,
}

/// Request one role by name.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetRoleReq {
    pub v: u32,
    pub name: String,
}

/// Request one user's bound role names.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetBindingsReq {
    pub v: u32,
    pub user_id: u32,
}

/// One user's bound role names.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingsReply {
    pub v: u32,
    pub roles: Vec<String>,
}

/// Define or replace a role (upsert, carries the full grant set).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DefineRoleReq {
    pub v: u32,
    pub role: Role,
}

/// Delete a role by name.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteRoleReq {
    pub v: u32,
    pub name: String,
}

/// Bind roles to a user (replace the user's whole role set).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BindRolesReq {
    pub v: u32,
    pub user_id: u32,
    pub roles: Vec<String>,
    /// Compare-and-swap precondition: apply only if the binding's current
    /// revision equals this. Absent means an unconditional replace (the prior
    /// behavior), so a caller opts into optimistic concurrency. A mismatch fails
    /// with [`AuthzError::Conflict`]. Skip-none, so a request without it stays
    /// byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_revision: Option<u64>,
}

/// Which authorization subject an [`AuthzHistoryReq`] reads the change log of.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthzSubject {
    /// One role by name.
    Role(String),
    /// One user's bindings.
    Binding { user_id: u32 },
    /// Every authorization change.
    All,
}

/// Read the authorization change history for a subject, paged by revision. The
/// audit surface the first compliance conversation opens: who granted what, when.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthzHistoryReq {
    pub v: u32,
    pub subject: AuthzSubject,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_revision: Option<u64>,
    pub limit: u32,
}

/// What an [`AuthzEvent`] recorded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthzEventKind {
    /// A role was defined or replaced.
    RoleDefined(String),
    /// A role was deleted.
    RoleDeleted(String),
    /// A user's role set was rebound.
    RolesBound { user_id: u32, roles: Vec<String> },
}

/// One recorded authorization change: its revision, who made it, when, and what.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzEvent {
    pub revision: u64,
    pub actor: String,
    pub at_micros: u64,
    pub op: AuthzEventKind,
}

/// A page of authorization change history.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzHistoryReply {
    pub v: u32,
    pub events: Vec<AuthzEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_revision: Option<u64>,
}

/// Reply to any authorization command, shaped per request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AuthzReply {
    /// A mutating command applied (`define_role`, `delete_role`, `bind_roles`).
    Ok,
    /// `whoami`: the caller's effective capabilities.
    Whoami(WhoamiReply),
    /// `list_roles`: every matching role.
    Roles(ListRolesReply),
    /// `get_role`: the role with the requested name, or `None`.
    Role(Option<Role>),
    /// `get_bindings`: one user's bound role names.
    Bindings(BindingsReply),
    /// `history`: a page of the authorization change log.
    History(AuthzHistoryReply),
    Err(AuthzError),
}

/// An authorization command failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[non_exhaustive]
pub enum AuthzError {
    #[error("authz not supported: {0}")]
    Unsupported(String),
    #[error("unauthorized")]
    Unauthorized,
    #[error("unknown role: {0}")]
    UnknownRole(String),
    /// A define or bind named a role that fails [`validate_role_name`].
    #[error("invalid role name: {0}")]
    InvalidName(String),
    /// A compare-and-swap bind lost the precondition: the binding's current
    /// revision is not the one the request expected, so a concurrent admin got
    /// there first. The caller re-reads and retries.
    #[error("revision conflict: current is {current_revision}")]
    Conflict { current_revision: u64 },
    #[error("unsupported authz op version (expected {expected}, got {got})")]
    Version { expected: u32, got: u32 },
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_role_when_round_tripped_then_should_preserve_grants() {
        let role = Role {
            name: "kv-reader".to_string(),
            grants: vec![
                Grant {
                    effect: Effect::Allow,
                    feature: Feature::Kv,
                    action: Action::Read,
                    resource: ResourcePattern::prefix("agent-abc/"),
                },
                Grant {
                    effect: Effect::Deny,
                    feature: Feature::Kv,
                    action: Action::Read,
                    resource: ResourcePattern::literal("agent-abc/secret"),
                },
            ],
        };
        let bytes = encode_named(&role).expect("role serializes");
        let back: Role = decode_named(&bytes).expect("role deserializes");
        assert_eq!(back, role);
    }

    #[test]
    fn given_role_names_when_validated_then_should_enforce_charset_and_length() {
        assert!(validate_role_name("kv-reader").is_ok());
        assert!(validate_role_name("ops.admin_2").is_ok());
        assert!(validate_role_name(&"r".repeat(MAX_ROLE_NAME_BYTES)).is_ok());
        assert!(validate_role_name("").is_err(), "empty");
        assert!(validate_role_name("bad name").is_err(), "space");
        assert!(validate_role_name("rôle").is_err(), "non-ascii");
        assert!(validate_role_name(&"r".repeat(MAX_ROLE_NAME_BYTES + 1)).is_err());
    }

    #[test]
    fn given_a_resource_pattern_when_matched_then_should_honor_its_kind() {
        assert!(ResourcePattern::all().matches(Some("anything")));
        assert!(ResourcePattern::all().matches(None));
        assert!(ResourcePattern::literal("ns").matches(Some("ns")));
        assert!(!ResourcePattern::literal("ns").matches(Some("ns2")));
        assert!(ResourcePattern::prefix("agent-").matches(Some("agent-abc")));
        assert!(!ResourcePattern::prefix("agent-").matches(Some("other")));
        // Unkeyed requests are whole-surface operations, so scoped grants must
        // not widen to them.
        assert!(!ResourcePattern::literal("ns").matches(None));
        assert!(!ResourcePattern::prefix("agent-").matches(None));
    }

    #[test]
    fn given_delegation_when_checked_then_agent_is_intersected_with_the_user() {
        let allow = |feature, action, resource| Grant {
            effect: Effect::Allow,
            feature,
            action,
            resource,
        };
        // Agent may read+write kv anywhere. The user it acts for may only read kv
        // under `shared/`. The intersection permits only what BOTH allow.
        let agent = vec![
            allow(Feature::Kv, Action::Read, ResourcePattern::all()),
            allow(Feature::Kv, Action::Write, ResourcePattern::all()),
        ];
        let user = vec![allow(
            Feature::Kv,
            Action::Read,
            ResourcePattern::prefix("shared/"),
        )];
        assert!(delegated_allow(
            &agent,
            &user,
            Feature::Kv,
            Action::Read,
            Some("shared/x")
        ));
        // Outside the user's prefix: agent alone would allow, the user does not.
        assert!(!delegated_allow(
            &agent,
            &user,
            Feature::Kv,
            Action::Read,
            Some("private/x")
        ));
        // The user cannot write at all, so the agent cannot write on its behalf.
        assert!(!delegated_allow(
            &agent,
            &user,
            Feature::Kv,
            Action::Write,
            Some("shared/x")
        ));
        // An empty grant set permits nothing.
        assert!(!grants_allow(&[], Feature::Kv, Action::Read, None));
    }

    #[test]
    fn given_a_command_code_when_classified_then_should_map_to_feature_and_action() {
        assert_eq!(
            feature_action(AGDX_KV_GET_CODE),
            Some((Feature::Kv, Action::Read))
        );
        assert_eq!(
            feature_action(AGDX_KV_SET_CODE),
            Some((Feature::Kv, Action::Write))
        );
        assert_eq!(
            feature_action(AGDX_KV_DELETE_CODE),
            Some((Feature::Kv, Action::Delete))
        );
        assert_eq!(
            feature_action(AGDX_REGISTER_SCHEMA_CODE),
            Some((Feature::Projection, Action::Admin))
        );
        assert_eq!(
            feature_action(AGDX_QUERY_CODE),
            Some((Feature::Query, Action::Read))
        );
        assert_eq!(
            feature_action(AGDX_GRAPH_UPSERT_CODE),
            Some((Feature::Graph, Action::Write))
        );
        // No capability semantics: hello, batch, and the authz band self-gate.
        assert_eq!(feature_action(AGDX_HELLO_CODE), None);
        assert_eq!(feature_action(AGDX_BATCH_CODE), None);
        assert_eq!(feature_action(AGDX_AUTHZ_WHOAMI_CODE), None);
    }

    #[test]
    fn given_feature_action_pairs_when_indexed_then_should_fit_a_u64_mask() {
        use strum::VariantArray;
        let mut seen = std::collections::HashSet::new();
        for &feature in Feature::VARIANTS {
            for &action in Action::VARIANTS {
                let index = action_index(feature, action);
                assert!(index < 64, "index {index} must fit a u64 mask");
                assert!(seen.insert(index), "index {index} collided");
            }
        }
    }

    #[test]
    fn given_an_unknown_feature_or_action_when_decoded_then_should_be_unrecognized_and_deny() {
        // A grant naming a feature and action a newer peer added decodes to the
        // Unrecognized catch-all rather than failing the whole grant set.
        let json = r#"{"effect":"allow","feature":"quantum","action":"teleport","resource":{"kind":"all"}}"#;
        let grant: Grant =
            serde_json::from_str(json).expect("an unknown feature/action still decodes");
        assert_eq!(grant.feature, Feature::Unrecognized);
        assert_eq!(grant.action, Action::Unrecognized);
        // An unrecognized capability matches no real request (requests are only
        // ever classified into known features and actions by `feature_action`),
        // so it is inert: default-deny, never a silent allow.
        assert!(!grants_allow(&[grant], Feature::Kv, Action::Read, None));
        // Displaying the deny sink must never panic (a panicking Display let one
        // foreign grant crash a UI rendering a grant set) and the string parses
        // back into the same sink.
        assert_eq!(Feature::Unrecognized.to_string(), "unrecognized");
        assert_eq!(Action::Unrecognized.to_string(), "unrecognized");
        assert_eq!("unrecognized".parse(), Ok(Feature::Unrecognized));
        assert_eq!("unrecognized".parse(), Ok(Action::Unrecognized));
    }

    #[test]
    fn given_an_authz_reply_when_round_tripped_then_should_preserve_the_variant() {
        let reply = AuthzReply::Whoami(WhoamiReply {
            v: AUTHZ_OP_VERSION,
            roles: vec!["admin".to_string()],
            grants: vec![Grant {
                effect: Effect::Allow,
                feature: Feature::Kv,
                action: Action::Write,
                resource: ResourcePattern::all(),
            }],
        });
        let bytes = encode_named(&reply).expect("reply serializes");
        let back: AuthzReply = decode_named(&bytes).expect("reply deserializes");
        assert_eq!(back, reply);
    }
}
