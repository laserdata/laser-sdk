use crate::authz::Grant;
use crate::codes::FILTER_OP_VERSION;
use crate::error::InvalidError;
use crate::filter::expr::{ConsumerFilter, FilterCodec};
use crate::filter::read::{FilterError, validate_name};
use crate::limits::{MAX_FILTER_CATALOG_PAGE, MAX_FILTER_DESCRIPTION_BYTES, MAX_FILTER_NAME_BYTES};
use crate::schema::Digest32;
use crate::validate::{Validate, validate_safelisted_name};
use serde::{Deserialize, Serialize};

/// Lifecycle of a saved filter.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum FilterState {
    /// Can be revised and bound.
    Active,
    /// Hidden from new bindings. Existing bindings keep executing.
    Archived,
    /// A tombstone: the id, name, and revisions are never reused.
    Dropped,
}

/// A consumer group's exact identity, stamped by the streaming server from its
/// own metadata. A recreated stream, topic, or group with the same names gets
/// a different identity, so it never inherits an old binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FilterGroupIdentity {
    pub stream_id: u32,
    pub stream_created_at_micros: u64,
    pub topic_id: u32,
    pub topic_created_at_micros: u64,
    pub group_id: u64,
}

/// A consumer group by name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterGroupRef {
    pub stream: String,
    pub topic: String,
    pub group: String,
}

/// One immutable revision of a saved filter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterRevisionInfo {
    #[serde(default = "revision_enabled")]
    pub enabled: bool,
    pub revision: u32,
    pub digest: Digest32,
    pub filter: ConsumerFilter,
    pub created_at_micros: u64,
}

const fn revision_enabled() -> bool {
    true
}

/// A saved filter as a list row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterSummary {
    pub id: u32,
    pub name: String,
    pub description: String,
    pub state: FilterState,
    pub latest_revision: u32,
    pub latest_digest: Digest32,
    pub codec: FilterCodec,
    /// Consumer groups currently bound to any revision of this filter.
    pub bindings: u32,
    pub created_at_micros: u64,
    pub updated_at_micros: u64,
}

/// A saved filter with its latest revision and bindings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterDetail {
    pub summary: FilterSummary,
    pub latest: FilterRevisionInfo,
    pub bindings: Vec<FilterBinding>,
}

/// A consumer group pinned to one revision. The streaming server applies that
/// revision to every filtered read of the group, and rejects a reader that
/// brings a different filter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterBinding {
    pub group: FilterGroupRef,
    pub identity: FilterGroupIdentity,
    pub filter_id: u32,
    pub revision: u32,
    pub digest: Digest32,
    pub bound_at_micros: u64,
}

/// One catalog change.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterMutation {
    /// Pause or resume new reads without changing the revision's policy.
    SetRevisionEnabled {
        filter_id: u32,
        revision: u32,
        enabled: bool,
    },
    /// Save a new filter as revision 1. The name must be unused.
    Register {
        name: String,
        description: String,
        filter: ConsumerFilter,
    },
    /// Add a revision. `expected_revision` must be the latest one.
    Revise {
        filter_id: u32,
        expected_revision: u32,
        filter: ConsumerFilter,
    },
    /// Replace the description. Executable content is unchanged.
    Describe { filter_id: u32, description: String },
    /// Hide from new bindings.
    Archive { filter_id: u32 },
    /// Tombstone. Rejected while any group is bound.
    Drop { filter_id: u32 },
    /// Pin a consumer group to a revision. The group must exist.
    Bind {
        group: FilterGroupRef,
        filter_id: u32,
        revision: u32,
    },
    /// Release a group, conditional on the digest it is bound to.
    Unbind {
        group: FilterGroupRef,
        expected_digest: Digest32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_identity: Option<FilterGroupIdentity>,
    },
}

/// A catalog mutation with the caller's idempotency key. A retry with the same
/// `operation_id` returns the outcome of the first attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterMutationRequest {
    pub v: u32,
    #[serde(with = "crate::encoding::u128_text")]
    pub operation_id: u128,
    pub mutation: FilterMutation,
}

/// Operational limits captured when the plane accepts a catalog command.
/// Zero disables a limit. Older commands keep the original defaults on replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterCatalogLimits {
    pub max_definitions: u32,
    pub max_tombstones: u32,
    pub max_revisions: u32,
    pub max_group_identities: u32,
    /// Encoded bytes of every stored filter revision together. Bounds the
    /// catalog snapshot, which the count limits alone let grow to gigabytes.
    #[serde(default = "default_max_catalog_bytes")]
    pub max_catalog_bytes: u64,
}

impl Default for FilterCatalogLimits {
    fn default() -> Self {
        Self {
            max_definitions: 1_024,
            max_tombstones: 4_096,
            max_revisions: 256,
            max_group_identities: 4_096,
            max_catalog_bytes: default_max_catalog_bytes(),
        }
    }
}

const fn default_max_catalog_bytes() -> u64 {
    64 * 1024 * 1024
}

/// The mutation the catalog appends to its control log, stamped by the
/// streaming server with the acting user and, for a bind or unbind, the
/// group's verified identity, then by the plane with the caller's grants and,
/// for a registration, the allocated filter id. Applying it is deterministic,
/// so every replica, and every replay of the log, records the same outcome.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterCatalogCommand {
    #[serde(with = "crate::encoding::u128_text")]
    pub operation_id: u128,
    pub actor_user_id: u32,
    pub mutation: FilterMutation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<FilterGroupIdentity>,
    /// The id a registration takes, allocated by the plane before the append
    /// so a replay of any part of the log assigns the same id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_id: Option<u32>,
    /// The grants the mutation is authorized with again when it applies,
    /// against the catalog as it stands at that point of the log.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<Grant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<FilterCatalogLimits>,
}

/// A saved revision, by identity and digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterRevisionRef {
    pub filter_id: u32,
    pub revision: u32,
    pub digest: Digest32,
}

/// What an applied mutation produced.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterMutationResult {
    /// The revision now accepts or refuses new reads.
    RevisionState {
        filter_id: u32,
        revision: u32,
        enabled: bool,
    },
    /// The first immutable revision of a newly registered filter.
    Registered(FilterRevisionRef),
    /// The newly appended immutable revision.
    Revised(FilterRevisionRef),
    /// The filter description was replaced.
    Described { filter_id: u32 },
    /// The filter was archived.
    Archived { filter_id: u32 },
    /// The filter was dropped and its name remains reserved.
    Dropped { filter_id: u32 },
    /// The exact consumer group identity and immutable policy binding.
    Bound(FilterBinding),
    /// The exact binding that was released.
    Unbound(FilterBinding),
}

/// Where a mutation stands. `pending` means it entered the control log but has
/// not been applied yet: read its outcome later by operation id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterMutationStatus {
    Pending,
    Applied(FilterMutationResult),
    Rejected(FilterError),
}

/// The authoritative outcome of one mutation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterMutationOutcome {
    pub v: u32,
    #[serde(with = "crate::encoding::u128_text")]
    pub operation_id: u128,
    pub status: FilterMutationStatus,
}

/// Read one saved filter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetFilter {
    pub v: u32,
    pub filter_id: u32,
}

/// List saved filters, newest first by id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListFilters {
    pub v: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<FilterState>,
    /// Only filters with a lower id. Passing the last id of one page reads the
    /// next page stably, while registrations and drops land in between.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_id: Option<u32>,
    #[serde(default)]
    pub page: u32,
    pub page_size: u32,
}

/// A page of saved filters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterPage {
    pub items: Vec<FilterSummary>,
    pub page: u32,
    pub page_size: u32,
    pub total: u32,
}

/// List one filter's revisions, newest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListFilterRevisions {
    pub v: u32,
    pub filter_id: u32,
    #[serde(default)]
    pub page: u32,
    pub page_size: u32,
}

/// A page of revisions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterRevisionPage {
    pub filter_id: u32,
    pub items: Vec<FilterRevisionInfo>,
    pub page: u32,
    pub page_size: u32,
    pub total: u32,
}

/// Read one consumer group's binding. The streaming server stamps `identity`
/// from the names before it forwards the request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetFilterBinding {
    pub v: u32,
    pub group: FilterGroupRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<FilterGroupIdentity>,
}

/// List bindings, optionally narrowed to one filter or one source.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListFilterBindings {
    pub v: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default)]
    pub page: u32,
    pub page_size: u32,
}

/// A page of bindings.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterBindingPage {
    pub items: Vec<FilterBinding>,
    pub page: u32,
    pub page_size: u32,
    pub total: u32,
}

/// Read a mutation's outcome by its operation id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetFilterOperation {
    pub v: u32,
    #[serde(with = "crate::encoding::u128_text")]
    pub operation_id: u128,
}

/// The policy a filtered read resolves: a saved revision, or a group binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterPolicyRef {
    Revision { filter_id: u32, revision: u32 },
    Binding(FilterGroupIdentity),
}

/// Internal: the streaming server resolves a read's execution policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveFilterPolicy {
    #[serde(default)]
    pub allow_disabled: bool,
    pub v: u32,
    pub policy: FilterPolicyRef,
}

/// Internal: wait until the catalog version differs from `since`, or the wait
/// elapses. A zero wait confirms the current version before a cached read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchFilterCatalog {
    pub v: u32,
    pub since: u64,
    pub max_wait_ms: u32,
}

/// Internal: an opaque catalog version. It changes on each control-log fold
/// and starts with a random token on each plane process to fence old caches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilterCatalogVersion {
    pub v: u32,
    pub version: u64,
}

/// An execution policy: exactly one revision, with its lifecycle state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResolvedFilterPolicy {
    pub filter_id: u32,
    pub revision: u32,
    pub digest: Digest32,
    pub state: FilterState,
    pub filter: ConsumerFilter,
}

/// A successful catalog reply.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterCatalogOutcome {
    Filter(FilterDetail),
    Filters(FilterPage),
    Revisions(FilterRevisionPage),
    Binding(FilterBinding),
    Bindings(FilterBindingPage),
    Mutation(FilterMutationOutcome),
    Policy(ResolvedFilterPolicy),
}

/// The reply to every catalog command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterCatalogReply {
    Ok(Box<FilterCatalogOutcome>),
    Err(FilterError),
}

fn validate_version(v: u32) -> Result<(), InvalidError> {
    if v == FILTER_OP_VERSION {
        Ok(())
    } else {
        Err(InvalidError::new(format!(
            "filter catalog version {v} is not supported, expected {FILTER_OP_VERSION}"
        )))
    }
}

fn validate_page_size(page_size: u32) -> Result<(), InvalidError> {
    if page_size == 0 || page_size > MAX_FILTER_CATALOG_PAGE {
        return Err(InvalidError::new(format!(
            "page_size must be 1..={MAX_FILTER_CATALOG_PAGE}"
        )));
    }
    Ok(())
}

fn validate_description(description: &str) -> Result<(), InvalidError> {
    if description.len() > MAX_FILTER_DESCRIPTION_BYTES {
        return Err(InvalidError::new(format!(
            "description exceeds {MAX_FILTER_DESCRIPTION_BYTES}B"
        )));
    }
    Ok(())
}

impl Validate for FilterGroupRef {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_name("stream", &self.stream)?;
        validate_name("topic", &self.topic)?;
        validate_name("consumer group", &self.group)
    }
}

impl Validate for FilterMutation {
    fn validate(&self) -> Result<(), InvalidError> {
        match self {
            Self::Register {
                name,
                description,
                filter,
            } => {
                validate_safelisted_name("filter name", name, MAX_FILTER_NAME_BYTES)?;
                validate_description(description)?;
                filter.validate()
            }
            Self::Revise { filter, .. } => filter.validate(),
            Self::Describe { description, .. } => validate_description(description),
            Self::Archive { .. } | Self::Drop { .. } => Ok(()),
            Self::SetRevisionEnabled { revision, .. } => {
                if *revision == 0 {
                    Err(InvalidError::new("revision must be positive"))
                } else {
                    Ok(())
                }
            }
            Self::Bind { group, .. } => group.validate(),
            Self::Unbind {
                group,
                expected_digest,
                ..
            } => {
                group.validate()?;
                expected_digest.validate()
            }
        }
    }
}

impl Validate for FilterMutationRequest {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_version(self.v)?;
        if self.operation_id == 0 {
            return Err(InvalidError::new("operation_id must not be zero"));
        }
        self.mutation.validate()
    }
}

impl Validate for ListFilters {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_version(self.v)?;
        validate_page_size(self.page_size)?;
        if let Some(fragment) = &self.name_contains
            && fragment.len() > MAX_FILTER_NAME_BYTES
        {
            return Err(InvalidError::new("name filter exceeds the name cap"));
        }
        Ok(())
    }
}

impl Validate for ListFilterRevisions {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_version(self.v)?;
        validate_page_size(self.page_size)
    }
}

impl Validate for ListFilterBindings {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_version(self.v)?;
        validate_page_size(self.page_size)?;
        if let Some(stream) = &self.stream {
            validate_name("stream", stream)?;
        }
        if let Some(topic) = &self.topic {
            validate_name("topic", topic)?;
        }
        Ok(())
    }
}

impl Validate for GetFilterBinding {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_version(self.v)?;
        self.group.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::expr::FilterExpr;
    use crate::query::CmpOp;

    fn register() -> FilterMutationRequest {
        FilterMutationRequest {
            v: FILTER_OP_VERSION,
            operation_id: 42,
            mutation: FilterMutation::Register {
                name: "sats-safe-mode".to_owned(),
                description: "Satellites entering safe mode".to_owned(),
                filter: ConsumerFilter::json(FilterExpr::pred("table", CmpOp::Eq, "satellites")),
            },
        }
    }

    #[test]
    fn given_a_register_mutation_when_validated_then_should_pass() {
        register().validate().expect("valid");
    }

    #[test]
    fn given_bad_mutations_when_validated_then_should_be_rejected() {
        let mut zero_operation = register();
        zero_operation.operation_id = 0;
        let mut bad_name = register();
        if let FilterMutation::Register { name, .. } = &mut bad_name.mutation {
            *name = "has space".to_owned();
        }
        let unbind = FilterMutationRequest {
            v: FILTER_OP_VERSION,
            operation_id: 1,
            mutation: FilterMutation::Unbind {
                group: FilterGroupRef {
                    stream: "s".to_owned(),
                    topic: "t".to_owned(),
                    group: "g".to_owned(),
                },
                expected_digest: Digest32(vec![1, 2]),
                expected_identity: None,
            },
        };
        for request in [zero_operation, bad_name, unbind] {
            assert!(request.validate().is_err(), "{request:?} must be rejected");
        }
    }

    #[test]
    fn given_list_requests_when_page_size_is_out_of_range_then_should_be_rejected() {
        let list = ListFilters {
            v: FILTER_OP_VERSION,
            name_contains: None,
            state: Some(FilterState::Active),
            before_id: None,
            page: 0,
            page_size: MAX_FILTER_CATALOG_PAGE + 1,
        };
        assert!(list.validate().is_err());
        let list = ListFilters {
            page_size: 20,
            ..list
        };
        list.validate().expect("valid");
    }
}
