use crate::error::{LaserError, TransportError, decode_managed_reply};
use crate::iggy::prelude::Identifier;
use crate::laser::Laser;
use crate::nodes::{Target, is_route_stale, partition_primary};
use crate::retry::{
    FILTER_MUTATION_ATTEMPTS as MUTATION_ATTEMPTS,
    FILTER_MUTATION_BACKOFF as MUTATION_RETRY_BACKOFF,
};
#[cfg(feature = "filters")]
use laser_wire::codes::AGDX_LIST_FILTER_BINDINGS_CODE;
use laser_wire::codes::{
    AGDX_FILTER_MUTATE_CODE, AGDX_FILTER_OPERATION_CODE, AGDX_FILTER_PREVIEW_CODE,
    AGDX_FILTER_TEST_CODE, AGDX_GET_FILTER_BINDING_CODE, AGDX_LIST_FILTER_REVISIONS_CODE,
    AGDX_LIST_FILTERS_CODE, FILTER_OP_VERSION,
};
use laser_wire::filter::{
    CatalogPosition, ConsumerFilter, FilterBinding, FilterCatalogOutcome, FilterCatalogReply,
    FilterError, FilterErrorReason, FilterGroupIdentity, FilterGroupRef, FilterHeader,
    FilterMutation, FilterMutationOutcome, FilterMutationRequest, FilterMutationResult,
    FilterMutationStatus, FilterOutcome, FilterPreview, FilterPreviewRequest, FilterRef,
    FilterReply, FilterRevisionPage, FilterSource, FilterSummary, FilterTestRequest,
    FilterTestResult, GetFilterBinding, GetFilterOperation, GroupFilterSpec, ListFilterRevisions,
    ListFilters,
};
#[cfg(feature = "filters")]
use laser_wire::filter::{FilterBindingPage, ListFilterBindings};
use laser_wire::framing::encode_named;
use laser_wire::limits::{MAX_FILTER_PREVIEW_EXAMINED, MAX_FILTER_PREVIEW_RECORDS};
use laser_wire::validate::Validate;
use serde::Serialize;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const DEFAULT_PREVIEW_RECORDS: u32 = 20;
const OUTCOME_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// How long the typed catalog verbs wait for a mutation's authoritative
/// outcome before they report it as ambiguous.
pub const DEFAULT_OUTCOME_WAIT: Duration = crate::retry::FILTER_OUTCOME_WAIT;

impl Laser {
    /// The catalog and native filter commands behind a consumer group's
    /// filter handle. Native commands need a server that advertises consumer
    /// filters. The catalog also needs a managed backend. Either missing answers
    /// `LaserError::Unsupported`.
    pub(crate) fn filters(&self) -> Filters<'_> {
        Filters { laser: self }
    }
}

/// The catalog and native filter commands. Reached through
/// [`ConsumerGroup::filter`](crate::stream::ConsumerGroup::filter).
pub(crate) struct Filters<'a> {
    laser: &'a Laser,
}

/// A mutation's recorded result and the control-log position it was applied
/// at.
pub(crate) struct AppliedMutation {
    pub(crate) result: FilterMutationResult,
    pub(crate) catalog_position: Option<CatalogPosition>,
}

impl Filters<'_> {
    /// Evaluate `filter` against one supplied record and explain the verdict.
    /// Nothing is read from a stream and nothing is stored.
    pub async fn test(
        &self,
        filter: FilterRef,
        payload: impl Into<Vec<u8>>,
        headers: Vec<FilterHeader>,
    ) -> Result<FilterTestResult, LaserError> {
        let request = FilterTestRequest {
            v: FILTER_OP_VERSION,
            filter,
            payload: payload.into(),
            headers,
        };
        request.validate()?;
        match self.native(AGDX_FILTER_TEST_CODE, &request).await? {
            FilterOutcome::Tested(result) => Ok(result),
            _ => Err(unexpected("test")),
        }
    }

    /// Preview `filter` over the stored records of one partition. A preview
    /// joins no group, stores no offset, and changes no consumer state.
    pub fn preview(
        &self,
        stream: impl Into<String>,
        topic: impl Into<String>,
        partition_id: u32,
        filter: FilterRef,
    ) -> FilterPreviewBuilder {
        FilterPreviewBuilder {
            laser: self.laser.clone(),
            request: FilterPreviewRequest {
                v: FILTER_OP_VERSION,
                source: FilterSource {
                    stream: stream.into(),
                    topic: topic.into(),
                },
                partition_id,
                filter,
                from_offset: 0,
                max_examined: MAX_FILTER_PREVIEW_EXAMINED,
                max_records: DEFAULT_PREVIEW_RECORDS.min(MAX_FILTER_PREVIEW_RECORDS),
                explain: false,
            },
        }
    }

    /// One page of a filter's revisions, newest first. `page` counts from 1
    /// and `results` is the page length.
    pub async fn revisions(
        &self,
        filter_id: u32,
        page: u32,
        results: u32,
    ) -> Result<FilterRevisionPage, LaserError> {
        let request = ListFilterRevisions {
            v: FILTER_OP_VERSION,
            filter_id,
            page,
            results,
        };
        request.validate()?;
        match self
            .catalog(AGDX_LIST_FILTER_REVISIONS_CODE, &request)
            .await?
        {
            FilterCatalogOutcome::Revisions(revisions) => Ok(revisions),
            _ => Err(unexpected("revisions")),
        }
    }

    /// The own filter of one consumer group incarnation, bound or released,
    /// `None` when it has none.
    pub(crate) async fn owned(
        &self,
        owner: FilterGroupIdentity,
    ) -> Result<Option<FilterSummary>, LaserError> {
        let request = ListFilters {
            v: FILTER_OP_VERSION,
            name_contains: None,
            state: None,
            owner: Some(owner),
            page: 1,
            results: 1,
        };
        request.validate()?;
        match self.catalog(AGDX_LIST_FILTERS_CODE, &request).await? {
            FilterCatalogOutcome::Filters(filters) => {
                Ok(filters.items.into_iter().find(|summary| {
                    summary
                        .owner
                        .as_ref()
                        .is_some_and(|own| own.identity == owner)
                }))
            }
            _ => Err(unexpected("filters")),
        }
    }

    /// Drop a saved filter. The catalog releases every group bound to it and
    /// returns the control-log position the drop was applied at.
    pub(crate) async fn drop_filter(
        &self,
        filter_id: u32,
    ) -> Result<Option<CatalogPosition>, LaserError> {
        let applied = self.apply(FilterMutation::Drop { filter_id }).await?;
        match applied.result {
            FilterMutationResult::Dropped { .. } => Ok(applied.catalog_position),
            _ => Err(unexpected("drop")),
        }
    }

    /// The binding of one consumer group. `NotFound` when the group is unbound.
    pub async fn binding(&self, group: FilterGroupRef) -> Result<FilterBinding, LaserError> {
        let request = GetFilterBinding {
            v: FILTER_OP_VERSION,
            group,
            identity: None,
        };
        request.validate()?;
        match self.catalog(AGDX_GET_FILTER_BINDING_CODE, &request).await? {
            FilterCatalogOutcome::Binding(binding) => Ok(binding),
            _ => Err(unexpected("binding")),
        }
    }

    /// One page of bindings, optionally narrowed to one filter, one stream,
    /// or one stream and topic. `page` counts from 1 and `results` is the page
    /// length.
    #[cfg(feature = "filters")]
    pub async fn bindings(
        &self,
        filter_id: Option<u32>,
        stream: Option<&str>,
        topic: Option<&str>,
        page: u32,
        results: u32,
    ) -> Result<FilterBindingPage, LaserError> {
        let request = ListFilterBindings {
            v: FILTER_OP_VERSION,
            filter_id,
            stream: stream.map(str::to_owned),
            topic: topic.map(str::to_owned),
            page,
            results,
        };
        request.validate()?;
        match self
            .catalog(AGDX_LIST_FILTER_BINDINGS_CODE, &request)
            .await?
        {
            FilterCatalogOutcome::Bindings(bindings) => Ok(bindings),
            _ => Err(unexpected("bindings")),
        }
    }

    /// The recorded outcome of a mutation. `NotFound` until the catalog has
    /// applied it.
    pub async fn operation(&self, operation_id: u128) -> Result<FilterMutationOutcome, LaserError> {
        let request = GetFilterOperation {
            v: FILTER_OP_VERSION,
            operation_id,
        };
        match self.catalog(AGDX_FILTER_OPERATION_CODE, &request).await? {
            FilterCatalogOutcome::Mutation(outcome) => checked_outcome(operation_id, outcome),
            _ => Err(unexpected("operation")),
        }
    }

    /// Give `group` its policy in one catalog transaction: a definition saved
    /// as the group's own filter. A repeat with the same digest keeps the
    /// binding, another digest conflicts. The
    /// returned position is where the fold applied it, carried by the group's
    /// reads so they never see this configuration as absent.
    pub(crate) async fn configure_group(
        &self,
        group: FilterGroupRef,
        policy: GroupFilterSpec,
        operation_id: Option<u128>,
        expected_identity: FilterGroupIdentity,
    ) -> Result<(FilterBinding, Option<CatalogPosition>), LaserError> {
        let operation_id = operation_id.unwrap_or_else(|| u128::from(ulid::Ulid::generate()));
        let GroupFilterSpec::Definition { name, filter } = policy;
        let policy = GroupFilterSpec::Definition {
            name,
            filter: self.scoped_filter(filter),
        };
        let applied = self
            .apply_positioned(
                operation_id,
                FilterMutation::ConfigureGroup {
                    group,
                    policy,
                    expected_identity: Some(expected_identity),
                },
            )
            .await?;
        match applied.result {
            FilterMutationResult::Bound(binding) => Ok((binding, applied.catalog_position)),
            _ => Err(unexpected("configure_group")),
        }
    }

    /// Pause or resume a saved revision. Executable content and its digest
    /// stay unchanged, and records already delivered can still be acknowledged.
    pub async fn set_revision_enabled(
        &self,
        filter_id: u32,
        revision: u32,
        enabled: bool,
    ) -> Result<(), LaserError> {
        self.apply(FilterMutation::SetRevisionEnabled {
            filter_id,
            revision,
            enabled,
        })
        .await
        .map(|_| ())
    }

    /// Release this exact saved binding, including a group incarnation that
    /// was deleted and recreated under the same name, and return the
    /// control-log position the release was applied at.
    pub(crate) async fn release_group(
        &self,
        binding: FilterBinding,
    ) -> Result<(FilterBinding, Option<CatalogPosition>), LaserError> {
        let applied = self
            .apply_positioned(
                u128::from(ulid::Ulid::generate()),
                FilterMutation::Unbind {
                    group: binding.group,
                    expected_digest: binding.digest,
                    expected_identity: Some(binding.identity),
                },
            )
            .await?;
        match applied.result {
            FilterMutationResult::Unbound(binding) => Ok((binding, applied.catalog_position)),
            _ => Err(unexpected("unbind")),
        }
    }

    // A filter with writer schemas resolves them in the registry this handle
    // registers schemas in, unless the caller named one.
    fn scoped_filter(&self, mut filter: ConsumerFilter) -> ConsumerFilter {
        if !filter.schema_refs.is_empty() && filter.schema_stream.is_none() {
            filter.schema_stream = self.laser.resource_stream().map(str::to_owned);
        }
        filter
    }

    // Apply `mutation` under a fresh operation id and wait for its outcome.
    async fn apply(&self, mutation: FilterMutation) -> Result<AppliedMutation, LaserError> {
        self.apply_positioned(u128::from(ulid::Ulid::generate()), mutation)
            .await
    }

    async fn apply_positioned(
        &self,
        operation_id: u128,
        mutation: FilterMutation,
    ) -> Result<AppliedMutation, LaserError> {
        let outcome = self.submit(operation_id, mutation).await?;
        let outcome = match outcome.status {
            FilterMutationStatus::Pending => {
                self.wait_outcome(operation_id, DEFAULT_OUTCOME_WAIT)
                    .await?
            }
            _ => outcome,
        };
        Ok(AppliedMutation {
            result: settled(operation_id, outcome.status)?,
            catalog_position: outcome.catalog_position,
        })
    }

    /// Send one mutation under a caller-chosen `operation_id`. The returned
    /// outcome may be `pending`. Retrying with the same id never applies the
    /// mutation twice.
    pub async fn mutate(
        &self,
        operation_id: u128,
        mutation: FilterMutation,
    ) -> Result<FilterMutationOutcome, LaserError> {
        let request = FilterMutationRequest {
            v: FILTER_OP_VERSION,
            operation_id,
            mutation,
        };
        request.validate()?;
        match self.catalog(AGDX_FILTER_MUTATE_CODE, &request).await? {
            FilterCatalogOutcome::Mutation(outcome) => checked_outcome(operation_id, outcome),
            _ => Err(unexpected("mutate")),
        }
    }

    async fn wait_outcome(
        &self,
        operation_id: u128,
        timeout: Duration,
    ) -> Result<FilterMutationOutcome, LaserError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.operation(operation_id).await {
                Ok(FilterMutationOutcome {
                    status: FilterMutationStatus::Pending,
                    ..
                }) => {}
                Ok(outcome) => return Ok(outcome),
                Err(error) if error.is_not_found() || error.is_retryable() => {}
                Err(error) => return Err(error),
            }
            if Instant::now() >= deadline {
                return Err(LaserError::AmbiguousMutation {
                    operation_id: Some(operation_id),
                    message: format!("filter mutation {operation_id} has no outcome yet"),
                });
            }
            sleep(OUTCOME_POLL_INTERVAL).await;
        }
    }

    async fn submit(
        &self,
        operation_id: u128,
        mutation: FilterMutation,
    ) -> Result<FilterMutationOutcome, LaserError> {
        let mut attempt = 1;
        loop {
            match self.mutate(operation_id, mutation.clone()).await {
                Err(error) if error.is_retryable() && attempt < MUTATION_ATTEMPTS => {
                    attempt += 1;
                    sleep(MUTATION_RETRY_BACKOFF).await;
                }
                Err(error) if error.is_retryable() => {
                    return Err(LaserError::AmbiguousMutation {
                        operation_id: Some(operation_id),
                        message: format!("filter mutation {operation_id}: {error}"),
                    });
                }
                result => return result,
            }
        }
    }

    pub(crate) async fn native(
        &self,
        code: u32,
        request: &impl Serialize,
    ) -> Result<FilterOutcome, LaserError> {
        self.native_to(code, request, Target::Caller).await
    }

    // A request a partition's primary no longer takes goes to the caller's
    // node, which serves it from its own copy.
    async fn native_to(
        &self,
        code: u32,
        request: &impl Serialize,
        target: Target,
    ) -> Result<FilterOutcome, LaserError> {
        if !self.laser.capabilities().await.filters.native {
            return Err(LaserError::unsupported(
                "filters",
                "consumer filters are not served by this server",
            ));
        }
        let payload = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode filter request: {error}")))?;
        let reply = match self
            .laser
            .send_managed_to(code, payload.clone(), target)
            .await
        {
            Err(LaserError::Transport(TransportError(error)))
                if target != Target::Caller && is_route_stale(&error) =>
            {
                self.laser
                    .send_managed_to(code, payload, Target::Caller)
                    .await?
            }
            sent => sent?,
        }
        .0;
        match decode_managed_reply::<FilterReply>(&reply)? {
            FilterReply::Ok(outcome) => Ok(outcome),
            FilterReply::Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn catalog_capabilities(&self) -> Result<(), LaserError> {
        let capabilities = self.laser.capabilities().await;
        if !capabilities.filters.catalog {
            return Err(LaserError::unsupported_feature(
                "filters",
                "catalog",
                "the saved-filter catalog is not served by this deployment",
            ));
        }
        if let Some(versions) = capabilities.versions
            && versions.filter != FILTER_OP_VERSION
        {
            return Err(FilterError::new(
                FilterErrorReason::VersionSkew,
                format!(
                    "the catalog serves filter op version {}, this SDK speaks {FILTER_OP_VERSION}",
                    versions.filter
                ),
            )
            .into());
        }
        Ok(())
    }

    async fn catalog(
        &self,
        code: u32,
        request: &impl Serialize,
    ) -> Result<FilterCatalogOutcome, LaserError> {
        self.catalog_capabilities().await?;
        let payload = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode filter request: {error}")))?;
        let reply = self.laser.send_raw_with_response(code, payload).await?;
        match decode_managed_reply::<FilterCatalogReply>(&reply)? {
            FilterCatalogReply::Ok(outcome) => Ok(*outcome),
            FilterCatalogReply::Err(error) => Err(error.into()),
        }
    }
}

/// A preview request. Build it with
/// [`GroupFilter::preview`](crate::stream::GroupFilter::preview).
pub struct FilterPreviewBuilder {
    laser: Laser,
    request: FilterPreviewRequest,
}

impl FilterPreviewBuilder {
    /// The first offset to examine. Defaults to `0`.
    #[must_use]
    pub fn from_offset(mut self, offset: u64) -> Self {
        self.request.from_offset = offset;
        self
    }

    /// Most records to examine.
    #[must_use]
    pub fn max_examined(mut self, count: u32) -> Self {
        self.request.max_examined = count;
        self
    }

    /// Most selected records to return.
    #[must_use]
    pub fn max_records(mut self, count: u32) -> Self {
        self.request.max_records = count;
        self
    }

    /// Explain every returned record, rejected ones included.
    #[must_use]
    pub fn explain(mut self, explain: bool) -> Self {
        self.request.explain = explain;
        self
    }

    pub async fn send(self) -> Result<FilterPreview, LaserError> {
        self.request.validate()?;
        let target = match partition_primary(
            &self.laser.client(),
            &Identifier::named(&self.request.source.stream)?,
            &Identifier::named(&self.request.source.topic)?,
            self.request.partition_id,
        )
        .await
        {
            Ok(replica) => Target::Replica(replica),
            Err(_) => Target::Caller,
        };
        match self
            .laser
            .filters()
            .native_to(AGDX_FILTER_PREVIEW_CODE, &self.request, target)
            .await?
        {
            FilterOutcome::Preview(preview) => Ok(preview),
            _ => Err(unexpected("preview")),
        }
    }
}

fn settled(
    operation_id: u128,
    status: FilterMutationStatus,
) -> Result<FilterMutationResult, LaserError> {
    match status {
        FilterMutationStatus::Applied(result) => Ok(result),
        FilterMutationStatus::Rejected(error) => Err(error.into()),
        FilterMutationStatus::Pending => Err(LaserError::AmbiguousMutation {
            operation_id: Some(operation_id),
            message: format!("filter mutation {operation_id} has no outcome yet"),
        }),
    }
}

// An outcome is the requested operation's only when its version and every
// operation id it carries match the request.
fn checked_outcome(
    operation_id: u128,
    outcome: FilterMutationOutcome,
) -> Result<FilterMutationOutcome, LaserError> {
    let position_id = outcome
        .catalog_position
        .and_then(|position| position.operation_id);
    if outcome.v != FILTER_OP_VERSION
        || outcome.operation_id != operation_id
        || position_id.is_some_and(|position_id| position_id != operation_id)
    {
        return Err(LaserError::Protocol(
            "filter mutation outcome does not match the requested operation".to_owned(),
        ));
    }
    Ok(outcome)
}

fn unexpected(verb: &str) -> LaserError {
    LaserError::Protocol(format!("filters {verb}: unexpected reply variant"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::Capabilities;
    use crate::iggy::prelude::IggyClient;

    #[tokio::test]
    async fn given_an_owned_preview_builder_when_spawned_then_should_send_on_its_own_task() {
        let laser = Laser::from_client(IggyClient::default()).with_capabilities(Capabilities::OPEN);
        let preview = tokio::spawn(
            laser
                .filters()
                .preview(
                    "orbit",
                    "fleet",
                    0,
                    FilterRef::Revision {
                        filter_id: 1,
                        revision: 1,
                    },
                )
                .max_examined(0)
                .send(),
        );
        assert!(
            preview
                .await
                .expect("the preview task joins")
                .is_err_and(|error| matches!(error, LaserError::Invalid(_)))
        );
    }

    #[test]
    fn given_an_outcome_for_another_operation_when_checked_then_should_refuse_it() {
        let outcome = |v, operation_id, position_id: Option<u128>| FilterMutationOutcome {
            v,
            operation_id,
            status: FilterMutationStatus::Pending,
            catalog_position: position_id.map(|position_id| CatalogPosition {
                partition_id: 0,
                offset: 4,
                operation_id: Some(position_id),
                revision: None,
            }),
        };
        for (name, mismatched) in [
            ("outcome id", outcome(FILTER_OP_VERSION, 8, None)),
            ("position id", outcome(FILTER_OP_VERSION, 7, Some(8))),
            ("version", outcome(FILTER_OP_VERSION + 1, 7, None)),
        ] {
            let checked = checked_outcome(7, mismatched);
            assert!(
                matches!(&checked, Err(LaserError::Protocol(_))),
                "{name}: {checked:?}"
            );
        }
        let matching = outcome(FILTER_OP_VERSION, 7, Some(7));
        assert_eq!(
            checked_outcome(7, matching.clone()).expect("the matching outcome passes"),
            matching
        );
    }

    #[test]
    fn given_a_pending_outcome_when_settled_then_should_name_the_operation_to_repeat() {
        let refused = settled(7, FilterMutationStatus::Pending);
        assert!(
            matches!(
                refused,
                Err(LaserError::AmbiguousMutation {
                    operation_id: Some(7),
                    ..
                })
            ),
            "{refused:?}"
        );
    }
}
