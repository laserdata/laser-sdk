use crate::error::{LaserError, decode_managed_reply};
use crate::laser::Laser;
#[cfg(feature = "filters")]
use laser_wire::codes::AGDX_LIST_FILTER_BINDINGS_CODE;
use laser_wire::codes::{
    AGDX_FILTER_MUTATE_CODE, AGDX_FILTER_OPERATION_CODE, AGDX_FILTER_PREVIEW_CODE,
    AGDX_FILTER_TEST_CODE, AGDX_GET_FILTER_BINDING_CODE, AGDX_LIST_FILTER_REVISIONS_CODE,
    AGDX_LIST_FILTERS_CODE, FILTER_OP_VERSION,
};
use laser_wire::filter::{
    CatalogPosition, ConsumerFilter, FilterBinding, FilterCatalogOutcome, FilterCatalogReply,
    FilterError, FilterErrorReason, FilterGroupRef, FilterHeader, FilterMutation,
    FilterMutationOutcome, FilterMutationRequest, FilterMutationResult, FilterMutationStatus,
    FilterOutcome, FilterPage, FilterPreview, FilterPreviewRequest, FilterRef, FilterReply,
    FilterRevisionPage, FilterRevisionRef, FilterSource, FilterTestRequest, FilterTestResult,
    GetFilterBinding, GetFilterOperation, GroupFilterSpec, ListFilterRevisions, ListFilters,
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
const MUTATION_ATTEMPTS: u32 = 3;
const MUTATION_RETRY_BACKOFF: Duration = Duration::from_millis(200);
const OUTCOME_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// How long the typed catalog verbs wait for a mutation's authoritative
/// outcome before they report it as ambiguous.
pub const DEFAULT_OUTCOME_WAIT: Duration = Duration::from_secs(30);

impl Laser {
    /// The catalog and native filter commands behind a consumer group's
    /// filter handle. Native commands need a server that advertises consumer
    /// filters. The catalog also needs a managed plane. Either missing answers
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

impl<'a> Filters<'a> {
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
    ) -> FilterPreviewBuilder<'a> {
        FilterPreviewBuilder {
            filters: Filters { laser: self.laser },
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

    /// One page of a filter's revisions, newest first.
    pub async fn revisions(
        &self,
        filter_id: u32,
        page: u32,
        page_size: u32,
    ) -> Result<FilterRevisionPage, LaserError> {
        let request = ListFilterRevisions {
            v: FILTER_OP_VERSION,
            filter_id,
            page,
            page_size,
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

    /// One page of saved filters whose name contains `name_contains`, newest
    /// first.
    pub(crate) async fn list(
        &self,
        name_contains: &str,
        page: u32,
        page_size: u32,
    ) -> Result<FilterPage, LaserError> {
        let request = ListFilters {
            v: FILTER_OP_VERSION,
            name_contains: Some(name_contains.to_owned()),
            state: None,
            before_id: None,
            page,
            page_size,
        };
        request.validate()?;
        match self.catalog(AGDX_LIST_FILTERS_CODE, &request).await? {
            FilterCatalogOutcome::Filters(filters) => Ok(filters),
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
    /// or one stream and topic.
    #[cfg(feature = "filters")]
    pub async fn bindings(
        &self,
        filter_id: Option<u32>,
        stream: Option<&str>,
        topic: Option<&str>,
        page: u32,
        page_size: u32,
    ) -> Result<FilterBindingPage, LaserError> {
        let request = ListFilterBindings {
            v: FILTER_OP_VERSION,
            filter_id,
            stream: stream.map(str::to_owned),
            topic: topic.map(str::to_owned),
            page,
            page_size,
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
            FilterCatalogOutcome::Mutation(outcome) => Ok(outcome),
            _ => Err(unexpected("operation")),
        }
    }

    /// Add a revision. `expected_revision` must still be the latest one.
    pub async fn revise(
        &self,
        filter_id: u32,
        expected_revision: u32,
        filter: ConsumerFilter,
    ) -> Result<FilterRevisionRef, LaserError> {
        let mutation = FilterMutation::Revise {
            filter_id,
            expected_revision,
            filter,
        };
        match self.apply(mutation).await?.result {
            FilterMutationResult::Revised(revision) => Ok(revision),
            _ => Err(unexpected("revise")),
        }
    }

    /// Give `group` its policy in one catalog transaction: a definition saved
    /// as the group's own filter, or one of its existing revisions. A repeat
    /// with the same digest keeps the binding, another digest conflicts. The
    /// returned position is where the fold applied it, carried by the group's
    /// reads so they never see this configuration as absent.
    pub(crate) async fn configure_group(
        &self,
        group: FilterGroupRef,
        policy: GroupFilterSpec,
        operation_id: Option<u128>,
        expected_identity: laser_wire::filter::FilterGroupIdentity,
    ) -> Result<(FilterBinding, Option<CatalogPosition>), LaserError> {
        let operation_id = operation_id.unwrap_or_else(|| u128::from(ulid::Ulid::generate()));
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
            FilterCatalogOutcome::Mutation(outcome) => Ok(outcome),
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
                return Err(LaserError::AmbiguousMutation(format!(
                    "filter mutation {operation_id} has no outcome yet"
                )));
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
                    return Err(LaserError::AmbiguousMutation(format!(
                        "filter mutation {operation_id}: {error}"
                    )));
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
        if !self.laser.capabilities().await.filters.native {
            return Err(LaserError::unsupported(
                "filters",
                "consumer filters are not served by this server",
            ));
        }
        let payload = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode filter request: {error}")))?;
        let reply = self.laser.send_raw_with_response(code, payload).await?;
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

/// A preview request. Build it with [`Filters::preview`].
pub struct FilterPreviewBuilder<'a> {
    filters: Filters<'a>,
    request: FilterPreviewRequest,
}

impl FilterPreviewBuilder<'_> {
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
        match self
            .filters
            .native(AGDX_FILTER_PREVIEW_CODE, &self.request)
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
        FilterMutationStatus::Pending => Err(LaserError::AmbiguousMutation(format!(
            "filter mutation {operation_id} has no outcome yet"
        ))),
    }
}

fn unexpected(verb: &str) -> LaserError {
    LaserError::Protocol(format!("filters {verb}: unexpected reply variant"))
}
