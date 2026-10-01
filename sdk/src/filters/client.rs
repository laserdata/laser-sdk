use crate::error::{LaserError, decode_managed_reply};
use crate::filters::reader::FilteredReaderBuilder;
use crate::laser::Laser;
use laser_wire::codes::{
    AGDX_FILTER_MUTATE_CODE, AGDX_FILTER_OPERATION_CODE, AGDX_FILTER_PREVIEW_CODE,
    AGDX_FILTER_TEST_CODE, AGDX_FILTER_VALIDATE_CODE, AGDX_GET_FILTER_BINDING_CODE,
    AGDX_GET_FILTER_CODE, AGDX_LIST_FILTER_BINDINGS_CODE, AGDX_LIST_FILTER_REVISIONS_CODE,
    AGDX_LIST_FILTERS_CODE, FILTER_OP_VERSION,
};
use laser_wire::filter::{
    ConsumerFilter, FilterBinding, FilterBindingPage, FilterCatalogOutcome, FilterCatalogReply,
    FilterDetail, FilterError, FilterErrorReason, FilterGroupRef, FilterHeader, FilterMutation,
    FilterMutationOutcome, FilterMutationRequest, FilterMutationResult, FilterMutationStatus,
    FilterOutcome, FilterPage, FilterPreview, FilterPreviewRequest, FilterRef, FilterReply,
    FilterRevisionPage, FilterRevisionRef, FilterSource, FilterState, FilterTestRequest,
    FilterTestResult, FilterValidation, GetFilter, GetFilterBinding, GetFilterOperation,
    ListFilterBindings, ListFilterRevisions, ListFilters,
};
use laser_wire::framing::encode_named;
use laser_wire::limits::{MAX_FILTER_PREVIEW_EXAMINED, MAX_FILTER_PREVIEW_RECORDS};
use laser_wire::schema::Digest32;
use laser_wire::validate::Validate;
use serde::Serialize;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const DEFAULT_PAGE_SIZE: u32 = 50;
const DEFAULT_PREVIEW_RECORDS: u32 = 20;
const MUTATION_ATTEMPTS: u32 = 3;
const MUTATION_RETRY_BACKOFF: Duration = Duration::from_millis(200);
const OUTCOME_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// How long the typed catalog verbs wait for a mutation's authoritative
/// outcome before they report it as ambiguous.
pub const DEFAULT_OUTCOME_WAIT: Duration = Duration::from_secs(30);

impl Laser {
    /// Server-side consumer filters: filtered readers, previews, sample tests,
    /// and the saved-filter catalog. Native reads need a server that advertises
    /// consumer filters. The catalog also needs a managed plane. Either missing
    /// answers `LaserError::Unsupported`.
    pub fn filters(&self) -> Filters<'_> {
        Filters { laser: self }
    }
}

/// A handle to consumer filters. Build it with [`Laser::filters`].
pub struct Filters<'a> {
    laser: &'a Laser,
}

impl<'a> Filters<'a> {
    /// A filtered reader over `stream` / `topic`. Pick an independent consumer
    /// and partition, or a consumer group, then a filter and a start.
    pub fn reader(
        &self,
        stream: impl Into<String>,
        topic: impl Into<String>,
    ) -> FilteredReaderBuilder<'a> {
        FilteredReaderBuilder::new(
            self.laser,
            FilterSource {
                stream: stream.into(),
                topic: topic.into(),
            },
        )
    }

    /// Validate and compile `filter` on the server without running it.
    pub async fn validate(&self, filter: &ConsumerFilter) -> Result<FilterValidation, LaserError> {
        filter.validate()?;
        match self.native(AGDX_FILTER_VALIDATE_CODE, filter).await? {
            FilterOutcome::Validated(validation) => Ok(validation),
            _ => Err(unexpected("validate")),
        }
    }

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

    /// One saved filter with its latest revision and bindings.
    pub async fn get(&self, filter_id: u32) -> Result<FilterDetail, LaserError> {
        let request = GetFilter {
            v: FILTER_OP_VERSION,
            filter_id,
        };
        match self.catalog(AGDX_GET_FILTER_CODE, &request).await? {
            FilterCatalogOutcome::Filter(detail) => Ok(detail),
            _ => Err(unexpected("get")),
        }
    }

    /// Saved filters, newest first. Narrow and page with the returned builder.
    pub fn list(&self) -> FilterList<'a> {
        FilterList {
            filters: Filters { laser: self.laser },
            request: ListFilters {
                v: FILTER_OP_VERSION,
                name_contains: None,
                state: None,
                before_id: None,
                page: 0,
                page_size: DEFAULT_PAGE_SIZE,
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

    /// Save a new filter as revision 1.
    pub async fn register(
        &self,
        name: impl Into<String>,
        filter: ConsumerFilter,
        description: impl Into<String>,
    ) -> Result<FilterRevisionRef, LaserError> {
        let mutation = FilterMutation::Register {
            name: name.into(),
            description: description.into(),
            filter,
        };
        match self.apply(mutation).await? {
            FilterMutationResult::Registered(revision) => Ok(revision),
            _ => Err(unexpected("register")),
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
        match self.apply(mutation).await? {
            FilterMutationResult::Revised(revision) => Ok(revision),
            _ => Err(unexpected("revise")),
        }
    }

    /// Replace a filter's description. Its revisions are unchanged.
    pub async fn describe(
        &self,
        filter_id: u32,
        description: impl Into<String>,
    ) -> Result<(), LaserError> {
        let mutation = FilterMutation::Describe {
            filter_id,
            description: description.into(),
        };
        self.apply(mutation).await.map(|_| ())
    }

    /// Hide a filter from new bindings. Existing bindings keep executing.
    pub async fn archive(&self, filter_id: u32) -> Result<(), LaserError> {
        self.apply(FilterMutation::Archive { filter_id })
            .await
            .map(|_| ())
    }

    /// Delete a filter. Its id and name are never reused. `Conflict` while any
    /// consumer group is bound to it.
    pub async fn delete(&self, filter_id: u32) -> Result<(), LaserError> {
        self.apply(FilterMutation::Drop { filter_id })
            .await
            .map(|_| ())
    }

    /// Pin an existing consumer group to one revision. Every filtered read of
    /// the group then runs that revision.
    pub async fn bind(
        &self,
        group: FilterGroupRef,
        filter_id: u32,
        revision: u32,
    ) -> Result<FilterBinding, LaserError> {
        let mutation = FilterMutation::Bind {
            group,
            filter_id,
            revision,
        };
        match self.apply(mutation).await? {
            FilterMutationResult::Bound(binding) => Ok(binding),
            _ => Err(unexpected("bind")),
        }
    }

    /// Create the group if absent, then bind it to a saved revision. A failed
    /// bind can leave an unbound group. Retrying preserves an existing binding.
    pub async fn create_consumer_group(
        &self,
        group: FilterGroupRef,
        filter_id: u32,
        revision: u32,
    ) -> Result<FilterBinding, LaserError> {
        self.catalog_capabilities().await?;
        self.laser
            .stream(&group.stream)
            .topic(&group.topic)
            .ensure_consumer_group(&group.group)
            .await?;
        self.bind(group, filter_id, revision).await
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

    /// Release a consumer group, only while it is still bound to
    /// `expected_digest`.
    pub async fn unbind(
        &self,
        group: FilterGroupRef,
        expected_digest: Digest32,
    ) -> Result<FilterBinding, LaserError> {
        let mutation = FilterMutation::Unbind {
            group,
            expected_digest,
            expected_identity: None,
        };
        match self.apply(mutation).await? {
            FilterMutationResult::Unbound(binding) => Ok(binding),
            _ => Err(unexpected("unbind")),
        }
    }

    /// Release this exact saved binding, including a group incarnation that
    /// was deleted and recreated under the same name.
    pub async fn unbind_binding(
        &self,
        binding: FilterBinding,
    ) -> Result<FilterBinding, LaserError> {
        match self
            .apply(FilterMutation::Unbind {
                group: binding.group,
                expected_digest: binding.digest,
                expected_identity: Some(binding.identity),
            })
            .await?
        {
            FilterMutationResult::Unbound(binding) => Ok(binding),
            _ => Err(unexpected("unbind")),
        }
    }

    /// Submit a mutation under a new operation id and wait for its
    /// authoritative outcome. A lost reply is retried under the same id, which
    /// the catalog answers with the first attempt's outcome. A rejection is a
    /// typed `LaserError::Filter`. An outcome still pending after
    /// [`DEFAULT_OUTCOME_WAIT`] is `LaserError::AmbiguousMutation`. To recover
    /// such an outcome, or to resume the same mutation after a restart, use
    /// [`apply_as`](Self::apply_as) with an id you record first.
    pub async fn apply(
        &self,
        mutation: FilterMutation,
    ) -> Result<FilterMutationResult, LaserError> {
        self.apply_as(u128::from(ulid::Ulid::generate()), mutation)
            .await
    }

    /// [`apply`](Self::apply) under a caller-chosen `operation_id`, so a
    /// caller that records the id first can resume the same mutation after a
    /// crash.
    pub async fn apply_as(
        &self,
        operation_id: u128,
        mutation: FilterMutation,
    ) -> Result<FilterMutationResult, LaserError> {
        let outcome = self.submit(operation_id, mutation).await?;
        let status = match outcome.status {
            FilterMutationStatus::Pending => {
                return self
                    .wait_for_outcome(operation_id, DEFAULT_OUTCOME_WAIT)
                    .await;
            }
            status => status,
        };
        settled(operation_id, status)
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

    /// Wait for the outcome of `operation_id` for at most `timeout`.
    pub async fn wait_for_outcome(
        &self,
        operation_id: u128,
        timeout: Duration,
    ) -> Result<FilterMutationResult, LaserError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.operation(operation_id).await {
                Ok(FilterMutationOutcome {
                    status: FilterMutationStatus::Pending,
                    ..
                }) => {}
                Ok(outcome) => return settled(operation_id, outcome.status),
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

    async fn catalog_capabilities(&self) -> Result<(), LaserError> {
        let capabilities = self.laser.capabilities().await;
        if !capabilities.filters.native || !capabilities.filters.catalog {
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

/// A saved-filter list request. Build it with [`Filters::list`].
pub struct FilterList<'a> {
    filters: Filters<'a>,
    request: ListFilters,
}

impl FilterList<'_> {
    /// Only filters whose name contains `text`.
    #[must_use]
    pub fn name_contains(mut self, text: impl Into<String>) -> Self {
        self.request.name_contains = Some(text.into());
        self
    }

    /// Only filters in `state`.
    #[must_use]
    pub fn state(mut self, state: FilterState) -> Self {
        self.request.state = Some(state);
        self
    }

    /// Only filters with an id below `filter_id`. Pass the last id of the
    /// previous page, with page 0, to page stably while the catalog changes.
    #[must_use]
    pub const fn before(mut self, filter_id: u32) -> Self {
        self.request.before_id = Some(filter_id);
        self
    }

    /// The zero-based page and its size.
    #[must_use]
    pub fn page(mut self, page: u32, page_size: u32) -> Self {
        self.request.page = page;
        self.request.page_size = page_size;
        self
    }

    pub async fn send(self) -> Result<FilterPage, LaserError> {
        self.request.validate()?;
        match self
            .filters
            .catalog(AGDX_LIST_FILTERS_CODE, &self.request)
            .await?
        {
            FilterCatalogOutcome::Filters(page) => Ok(page),
            _ => Err(unexpected("list")),
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
