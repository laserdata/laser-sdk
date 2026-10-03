use crate::error::LaserError;
use crate::filters::FilterPreviewBuilder;
use crate::filters::reader::{FilteredReaderBuilder, SourceIncarnation};
use crate::laser::Laser;
use crate::stream::Topic;
use crate::stream::transport::ConsumerBuilder;
use iggy::prelude::{ConsumerGroupClient, ConsumerGroupDetails, Identifier, IggyError};
use laser_wire::filter::{
    CatalogPosition, ConsumerFilter, FilterBinding, FilterConsumer, FilterError, FilterErrorReason,
    FilterGroupIdentity, FilterGroupRef, FilterHeader, FilterMutation, FilterRef,
    FilterRevisionPage, FilterRevisionRef, FilterSource, FilterTestResult, GroupFilterSpec,
};
use laser_wire::validate::Validate;
use std::sync::{Arc, Mutex};

// Filters whose name contains a group's own filter name: the name is unique,
// so the first page holds it when it exists.
const GROUP_FILTER_LOOKUP_PAGE: u32 = 8;

/// A consumer group by name or by its native numeric id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupTarget {
    Name(String),
    Id(u64),
}

/// One consumer group of a topic. The group owns its filter policy: readers
/// built from this handle run whatever the group is configured with, bound
/// or unbound, and never name a filter themselves. Build it with
/// [`Topic::consumer_group`] or [`Topic::consumer_group_id`]. Handles are
/// free to construct and clone.
#[derive(Clone)]
pub struct ConsumerGroup {
    topic: Topic,
    target: GroupTarget,
    // The control-log position of the last configuration this handle, or a
    // clone of it, performed. Every read built from the handle carries it, so
    // the application never observes its own configuration as absent through
    // another node.
    configured_at: Arc<Mutex<Option<CatalogPosition>>>,
}

/// A consumer group as the server knows it after [`CreateConsumerGroup::build`]
/// or [`ConsumerGroup::info`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerGroupInfo {
    /// The native numeric group id.
    pub id: u32,
    pub name: String,
    /// The exact group incarnation inside its stream and topic incarnations.
    /// A recreated group is another identity and inherits no policy.
    pub identity: FilterGroupIdentity,
    /// The active policy, `None` for an unbound group whose readers receive
    /// every record.
    pub filter: Option<FilterBinding>,
}

impl ConsumerGroup {
    pub(crate) fn new(topic: Topic, target: GroupTarget) -> Self {
        Self {
            topic,
            target,
            configured_at: Arc::new(Mutex::new(None)),
        }
    }

    /// The group name, `None` for a handle addressed by numeric id.
    pub fn name(&self) -> Option<&str> {
        match &self.target {
            GroupTarget::Name(name) => Some(name),
            GroupTarget::Id(_) => None,
        }
    }

    /// The native numeric id, `None` for a handle addressed by name.
    pub const fn id(&self) -> Option<u64> {
        match self.target {
            GroupTarget::Id(id) => Some(id),
            GroupTarget::Name(_) => None,
        }
    }

    pub const fn topic(&self) -> &Topic {
        &self.topic
    }

    /// Create the group, with an optional filter policy configured in the
    /// same call. Idempotent: an existing group is kept, and a repeated
    /// policy with the same digest keeps its binding.
    pub fn create(&self) -> CreateConsumerGroup {
        CreateConsumerGroup {
            group: self.clone(),
            policy: None,
            operation_id: None,
        }
    }

    /// The group's filter policy: configure it, inspect it, draft and pause
    /// revisions, release it, preview and sample-test it.
    pub fn filter(&self) -> GroupFilter {
        GroupFilter {
            group: self.clone(),
        }
    }

    /// Build a live, load-balanced consumer of this group with server-backed
    /// offsets. On a server that resolves group policies the consumer runs
    /// the group's policy, filtered or unfiltered, and acknowledges through
    /// the fenced group contract. On Apache Iggy it is the native group
    /// consumer. The built consumer implements `futures::Stream`.
    pub fn consumer(&self) -> ConsumerBuilder {
        ConsumerBuilder::group(self.clone())
    }

    /// The match-oriented reader of this group's filter policy: pages of
    /// records with their original offsets, an independent scan budget, and
    /// explicit acknowledgments. An unbound group receives every record.
    ///
    /// # Errors
    ///
    /// [`LaserError::NoStream`] when the topic has no stream.
    pub fn reader(&self) -> Result<FilteredReaderBuilder<'_>, LaserError> {
        self.reader_with(FilterRef::Group)
    }

    /// The group as the server knows it: its id, exact identity and active
    /// policy.
    pub async fn info(&self) -> Result<ConsumerGroupInfo, LaserError> {
        let details = self.native().await?;
        let identity = self.identity_of(details.id).await?;
        let filter = if self.laser().capabilities().await.filters.catalog {
            self.filter().get().await?
        } else {
            None
        };
        if filter
            .as_ref()
            .is_some_and(|binding| binding.identity != identity)
        {
            return Err(FilterError::new(
                FilterErrorReason::SourceChanged,
                "the group incarnation changed while its policy was inspected",
            )
            .into());
        }
        Ok(ConsumerGroupInfo {
            id: details.id,
            name: details.name,
            identity,
            filter,
        })
    }

    pub(crate) fn reader_with(
        &self,
        filter: FilterRef,
    ) -> Result<FilteredReaderBuilder<'_>, LaserError> {
        Ok(FilteredReaderBuilder::new(
            self.laser(),
            self.source()?,
            self.selector(),
            filter,
            self.catalog_position(),
        ))
    }

    /// Create the native group by name without touching its policy.
    pub(crate) async fn ensure_native(&self) -> Result<(), LaserError> {
        let GroupTarget::Name(name) = &self.target else {
            return Err(LaserError::Invalid(
                "a consumer group is created by name, address it with consumer_group(name)"
                    .to_owned(),
            ));
        };
        let stream = Identifier::named(self.topic.stream()?)?;
        let topic = Identifier::named(self.topic.name())?;
        match self
            .laser()
            .client()
            .create_consumer_group(&stream, &topic, name)
            .await
        {
            Ok(_) | Err(IggyError::ConsumerGroupNameAlreadyExists(..)) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn laser(&self) -> &Laser {
        self.topic.laser()
    }

    pub(crate) fn selector(&self) -> FilterConsumer {
        match &self.target {
            GroupTarget::Name(name) => FilterConsumer::Group(name.clone()),
            GroupTarget::Id(id) => FilterConsumer::GroupId(*id),
        }
    }

    pub(crate) fn source(&self) -> Result<FilterSource, LaserError> {
        Ok(FilterSource {
            stream: self.topic.stream()?.to_owned(),
            topic: self.topic.name().to_owned(),
        })
    }

    pub(crate) fn catalog_position(&self) -> Option<CatalogPosition> {
        *self
            .configured_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) async fn native_name(&self) -> Result<String, LaserError> {
        match &self.target {
            GroupTarget::Name(name) => Ok(name.clone()),
            GroupTarget::Id(_) => Ok(self.native().await?.name),
        }
    }

    fn remember_position(&self, position: Option<CatalogPosition>) {
        let Some(position) = position else {
            return;
        };
        let mut configured_at = self
            .configured_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if configured_at.is_none_or(|known| {
            position.operation_id.is_some() && known.operation_id != position.operation_id
                || known.partition_id != position.partition_id
                || known.offset < position.offset
        }) {
            *configured_at = Some(position);
        }
    }

    // The names the catalog addresses the group by. A numeric handle reads
    // them from the server.
    async fn group_ref(&self) -> Result<FilterGroupRef, LaserError> {
        let source = self.source()?;
        let group = match &self.target {
            GroupTarget::Name(name) => name.clone(),
            GroupTarget::Id(_) => self.native().await?.name,
        };
        Ok(FilterGroupRef {
            stream: source.stream,
            topic: source.topic,
            group,
        })
    }

    async fn native(&self) -> Result<ConsumerGroupDetails, LaserError> {
        let stream = Identifier::named(self.topic.stream()?)?;
        let topic = Identifier::named(self.topic.name())?;
        let group = match &self.target {
            GroupTarget::Name(name) => Identifier::named(name)?,
            GroupTarget::Id(id) => Identifier::numeric(
                u32::try_from(*id)
                    .map_err(|_| LaserError::Config("consumer group id exceeds 32 bits"))?,
            )?,
        };
        self.laser()
            .client()
            .get_consumer_group(&stream, &topic, &group)
            .await?
            .ok_or_else(|| {
                FilterError::new(
                    FilterErrorReason::NotFound,
                    format!("consumer group {} does not exist", self.selector()),
                )
                .into()
            })
    }

    async fn identity_of(&self, group_id: u32) -> Result<FilterGroupIdentity, LaserError> {
        let source = self.source()?;
        Ok(SourceIncarnation::read(&self.laser().client(), &source)
            .await?
            .identity(u64::from(group_id)))
    }
}

/// Creates a consumer group. Build it with [`ConsumerGroup::create`].
pub struct CreateConsumerGroup {
    group: ConsumerGroup,
    policy: Option<GroupFilterSpec>,
    operation_id: Option<u128>,
}

impl CreateConsumerGroup {
    /// Configure the group with `filter` once it exists. The definition is
    /// saved as the group's own filter.
    #[must_use]
    pub fn filter(mut self, filter: ConsumerFilter) -> Self {
        self.policy = Some(GroupFilterSpec::Definition(filter));
        self
    }

    /// Configure the group with a definition or one of its own revisions.
    #[must_use]
    pub fn policy(mut self, policy: GroupFilterSpec) -> Self {
        self.policy = Some(policy);
        self
    }

    /// The idempotency key of the policy configuration. Record it first to
    /// resume the same configuration after a crash and read its first
    /// outcome.
    #[must_use]
    pub const fn operation_id(mut self, operation_id: u128) -> Self {
        self.operation_id = Some(operation_id);
        self
    }

    /// Create the group, then configure its policy when one was given. A
    /// policy is preflighted against the server's capabilities before the
    /// native group is created. A group that exists with the same policy is
    /// returned unchanged, one that runs another policy is a typed conflict.
    ///
    /// # Errors
    ///
    /// [`LaserError::ConsumerGroupSetup`] when the group exists but its
    /// policy could not be configured. The group is left as it is.
    pub async fn build(self) -> Result<ConsumerGroupInfo, LaserError> {
        let group = self.group;
        if let Some(policy) = &self.policy {
            FilterMutation::ConfigureGroup {
                group: group.group_ref().await?,
                policy: policy.clone(),
                expected_identity: None,
            }
            .validate()?;
            group.laser().filters().catalog_capabilities().await?;
        }
        group.ensure_native().await?;
        let details = group.native().await?;
        let identity = group.identity_of(details.id).await?;
        let filter = match self.policy {
            Some(policy) => match group
                .filter()
                .configure_scoped_as(self.operation_id, policy, identity)
                .await
            {
                Ok(binding) => Some(binding),
                Err(source) => {
                    return Err(LaserError::ConsumerGroupSetup {
                        group_id: details.id,
                        name: details.name,
                        identity,
                        source: Box::new(source),
                    });
                }
            },
            None if group.laser().capabilities().await.filters.catalog => {
                group.filter().get().await?
            }
            None => None,
        };
        Ok(ConsumerGroupInfo {
            id: details.id,
            name: details.name,
            identity,
            filter,
        })
    }
}

/// A consumer group's filter policy. Build it with [`ConsumerGroup::filter`].
/// Every verb needs a managed plane that serves the filter catalog.
pub struct GroupFilter {
    group: ConsumerGroup,
}

impl GroupFilter {
    /// Give the group `filter` as its policy. The definition is saved as the
    /// group's own filter and the group is bound to it in one catalog
    /// transaction. The same digest again keeps the binding. Another digest
    /// on a group that runs a policy is a typed conflict: create a new group
    /// for another policy.
    pub async fn configure(&self, filter: ConsumerFilter) -> Result<FilterBinding, LaserError> {
        self.configure_as(None, GroupFilterSpec::Definition(filter))
            .await
    }

    /// [`configure`](Self::configure) with a definition or one of the group's
    /// own revisions.
    pub async fn configure_with(
        &self,
        policy: GroupFilterSpec,
    ) -> Result<FilterBinding, LaserError> {
        self.configure_as(None, policy).await
    }

    /// [`configure_with`](Self::configure_with) under a caller-chosen
    /// operation id, so a caller that records the id first resumes the same
    /// configuration after a crash and reads its first outcome.
    pub async fn configure_as(
        &self,
        operation_id: Option<u128>,
        policy: GroupFilterSpec,
    ) -> Result<FilterBinding, LaserError> {
        let details = self.group.native().await?;
        let identity = self.group.identity_of(details.id).await?;
        self.configure_scoped_as(operation_id, policy, identity)
            .await
    }

    async fn configure_scoped_as(
        &self,
        operation_id: Option<u128>,
        policy: GroupFilterSpec,
        identity: FilterGroupIdentity,
    ) -> Result<FilterBinding, LaserError> {
        let group = self.group.group_ref().await?;
        let (binding, position) = self
            .group
            .laser()
            .filters()
            .configure_group(group, policy, operation_id, identity)
            .await?;
        if binding.identity != identity {
            return Err(FilterError::new(
                FilterErrorReason::SourceChanged,
                "configured binding identifies another group incarnation",
            )
            .into());
        }
        self.group.remember_position(position);
        Ok(binding)
    }

    /// The active policy, `None` for an unbound group.
    pub async fn get(&self) -> Result<Option<FilterBinding>, LaserError> {
        self.group.laser().filters().catalog_capabilities().await?;
        let details = self.group.native().await?;
        let identity = self.group.identity_of(details.id).await?;
        let group = self.group.group_ref().await?;
        match self.group.laser().filters().binding(group).await {
            Ok(binding) if binding.identity == identity => Ok(Some(binding)),
            Ok(_) => Err(FilterError::new(
                FilterErrorReason::SourceChanged,
                "the group incarnation changed while its policy was inspected",
            )
            .into()),
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// One page of the group's own filter revisions, newest first.
    pub async fn revisions(
        &self,
        page: u32,
        page_size: u32,
    ) -> Result<FilterRevisionPage, LaserError> {
        let active = self.active().await?;
        self.group
            .laser()
            .filters()
            .revisions(active.filter_id, page, page_size)
            .await
    }

    /// Draft a revision of the group's own filter. Readers keep running the
    /// active revision. A different digest requires a separate group.
    /// `expected_revision` must still be the latest.
    pub async fn revise(
        &self,
        expected_revision: u32,
        filter: ConsumerFilter,
    ) -> Result<FilterRevisionRef, LaserError> {
        let active = self.active().await?;
        self.group
            .laser()
            .filters()
            .revise(active.filter_id, expected_revision, filter)
            .await
    }

    /// Pause or resume a revision of the group's own filter. A paused active
    /// revision refuses new reads, while records already delivered can still
    /// be acknowledged.
    pub async fn set_revision_enabled(
        &self,
        revision: u32,
        enabled: bool,
    ) -> Result<(), LaserError> {
        let active = self.active().await?;
        self.group
            .laser()
            .filters()
            .set_revision_enabled(active.filter_id, revision, enabled)
            .await
    }

    /// Delete the group's own filter with every revision. A bound group is
    /// released first, so its consumers receive every record from their next
    /// poll. Nothing of the filter stays in the catalog. `Ok(false)` when the
    /// group has no filter of its own.
    pub async fn delete(&self) -> Result<bool, LaserError> {
        self.group.laser().filters().catalog_capabilities().await?;
        let details = self.group.native().await?;
        let identity = self.group.identity_of(details.id).await?;
        let name = group_filter_name(&identity);
        let filters = self.group.laser().filters();
        let mut before_id = None;
        loop {
            let page = filters
                .list_before(&name, before_id, GROUP_FILTER_LOOKUP_PAGE)
                .await?;
            let next = page.items.last().map(|summary| summary.id);
            let count = page.items.len();
            if let Some(own) = page.items.into_iter().find(|summary| summary.name == name) {
                let position = filters.drop_filter(own.id).await?;
                self.group.remember_position(position);
                return Ok(true);
            }
            let Some(next) = next else {
                return Ok(false);
            };
            if before_id.is_some_and(|before| next >= before) {
                return Err(FilterError::new(
                    FilterErrorReason::CatalogUnavailable,
                    "the catalog lookup cursor did not advance",
                )
                .into());
            }
            if count as u64 >= u64::from(page.total) {
                return Ok(false);
            }
            before_id = Some(next);
        }
    }

    /// Release the group's policy. Its readers then receive every record. A
    /// released group may only be configured with the digest it ran.
    pub async fn release(&self) -> Result<FilterBinding, LaserError> {
        let active = self.active().await?;
        let (released, position) = self.group.laser().filters().release_group(active).await?;
        self.group.remember_position(position);
        Ok(released)
    }

    /// Preview the active policy over the stored records of one partition. A
    /// preview joins no group, stores no offset, and changes no consumer
    /// state.
    pub async fn preview(&self, partition_id: u32) -> Result<FilterPreviewBuilder<'_>, LaserError> {
        let active = self.active().await?;
        let source = self.group.source()?;
        Ok(self.group.laser().filters().preview(
            source.stream,
            source.topic,
            partition_id,
            FilterRef::Revision {
                filter_id: active.filter_id,
                revision: active.revision,
            },
        ))
    }

    /// Evaluate the active policy against one supplied record and explain the
    /// verdict. Nothing is read from a stream and nothing is stored.
    pub async fn test(
        &self,
        payload: impl Into<Vec<u8>>,
        headers: Vec<FilterHeader>,
    ) -> Result<FilterTestResult, LaserError> {
        let active = self.active().await?;
        self.group
            .laser()
            .filters()
            .test(
                FilterRef::Revision {
                    filter_id: active.filter_id,
                    revision: active.revision,
                },
                payload,
                headers,
            )
            .await
    }

    async fn active(&self) -> Result<FilterBinding, LaserError> {
        self.get().await?.ok_or_else(|| {
            FilterError::new(
                FilterErrorReason::NotFound,
                "the consumer group has no filter policy",
            )
            .into()
        })
    }
}

// The catalog names a group's own filter by the ids of the group incarnation
// it was configured for.
fn group_filter_name(identity: &FilterGroupIdentity) -> String {
    format!(
        "group:{}:{}:{}:{}:{}",
        identity.stream_id,
        identity.stream_created_at_micros,
        identity.topic_id,
        identity.topic_created_at_micros,
        identity.group_id
    )
}
