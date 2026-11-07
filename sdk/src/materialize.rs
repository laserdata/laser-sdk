use crate::error::LaserError;
use crate::laser::Laser;
use crate::stream::Topic;
use laser_wire::control::{
    ControlCommand, InvalidRecordPolicy, OversizePolicy, Projection, ProjectionBinding,
    ProjectionId, ProjectionKind, RetentionPolicy, SourceSelector,
};
use laser_wire::destination::BackendBinding;
pub use laser_wire::hashing::materialization_operation_id;
use laser_wire::validate::Validate;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout_at};

use laser_wire::browse::ProjectionInfo;

pub use laser_wire::browse::{BindingStatus, FailureClass, MaterializationReadiness, StallReason};

/// The first pause between two status reads of a wait.
pub const MATERIALIZATION_POLL_START: Duration = Duration::from_millis(50);
/// The longest pause between two status reads of a wait. Each pause doubles
/// the one before it up to this cap.
pub const MATERIALIZATION_POLL_MAX: Duration = Duration::from_secs(1);

/// The setup of one topic's materialization, built with
/// [`Topic::materialize`]. [`apply`](Self::apply) registers the projection,
/// applies its binding and returns the [`Materialization`] to wait on.
#[derive(Clone)]
#[must_use = "call .apply().await to set up the materialization"]
pub struct MaterializeRequest {
    topic: Topic,
    index: Option<String>,
    projection_id: Option<String>,
    projection: Option<Projection>,
    backend: Option<BackendBinding>,
    retention: Option<RetentionPolicy>,
    invalid_records: InvalidRecordPolicy,
    oversize: OversizePolicy,
    operation_id: u128,
    explicit_operation: bool,
}

impl Topic {
    /// Start a materialization setup for this topic. Without further setters
    /// it materializes every record whole into an index named after the topic.
    pub fn materialize(&self) -> MaterializeRequest {
        MaterializeRequest {
            topic: self.clone(),
            index: None,
            projection_id: None,
            projection: None,
            backend: None,
            retention: None,
            invalid_records: InvalidRecordPolicy::Block,
            oversize: OversizePolicy::Block,
            operation_id: u128::from(ulid::Ulid::generate()),
            explicit_operation: false,
        }
    }

    /// The [`Materialization`] `apply` returned for the setup whose projection
    /// is `projection_id`, read under this topic's stream. A setup without an
    /// explicit projection id or typed projection uses the index name, which
    /// defaults to the topic name.
    pub fn materialization(
        &self,
        projection_id: impl Into<String>,
    ) -> Result<Materialization, LaserError> {
        let stream = self.stream()?.to_owned();
        Ok(Materialization {
            laser: self.laser().clone().with_default_stream(&stream),
            projection_id: projection_id.into(),
            binding: Some((SourceSelector::new(stream, self.name()), None)),
        })
    }
}

impl MaterializeRequest {
    /// The index the binding writes, the topic name by default.
    pub fn index(mut self, index: impl Into<String>) -> Self {
        self.index = Some(index.into());
        self.changed()
    }
    /// The projection id, the typed projection's id or else the index name by
    /// default.
    pub fn projection_id(mut self, id: impl Into<String>) -> Self {
        self.projection_id = Some(id.into());
        self.changed()
    }
    /// Materialize the fields `projection` extracts instead of whole records.
    pub fn typed(mut self, projection: Projection) -> Self {
        self.projection = Some(projection);
        self.changed()
    }
    /// The backend the index lives on, the deployment default when unset.
    pub fn backend(mut self, backend: BackendBinding) -> Self {
        self.backend = Some(backend);
        self.changed()
    }
    /// How long rows stay. Keyed history keeps them by default.
    pub fn retention(mut self, retention: RetentionPolicy) -> Self {
        self.retention = Some(retention);
        self.changed()
    }
    /// What a record that does not materialize does: hold its partition
    /// (the default) or skip to the dead-letter topic.
    pub fn invalid_records(mut self, policy: InvalidRecordPolicy) -> Self {
        self.invalid_records = policy;
        self.changed()
    }
    /// What a body over the backend limit does: hold its partition (the
    /// default), skip, or keep a reference instead of the body.
    pub fn oversize(mut self, policy: OversizePolicy) -> Self {
        self.oversize = policy;
        self.changed()
    }
    /// Pin the setup identity. Without it every content change mints a fresh
    /// one, so a corrected setup is never refused as a replay of a refused one.
    pub fn operation_id(mut self, id: u128) -> Self {
        self.operation_id = id;
        self.explicit_operation = true;
        self
    }

    /// The setup identity `apply` sends. Reuse the same request after an
    /// uncertain response so the setup applies once.
    pub fn operation(&self) -> u128 {
        self.operation_id
    }

    fn changed(mut self) -> Self {
        if !self.explicit_operation {
            self.operation_id = u128::from(ulid::Ulid::generate());
        }
        self
    }

    pub async fn apply(self) -> Result<Materialization, LaserError> {
        let stream = self.topic.stream()?.to_owned();
        let laser = self.topic.laser().clone().with_default_stream(&stream);
        let capabilities = laser.capabilities().await;
        if !capabilities.managed || !capabilities.materializations {
            return Err(LaserError::unsupported(
                "materializations",
                "materializations are not served by this deployment",
            ));
        }
        let operation_id = self.operation_id;
        let topic = self.topic.name().to_owned();
        let (projection, binding) = self.plan(&stream)?;
        let id = projection.id.as_str().to_owned();
        let index = binding.index.clone();
        let projection = laser.scoped_projection(projection);
        let binding = laser.scoped_binding(binding);
        laser
            .publish_control_with_operation(
                ControlCommand::RegisterProjection(projection),
                Some(&stream),
                materialization_operation_id(operation_id, 1),
            )
            .await?;
        laser
            .publish_control_with_operation(
                ControlCommand::ApplyBinding(binding),
                Some(&stream),
                materialization_operation_id(operation_id, 2),
            )
            .await?;
        Ok(Materialization {
            laser,
            projection_id: id,
            binding: Some((SourceSelector::new(stream, topic), Some(index))),
        })
    }

    // The projection and binding `apply` publishes, refused here on every rule
    // the managed backend checks, so a refused binding never leaves a
    // registered projection behind.
    fn plan(self, stream: &str) -> Result<(Projection, ProjectionBinding), LaserError> {
        if self.operation_id == 0 {
            return Err(LaserError::Invalid(
                "operation id must be nonzero".to_owned(),
            ));
        }
        let index = self.index.unwrap_or_else(|| self.topic.name().to_owned());
        let id = match (self.projection_id, &self.projection) {
            (Some(id), _) => id,
            (None, Some(typed)) => typed.id.as_str().to_owned(),
            (None, None) => index.clone(),
        };
        let mut projection = self
            .projection
            .unwrap_or_else(|| Projection::builder(id.as_str()).record().build());
        if projection.kind == ProjectionKind::Graph {
            return Err(LaserError::Invalid(
                "materialization requires a row or record projection".to_owned(),
            ));
        }
        projection.id = ProjectionId::new(&id);
        if !projection.history.is_append()
            && self.retention.is_some_and(|retention| {
                !matches!(
                    retention,
                    RetentionPolicy::Keep
                        | RetentionPolicy::KeepUntilSourceDeleted
                        | RetentionPolicy::TimeToLive { .. }
                )
            })
        {
            return Err(LaserError::Invalid(
                "keyed history requires Keep, KeepUntilSourceDeleted or TimeToLive retention"
                    .to_owned(),
            ));
        }
        if self.oversize == OversizePolicy::Reference && projection.kind != ProjectionKind::Record {
            return Err(LaserError::Invalid(
                "body references apply only to a whole-record projection".to_owned(),
            ));
        }
        let mut builder = ProjectionBinding::builder()
            .source(stream, self.topic.name())
            .index(&index)
            .allow(id.as_str())
            .default_projection(id.as_str())
            .invalid_records(self.invalid_records)
            .oversize(self.oversize);
        if let Some(backend) = self.backend {
            builder = builder.backend(backend);
        }
        if let Some(retention) = self.retention {
            builder = builder.retention(retention);
        } else if !projection.history.is_append() {
            builder = builder.retention(RetentionPolicy::Keep);
        }
        let binding = builder.build();
        projection.validate()?;
        binding.validate()?;
        Ok((projection, binding))
    }
}

/// A handle to one materialization's progress, from
/// [`MaterializeRequest::apply`], [`Topic::materialization`] or
/// [`Laser::materialization`].
#[derive(Clone)]
pub struct Materialization {
    laser: Laser,
    projection_id: String,
    // The source this handle follows, and the index `apply` bound it to.
    binding: Option<(SourceSelector, Option<String>)>,
}

impl Laser {
    /// Every binding of projection `projection_id`, named under this handle's
    /// default stream. [`Topic::materialization`] reads under the topic's own
    /// stream instead.
    pub fn materialization(&self, projection_id: impl Into<String>) -> Materialization {
        Materialization {
            laser: self.clone(),
            projection_id: projection_id.into(),
            binding: None,
        }
    }
}

impl Materialization {
    /// The status of each binding this handle follows, or `None` when no
    /// projection has the id or, for a handle tied to one source, when that
    /// source is no longer bound to its index.
    pub async fn status(&self) -> Result<Option<Vec<BindingStatus>>, LaserError> {
        Ok(self
            .laser
            .projections()
            .status_info(&self.projection_id)
            .await?
            .and_then(|info| followed(info, self.binding.as_ref())))
    }

    /// Wait until every binding this handle follows reaches `level`, a
    /// record or a gap holds one, realization fails for good, the binding is
    /// gone, or `timeout` passes. Reads back off from
    /// [`MATERIALIZATION_POLL_START`] to [`MATERIALIZATION_POLL_MAX`].
    pub async fn wait(
        &self,
        level: MaterializationReadiness,
        timeout: Duration,
    ) -> Result<MaterializationWait, LaserError> {
        wait_for(level, timeout, || self.status()).await
    }
}

/// How a [`Materialization::wait`] ended.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum MaterializationWait {
    /// Every binding reached the level.
    Ready { status: Vec<BindingStatus> },
    /// A record or a history gap holds a partition.
    Blocked { status: Vec<BindingStatus> },
    /// Creating a destination table failed in a way no retry recovers.
    Failed { status: Vec<BindingStatus> },
    /// No projection has the id, or the followed source is not bound to it.
    NotFound,
    /// The timeout passed first, with the last status read.
    TimedOut { status: Vec<BindingStatus> },
}

// The statuses of the bindings a handle follows, `None` when it follows one
// source and that source is no longer bound to its index.
fn followed(
    info: ProjectionInfo,
    binding: Option<&(SourceSelector, Option<String>)>,
) -> Option<Vec<BindingStatus>> {
    let Some((source, index)) = binding else {
        return Some(info.status);
    };
    let follows = |candidate: &SourceSelector, bound: &str| {
        candidate == source && index.as_deref().is_none_or(|index| index == bound)
    };
    info.bindings
        .iter()
        .any(|binding| follows(&binding.source, &binding.index))
        .then(|| {
            info.status
                .into_iter()
                .filter(|status| follows(&status.source, &status.index))
                .collect()
        })
}

async fn wait_for<Read, Reading>(
    level: MaterializationReadiness,
    timeout: Duration,
    mut read: Read,
) -> Result<MaterializationWait, LaserError>
where
    Read: FnMut() -> Reading,
    Reading: Future<Output = Result<Option<Vec<BindingStatus>>, LaserError>>,
{
    let deadline = Instant::now() + timeout;
    let mut target: Option<Vec<BindingStatus>> = None;
    let mut last = Vec::new();
    let mut pause = MATERIALIZATION_POLL_START;
    loop {
        let statuses = match timeout_at(deadline, read()).await {
            Ok(result) => result?,
            Err(_) => return Ok(MaterializationWait::TimedOut { status: last }),
        };
        let Some(statuses) = statuses else {
            return Ok(MaterializationWait::NotFound);
        };
        if statuses.iter().any(BindingStatus::is_blocked) {
            return Ok(MaterializationWait::Blocked { status: statuses });
        }
        if statuses.iter().any(BindingStatus::is_failed) {
            return Ok(MaterializationWait::Failed { status: statuses });
        }
        // A recreated source no longer holds the heads the target captured.
        if target.as_ref().is_some_and(|targets| {
            targets.iter().any(|wanted| {
                statuses.iter().any(|status| {
                    status.source == wanted.source
                        && status.index == wanted.index
                        && status.source_incarnation.is_some()
                        && status.source_incarnation != wanted.source_incarnation
                })
            })
        }) {
            target = None;
        }
        if target.is_none()
            && !statuses.is_empty()
            && statuses
                .iter()
                .all(|status| status.source_incarnation.is_some())
        {
            target = Some(statuses.clone());
        }
        let ready = !statuses.is_empty()
            && statuses.iter().all(|status| {
                if level <= MaterializationReadiness::Realized {
                    return status.readiness() >= level;
                }
                target.as_ref().is_some_and(|targets| {
                    targets.iter().any(|wanted| {
                        wanted.source == status.source
                            && wanted.index == status.index
                            && status.caught_up_to(wanted)
                    })
                })
            })
            && target.as_ref().is_none_or(|targets| {
                targets.iter().all(|wanted| {
                    statuses.iter().any(|status| {
                        status.source == wanted.source && status.index == wanted.index
                    })
                })
            });
        if ready {
            return Ok(MaterializationWait::Ready { status: statuses });
        }
        last = statuses;
        if timeout_at(deadline, sleep(pause)).await.is_err() {
            return Ok(MaterializationWait::TimedOut { status: last });
        }
        pause = (pause * 2).min(MATERIALIZATION_POLL_MAX);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::Capabilities;
    use crate::iggy::prelude::IggyClient;

    #[test]
    fn given_a_content_change_without_a_pinned_identity_when_set_then_should_mint_a_new_operation()
    {
        let laser = Laser::from_client(IggyClient::default()).with_default_stream("shop");
        let request = laser.topic("orders").materialize();
        let first = request.operation();
        assert_eq!(
            request.clone().operation(),
            first,
            "a reused request keeps it"
        );
        assert_ne!(request.clone().index("orders_rows").operation(), first);
        let pinned = request.operation_id(7).index("orders_rows");
        assert_eq!(pinned.operation(), 7, "a pinned identity survives changes");
    }

    #[test]
    fn given_a_typed_projection_without_a_projection_id_when_planned_then_should_keep_its_id() {
        let laser = Laser::from_client(IggyClient::default()).with_default_stream("shop");
        let typed = Projection::builder("order.v2").field("total").build();
        let (projection, binding) = laser
            .topic("orders")
            .materialize()
            .typed(typed)
            .plan("shop")
            .expect("the setup plans");
        assert_eq!(projection.id.as_str(), "order.v2");
        assert_eq!(
            binding.default_projection,
            Some(ProjectionId::new("order.v2"))
        );
        assert_eq!(binding.index, "orders");
    }

    #[test]
    fn given_a_binding_the_backend_would_refuse_when_planned_then_should_refuse_before_registering()
    {
        let laser = Laser::from_client(IggyClient::default()).with_default_stream("shop");
        let keyed = Projection::builder("order.v1")
            .field("total")
            .keyed(
                laser_wire::control::ProjectionKey::Header {
                    name: "order_id".to_owned(),
                },
                Some("deleted"),
            )
            .build();
        let mirrored = laser
            .topic("orders")
            .materialize()
            .typed(keyed)
            .retention(RetentionPolicy::MirrorLog)
            .plan("shop");
        assert!(matches!(mirrored, Err(LaserError::Invalid(_))));
        let typed = Projection::builder("order.v1").field("total").build();
        let referenced = laser
            .topic("orders")
            .materialize()
            .typed(typed)
            .oversize(OversizePolicy::Reference)
            .plan("shop");
        assert!(matches!(referenced, Err(LaserError::Invalid(_))));
    }

    #[test]
    fn given_a_keyed_projection_without_a_delete_header_when_planned_then_should_register_it_as_built()
     {
        let laser = Laser::from_client(IggyClient::default()).with_default_stream("shop");
        let keyed = Projection::builder("order.v1")
            .field("total")
            .keyed(
                laser_wire::control::ProjectionKey::Header {
                    name: "order_id".to_owned(),
                },
                None,
            )
            .build();
        let (projection, binding) = laser
            .topic("orders")
            .materialize()
            .typed(keyed.clone())
            .plan("shop")
            .expect("a keyed view that never deletes plans");
        assert_eq!(
            (projection, binding.retention),
            (keyed, Some(RetentionPolicy::Keep))
        );
    }

    #[tokio::test]
    async fn given_original_iggy_when_materializing_then_should_refuse_without_a_managed_write() {
        let laser = Laser::from_client(IggyClient::default())
            .with_capabilities(Capabilities::OPEN)
            .with_default_stream("shop");
        assert!(matches!(
            laser.topic("orders").materialize().apply().await,
            Err(LaserError::Unsupported { .. })
        ));
        assert!(matches!(
            laser.materialization("orders").status().await,
            Err(LaserError::Unsupported { .. })
        ));
    }

    #[tokio::test]
    async fn given_injected_materializations_when_materializing_then_should_pass_the_capability_gate()
     {
        let laser = Laser::from_client(IggyClient::default())
            .with_capabilities(Capabilities {
                managed: true,
                materializations: true,
                ..Capabilities::OPEN
            })
            .with_default_stream("shop");
        let refused = laser
            .topic("orders")
            .materialize()
            .operation_id(0)
            .apply()
            .await;
        assert!(
            matches!(refused, Err(LaserError::Invalid(_))),
            "the gate must read the capability set, got {:?}",
            refused.err()
        );
        // A refused gate answers at once, a passed one goes on to send.
        let status = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            laser.materialization("orders").status(),
        )
        .await;
        assert!(
            !matches!(status, Ok(Err(LaserError::Unsupported { .. }))),
            "the status gate must read the capability set, got {status:?}"
        );
    }

    #[tokio::test]
    async fn given_an_absent_projection_when_waiting_then_should_report_not_found() {
        let outcome = wait_for(
            MaterializationReadiness::Realized,
            Duration::from_millis(300),
            || async { Ok(None) },
        )
        .await
        .expect("wait");
        assert_eq!(outcome, MaterializationWait::NotFound);
    }

    #[tokio::test]
    async fn given_a_status_read_refused_as_unauthorized_when_waiting_then_should_end_with_that_error()
     {
        let started = tokio::time::Instant::now();
        let outcome = wait_for(
            MaterializationReadiness::Realized,
            Duration::from_secs(30),
            || async {
                Err(laser_wire::query::QueryError::Unauthorized(
                    "the caller may read no index of projection `orders`".to_owned(),
                )
                .into())
            },
        )
        .await;
        let error = outcome.expect_err("an unauthorized status read ends the wait");
        assert_eq!(error.code(), laser_wire::result::ResultCode::Forbidden);
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the wait must not run out its timeout"
        );
    }

    #[tokio::test]
    async fn given_a_permanent_realization_failure_when_waiting_then_should_report_failed() {
        let mut failed = status(7, 5, None);
        failed.realization = laser_wire::browse::RealizationState::Pending;
        failed.failure = Some(FailureClass::Schema);
        let reported = failed.clone();
        let outcome = wait_for(
            MaterializationReadiness::Realized,
            Duration::from_millis(300),
            || {
                let failed = failed.clone();
                async move { Ok(Some(vec![failed])) }
            },
        )
        .await
        .expect("wait");
        assert_eq!(
            outcome,
            MaterializationWait::Failed {
                status: vec![reported]
            }
        );
        let mut transient = status(7, 5, None);
        transient.realization = laser_wire::browse::RealizationState::Pending;
        transient.failure = Some(FailureClass::Transient);
        let outcome = wait_for(
            MaterializationReadiness::Realized,
            Duration::from_millis(120),
            || {
                let transient = transient.clone();
                async move { Ok(Some(vec![transient])) }
            },
        )
        .await
        .expect("wait");
        assert!(
            matches!(outcome, MaterializationWait::TimedOut { .. }),
            "a transient failure keeps waiting: {outcome:?}"
        );
    }

    #[tokio::test]
    async fn given_a_source_recreated_during_the_wait_when_caught_up_then_should_measure_the_new_incarnation()
     {
        let reads = std::sync::Mutex::new(vec![
            status(8, 3, Some(3)),
            status(8, 3, Some(0)),
            status(7, 5, Some(1)),
        ]);
        let outcome = wait_for(
            MaterializationReadiness::CaughtUp,
            Duration::from_secs(2),
            || {
                let next = {
                    let mut reads = reads.lock().expect("reads");
                    if reads.len() > 1 {
                        reads.pop()
                    } else {
                        reads.first().cloned()
                    }
                };
                async move { Ok(next.map(|status| vec![status])) }
            },
        )
        .await
        .expect("wait");
        assert_eq!(
            outcome,
            MaterializationWait::Ready {
                status: vec![status(8, 3, Some(3))]
            },
            "the heads of the retired incarnation are gone, so the wait measures the live one"
        );
    }

    #[tokio::test]
    async fn given_a_slow_materialization_when_waiting_then_should_back_off_between_reads() {
        let reads = std::sync::atomic::AtomicUsize::new(0);
        let outcome = wait_for(
            MaterializationReadiness::CaughtUp,
            Duration::from_millis(600),
            || {
                reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Ok(Some(vec![status(7, 5, Some(1))])) }
            },
        )
        .await
        .expect("wait");
        assert!(matches!(outcome, MaterializationWait::TimedOut { .. }));
        let reads = reads.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            reads <= 5,
            "pauses of 50, 100, 200 and 400 ms leave at most five reads in 600 ms, got {reads}"
        );
    }

    #[test]
    fn given_a_source_moved_to_another_index_when_followed_then_should_report_no_status() {
        let moved = ProjectionInfo {
            projection: Projection::builder("orders").record().build(),
            bindings: vec![
                ProjectionBinding::builder()
                    .source("shop", "orders")
                    .index("orders_v2")
                    .allow("orders")
                    .default_projection("orders")
                    .build(),
            ],
            status: vec![BindingStatus {
                index: "orders_v2".to_owned(),
                ..status(7, 5, Some(5))
            }],
        };
        let applied = (
            SourceSelector::new("shop", "orders"),
            Some("orders".to_owned()),
        );
        assert_eq!(followed(moved.clone(), Some(&applied)), None);
        let attached = (SourceSelector::new("shop", "orders"), None);
        assert_eq!(
            followed(moved.clone(), Some(&attached)).map(|statuses| statuses.len()),
            Some(1),
            "a re-attached handle follows the source to any index"
        );
        assert_eq!(
            followed(moved, None).map(|statuses| statuses.len()),
            Some(1)
        );
    }

    #[test]
    fn given_a_topic_when_reattaching_then_should_read_under_its_stream_and_follow_its_source() {
        let laser = Laser::from_client(IggyClient::default());
        let attached = laser
            .stream("shop")
            .topic("orders")
            .materialization("orders_rows")
            .expect("a topic of a named stream re-attaches");
        assert_eq!(attached.projection_id, "orders_rows");
        assert_eq!(
            attached.binding,
            Some((SourceSelector::new("shop", "orders"), None))
        );
        assert_eq!(
            attached.laser.resource_name("orders_rows"),
            "stream:shop/orders_rows",
            "the id is scoped to the topic's stream, not the handle's default"
        );
    }

    fn status(incarnation: u64, head: u64, applied: Option<u64>) -> BindingStatus {
        BindingStatus {
            history: Default::default(),
            source: SourceSelector::new("shop", "orders"),
            index: "orders".to_owned(),
            realization: laser_wire::browse::RealizationState::Realized,
            realization_reason: None,
            failure: None,
            stall: None,
            source_incarnation: Some(incarnation),
            partitions: vec![laser_wire::browse::PartitionProgress {
                partition_id: 0,
                head_offset: Some(head),
                applied_offset: applied,
                first_offset: applied.map(|_| 0),
            }],
            holds: Vec::new(),
            accepted_gaps: Vec::new(),
            skipped_records: 0,
        }
    }
}
