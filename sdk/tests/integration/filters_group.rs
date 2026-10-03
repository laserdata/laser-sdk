// Consumer groups that own their filter policy, against the fork with a
// stand-in catalog. The stand-in answers the backend probe and keeps the
// group policies the tests configure: a group's own filter and revisions, its
// binding with a policy generation, and the unbound answer for a group
// without one. It runs on its own thread and runtime because the server
// outlives each test's runtime.

use crate::harness;
use crate::test_iggy::TestIggy;
use laser_sdk::capabilities::{Capabilities, HelloOutcome};
use laser_sdk::filters::{
    CatalogPosition, ConsumerFilter, ExecutionMode, FaultReason, FilterBinding, FilterBindingPage,
    FilterError, FilterErrorReason, FilterExpr, FilterGroupIdentity, FilterGroupRef,
    FilterMutation, FilterMutationOutcome, FilterMutationResult, FilterMutationStatus,
    FilterRevisionInfo, FilterRevisionPage, FilterRevisionRef, FilterState, FilteredStart,
    GroupFilterSpec, GroupPolicyUnbound,
};
use laser_sdk::iggy::prelude::{
    Consumer, ConsumerGroupClient, ConsumerOffsetClient, HeaderKey, HeaderValue, Identifier,
    PartitionClient,
};
use laser_sdk::prelude::full::{ConsumerGroup, Laser, LaserError};
use laser_sdk::prelude::{CommitPolicy, ConsumerStart, ProducerMessage, Routing};
use laser_sdk::query::CmpOp;
use laser_sdk::wire::codes::{
    AGDX_BACKEND_HELLO_CODE, AGDX_FILTER_MUTATE_CODE, AGDX_FILTER_OPERATION_CODE,
    AGDX_GET_FILTER_BINDING_CODE, AGDX_LIST_FILTER_BINDINGS_CODE, AGDX_LIST_FILTER_REVISIONS_CODE,
    AGDX_LIST_FILTERS_CODE, AGDX_RESOLVE_FILTER_POLICY_CODE, CONTROL_OP_VERSION, FILTER_OP_VERSION,
    FORK_OP_VERSION, KV_OP_VERSION, QUERY_OP_VERSION,
};
use laser_sdk::wire::filter::FilterMutationRequest;
use laser_sdk::wire::filter::{
    FilterCatalogCommand, FilterCatalogOutcome, FilterCatalogReply, FilterPage, FilterPolicyRef,
    FilterSummary, GetFilterBinding, ListFilterBindings, ListFilterRevisions, ListFilters,
    ResolveFilterPolicy, ResolvedFilterPolicy,
};
use laser_sdk::wire::forward::ForwardedCommand;
use laser_sdk::wire::framing::{decode_named, encode_named};
use laser_sdk::wire::hello::{BackendAnnounce, OpVersions};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::OnceCell;

const TOPIC: &str = "fleet_changes";
const GROUP: &str = "anomaly-desk";
const READ_TIMEOUT: Duration = Duration::from_secs(15);
const PAGES_PER_PARTITION: u32 = 12;

const SAFE_MODE: &str = r#"{"op":"u","table":"satellites","changed":["mode"],"after":{"id":"sat-042","name":"Kestrel-42","mode":"safe","orbit":"leo","battery_pct":61}}"#;
const DECOMMISSION: &str = r#"{"op":"d","table":"satellites","before":{"id":"sat-042"}}"#;
const GROUND_STATION: &str = r#"{"op":"u","table":"ground_stations","changed":["status"],"after":{"id":"svalbard","status":"online"}}"#;

static CATALOG_SERVER: OnceCell<(TestIggy, tempfile::TempDir)> = OnceCell::const_new();

fn bound_filter() -> ConsumerFilter {
    ConsumerFilter::json(FilterExpr::any([
        FilterExpr::pred("op", CmpOp::Eq, "d"),
        FilterExpr::pred("changed", CmpOp::Contains, "mode"),
    ]))
}

/// The group policies the stand-in holds.
#[derive(Default)]
struct CatalogState {
    next_filter_id: u32,
    offset: u64,
    filters: HashMap<u32, Vec<FilterRevisionInfo>>,
    owners: HashMap<FilterGroupIdentity, u32>,
    bindings: HashMap<FilterGroupIdentity, FilterBinding>,
    generations: HashMap<FilterGroupIdentity, u64>,
}

impl CatalogState {
    fn bump(&mut self, identity: FilterGroupIdentity) -> u64 {
        let generation = self.generations.entry(identity).or_default();
        *generation += 1;
        *generation
    }

    fn mutate(
        &mut self,
        command: FilterCatalogCommand,
    ) -> Result<FilterMutationResult, FilterError> {
        match command.mutation {
            FilterMutation::ConfigureGroup {
                group,
                policy,
                expected_identity,
            } => {
                let identity = command.identity.ok_or_else(|| {
                    FilterError::new(
                        FilterErrorReason::InvalidRequest,
                        "the streaming server stamps the group identity",
                    )
                })?;
                if expected_identity.is_some_and(|expected| expected != identity) {
                    return Err(FilterError::new(
                        FilterErrorReason::SourceChanged,
                        "the group incarnation changed before configuration",
                    ));
                }
                let (filter_id, revision, digest) = match policy {
                    GroupFilterSpec::Definition(filter) => {
                        let digest = filter.digest();
                        if let Some(existing) = self.bindings.get(&identity) {
                            if existing.digest == digest {
                                return Ok(FilterMutationResult::Bound(existing.clone()));
                            }
                            return Err(FilterError::new(
                                FilterErrorReason::Conflict,
                                "the group already runs another policy",
                            ));
                        }
                        let filter_id = match self.owners.get(&identity) {
                            Some(filter_id) => *filter_id,
                            None => {
                                self.next_filter_id += 1;
                                self.owners.insert(identity, self.next_filter_id);
                                self.next_filter_id
                            }
                        };
                        let revisions = self.filters.entry(filter_id).or_default();
                        let revision = match revisions.iter().find(|info| info.digest == digest) {
                            Some(info) => info.revision,
                            None => {
                                let revision = revisions.len() as u32 + 1;
                                revisions.push(FilterRevisionInfo {
                                    enabled: true,
                                    revision,
                                    digest: digest.clone(),
                                    filter,
                                    created_at_micros: 1,
                                });
                                revision
                            }
                        };
                        (filter_id, revision, digest)
                    }
                    GroupFilterSpec::Revision {
                        filter_id,
                        revision,
                    } => {
                        if self.owners.get(&identity) != Some(&filter_id) {
                            return Err(FilterError::new(
                                FilterErrorReason::Conflict,
                                "not the group's own filter",
                            ));
                        }
                        let digest = self
                            .filters
                            .get(&filter_id)
                            .and_then(|revisions| {
                                revisions.iter().find(|info| info.revision == revision)
                            })
                            .map(|info| info.digest.clone())
                            .ok_or_else(|| {
                                FilterError::new(FilterErrorReason::NotFound, "no such revision")
                            })?;
                        if let Some(existing) = self.bindings.get(&identity) {
                            if existing.digest == digest {
                                return Ok(FilterMutationResult::Bound(existing.clone()));
                            }
                            return Err(FilterError::new(
                                FilterErrorReason::Conflict,
                                "the group already runs another policy",
                            ));
                        }
                        (filter_id, revision, digest)
                    }
                };
                let binding = FilterBinding {
                    group,
                    identity,
                    filter_id,
                    revision,
                    digest,
                    bound_at_micros: 1,
                    policy_generation: self.bump(identity),
                };
                self.bindings.insert(identity, binding.clone());
                Ok(FilterMutationResult::Bound(binding))
            }
            FilterMutation::Revise {
                filter_id,
                expected_revision,
                filter,
            } => {
                let revisions = self.filters.get_mut(&filter_id).ok_or_else(|| {
                    FilterError::new(FilterErrorReason::NotFound, "no such filter")
                })?;
                let latest = revisions.last().map_or(0, |info| info.revision);
                if latest != expected_revision {
                    return Err(FilterError::new(
                        FilterErrorReason::Conflict,
                        "not the latest revision",
                    ));
                }
                let digest = filter.digest();
                revisions.push(FilterRevisionInfo {
                    enabled: true,
                    revision: latest + 1,
                    digest: digest.clone(),
                    filter,
                    created_at_micros: 1,
                });
                Ok(FilterMutationResult::Revised(FilterRevisionRef {
                    filter_id,
                    revision: latest + 1,
                    digest,
                }))
            }
            FilterMutation::SetRevisionEnabled {
                filter_id,
                revision,
                enabled,
            } => {
                let info = self
                    .filters
                    .get_mut(&filter_id)
                    .and_then(|revisions| {
                        revisions.iter_mut().find(|info| info.revision == revision)
                    })
                    .ok_or_else(|| {
                        FilterError::new(FilterErrorReason::NotFound, "no such revision")
                    })?;
                info.enabled = enabled;
                Ok(FilterMutationResult::RevisionState {
                    filter_id,
                    revision,
                    enabled,
                })
            }
            FilterMutation::Unbind {
                expected_digest,
                expected_identity,
                ..
            } => {
                let identity = expected_identity.or(command.identity).ok_or_else(|| {
                    FilterError::new(FilterErrorReason::InvalidRequest, "no identity")
                })?;
                let binding = self.bindings.get(&identity).cloned().ok_or_else(|| {
                    FilterError::new(FilterErrorReason::NotFound, "the group is not bound")
                })?;
                if binding.digest != expected_digest {
                    return Err(FilterError::new(
                        FilterErrorReason::Conflict,
                        "bound to another digest",
                    ));
                }
                self.bindings.remove(&identity);
                self.bump(identity);
                Ok(FilterMutationResult::Unbound(binding))
            }
            FilterMutation::Drop { filter_id } => {
                if self.filters.remove(&filter_id).is_none() {
                    return Err(FilterError::new(
                        FilterErrorReason::NotFound,
                        "no such filter",
                    ));
                }
                let released: Vec<FilterGroupIdentity> = self
                    .bindings
                    .iter()
                    .filter(|(_, binding)| binding.filter_id == filter_id)
                    .map(|(identity, _)| *identity)
                    .collect();
                for identity in released {
                    self.bindings.remove(&identity);
                    self.bump(identity);
                }
                self.owners.retain(|_, owned| *owned != filter_id);
                Ok(FilterMutationResult::Dropped { filter_id })
            }
            _ => Err(FilterError::new(
                FilterErrorReason::Unsupported,
                "the stand-in catalog serves group policies only",
            )),
        }
    }

    fn resolve(&self, request: &ResolveFilterPolicy) -> Result<FilterCatalogOutcome, FilterError> {
        let (filter_id, revision, policy_generation) = match request.policy {
            FilterPolicyRef::Binding(identity) => match self.bindings.get(&identity) {
                Some(binding) => (
                    binding.filter_id,
                    binding.revision,
                    binding.policy_generation,
                ),
                None if request.allow_unbound => {
                    return Ok(FilterCatalogOutcome::Unbound(GroupPolicyUnbound {
                        identity,
                        policy_generation: self.generations.get(&identity).copied().unwrap_or(0),
                    }));
                }
                None => {
                    return Err(FilterError::new(
                        FilterErrorReason::NotFound,
                        "this consumer group has no filter binding",
                    ));
                }
            },
            FilterPolicyRef::Revision {
                filter_id,
                revision,
            } => (filter_id, revision, 0),
        };
        let info = self
            .filters
            .get(&filter_id)
            .and_then(|revisions| revisions.iter().find(|info| info.revision == revision))
            .ok_or_else(|| FilterError::new(FilterErrorReason::NotFound, "no such revision"))?;
        if !info.enabled && !request.allow_disabled {
            return Err(FilterError::new(
                FilterErrorReason::RevisionDisabled,
                "the revision is paused",
            ));
        }
        Ok(FilterCatalogOutcome::Policy(ResolvedFilterPolicy {
            filter_id,
            revision,
            digest: info.digest.clone(),
            state: FilterState::Active,
            filter: info.filter.clone(),
            policy_generation,
        }))
    }

    fn answer(&mut self, forwarded: &ForwardedCommand) -> Option<Vec<u8>> {
        let reply = match forwarded.command_code {
            AGDX_BACKEND_HELLO_CODE => {
                return encode_named(&BackendAnnounce::new(
                    OpVersions::new(
                        QUERY_OP_VERSION,
                        CONTROL_OP_VERSION,
                        KV_OP_VERSION,
                        FORK_OP_VERSION,
                    )
                    .with_filter(FILTER_OP_VERSION),
                ))
                .ok();
            }
            AGDX_FILTER_MUTATE_CODE => {
                let command: FilterCatalogCommand = decode_named(&forwarded.payload).ok()?;
                let operation_id = command.operation_id;
                let status = match self.mutate(command) {
                    Ok(result) => FilterMutationStatus::Applied(result),
                    Err(error) => FilterMutationStatus::Rejected(error),
                };
                self.offset += 1;
                FilterCatalogReply::Ok(Box::new(FilterCatalogOutcome::Mutation(
                    FilterMutationOutcome {
                        v: FILTER_OP_VERSION,
                        operation_id,
                        status,
                        catalog_position: Some(CatalogPosition {
                            partition_id: 1,
                            offset: self.offset,
                            operation_id: Some(operation_id),
                        }),
                    },
                )))
            }
            AGDX_RESOLVE_FILTER_POLICY_CODE => {
                let request: ResolveFilterPolicy = decode_named(&forwarded.payload).ok()?;
                match self.resolve(&request) {
                    Ok(outcome) => FilterCatalogReply::Ok(Box::new(outcome)),
                    Err(error) => FilterCatalogReply::Err(error),
                }
            }
            AGDX_GET_FILTER_BINDING_CODE => {
                let request: GetFilterBinding = decode_named(&forwarded.payload).ok()?;
                match request
                    .identity
                    .and_then(|identity| self.bindings.get(&identity))
                {
                    Some(binding) => FilterCatalogReply::Ok(Box::new(
                        FilterCatalogOutcome::Binding(binding.clone()),
                    )),
                    None => FilterCatalogReply::Err(FilterError::new(
                        FilterErrorReason::NotFound,
                        "the group is not bound",
                    )),
                }
            }
            AGDX_LIST_FILTERS_CODE => {
                let request: ListFilters = decode_named(&forwarded.payload).ok()?;
                let mut items: Vec<FilterSummary> = self
                    .owners
                    .iter()
                    .filter_map(|(identity, filter_id)| {
                        let name = format!(
                            "group:{}:{}:{}:{}:{}",
                            identity.stream_id,
                            identity.stream_created_at_micros,
                            identity.topic_id,
                            identity.topic_created_at_micros,
                            identity.group_id
                        );
                        if request
                            .name_contains
                            .as_deref()
                            .is_some_and(|fragment| !name.contains(fragment))
                        {
                            return None;
                        }
                        let latest = self.filters.get(filter_id)?.last()?;
                        Some(FilterSummary {
                            id: *filter_id,
                            name,
                            description: String::new(),
                            state: FilterState::Active,
                            latest_revision: latest.revision,
                            latest_digest: latest.digest.clone(),
                            codec: latest.filter.codec,
                            bindings: self
                                .bindings
                                .values()
                                .filter(|binding| binding.filter_id == *filter_id)
                                .count() as u32,
                            created_at_micros: latest.created_at_micros,
                            updated_at_micros: latest.created_at_micros,
                        })
                    })
                    .collect();
                items.sort_by_key(|summary| std::cmp::Reverse(summary.id));
                let total = items.len() as u32;
                FilterCatalogReply::Ok(Box::new(FilterCatalogOutcome::Filters(FilterPage {
                    items,
                    page: request.page,
                    page_size: request.page_size,
                    total,
                })))
            }
            AGDX_LIST_FILTER_REVISIONS_CODE => {
                let request: ListFilterRevisions = decode_named(&forwarded.payload).ok()?;
                let mut items = self
                    .filters
                    .get(&request.filter_id)
                    .cloned()
                    .unwrap_or_default();
                items.reverse();
                let total = items.len() as u32;
                FilterCatalogReply::Ok(Box::new(FilterCatalogOutcome::Revisions(
                    FilterRevisionPage {
                        filter_id: request.filter_id,
                        items,
                        page: request.page,
                        page_size: request.page_size,
                        total,
                    },
                )))
            }
            AGDX_LIST_FILTER_BINDINGS_CODE => {
                let request: ListFilterBindings = decode_named(&forwarded.payload).ok()?;
                let items: Vec<FilterBinding> = self
                    .bindings
                    .values()
                    .filter(|binding| {
                        request
                            .stream
                            .as_ref()
                            .is_none_or(|stream| *stream == binding.group.stream)
                            && request
                                .topic
                                .as_ref()
                                .is_none_or(|topic| *topic == binding.group.topic)
                    })
                    .cloned()
                    .collect();
                let total = items.len() as u32;
                FilterCatalogReply::Ok(Box::new(FilterCatalogOutcome::Bindings(
                    FilterBindingPage {
                        items,
                        page: request.page,
                        page_size: request.page_size,
                        total,
                    },
                )))
            }
            AGDX_FILTER_OPERATION_CODE => FilterCatalogReply::Err(FilterError::new(
                FilterErrorReason::NotFound,
                "the stand-in applies every mutation at once",
            )),
            _ => return None,
        };
        encode_named(&reply).ok()
    }
}

async fn catalog_server() -> &'static TestIggy {
    let (server, _socket_dir) = CATALOG_SERVER.get_or_init(start_catalog_server).await;
    server
}

async fn start_catalog_server() -> (TestIggy, tempfile::TempDir) {
    let socket_dir = tempfile::tempdir().expect("socket directory");
    let socket = socket_dir.path().join("plane.sock");
    spawn_catalog(socket.clone());
    let server = TestIggy::start_with(vec![
        ("IGGY_PLANE_ENABLED".to_owned(), "true".to_owned()),
        (
            "IGGY_PLANE_SOCKET_PATH".to_owned(),
            socket.display().to_string(),
        ),
    ])
    .await;
    (server, socket_dir)
}

fn spawn_catalog(socket: PathBuf) {
    let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind the catalog");
    listener
        .set_nonblocking(true)
        .expect("nonblocking catalog socket");
    let state = Arc::new(Mutex::new(CatalogState::default()));
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("catalog runtime");
        runtime.block_on(async move {
            let listener = UnixListener::from_std(listener).expect("adopt the catalog socket");
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(serve(socket, Arc::clone(&state)));
            }
        });
    });
}

async fn serve(mut socket: UnixStream, state: Arc<Mutex<CatalogState>>) {
    loop {
        let mut length = [0u8; 4];
        if socket.read_exact(&mut length).await.is_err() {
            return;
        }
        let mut frame = vec![0u8; u32::from_le_bytes(length) as usize];
        if socket.read_exact(&mut frame).await.is_err() {
            return;
        }
        let Ok(forwarded) = decode_named::<ForwardedCommand>(&frame) else {
            return;
        };
        let reply = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .answer(&forwarded);
        let Some(reply) = reply else {
            return;
        };
        let written = socket
            .write_all(
                &u32::try_from(reply.len())
                    .expect("small reply")
                    .to_le_bytes(),
            )
            .await;
        if written.is_err() || socket.write_all(&reply).await.is_err() {
            return;
        }
    }
}

async fn publish(laser: &Laser, topic: &str, payloads: &[&str], partition: u32) {
    let producer = laser
        .topic(topic)
        .producer()
        .partitions(partition + 1)
        .build()
        .await
        .expect("the producer initializes");
    producer
        .send_batch_with_routing(
            payloads
                .iter()
                .map(|payload| ProducerMessage::new(payload.as_bytes().to_vec())),
            Some(Routing::Partition(partition)),
        )
        .await
        .expect("the fixtures publish");
}

/// Publish the three fixtures on partition 0 and return the stream name.
async fn group_source(laser: &Laser) -> String {
    publish(laser, TOPIC, &[SAFE_MODE, GROUND_STATION, DECOMMISSION], 0).await;
    laser.default_stream().expect("stream").to_owned()
}

/// Create `GROUP` with the bound filter as its policy.
async fn bound_group(laser: &Laser) -> ConsumerGroup {
    let group = laser.topic(TOPIC).consumer_group(GROUP);
    let created = group
        .create()
        .filter(bound_filter())
        .build()
        .await
        .expect("the group is created with its filter");
    assert_eq!(
        created.filter.as_ref().map(|binding| binding.revision),
        Some(1)
    );
    group
}

async fn stored_group_offset(laser: &Laser, stream: &str, partition: u32) -> Option<u64> {
    laser
        .client()
        .get_consumer_offset(
            &Consumer::group(Identifier::named(GROUP).expect("group name")),
            &Identifier::named(stream).expect("stream name"),
            &Identifier::named(TOPIC).expect("topic name"),
            Some(partition),
        )
        .await
        .expect("the group offset reads")
        .map(|offset| offset.stored_offset)
}

fn offsets(page: &laser_sdk::filters::MatchedPage) -> Vec<u64> {
    page.records.iter().map(|record| record.offset).collect()
}

#[tokio::test]
async fn given_a_group_created_with_a_filter_when_consumed_then_should_deliver_only_matches_and_commit_the_scanned_prefix()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    assert!(laser.capabilities().await.filters.group_policy_reads);
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;

    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .poll_interval(Duration::from_millis(5))
        .build()
        .await
        .expect("the group consumer builds");
    let mut delivered = Vec::new();
    for _ in 0..2 {
        let message = consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("a matching record arrives");
        consumer.commit(&message).await.expect("the record commits");
        delivered.push(message.position.offset);
    }
    assert_eq!(delivered, vec![0, 2], "the server ran the group's policy");
    assert_eq!(consumer.last_consumed_offset(0), Some(2));
    assert_eq!(consumer.last_stored_offset(0), Some(2));
    assert!(
        matches!(
            consumer.store_offset(1, Some(0)).await,
            Err(LaserError::Invalid(_))
        ),
        "explicit offsets bypass the acknowledgment contract"
    );
    consumer.shutdown().await.expect("the consumer shuts down");

    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
    let info = group.info().await.expect("the group exists");
    assert_eq!(
        info.filter.map(|binding| binding.policy_generation),
        Some(1)
    );
}

#[tokio::test]
async fn given_an_unbound_group_when_consumed_then_should_deliver_every_record_through_the_group_engine()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = laser.topic(TOPIC).consumer_group(GROUP);
    let created = group.create().build().await.expect("a plain group");
    assert!(created.filter.is_none());

    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .poll_interval(Duration::from_millis(5))
        .build()
        .await
        .expect("the group consumer builds");
    let mut delivered = Vec::new();
    for _ in 0..3 {
        let message = consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("a record arrives");
        delivered.push(message.position.offset);
    }
    consumer.shutdown().await.expect("the consumer shuts down");

    assert_eq!(
        delivered,
        vec![0, 1, 2],
        "an unbound group receives everything"
    );
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
}

#[tokio::test]
async fn given_an_unbound_group_when_reading_pages_then_should_return_records_without_evaluating() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = laser.topic(TOPIC).consumer_group(GROUP);
    group.create().build().await.expect("plain group");
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .count(3)
        .max_examined(3)
        .build()
        .await
        .expect("an automatic group reader accepts an unbound group");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("page deadline")
        .expect("page");
    assert_eq!(offsets(&page), vec![0, 1, 2]);
    assert_eq!(page.policy.mode, ExecutionMode::Unfiltered);
    assert!(page.records.iter().all(|record| !record.evaluated));
    reader.ack_page(&page).await.expect("acknowledge");
    reader.close().await.expect("close");
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
}

#[tokio::test]
async fn given_automatic_commit_policies_when_delivering_a_record_then_should_not_store_it_before_returning()
 {
    for policy in [
        CommitPolicy::Each,
        CommitPolicy::All,
        CommitPolicy::Every(1),
        CommitPolicy::Interval(Duration::from_millis(1)),
        CommitPolicy::IntervalOrEach(Duration::from_millis(1)),
        CommitPolicy::Polling,
    ] {
        let laser = harness::connected_laser_on(catalog_server().await).await;
        let stream = group_source(&laser).await;
        let group = bound_group(&laser).await;
        let mut consumer = group
            .consumer()
            .start_at(ConsumerStart::First)
            .batch_length(2)
            .commit_policy(policy)
            .build()
            .await
            .expect("group consumer");
        let first = consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("first delivery");
        assert_eq!(first.position.offset, 0);
        assert_eq!(
            stored_group_offset(&laser, &stream, 0).await,
            None,
            "{policy:?}"
        );
        consumer.shutdown().await.expect("clean stop");
        assert_eq!(
            stored_group_offset(&laser, &stream, 0).await,
            Some(1),
            "{policy:?}"
        );
    }
}

#[tokio::test]
async fn given_an_interval_policy_when_the_partition_is_idle_then_should_store_the_delivered_prefix_before_the_next_page()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Interval(Duration::from_millis(400)))
        .poll_interval(Duration::from_millis(10))
        .build()
        .await
        .expect("group consumer");
    consumer.next_within(READ_TIMEOUT).await.expect("first");
    let last = consumer.next_within(READ_TIMEOUT).await.expect("second");
    assert_eq!(last.position.offset, 2);
    // Nothing new arrives. The interval elapses inside this wait, so the
    // engine must store between its idle rounds instead of inside a blocked
    // page read.
    assert!(matches!(
        consumer.next_within(Duration::from_millis(1500)).await,
        Err(LaserError::Timeout(_))
    ));
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        Some(2),
        "the interval stores the examined prefix while the partition is idle"
    );
    consumer.shutdown().await.expect("clean stop");
}

#[tokio::test]
async fn given_a_timed_out_next_when_manually_committing_then_should_release_the_in_flight_read_lock()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .poll_interval(Duration::from_millis(5))
        .build()
        .await
        .expect("consumer");
    consumer.next_within(READ_TIMEOUT).await.expect("first");
    let second = consumer.next_within(READ_TIMEOUT).await.expect("second");
    assert!(matches!(
        consumer.next_within(Duration::from_millis(50)).await,
        Err(LaserError::Timeout(_))
    ));
    tokio::time::timeout(Duration::from_secs(2), consumer.commit(&second))
        .await
        .expect("manual commit must not wait for another read to complete")
        .expect("commit");
    consumer.shutdown().await.expect("close");
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
}

#[tokio::test]
async fn given_a_native_group_by_numeric_id_when_consuming_then_should_resolve_its_name() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    group_source(&laser).await;
    let mut capabilities = Capabilities::OPEN;
    capabilities.hello = HelloOutcome::Rejected;
    let native = laser.with_capabilities(capabilities);
    let info = native
        .topic(TOPIC)
        .consumer_group(GROUP)
        .create()
        .build()
        .await
        .expect("native group");
    let mut consumer = native
        .topic(TOPIC)
        .consumer_group_id(u64::from(info.id))
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("a numeric group can use the native fallback");
    let first = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect("native delivery");
    assert_eq!(first.position.offset, 0);
    consumer.commit(&first).await.expect("native commit");
    consumer.shutdown().await.expect("native close");
}

#[tokio::test]
async fn given_a_cancelled_binding_delivery_when_returned_then_should_redeliver_without_acknowledging()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .build()
        .await
        .expect("consumer");
    let original = consumer.next_within(READ_TIMEOUT).await.expect("first");
    consumer
        .return_delivery(original)
        .await
        .expect("cancelled handoff is returned");
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, None);
    let repeated = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect("returned delivery");
    assert_eq!(repeated.position.offset, 0);
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, None);
    consumer
        .return_delivery(repeated)
        .await
        .expect("return again");
    consumer.shutdown().await.expect("close");
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        None,
        "shutdown cannot acknowledge an undelivered binding result"
    );
}

#[tokio::test]
async fn given_a_policy_released_while_consuming_then_should_continue_unfiltered_and_redeliver_the_unstored_record()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .poll_interval(Duration::from_millis(5))
        .build()
        .await
        .expect("consumer");
    assert_eq!(
        consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("first")
            .position
            .offset,
        0
    );
    group
        .filter()
        .release()
        .await
        .expect("release while reading");
    let repeated = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect("the consumer goes on without an error");
    assert_eq!(
        repeated.position.offset, 0,
        "the record read under the old policy was not stored, so it is delivered again"
    );
    assert_eq!(
        consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("newly included record")
            .position
            .offset,
        1
    );
    consumer.shutdown().await.expect("close");
}

#[tokio::test]
async fn given_a_bound_group_when_reading_pages_then_should_run_the_bound_revision_and_store_group_progress()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    assert!(laser.capabilities().await.filters.catalog);
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the group reader builds");
    assert_eq!(reader.partitions(), vec![0]);

    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the bound read succeeds");

    assert_eq!(offsets(&page), vec![0, 2]);
    let binding = group
        .filter()
        .get()
        .await
        .expect("readable")
        .expect("bound");
    assert_eq!(page.policy.filter_id, Some(binding.filter_id));
    assert_eq!(page.policy.revision, Some(1));
    assert_eq!(page.policy.digest, Some(binding.digest));
    assert_eq!(page.policy.mode, ExecutionMode::Filtered);
    assert_eq!(page.policy.policy_generation, 1);
    reader.ack_page(&page).await.expect("the page acknowledges");
    reader.close().await.expect("the reader closes");
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
}

#[tokio::test]
async fn given_a_configured_group_when_created_again_then_should_keep_its_binding_and_refuse_another_policy()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    group_source(&laser).await;
    let group = bound_group(&laser).await;
    let first = group
        .filter()
        .get()
        .await
        .expect("readable")
        .expect("bound");

    let again = group
        .create()
        .filter(bound_filter())
        .build()
        .await
        .expect("the same policy is idempotent");
    assert_eq!(again.filter, Some(first.clone()));

    let refused = group
        .create()
        .filter(ConsumerFilter::json(FilterExpr::present("kind")))
        .build()
        .await
        .expect_err("another policy conflicts");
    match refused {
        LaserError::ConsumerGroupSetup {
            group_id,
            name,
            identity,
            source,
        } => {
            assert_eq!(name, GROUP);
            assert_eq!(u64::from(group_id), identity.group_id);
            assert_eq!(identity, first.identity);
            assert_eq!(source.filter_reason(), Some(FilterErrorReason::Conflict));
        }
        other => panic!("a group setup failure, got {other:?}"),
    }
    assert_eq!(
        group.filter().get().await.expect("readable"),
        Some(first),
        "the running policy is preserved"
    );
}

#[tokio::test]
async fn given_lost_membership_when_reading_again_then_should_rejoin_from_committed_progress() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("reader joins");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("page timely")
        .expect("page");
    reader
        .ack_page(&page)
        .await
        .expect("first page acknowledged");
    reader
        .coordinator()
        .leave_consumer_group(
            &Identifier::named(&stream).expect("stream"),
            &Identifier::named(TOPIC).expect("topic"),
            &Identifier::named(GROUP).expect("group"),
        )
        .await
        .expect("membership removed");
    publish(&laser, TOPIC, &[DECOMMISSION], 0).await;
    let next = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("rejoin and read timely")
        .expect("read after rejoin");
    assert_eq!(offsets(&next), vec![3]);
    assert!(
        reader.ack_page(&page).await.is_err(),
        "old membership pages cannot acknowledge the new reader epoch"
    );
    reader.ack_page(&next).await.expect("new page acknowledges");
    reader.close().await.expect("close");
}

#[tokio::test]
async fn given_two_partitions_on_one_node_when_reading_many_pages_then_should_keep_one_data_connection()
 {
    let (server, _socket_dir) = start_catalog_server().await;
    let laser = harness::connected_laser_on(&server).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    laser
        .client()
        .create_partitions(&stream_id, &topic_id, 1)
        .await
        .expect("second partition");
    for partition in 0..2 {
        publish(
            &laser,
            TOPIC,
            &vec![SAFE_MODE; PAGES_PER_PARTITION as usize],
            partition,
        )
        .await;
    }
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .count(1)
        .build()
        .await
        .expect("the group reader builds");
    let mut partitions_read = BTreeSet::new();
    for _ in 0..PAGES_PER_PARTITION * 2 {
        let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
            .await
            .expect("a page arrives")
            .expect("the bound read succeeds");
        partitions_read.insert(page.partition_id);
        reader.ack_page(&page).await.expect("the page acknowledges");
    }
    assert_eq!(partitions_read.len(), 2, "both partitions were read");
    assert_eq!(
        reader.data_connections_opened(),
        1,
        "every page and acknowledgment used one connection"
    );
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_revoked_partition_when_draining_then_should_route_acknowledgments_separately() {
    let server = catalog_server().await;
    let laser = harness::connected_laser_on(server).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    laser
        .client()
        .create_partitions(&stream_id, &topic_id, 1)
        .await
        .expect("second partition");
    publish(&laser, TOPIC, &[SAFE_MODE], 1).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("first member");
    let first = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("first timely")
        .expect("first page");
    let second = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("second timely")
        .expect("second page");
    let peer = Laser::connect(&server.connection_string())
        .await
        .expect("peer connects");
    peer.client()
        .join_consumer_group(
            &stream_id,
            &topic_id,
            &Identifier::named(GROUP).expect("group"),
        )
        .await
        .expect("peer joins");
    let revoked = tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            reader.try_next_page().await.expect("refreshes membership");
            if let Some(partition) = [0, 1]
                .into_iter()
                .find(|partition| !reader.partitions().contains(partition))
            {
                break partition;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a partition is revoked");
    let page = if first.partition_id == revoked {
        &first
    } else {
        &second
    };
    reader
        .ack_page(page)
        .await
        .expect("revoked partition can drain through offset routing");
    assert_eq!(
        stored_group_offset(&laser, &stream, revoked).await,
        page.safe_ack_offset
    );
    reader.close().await.expect("reader closes");
    peer.close().await.expect("peer closes");
}

#[tokio::test]
async fn given_a_recreated_group_when_acknowledging_an_old_page_then_should_not_advance_the_new_group()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    let group_id = Identifier::named(GROUP).expect("group");
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("reader");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("page timely")
        .expect("page");
    laser
        .client()
        .delete_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("delete old group");
    laser
        .client()
        .create_consumer_group(&stream_id, &topic_id, GROUP)
        .await
        .expect("new group");
    laser
        .client()
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("join new incarnation");
    let result = reader.ack_page(&page).await;
    assert!(
        matches!(result, Err(ref error) if error.filter_reason() == Some(FilterErrorReason::SourceChanged)),
        "old page cannot acknowledge a new group: {result:?}"
    );
    assert!(
        stored_group_offset(&laser, &stream, page.partition_id)
            .await
            .is_none(),
        "new group has no acknowledged records"
    );
    assert_eq!(
        group.filter().get().await.expect("readable"),
        None,
        "a recreated group inherits no policy"
    );
    let _ = reader.close().await;
}

#[tokio::test]
async fn given_a_recreated_group_when_configuring_with_its_old_identity_then_should_leave_the_replacement_unbound()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let original = group.info().await.expect("original group");
    laser
        .client()
        .delete_consumer_group(
            &Identifier::named(&stream).expect("stream"),
            &Identifier::named(TOPIC).expect("topic"),
            &Identifier::named(GROUP).expect("group"),
        )
        .await
        .expect("delete original group");
    let replacement = group.create().build().await.expect("replacement group");
    assert_ne!(replacement.identity, original.identity);
    let mutation = FilterMutationRequest {
        v: FILTER_OP_VERSION,
        operation_id: u128::from(ulid::Ulid::generate()),
        mutation: FilterMutation::ConfigureGroup {
            group: FilterGroupRef {
                stream,
                topic: TOPIC.to_owned(),
                group: GROUP.to_owned(),
            },
            policy: GroupFilterSpec::Definition(bound_filter()),
            expected_identity: Some(original.identity),
        },
    };
    let reply = laser
        .client()
        .send_binary_request(
            AGDX_FILTER_MUTATE_CODE,
            encode_named(&mutation).expect("mutation encodes").into(),
        )
        .await
        .expect("typed refusal");
    let reply: FilterCatalogReply = decode_named(&reply).expect("mutation reply");
    assert!(
        matches!(reply, FilterCatalogReply::Err(error) if error.reason == FilterErrorReason::SourceChanged)
    );
    assert!(
        group
            .filter()
            .get()
            .await
            .expect("replacement policy reads")
            .is_none()
    );
}

#[tokio::test]
async fn given_a_numeric_group_handle_when_reading_then_should_pin_the_topic_incarnation() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    let named = Identifier::named(GROUP).expect("group");
    let created = bound_group(&laser)
        .await
        .info()
        .await
        .expect("the group exists");
    let by_id = laser.topic(TOPIC).consumer_group_id(u64::from(created.id));
    assert_eq!(
        by_id.filter().get().await.expect("readable"),
        created.filter,
        "a numeric handle reads the same policy"
    );
    let mut reader = by_id
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("numeric group");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("deadline")
        .expect("page");
    assert_eq!(page.policy.group_id, Some(u64::from(created.id)));
    reader.ack_page(&page).await.expect("ack");
    reader.close().await.expect("close");
    laser
        .client()
        .delete_consumer_group(&stream_id, &topic_id, &named)
        .await
        .expect("delete group");
    laser
        .client()
        .create_consumer_group(&stream_id, &topic_id, GROUP)
        .await
        .expect("recreate name");
    assert!(
        by_id
            .reader()
            .expect("a stream is set")
            .build()
            .await
            .is_err(),
        "old id must not join the replacement group"
    );
}

#[tokio::test]
async fn given_out_of_order_acks_when_acknowledging_then_should_store_only_the_completed_prefix() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .count(1)
        .build()
        .await
        .expect("the reader builds");
    let first = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("the read succeeds");
    let second = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("the read succeeds");
    assert_eq!((first.offset, second.offset), (0, 2));

    reader
        .ack(&second)
        .await
        .expect("the later record acknowledges");
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        None,
        "the earlier record is still open"
    );
    reader
        .ack(&first)
        .await
        .expect("the earlier record acknowledges");

    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_malformed_record_under_stop_when_reading_then_should_deliver_earlier_matches_then_the_fault()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    publish(&laser, TOPIC, &[SAFE_MODE, "not json", DECOMMISSION], 0).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the reader builds");

    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the read succeeds");
    assert_eq!(offsets(&page), vec![0]);
    reader.ack_page(&page).await.expect("the page acknowledges");
    let fault = reader
        .next_page()
        .await
        .expect_err("the fault stops the reader");

    assert!(matches!(
        fault,
        LaserError::FilterFault {
            partition_id: 0,
            offset: 1,
            reason: FaultReason::Malformed,
        }
    ));
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        Some(0),
        "the fault stays unacknowledged"
    );
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_local_guard_when_reading_then_should_agree_with_the_server() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .local_guard(true)
        .build()
        .await
        .expect("the reader builds");

    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the local evaluation agrees");

    assert_eq!(offsets(&page), vec![0, 2]);
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_paused_revision_when_reading_then_should_refuse_new_reads_until_resumed() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .count(1)
        .build()
        .await
        .expect("the reader builds");
    let first = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("the read succeeds");
    group
        .filter()
        .set_revision_enabled(1, false)
        .await
        .expect("the revision pauses");
    reader
        .ack(&first)
        .await
        .expect("delivered work still acknowledges");
    let refused = reader.try_next_page().await.expect_err("new reads stop");
    assert_eq!(
        refused.filter_reason(),
        Some(FilterErrorReason::RevisionDisabled)
    );
    group
        .filter()
        .set_revision_enabled(1, true)
        .await
        .expect("the revision resumes");
    let second = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("reads resume");
    assert_eq!(second.offset, 2);
    let revisions = group
        .filter()
        .revisions(0, 10)
        .await
        .expect("revisions list");
    assert_eq!(revisions.total, 1);
    reader.close().await.expect("close");
}

#[tokio::test]
async fn given_a_group_with_a_filter_when_deleted_then_should_release_it_and_remove_its_filter() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let before = group
        .filter()
        .get()
        .await
        .expect("readable")
        .expect("bound");
    assert!(group.filter().delete().await.expect("the filter deletes"));
    assert_eq!(group.filter().get().await.expect("readable"), None);
    assert!(
        !group.filter().delete().await.expect("nothing left"),
        "a second delete finds no filter"
    );
    let again = group
        .filter()
        .configure(bound_filter())
        .await
        .expect("the same group configures again");
    assert_ne!(again.filter_id, before.filter_id, "the id is not reused");
    assert_eq!(again.digest, before.digest);
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, None);
    group.filter().delete().await.expect("cleanup");
}

#[tokio::test]
async fn given_a_released_group_when_consumed_then_should_deliver_every_record_and_keep_its_digest()
{
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let released = group.filter().release().await.expect("the policy releases");
    assert_eq!(released.policy_generation, 1);
    assert_eq!(group.filter().get().await.expect("readable"), None);

    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::All)
        .poll_interval(Duration::from_millis(5))
        .build()
        .await
        .expect("the group consumer builds");
    let mut delivered = Vec::new();
    for _ in 0..3 {
        delivered.push(
            consumer
                .next_within(READ_TIMEOUT)
                .await
                .expect("a record arrives")
                .position
                .offset,
        );
    }
    consumer.shutdown().await.expect("shutdown");
    assert_eq!(
        delivered,
        vec![0, 1, 2],
        "a released group receives everything"
    );
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));

    let again = group
        .filter()
        .configure(bound_filter())
        .await
        .expect("the digest it ran binds again");
    assert_eq!(again.policy_generation, 3);
    let refused = group
        .filter()
        .configure(ConsumerFilter::json(FilterExpr::present("kind")))
        .await
        .expect_err("another digest conflicts");
    assert_eq!(refused.filter_reason(), Some(FilterErrorReason::Conflict));
}

#[tokio::test]
async fn given_typed_custom_headers_when_filtered_then_should_preserve_types_and_payload() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let producer = laser
        .topic(TOPIC)
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("producer");
    for priority in [
        HeaderValue::from(1_u8),
        HeaderValue::from(2_u8),
        HeaderValue::try_from("2").expect("text"),
    ] {
        let message = ProducerMessage::new(vec![0xff, 0x00])
            .header(
                HeaderKey::try_from("routing.priority").expect("key"),
                priority,
            )
            .header(
                HeaderKey::try_from("armed").expect("key"),
                HeaderValue::from(true),
            )
            .header(
                HeaderKey::try_from("temperature").expect("key"),
                HeaderValue::from(-1.5_f32),
            )
            .header(
                HeaderKey::try_from("sequence").expect("key"),
                HeaderValue::from(u64::MAX),
            );
        producer
            .send_to_partition(message, 0)
            .await
            .expect("publish");
    }
    let group = laser.topic(TOPIC).consumer_group("typed-headers");
    group
        .create()
        .filter(ConsumerFilter::headers_only(FilterExpr::all([
            FilterExpr::header("routing.priority", CmpOp::Eq, 2_i32),
            FilterExpr::header("armed", CmpOp::Eq, true),
            FilterExpr::header("temperature", CmpOp::Lt, 0_i32),
            FilterExpr::header("sequence", CmpOp::Gt, i64::MAX),
        ])))
        .build()
        .await
        .expect("the group is created with its filter");
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .local_guard(true)
        .build()
        .await
        .expect("reader");
    let record = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("read deadline")
        .expect("record");
    assert_eq!(record.offset, 1);
    assert_eq!(record.message.payload.as_ref(), &[0xff, 0x00]);
    reader.ack(&record).await.expect("acknowledge");
    assert!(
        reader
            .try_next_page()
            .await
            .expect("remaining records")
            .is_none()
    );
    reader.close().await.expect("close");
}
