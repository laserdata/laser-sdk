use crate::control::{
    GapAcceptance, OversizePolicy, Projection, ProjectionBinding, ProjectionHistory,
    ProjectionKind, SchemaDef, SchemaSource, SourceSelector,
};
use crate::query::QueryError;
use serde::{Deserialize, Serialize};

/// A registered projection plus the bindings that route topics into it. The full
/// picture of one materialized view: its extraction schema, expected content
/// type, indexed fields, and where it applies.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectionInfo {
    pub projection: Projection,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bindings: Vec<ProjectionBinding>,
    /// How far each binding has materialized on the answering node, present
    /// only when the request asked for it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub status: Vec<BindingStatus>,
}

/// One binding's materialization as the answering node sees it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BindingStatus {
    #[serde(default, skip_serializing_if = "ProjectionHistory::is_append")]
    pub history: ProjectionHistory,
    pub source: SourceSelector,
    pub index: String,
    pub realization: RealizationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realization_reason: Option<String>,
    /// The class of the last failed attempt to create the destination table,
    /// present while realization is pending after a failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureClass>,
    /// Why the binding makes no visible progress although no record holds it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stall: Option<StallReason>,
    /// Creation time of the source topic incarnation being materialized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_incarnation: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<PartitionProgress>,
    /// Records or history gaps that stop a partition from advancing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub holds: Vec<Hold>,
    /// Missing history an operator accepted for this source.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub accepted_gaps: Vec<GapAcceptance>,
    /// Records skipped from this binding's target and dead-lettered.
    #[serde(default)]
    pub skipped_records: u64,
}

/// Whether a binding's destination table exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RealizationState {
    Pending,
    /// Another node owns the shared destination and creates the table.
    AwaitingOwner,
    Realized,
    #[serde(other)]
    Unknown,
}

/// Why the destination table could not be created.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FailureClass {
    /// The binding names a backend the server cannot resolve.
    Configuration,
    /// The destination refused the table definition.
    Schema,
    /// The destination refused the statement itself.
    Poison,
    /// The destination failed in a way no retry recovers.
    Fatal,
    /// The destination was briefly out of reach.
    Transient,
    /// The destination ran out of capacity.
    Capacity,
    /// The destination accepts no writes for now.
    ReadOnly,
    #[serde(other)]
    Unknown,
}

/// What keeps a binding from visible progress when no record holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StallReason {
    /// The source topic does not exist.
    SourceMissing,
    /// The source partition heads could not be read.
    SourceUnreadable,
    /// The destination's progress could not be read.
    DestinationUnreadable,
    /// The destination refuses writes for now, so the projector waits.
    Backpressure,
    #[serde(other)]
    Unknown,
}

/// One source partition's progress into a binding's destination.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionProgress {
    pub partition_id: u32,
    /// The last offset the source partition holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_offset: Option<u64>,
    /// The last offset the destination committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_offset: Option<u64>,
    /// The first offset the binding materialized for this incarnation, when
    /// the answering node observed it. Above zero, the history started late.
    /// `None` means unknown, never complete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_offset: Option<u64>,
}

/// Why a partition does not advance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hold {
    pub partition_id: u32,
    pub offset: u64,
    pub cause: HoldCause,
    pub reason: String,
}

/// What holds a partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HoldCause {
    Extraction,
    Schema,
    Destination,
    Oversize,
    MissingHistory,
    /// The binding was applied for an earlier incarnation of its source
    /// stream. Apply it again to materialize the live one.
    RetiredIncarnation,
    #[serde(other)]
    Unknown,
}

/// How far a materialization setup has come, weakest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum MaterializationReadiness {
    /// The definitions are durably appended.
    Accepted,
    /// The answering node applied the definitions.
    Applied,
    /// The destination table exists.
    Realized,
    /// The destination reached the source heads captured after apply.
    CaughtUp,
}

impl BindingStatus {
    /// The level this status proves on its own: `Realized` once the table
    /// exists, else `Applied`. Catch-up needs a captured target, see
    /// [`caught_up_to`](Self::caught_up_to).
    pub fn readiness(&self) -> MaterializationReadiness {
        if self.realization == RealizationState::Realized {
            MaterializationReadiness::Realized
        } else {
            MaterializationReadiness::Applied
        }
    }

    /// Whether this status reached every head `target` captured, on the same
    /// source incarnation. A partition the target saw empty needs nothing.
    pub fn caught_up_to(&self, target: &BindingStatus) -> bool {
        self.realization == RealizationState::Realized
            && !self.is_blocked()
            && self.source_incarnation.is_some()
            && self.source_incarnation == target.source_incarnation
            && self.source == target.source
            && self.index == target.index
            && self.history == target.history
            && target.partitions.iter().all(|wanted| {
                wanted.head_offset.is_none_or(|head| {
                    self.partitions.iter().any(|seen| {
                        seen.partition_id == wanted.partition_id
                            && seen.applied_offset.is_some_and(|applied| applied >= head)
                    })
                })
            })
    }

    /// Whether a record or a history gap stops a partition.
    pub fn is_blocked(&self) -> bool {
        !self.holds.is_empty()
    }

    /// Whether the destination table failed in a way no retry recovers, so
    /// the binding or its backend must change.
    pub fn is_failed(&self) -> bool {
        self.failure.is_some_and(FailureClass::is_permanent)
    }

    /// Whether the destination provably holds every record of the source
    /// incarnation it has passed: every partition with progress started at
    /// offset zero, no gap was accepted, no record was skipped and, under
    /// `oversize`, no body is kept only by reference. An unknown first offset
    /// is never complete, and a partition the binding never read is complete
    /// only while no first offset above zero says its history expired unread.
    pub fn history_complete(&self, oversize: OversizePolicy) -> bool {
        self.history.is_append()
            && self.realization == RealizationState::Realized
            && self.source_incarnation.is_some()
            && !self.is_blocked()
            && oversize != OversizePolicy::Reference
            && self.accepted_gaps.is_empty()
            && self.skipped_records == 0
            && self.partitions.iter().all(|partition| {
                partition.first_offset == Some(0)
                    || (partition.applied_offset.is_none() && partition.first_offset.is_none())
            })
    }
}

impl FailureClass {
    /// Whether the failure lasts until the binding or its backend changes.
    pub fn is_permanent(self) -> bool {
        matches!(
            self,
            Self::Configuration | Self::Schema | Self::Poison | Self::Fatal
        )
    }
}

/// A writer schema plus its lifecycle state. A `dropped` schema is hidden
/// from the active set and its id rejects re-registration with a different
/// definition, but records stamped with the id keep decoding (ids are
/// permanent). Wire mirrors stay constructible (wire-stability-bound, not
/// API-stability-bound).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub schema: SchemaDef,
    #[serde(default)]
    pub dropped: bool,
}

/// Request to read one projection's details by id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetProjection {
    pub v: u32,
    pub id: String,
    /// Also report each binding's materialization status.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub status: bool,
}

/// Request to list registered projections, optionally filtered. Empty filters
/// list every projection. `topics` keeps projections bound to any of the named
/// source topics. `name_contains` keeps those whose name contains the substring.
/// `id_prefix` keeps those whose id starts with it. `search` is the single-box
/// convenience that matches the substring against the name OR the id. `kinds`
/// keeps projections of the named kinds. The filters compose (AND). The reply is one
/// page of the matches in id order.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ListProjections {
    pub v: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kinds: Vec<ProjectionKind>,
    /// The stream whose resources the listing covers. Required in stream
    /// tenancy, where the server authorizes the stream and lists only its
    /// resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    /// The 1-based page. Zero reads the first page.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub page: u32,
    /// The projections per page. Zero leaves it to the server.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub results: u32,
}

/// One page of a projection listing. `page` counts from 1.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectionPage {
    pub items: Vec<ProjectionInfo>,
    pub page: u32,
    pub total_pages: u32,
    pub total_results: u32,
}

/// Request to read one registered writer schema by id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GetSchema {
    pub v: u32,
    pub id: u32,
    /// The stream whose registry holds `id`. Absent for the deployment-wide
    /// registry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
}

/// Request to list registered writer schemas, optionally filtered.
/// `name_contains` keeps those whose optional name contains the substring, and
/// an absent filter lists every schema. The filter lives here, not only on the
/// HTTP query, so the server pushes it down rather than the client paging the
/// whole set.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListSchemas {
    pub v: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
    /// The stream whose registry is listed. Absent for the deployment-wide
    /// registry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    /// The 1-based page. Zero reads the first page.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub page: u32,
    /// The schemas per page. Zero leaves it to the server.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub results: u32,
}

/// One page of a writer-schema listing, ordered by id. `page` counts from 1.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaPage {
    pub items: Vec<SchemaInfo>,
    pub page: u32,
    pub total_pages: u32,
    pub total_results: u32,
}

/// The synchronous register request: no id, LaserData Cloud validates the
/// definition, allocates the next free id, durably appends the control
/// event, and replies `SchemaRegistered(id)`. Distinct from
/// `ControlCommand::RegisterSchema`, the durable log form that carries the
/// allocated id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterSchema {
    pub v: u32,
    pub source: SchemaSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    /// The stream whose registry allocates the id. Absent for the
    /// deployment-wide registry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
}

/// Request to decode one record body under the schema registered for `id`,
/// the same decode the projector runs on `agdx.sid`-stamped records. Read-only
/// convenience for consoles and tools that hold a schema-first (Avro/Protobuf)
/// payload and want its JSON form without re-implementing the codecs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecodeRecord {
    pub v: u32,
    pub id: u32,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
    /// The stream the record was read from: `id` resolves in its registry,
    /// then in the deployment-wide one. Absent for the deployment-wide
    /// registry alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
}

/// Reply to a registry browse: `Ok` with the result, or `Err`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BrowseReply {
    Ok(BrowseOutcome),
    Err(QueryError),
}

/// The result of a registry browse, shaped per request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BrowseOutcome {
    /// `list_projections`: one page of the matching projections.
    Projections(ProjectionPage),
    /// `get projection`: the projection with the requested id, or `None`.
    Projection(Option<Box<ProjectionInfo>>),
    /// `list schemas`: one page of writer schemas, active and tombstoned.
    Schemas(SchemaPage),
    /// `get schema`: the schema occupying the requested id, or `None`.
    Schema(Option<SchemaInfo>),
    /// `register schema`: the LaserData-Cloud-allocated id, already durably appended
    /// to the control topic (visibility follows within the apply latency).
    SchemaRegistered(u32),
    /// `decode record`: the payload's JSON form under the requested schema,
    /// or `None` when the body does not decode under it. A `serde_json::Value`
    /// because the decoded shape is arbitrary (object, array, scalar) and the
    /// LaserData Cloud encodes exactly that value into the reply.
    Decoded(Option<serde_json::Value>),
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::QUERY_OP_VERSION;
    use crate::content::ContentType;
    use crate::control::ProjectionBinding;
    use crate::framing::{decode_named, encode_named};

    fn status(
        realization: RealizationState,
        partitions: &[(u32, Option<u64>, Option<u64>)],
    ) -> BindingStatus {
        BindingStatus {
            history: ProjectionHistory::Append,
            source: SourceSelector::new("fleet", "readings"),
            index: "readings_rows".to_owned(),
            realization,
            realization_reason: None,
            failure: None,
            stall: None,
            source_incarnation: Some(7),
            partitions: partitions
                .iter()
                .map(
                    |(partition_id, head_offset, applied_offset)| PartitionProgress {
                        partition_id: *partition_id,
                        head_offset: *head_offset,
                        applied_offset: *applied_offset,
                        first_offset: applied_offset.map(|_| 0),
                    },
                )
                .collect(),
            holds: Vec::new(),
            accepted_gaps: Vec::new(),
            skipped_records: 0,
        }
    }

    #[test]
    fn given_a_missing_incarnation_or_a_hold_when_checking_catch_up_then_should_refuse() {
        let target = status(RealizationState::Realized, &[(0, Some(10), Some(10))]);
        let mut unknown = target.clone();
        unknown.source_incarnation = None;
        assert!(!unknown.caught_up_to(&unknown));
        let mut blocked = target.clone();
        blocked.holds.push(Hold {
            partition_id: 0,
            offset: 11,
            cause: HoldCause::Schema,
            reason: "invalid".to_owned(),
        });
        assert!(!blocked.caught_up_to(&target));
    }

    #[test]
    fn given_a_captured_target_when_the_destination_reaches_it_then_should_be_caught_up() {
        let target = status(
            RealizationState::Realized,
            &[(0, Some(10), Some(4)), (1, None, None)],
        );
        let behind = status(RealizationState::Realized, &[(0, Some(12), Some(9))]);
        assert!(!behind.caught_up_to(&target));
        let reached = status(RealizationState::Realized, &[(0, Some(15), Some(10))]);
        assert!(
            reached.caught_up_to(&target),
            "a newer head does not move the target"
        );
        let unrealized = status(RealizationState::AwaitingOwner, &[(0, Some(15), Some(10))]);
        assert!(!unrealized.caught_up_to(&target));
        let mut recreated = reached.clone();
        recreated.source_incarnation = Some(8);
        assert!(
            !recreated.caught_up_to(&target),
            "offsets of another incarnation never compare"
        );
        assert_eq!(reached.readiness(), MaterializationReadiness::Realized);
        assert_eq!(unrealized.readiness(), MaterializationReadiness::Applied);
    }

    #[test]
    fn given_a_late_start_a_gap_a_skip_or_references_when_checked_then_should_not_claim_complete_history()
     {
        let complete = status(RealizationState::Realized, &[(0, Some(3), Some(3))]);
        assert!(complete.history_complete(OversizePolicy::Block));
        assert!(!complete.history_complete(OversizePolicy::Reference));
        let mut late = complete.clone();
        late.partitions[0].first_offset = Some(5);
        assert!(!late.history_complete(OversizePolicy::Block));
        let mut unknown = complete.clone();
        unknown.partitions[0].first_offset = None;
        assert!(
            !unknown.history_complete(OversizePolicy::Block),
            "unknown is never complete"
        );
        let mut skipped = complete.clone();
        skipped.skipped_records = 1;
        assert!(!skipped.history_complete(OversizePolicy::Block));
        let mut held = complete.clone();
        held.holds.push(Hold {
            partition_id: 0,
            offset: 4,
            cause: HoldCause::Oversize,
            reason: "too large".to_owned(),
        });
        assert!(held.is_blocked());
    }

    #[test]
    fn given_a_partition_whose_records_expired_unread_when_checked_then_should_not_claim_complete_history()
     {
        let mut expired = status(RealizationState::Realized, &[(0, Some(3), Some(3))]);
        expired.partitions.push(PartitionProgress {
            partition_id: 1,
            head_offset: None,
            applied_offset: None,
            first_offset: Some(5),
        });
        assert!(
            !expired.history_complete(OversizePolicy::Block),
            "records 0 to 4 of partition 1 expired before the binding read them"
        );
        let mut never_written = status(RealizationState::Realized, &[(0, Some(3), Some(3))]);
        never_written.partitions.push(PartitionProgress {
            partition_id: 1,
            head_offset: None,
            applied_offset: None,
            first_offset: None,
        });
        assert!(never_written.history_complete(OversizePolicy::Block));
    }

    #[test]
    fn given_realization_failures_when_checked_then_should_fail_only_on_a_permanent_class() {
        let mut failed = status(RealizationState::Pending, &[]);
        assert!(!failed.is_failed());
        for (class, permanent) in [
            (FailureClass::Configuration, true),
            (FailureClass::Schema, true),
            (FailureClass::Poison, true),
            (FailureClass::Fatal, true),
            (FailureClass::Transient, false),
            (FailureClass::Capacity, false),
            (FailureClass::ReadOnly, false),
            (FailureClass::Unknown, false),
        ] {
            failed.failure = Some(class);
            assert_eq!(failed.is_failed(), permanent, "{class:?}");
        }
    }

    #[test]
    fn given_a_failure_a_stall_and_a_retired_hold_when_round_tripped_then_should_preserve_them() {
        let mut stalled = status(RealizationState::Pending, &[(0, Some(3), None)]);
        stalled.failure = Some(FailureClass::ReadOnly);
        stalled.stall = Some(StallReason::Backpressure);
        stalled.holds.push(Hold {
            partition_id: 0,
            offset: 0,
            cause: HoldCause::RetiredIncarnation,
            reason: "applied for an earlier stream".to_owned(),
        });
        let back: BindingStatus =
            decode_named(&encode_named(&stalled).expect("serializes")).expect("deserializes");
        assert_eq!(back, stalled);
        let json = serde_json::to_value(&stalled).expect("serializes");
        assert_eq!(
            (&json["failure"], &json["stall"], &json["holds"][0]["cause"]),
            (
                &serde_json::json!("read_only"),
                &serde_json::json!("backpressure"),
                &serde_json::json!("retired_incarnation")
            )
        );
        let newer: BindingStatus = serde_json::from_value(serde_json::json!({
            "source": {"stream": "fleet", "topic": "readings"},
            "index": "readings_rows",
            "realization": "pending",
            "failure": "quota",
            "stall": "paused",
        }))
        .expect("a newer class decodes");
        assert_eq!(
            (newer.failure, newer.stall),
            (Some(FailureClass::Unknown), Some(StallReason::Unknown))
        );
        let quiet =
            serde_json::to_value(status(RealizationState::Realized, &[])).expect("serializes");
        assert!(
            quiet.get("failure").is_none() && quiet.get("stall").is_none(),
            "absent fields are omitted: {quiet}"
        );
    }

    #[test]
    fn given_a_projection_listing_when_encoded_then_should_send_its_page_only_when_set() {
        let first = ListProjections {
            v: QUERY_OP_VERSION,
            ..ListProjections::default()
        };
        let json = serde_json::to_value(&first).expect("serializes");
        assert!(
            json.get("page").is_none()
                && json.get("results").is_none()
                && json.get("kinds").is_none(),
            "{json}"
        );
        let third = ListProjections {
            v: QUERY_OP_VERSION,
            kinds: vec![ProjectionKind::Row, ProjectionKind::Record],
            page: 3,
            results: 20,
            ..ListProjections::default()
        };
        let back: ListProjections =
            decode_named(&encode_named(&third).expect("serializes")).expect("deserializes");
        assert_eq!(
            (back.kinds, back.page, back.results),
            (vec![ProjectionKind::Row, ProjectionKind::Record], 3, 20)
        );
    }

    #[test]
    fn given_a_browse_reply_when_round_tripped_then_should_preserve_projection_details() {
        let info = ProjectionInfo {
            projection: Projection::builder("reading.v1")
                .name("reading")
                .version(1)
                .content_type(ContentType::Json)
                .fields(["reading_id", "cpu"])
                .build(),
            bindings: vec![
                ProjectionBinding::builder()
                    .source("fleet", "readings")
                    .allow("reading.v1")
                    .default_projection("reading.v1")
                    .index("readings_rows")
                    .build(),
            ],
            status: Vec::new(),
        };
        let reply = BrowseReply::Ok(BrowseOutcome::Projections(ProjectionPage {
            items: vec![info],
            page: 1,
            total_pages: 1,
            total_results: 1,
        }));
        let bytes = encode_named(&reply).expect("the reply serializes");
        let back: BrowseReply = decode_named(&bytes).expect("the reply deserializes");
        let BrowseReply::Ok(BrowseOutcome::Projections(page)) = back else {
            panic!("expected an Ok(Projections) browse reply");
        };
        assert_eq!((page.page, page.total_pages, page.total_results), (1, 1, 1));
        let list = page.items;
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].projection.id.as_str(), "reading.v1");
        assert_eq!(list[0].projection.extraction.fields.len(), 2);
        assert_eq!(list[0].bindings[0].index, "readings_rows");
    }

    #[test]
    fn given_a_decode_record_when_round_tripped_then_should_preserve_payload_bytes() {
        let request = DecodeRecord {
            v: QUERY_OP_VERSION,
            id: 7,
            payload: vec![0xff, 0x00, 0x10],
            stream: None,
        };
        let bytes = encode_named(&request).expect("serializes");
        let back: DecodeRecord = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.id, 7);
        assert_eq!(back.payload, vec![0xff, 0x00, 0x10]);
        assert_eq!(back.stream, None);
    }

    #[test]
    fn given_schema_requests_with_a_stream_when_round_tripped_then_should_preserve_it() {
        let get = GetSchema {
            v: QUERY_OP_VERSION,
            id: 7,
            stream: Some("acme".to_owned()),
        };
        let back: GetSchema =
            decode_named(&encode_named(&get).expect("serializes")).expect("deserializes");
        assert_eq!((back.id, back.stream.as_deref()), (7, Some("acme")));
        let list = ListSchemas {
            v: QUERY_OP_VERSION,
            name_contains: None,
            stream: Some("acme".to_owned()),
            page: 0,
            results: 0,
        };
        let back: ListSchemas =
            decode_named(&encode_named(&list).expect("serializes")).expect("deserializes");
        assert_eq!(back.stream.as_deref(), Some("acme"));
        let register = RegisterSchema {
            v: QUERY_OP_VERSION,
            source: SchemaSource::JsonSchema {
                schema: "{}".to_owned(),
            },
            name: None,
            version: None,
            stream: Some("acme".to_owned()),
        };
        let back: RegisterSchema =
            decode_named(&encode_named(&register).expect("serializes")).expect("deserializes");
        assert_eq!(back.stream.as_deref(), Some("acme"));
        let decode = DecodeRecord {
            v: QUERY_OP_VERSION,
            id: 7,
            payload: vec![1],
            stream: Some("acme".to_owned()),
        };
        let back: DecodeRecord =
            decode_named(&encode_named(&decode).expect("serializes")).expect("deserializes");
        assert_eq!(back.stream.as_deref(), Some("acme"));
    }

    #[test]
    fn given_schema_requests_without_a_stream_when_encoded_then_should_omit_it() {
        let get = GetSchema {
            v: QUERY_OP_VERSION,
            id: 7,
            stream: None,
        };
        let list = ListSchemas {
            v: QUERY_OP_VERSION,
            name_contains: None,
            stream: None,
            page: 0,
            results: 0,
        };
        for json in [
            serde_json::to_string(&get).expect("serializes"),
            serde_json::to_string(&list).expect("serializes"),
        ] {
            assert!(
                !json.contains("stream"),
                "an unset stream is omitted: {json}"
            );
        }
    }
}
