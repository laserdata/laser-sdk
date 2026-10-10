use crate::error::LaserError;
use crate::iggy::prelude::IggyMessage;
use crate::laser::Laser;
use laser_wire::browse::{BrowseOutcome, BrowseReply, GetSchema};
use laser_wire::codes::{AGDX_GET_SCHEMA_CODE, QUERY_OP_VERSION};
use laser_wire::filter::eval::{CompiledFilter, DecodeLimits, FilterRecord};
use laser_wire::filter::headers;
use laser_wire::filter::{
    ConsumerFilter, FaultPolicy, FaultReason, FilterError, FilterErrorReason, FilteredPage,
    RecordPolicy, Verdict,
};
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::limits::{MAX_FILTER_PARSE_DEPTH, MAX_FILTERED_PAGE_BYTES};
use std::collections::BTreeSet;

// Selected records fit within the wire page cap. Pass-through faults use
// the reported server bounds so tighter limits remain verifiable.
const GUARD_LIMITS: DecodeLimits = DecodeLimits {
    max_payload_bytes: MAX_FILTERED_PAGE_BYTES as usize,
    max_depth: MAX_FILTER_PARSE_DEPTH,
};

/// Re-evaluates every returned record with the shared evaluator before the
/// caller sees it. A record the server selected but the local evaluation does
/// not fails the page, so a disagreement surfaces before anything is handled
/// or acknowledged.
pub(crate) struct LocalGuard {
    compiled: CompiledFilter,
}

impl LocalGuard {
    pub(crate) fn new(filter: &ConsumerFilter) -> Result<Self, LaserError> {
        Ok(Self {
            compiled: CompiledFilter::compile(filter)?,
        })
    }

    pub(crate) async fn load(laser: &Laser, filter: &ConsumerFilter) -> Result<Self, LaserError> {
        if filter.schema_refs.is_empty() {
            return Self::new(filter);
        }
        let mut schemas = Vec::with_capacity(filter.schema_refs.len());
        for id in &filter.schema_refs {
            let payload = encode_named(&GetSchema {
                v: QUERY_OP_VERSION,
                id: *id,
                stream: filter
                    .schema_stream
                    .clone()
                    .or_else(|| laser.resource_stream().map(str::to_owned)),
            })?;
            let reply = laser
                .send_raw_with_response(AGDX_GET_SCHEMA_CODE, payload)
                .await?;
            match decode_named::<BrowseReply>(&reply)? {
                BrowseReply::Ok(BrowseOutcome::Schema(Some(info))) if info.schema.id == *id => {
                    schemas.push(info.schema)
                }
                BrowseReply::Err(error) => return Err(LaserError::Query(error)),
                _ => {
                    return Err(FilterError::new(
                        FilterErrorReason::NotFound,
                        format!("writer schema {id} was not returned"),
                    )
                    .into());
                }
            }
        }
        Ok(Self {
            compiled: CompiledFilter::compile_with_schemas(filter, &schemas)?,
        })
    }

    pub(crate) fn check(
        &self,
        page: &FilteredPage,
        messages: &[IggyMessage],
    ) -> Result<(), LaserError> {
        if page.policy.digest.as_ref() != Some(self.compiled.digest()) {
            return Err(FilterError::new(
                FilterErrorReason::Conflict,
                "the server ran another filter than the one the local guard checks",
            )
            .into());
        }
        let unevaluated: BTreeSet<u64> = page.unevaluated.iter().copied().collect();
        let filter = self.compiled.filter();
        if !unevaluated.is_empty()
            && filter.fault_policy != FaultPolicy::Pass
            && filter.foreign_policy != RecordPolicy::Pass
            && filter.mismatch_policy != RecordPolicy::Pass
        {
            return Err(FilterError::new(
                FilterErrorReason::Backend,
                "the server returned unevaluated records without a pass policy",
            )
            .into());
        }
        let limits = match page.evaluation_limits.as_deref() {
            Some(limits)
                if limits.max_payload_bytes > 0
                    && limits.max_payload_bytes <= MAX_FILTERED_PAGE_BYTES
                    && limits.max_depth > 0
                    && limits.max_depth as usize <= MAX_FILTER_PARSE_DEPTH =>
            {
                DecodeLimits {
                    max_payload_bytes: limits.max_payload_bytes as usize,
                    max_depth: limits.max_depth as usize,
                }
            }
            Some(_) => {
                return Err(FilterError::new(
                    FilterErrorReason::Backend,
                    "the server returned invalid decoder limits",
                )
                .into());
            }
            None if !unevaluated.is_empty() && filter.fault_policy == FaultPolicy::Pass => {
                return Err(LaserError::unsupported(
                    "filters",
                    "local verification of pass-through decode faults requires the server's evaluation limits",
                ));
            }
            None => GUARD_LIMITS,
        };
        for message in messages {
            let offset = message.header.offset;
            let raw = message.user_headers.as_deref().unwrap_or_default();
            let headers = headers::decode_for(self.compiled.header_need(), raw);
            let (verdict, fault) = headers.as_ref().map_or(
                (Verdict::Fault, Some(FaultReason::Malformed)),
                |headers| {
                    self.compiled.evaluate_with_fault(
                        &FilterRecord {
                            payload: &message.payload,
                            headers,
                        },
                        &limits,
                    )
                },
            );
            let agrees = if unevaluated.contains(&offset) {
                verdict == Verdict::Fault
                    && fault
                        .is_some_and(|reason| self.compiled.policy_for(reason) == FaultPolicy::Pass)
            } else {
                verdict == Verdict::Selected
            };
            if !agrees {
                return Err(FilterError::new(FilterErrorReason::Backend,
                    format!("the server's evaluation of offset {offset} on partition {} disagrees with the local guard", page.partition_id)).into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::codes::FILTER_OP_VERSION;
    use laser_wire::filter::{AppliedPolicy, FilterExpr, ReadMode, SourceGeneration, StopReason};
    use laser_wire::query::CmpOp;

    fn filter() -> ConsumerFilter {
        ConsumerFilter::json(FilterExpr::pred("table", CmpOp::Eq, "satellites"))
    }

    fn page(digest: laser_wire::schema::Digest32) -> FilteredPage {
        FilteredPage {
            v: FILTER_OP_VERSION,
            partition_id: 0,
            policy: AppliedPolicy {
                group_id: None,
                digest: Some(digest),
                filter_id: None,
                revision: None,
                mode: laser_wire::filter::ExecutionMode::Filtered,
                policy_generation: 0,
            },
            generation: SourceGeneration {
                stream_id: 1,
                stream_created_at_micros: 1,
                topic_id: 1,
                topic_created_at_micros: 1,
                partition_id: 0,
                partition_created_revision: 1,
                purge_generation: 0,
            },
            read_mode: ReadMode::Primary,
            next_scan_offset: Some(1),
            safe_ack_offset: Some(0),
            frontier: 1,
            examined: 1,
            matched: 1,
            stop: StopReason::Filled,
            fault: None,
            unevaluated: Vec::new(),
            evaluation_limits: None,
            records: Vec::new(),
        }
    }

    fn message(payload: &str) -> IggyMessage {
        IggyMessage::builder()
            .payload(bytes::Bytes::from(payload.to_owned()))
            .build()
            .expect("a message builds")
    }

    #[test]
    fn given_records_the_filter_selects_when_checked_then_should_pass() {
        let guard = LocalGuard::new(&filter()).expect("compiles");
        let digest = filter().digest();
        guard
            .check(&page(digest), &[message(r#"{"table":"satellites"}"#)])
            .expect("the local evaluation agrees");
    }

    #[test]
    fn given_a_record_the_filter_rejects_when_checked_then_should_fail_before_exposure() {
        let guard = LocalGuard::new(&filter()).expect("compiles");
        let error = guard
            .check(
                &page(filter().digest()),
                &[message(r#"{"table":"ground_stations"}"#)],
            )
            .expect_err("the local evaluation disagrees");
        assert_eq!(error.filter_reason(), Some(FilterErrorReason::Backend));
    }

    #[test]
    fn given_a_page_run_under_another_filter_when_checked_then_should_report_a_conflict() {
        let guard = LocalGuard::new(&filter()).expect("compiles");
        let other = ConsumerFilter::json(FilterExpr::present("kind")).digest();
        let error = guard.check(&page(other), &[]).expect_err("digests differ");
        assert_eq!(error.filter_reason(), Some(FilterErrorReason::Conflict));
    }
    #[test]
    fn given_a_forged_unevaluated_offset_without_pass_when_checked_then_should_reject() {
        let definition = filter();
        let guard = LocalGuard::new(&definition).expect("compiles");
        let mut page = page(definition.digest());
        page.unevaluated = vec![0];
        assert!(
            guard
                .check(&page, &[message(r#"{"table":"ground_stations"}"#)])
                .is_err()
        );
    }

    #[test]
    fn given_a_tighter_server_decoder_limit_when_pass_is_verified_then_should_reproduce_its_fault()
    {
        let definition = filter().with_fault_policy(FaultPolicy::Pass);
        let guard = LocalGuard::new(&definition).expect("compiles");
        let mut page = page(definition.digest());
        page.unevaluated = vec![0];
        page.evaluation_limits = Some(Box::new(laser_wire::filter::FilterDecodeLimits {
            max_payload_bytes: 4,
            max_depth: 64,
        }));
        guard
            .check(&page, &[message(r#"{"table":"ground_stations"}"#)])
            .expect("the server's size limit faults this record");
        page.evaluation_limits = Some(Box::new(laser_wire::filter::FilterDecodeLimits {
            max_payload_bytes: 1024,
            max_depth: 64,
        }));
        assert!(
            guard
                .check(&page, &[message(r#"{"table":"ground_stations"}"#)])
                .is_err(),
            "a false marker cannot bypass evaluation"
        );
    }

    #[test]
    fn given_a_fault_with_the_wrong_pass_policy_when_checked_then_should_reject() {
        let definition = filter().with_foreign_policy(RecordPolicy::Pass);
        let guard = LocalGuard::new(&definition).expect("compiles");
        let mut page = page(definition.digest());
        page.unevaluated = vec![0];
        assert!(guard.check(&page, &[message("broken json")]).is_err());
    }
}
