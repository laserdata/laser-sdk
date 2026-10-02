use laser_wire::filter::{
    ExecutionMode, FaultReason, FilteredPage, FilteredStart, SourceGeneration, StopReason,
};
use laser_wire::schema::Digest32;
use std::collections::{BTreeSet, VecDeque};
use tokio::time::Instant;

/// One partition's read position and acknowledgment state.
///
/// Pages are read ahead of acknowledgments. A page's safe offset is stored
/// only once it and every earlier page are fully handled, so an acknowledgment
/// never covers a record the caller has not finished.
pub(crate) struct PartitionProgress {
    original: FilteredStart,
    start: FilteredStart,
    pages: VecDeque<PendingPage>,
    unstored: Option<AckTarget>,
    stored_offset: Option<u64>,
    blocked: Option<Blocked>,
    retry_at: Option<Instant>,
    first_sequence: Option<u64>,
    settled_sequence: u64,
}

/// The offset a completed prefix of pages makes safe to store, with the
/// history and the policy it was read under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AckTarget {
    pub(crate) group_id: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) generation: SourceGeneration,
    pub(crate) digest: Option<Digest32>,
    pub(crate) mode: ExecutionMode,
    pub(crate) policy_generation: u64,
}

/// Why a partition cannot be read past its current position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Blocked {
    Fault { offset: u64, reason: FaultReason },
    Oversized { offset: u64 },
}

struct PendingPage {
    sequence: u64,
    target: Option<AckTarget>,
    outstanding: BTreeSet<u64>,
}

impl PartitionProgress {
    pub(crate) fn new(start: FilteredStart) -> Self {
        Self {
            original: start.clone(),
            start,
            pages: VecDeque::new(),
            unstored: None,
            stored_offset: None,
            blocked: None,
            retry_at: None,
            first_sequence: None,
            settled_sequence: 0,
        }
    }

    /// Whether the partition waits out a delay after an error or a block, so
    /// one failing partition neither starves the others nor spins the reader.
    pub(crate) fn waiting(&self, now: Instant) -> bool {
        self.retry_at.is_some_and(|retry_at| now < retry_at)
    }

    pub(crate) const fn defer(&mut self, until: Instant) {
        self.retry_at = Some(until);
    }

    /// Where the next read starts.
    pub(crate) const fn start(&self) -> &FilteredStart {
        &self.start
    }

    pub(crate) const fn blocked(&self) -> Option<Blocked> {
        self.blocked
    }

    /// Pages read but not yet fully handled.
    pub(crate) fn in_flight(&self) -> usize {
        self.pages.len()
    }

    pub(crate) fn knows(&self, sequence: u64) -> bool {
        self.pages.iter().any(|page| page.sequence == sequence)
            || self
                .first_sequence
                .is_some_and(|first| sequence >= first && sequence <= self.settled_sequence)
    }

    /// Track a page and its returned `offsets`, and continue after it. A
    /// blocked partition is read again after its delay, so a block ends once
    /// the record ages out of the partition or the policy stops faulting.
    ///
    /// A page without records waits only for the pages before it, so behind
    /// an unfinished page it folds its safe offset into the newest one
    /// instead of queueing. Only pages that returned records hold a slot, and
    /// a caller holding one page on a quiet partition never fills the queue.
    pub(crate) fn record(
        &mut self,
        sequence: u64,
        page: &FilteredPage,
        offsets: impl IntoIterator<Item = u64>,
    ) {
        self.retry_at = None;
        self.first_sequence.get_or_insert(sequence);
        self.start = page.next_start(&self.original);
        self.blocked = match (page.stop, page.fault) {
            (StopReason::Fault, Some(fault)) => Some(Blocked::Fault {
                offset: fault.offset,
                reason: fault.reason,
            }),
            (StopReason::OversizedRecord, Some(fault)) => Some(Blocked::Oversized {
                offset: fault.offset,
            }),
            _ => None,
        };
        let target = page.safe_ack_offset.map(|offset| AckTarget {
            group_id: page.policy.group_id,
            offset,
            generation: page.generation,
            digest: page.policy.digest.clone(),
            mode: page.policy.mode,
            policy_generation: page.policy.policy_generation,
        });
        let outstanding: BTreeSet<u64> = offsets.into_iter().collect();
        match self.pages.back_mut() {
            Some(newest) if outstanding.is_empty() => {
                if target.is_some() {
                    newest.target = target;
                }
            }
            _ => self.pages.push_back(PendingPage {
                sequence,
                target,
                outstanding,
            }),
        }
        self.settle();
    }

    /// Mark one returned record handled. A record already settled is a no-op.
    pub(crate) fn complete_record(&mut self, sequence: u64, offset: u64) {
        if let Some(page) = self.pages.iter_mut().find(|page| page.sequence == sequence) {
            page.outstanding.remove(&offset);
        }
        self.settle();
    }

    pub(crate) fn complete_through(&mut self, sequence: u64, offset: u64) -> bool {
        let Some(mut target) = self
            .pages
            .iter()
            .find(|page| page.sequence == sequence)
            .and_then(|page| page.target.clone())
        else {
            return true;
        };
        if offset > target.offset {
            return false;
        }
        for page in &mut self.pages {
            if page.sequence < sequence {
                page.outstanding.clear();
            } else if page.sequence == sequence {
                page.outstanding.retain(|pending| *pending > offset);
            }
        }
        self.settle();
        target.offset = offset;
        if self
            .unstored
            .as_ref()
            .is_none_or(|pending| pending.offset < offset)
        {
            self.unstored = Some(target);
        }
        true
    }

    /// Mark every record of a page handled. A page already settled is a no-op.
    pub(crate) fn complete_page(&mut self, sequence: u64) {
        if let Some(page) = self.pages.iter_mut().find(|page| page.sequence == sequence) {
            page.outstanding.clear();
        }
        self.settle();
    }

    /// The newest offset the completed prefix makes safe and that is not yet
    /// stored.
    pub(crate) const fn unstored(&self) -> Option<&AckTarget> {
        self.unstored.as_ref()
    }

    /// `target` is stored. A newer target completed meanwhile stays pending.
    pub(crate) fn stored(&mut self, target: &AckTarget) {
        self.stored_offset = Some(
            self.stored_offset
                .map_or(target.offset, |stored| stored.max(target.offset)),
        );
        if self.unstored.as_ref() == Some(target) {
            self.unstored = None;
        }
    }

    /// The newest offset this reader stored for the partition.
    pub(crate) const fn stored_offset(&self) -> Option<u64> {
        self.stored_offset
    }

    // Retire the completed prefix. Its newest safe offset supersedes any older
    // unstored one, because a stored offset covers everything before it.
    fn settle(&mut self) {
        while self
            .pages
            .front()
            .is_some_and(|page| page.outstanding.is_empty())
        {
            if let Some(page) = self.pages.pop_front() {
                self.settled_sequence = page.sequence;
                if let Some(target) = page.target {
                    self.unstored = Some(target);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::codes::FILTER_OP_VERSION;
    use laser_wire::filter::{AppliedPolicy, Continuation, ReadMode, RecordFault};

    fn generation() -> SourceGeneration {
        SourceGeneration {
            stream_id: 1,
            stream_created_at_micros: 10,
            topic_id: 2,
            topic_created_at_micros: 20,
            partition_id: 0,
            partition_created_revision: 30,
            purge_generation: 0,
        }
    }

    fn page(next_scan_offset: Option<u64>, stop: StopReason) -> FilteredPage {
        FilteredPage {
            v: FILTER_OP_VERSION,
            partition_id: 0,
            policy: AppliedPolicy {
                group_id: None,
                digest: Some(Digest32::new([7; 32])),
                filter_id: None,
                revision: None,
                mode: ExecutionMode::Filtered,
                policy_generation: 0,
            },
            generation: generation(),
            read_mode: ReadMode::Primary,
            next_scan_offset,
            safe_ack_offset: next_scan_offset.and_then(|next| next.checked_sub(1)),
            frontier: 100,
            examined: 10,
            matched: 0,
            stop,
            fault: None,
            unevaluated: Vec::new(),
            evaluation_limits: None,
            records: Vec::new(),
        }
    }

    #[test]
    fn given_out_of_order_acks_when_settled_then_should_store_only_the_completed_prefix() {
        let mut progress = PartitionProgress::new(FilteredStart::Next);
        progress.record(1, &page(Some(10), StopReason::Filled), [3, 7]);
        progress.record(2, &page(Some(20), StopReason::Filled), [15]);

        progress.complete_page(2);
        assert_eq!(progress.unstored(), None, "the first page is still open");
        progress.complete_record(1, 3);
        assert_eq!(progress.unstored(), None, "offset 7 is still open");
        progress.complete_record(1, 7);

        assert_eq!(progress.unstored().map(|target| target.offset), Some(19));
        assert_eq!(progress.in_flight(), 0);
    }

    #[test]
    fn given_a_checkpoint_inside_a_page_when_acknowledging_through_it_then_should_leave_later_records_pending()
     {
        let mut progress = PartitionProgress::new(FilteredStart::First);
        progress.record(1, &page(Some(10), StopReason::Filled), [3, 7]);
        progress.record(2, &page(Some(20), StopReason::Filled), [11, 15]);
        assert!(progress.complete_through(2, 11));
        assert_eq!(progress.unstored().map(|target| target.offset), Some(11));
        assert_eq!(progress.in_flight(), 1);
        let stored = progress
            .unstored()
            .cloned()
            .expect("the checkpoint can be stored");
        progress.stored(&stored);
        assert!(progress.complete_through(2, 11));
        assert_eq!(progress.unstored().map(|target| target.offset), Some(11));
        progress.complete_record(2, 15);
        assert_eq!(progress.unstored().map(|target| target.offset), Some(19));
    }

    #[test]
    fn given_an_offset_past_the_page_when_acknowledging_through_it_then_should_keep_progress_unchanged()
     {
        let mut progress = PartitionProgress::new(FilteredStart::First);
        progress.record(1, &page(Some(10), StopReason::Filled), [3, 7]);
        assert!(!progress.complete_through(1, 10));
        assert_eq!(progress.unstored(), None);
        assert_eq!(progress.in_flight(), 1);
    }

    #[test]
    fn given_an_empty_page_when_recorded_then_should_make_its_scan_safe_at_once() {
        let mut progress = PartitionProgress::new(FilteredStart::Next);
        progress.record(1, &page(Some(500), StopReason::Budget), []);

        let target = progress
            .unstored()
            .cloned()
            .expect("an examined range is safe");
        assert_eq!(target.offset, 499);
        progress.stored(&target);
        assert_eq!(progress.unstored(), None);
    }

    #[test]
    fn given_a_page_that_examined_nothing_when_recorded_then_should_repeat_the_original_start() {
        let mut progress = PartitionProgress::new(FilteredStart::Next);
        let mut empty = page(None, StopReason::EndOfVisible);
        empty.safe_ack_offset = None;
        progress.record(1, &empty, []);

        assert_eq!(progress.start(), &FilteredStart::Next);
        assert_eq!(
            progress.unstored(),
            None,
            "nothing examined, nothing to store"
        );
    }

    #[test]
    fn given_a_scanned_page_when_recorded_then_should_continue_after_it() {
        let mut progress = PartitionProgress::new(FilteredStart::First);
        progress.record(1, &page(Some(42), StopReason::Budget), []);

        assert_eq!(
            progress.start(),
            &FilteredStart::Continue(Continuation {
                group_id: None,
                next_scan_offset: 42,
                generation: generation(),
                digest: Some(Digest32::new([7; 32])),
                read_mode: ReadMode::Primary,
                mode: ExecutionMode::Filtered,
                policy_generation: 0,
            })
        );
    }

    #[test]
    fn given_a_fault_stop_when_recorded_then_should_block_the_partition_at_the_fault() {
        let mut progress = PartitionProgress::new(FilteredStart::First);
        let mut faulted = page(Some(8), StopReason::Fault);
        faulted.fault = Some(RecordFault {
            offset: 8,
            reason: FaultReason::Malformed,
        });
        progress.record(1, &faulted, [2]);

        assert_eq!(
            progress.blocked(),
            Some(Blocked::Fault {
                offset: 8,
                reason: FaultReason::Malformed
            })
        );
        progress.complete_record(1, 2);
        assert_eq!(
            progress.unstored().map(|target| target.offset),
            Some(7),
            "the fault itself stays unacknowledged"
        );
    }

    #[test]
    fn given_empty_pages_behind_a_held_page_when_recorded_then_should_not_grow_the_queue() {
        let mut progress = PartitionProgress::new(FilteredStart::Next);
        progress.record(1, &page(Some(10), StopReason::Filled), [4]);
        for sequence in 2..200 {
            progress.record(sequence, &page(Some(10 + sequence), StopReason::Budget), []);
        }
        assert_eq!(progress.in_flight(), 1, "idle rounds hold no slot");
        assert_eq!(
            progress.unstored(),
            None,
            "the held record keeps the offset"
        );
        progress.complete_record(1, 4);
        assert_eq!(
            progress.unstored().map(|target| target.offset),
            Some(208),
            "the newest scan becomes safe with the held page"
        );
    }

    #[test]
    fn given_a_newer_completion_while_storing_when_stored_then_should_keep_the_newer_target() {
        let mut progress = PartitionProgress::new(FilteredStart::Next);
        progress.record(1, &page(Some(10), StopReason::Budget), []);
        let first = progress.unstored().cloned().expect("safe");
        progress.record(2, &page(Some(20), StopReason::Budget), []);

        progress.stored(&first);

        assert_eq!(progress.unstored().map(|target| target.offset), Some(19));
    }
    #[test]
    fn given_a_retired_assignment_when_new_pages_arrive_then_should_refuse_the_old_sequence() {
        let mut original = PartitionProgress::new(FilteredStart::Next);
        original.record(1, &page(Some(10), StopReason::Filled), [3]);
        original.complete_page(1);
        assert!(
            original.knows(1),
            "an already completed delivery is retryable in its assignment"
        );
        let mut reassigned = PartitionProgress::new(FilteredStart::Next);
        assert!(!reassigned.knows(1));
        reassigned.record(2, &page(Some(20), StopReason::Filled), [15]);
        assert!(!reassigned.knows(1));
        assert!(reassigned.knows(2));
    }
}
