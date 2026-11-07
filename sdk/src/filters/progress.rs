use laser_wire::filter::{
    AckContext, AckReceipt, AppliedPolicy, Continuation, FaultReason, FilteredPage, FilteredStart,
    SourceGeneration, StopReason,
};
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
    // The durable offset the last receipt reported for a source generation,
    // which another consumer can have moved past this reader's own progress.
    durable: Option<(SourceGeneration, u64)>,
    blocked: Option<Blocked>,
    // The acknowledgment that skips the blocking record, fenced like the page
    // that reported the block.
    skip_target: Option<AckTarget>,
    retry_at: Option<Instant>,
    first_sequence: Option<u64>,
    settled_sequence: u64,
    // The first offset no page examined yet, as the last page reported it.
    wait_from: Option<u64>,
}

/// The offset a completed prefix of pages makes safe to store, with the
/// group, the history and the partition owner it was read under.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AckTarget {
    pub(crate) group_id: Option<u64>,
    pub(crate) offset: u64,
    pub(crate) generation: SourceGeneration,
    pub(crate) ack_context: AckContext,
    pub(crate) policy: AppliedPolicy,
}

/// Why a partition cannot be read past its current position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Blocked {
    Fault { offset: u64, reason: FaultReason },
    Oversized { offset: u64 },
}

impl Blocked {
    const fn offset(self) -> u64 {
        match self {
            Self::Fault { offset, .. } | Self::Oversized { offset } => offset,
        }
    }
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
            durable: None,
            blocked: None,
            skip_target: None,
            retry_at: None,
            first_sequence: None,
            settled_sequence: 0,
            wait_from: None,
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

    /// The offset a filtered wait holds this partition from: the first record
    /// no page examined, else the start offset of an unread partition.
    pub(crate) const fn wait_offset(&self) -> u64 {
        match (self.wait_from, &self.original) {
            (Some(offset), _) => offset,
            (None, FilteredStart::Offset(offset)) => *offset,
            (None, _) => 0,
        }
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
        // A page that examined nothing saw the head as its frontier, so the
        // next record lands after it. A head of 0 may also be an empty
        // partition, whose next record is 0.
        self.wait_from = Some(match page.next_scan_offset {
            Some(next) => next,
            None if page.frontier == 0 => 0,
            None => page.frontier.saturating_add(1),
        });
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
        self.skip_target = self.blocked.map(|blocked| AckTarget {
            group_id: page.policy.group_id,
            offset: blocked.offset(),
            generation: page.generation,
            ack_context: page.ack_context,
            policy: page.policy.clone(),
        });
        let target = page.safe_ack_offset.map(|offset| AckTarget {
            group_id: page.policy.group_id,
            offset,
            generation: page.generation,
            ack_context: page.ack_context,
            policy: page.policy.clone(),
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
            self.pend(target);
        }
        true
    }

    /// Pass the record the partition is blocked at, as a native consumer seeks
    /// past it. The skip is acknowledged under the fence of the page that
    /// reported the block and, like an empty page, only once every earlier
    /// page is handled. `false` refuses an offset the partition is not
    /// blocked at.
    pub(crate) fn skip(&mut self, sequence: u64, offset: u64) -> bool {
        let Some(target) = self.skip_target.take_if(|target| target.offset == offset) else {
            return false;
        };
        let next = offset.saturating_add(1);
        self.blocked = None;
        self.retry_at = None;
        self.wait_from = Some(next);
        self.start = match &self.start {
            FilteredStart::Continue(continuation) => FilteredStart::Continue(Continuation {
                next_scan_offset: next,
                ..continuation.clone()
            }),
            _ => FilteredStart::Offset(next),
        };
        match self.pages.back_mut() {
            Some(newest) => newest.target = Some(target),
            None => self.pages.push_back(PendingPage {
                sequence,
                target: Some(target),
                outstanding: BTreeSet::new(),
            }),
        }
        self.settle();
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

    /// `receipt` answered the store of `target`: the same source generation
    /// and an offset at least the target's. Its offset is the durable
    /// progress, which only covers later acknowledgments and never marks a
    /// local record handled. A newer target completed meanwhile stays pending
    /// unless the durable progress covers it. `false` refuses a receipt that
    /// does not answer the target.
    pub(crate) fn acknowledged(&mut self, target: &AckTarget, receipt: &AckReceipt) -> bool {
        if receipt.generation != target.generation || receipt.offset < target.offset {
            return false;
        }
        self.stored_offset = Some(
            self.stored_offset
                .map_or(target.offset, |stored| stored.max(target.offset)),
        );
        let durable = match self.durable {
            Some((generation, durable)) if generation == receipt.generation => {
                durable.max(receipt.offset)
            }
            _ => receipt.offset,
        };
        self.durable = Some((receipt.generation, durable));
        if self
            .unstored
            .as_ref()
            .is_some_and(|pending| pending == target || self.covered(pending))
        {
            self.unstored = None;
        }
        true
    }

    /// The newest offset this reader stored for the partition.
    pub(crate) const fn stored_offset(&self) -> Option<u64> {
        self.stored_offset
    }

    // Keep `target` to store unless stored or durable progress covers it. A
    // covered target also covers an older one still pending.
    fn pend(&mut self, target: AckTarget) {
        if !self.covered(&target) {
            self.unstored = Some(target);
        } else if self
            .unstored
            .as_ref()
            .is_some_and(|pending| pending.offset <= target.offset)
        {
            self.unstored = None;
        }
    }

    fn covered(&self, target: &AckTarget) -> bool {
        self.stored_offset
            .is_some_and(|stored| stored >= target.offset)
            || self.durable.is_some_and(|(generation, durable)| {
                generation == target.generation && durable >= target.offset
            })
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
                    self.pend(target);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::codes::FILTER_OP_VERSION;
    use laser_wire::filter::{AppliedPolicy, Continuation, ExecutionMode, RecordFault};
    use laser_wire::schema::Digest32;
    use std::time::Duration;

    fn generation() -> SourceGeneration {
        SourceGeneration {
            stream_id: 1,
            stream_created_at_micros: 10,
            topic_id: 2,
            topic_created_at_micros: 20,
            partition_id: 0,
            partition_incarnation: 30,
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
            ack_context: AckContext::default(),
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
        assert!(progress.acknowledged(&stored, &receipt(&stored, stored.offset)));
        assert!(progress.complete_through(2, 11));
        assert_eq!(
            progress.unstored(),
            None,
            "an acknowledgment the stored offset covers is not sent again"
        );
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
        assert!(progress.acknowledged(&target, &receipt(&target, 499)));
        assert_eq!(progress.unstored(), None);
    }

    #[test]
    fn given_an_unread_partition_when_waiting_then_should_hold_from_its_start_offset() {
        assert_eq!(
            PartitionProgress::new(FilteredStart::Offset(7)).wait_offset(),
            7
        );
        assert_eq!(PartitionProgress::new(FilteredStart::Next).wait_offset(), 0);
    }

    #[test]
    fn given_a_scanned_page_when_waiting_then_should_hold_from_the_first_unexamined_record() {
        let mut progress = PartitionProgress::new(FilteredStart::Next);
        progress.record(1, &page(Some(42), StopReason::EndOfVisible), []);

        assert_eq!(progress.wait_offset(), 42);
    }

    #[test]
    fn given_a_page_that_examined_nothing_when_waiting_then_should_hold_past_the_head_it_saw() {
        let mut progress = PartitionProgress::new(FilteredStart::Next);
        let mut caught_up = page(None, StopReason::EndOfVisible);
        caught_up.safe_ack_offset = None;
        progress.record(1, &caught_up, []);
        assert_eq!(progress.wait_offset(), 101);

        let mut empty = page(None, StopReason::EndOfVisible);
        empty.safe_ack_offset = None;
        empty.frontier = 0;
        progress.record(2, &empty, []);
        assert_eq!(
            progress.wait_offset(),
            0,
            "an empty partition and one holding only offset 0 share a head of 0"
        );
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
    fn given_a_fault_when_skipped_then_should_store_it_under_the_fault_page_fence_once_earlier_pages_are_handled()
     {
        let mut progress = PartitionProgress::new(FilteredStart::First);
        progress.record(1, &page(Some(5), StopReason::Filled), [4]);
        let faulted = fault_page(Some(8), 8);
        progress.record(2, &faulted, []);

        assert!(progress.skip(3, 8));
        assert_eq!(progress.blocked(), None);
        assert_eq!(
            progress.unstored(),
            None,
            "record 4 is still open, so the skip waits for it"
        );
        progress.complete_record(1, 4);
        assert_eq!(
            progress.unstored(),
            Some(&AckTarget {
                group_id: faulted.policy.group_id,
                offset: 8,
                generation: faulted.generation,
                ack_context: faulted.ack_context,
                policy: faulted.policy.clone(),
            })
        );
    }

    #[test]
    fn given_a_skipped_fault_when_read_again_then_should_start_after_it() {
        let mut continued = PartitionProgress::new(FilteredStart::First);
        continued.record(1, &fault_page(Some(8), 8), []);
        continued.defer(Instant::now() + Duration::from_secs(60));

        assert!(continued.skip(2, 8));
        assert!(
            !continued.waiting(Instant::now()),
            "a skip reads the partition at once"
        );
        assert_eq!(continued.wait_offset(), 9);
        assert_eq!(
            continued.start(),
            &FilteredStart::Continue(Continuation {
                group_id: Some(3),
                next_scan_offset: 9,
                generation: generation(),
                digest: Some(Digest32::new([7; 32])),
                mode: ExecutionMode::Filtered,
                policy_generation: 2,
            })
        );

        let mut unresolved = PartitionProgress::new(FilteredStart::Next);
        unresolved.record(1, &fault_page(None, 8), []);
        assert!(unresolved.skip(2, 8));
        assert_eq!(unresolved.start(), &FilteredStart::Offset(9));
    }

    #[test]
    fn given_a_block_when_skipped_at_another_offset_or_again_then_should_refuse() {
        let mut progress = PartitionProgress::new(FilteredStart::First);
        progress.record(1, &fault_page(Some(8), 8), []);

        assert!(!progress.skip(2, 7), "the partition is blocked at 8");
        assert!(progress.blocked().is_some());
        assert_eq!(progress.unstored().map(|target| target.offset), Some(7));
        assert!(progress.skip(3, 8));
        assert!(!progress.skip(4, 8), "the block is gone");

        let mut cleared = PartitionProgress::new(FilteredStart::First);
        cleared.record(1, &fault_page(Some(8), 8), []);
        cleared.record(2, &page(Some(20), StopReason::Filled), []);
        assert!(
            !cleared.skip(3, 8),
            "a page past the record ended the block"
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

        assert!(progress.acknowledged(&first, &receipt(&first, first.offset)));

        assert_eq!(progress.unstored().map(|target| target.offset), Some(19));
    }
    #[test]
    fn given_a_stored_checkpoint_when_an_earlier_one_completes_then_should_send_nothing() {
        let mut progress = PartitionProgress::new(FilteredStart::First);
        progress.record(1, &page(Some(20), StopReason::Filled), [11, 15, 18]);
        assert!(progress.complete_through(1, 15));
        let target = progress
            .unstored()
            .cloned()
            .expect("the checkpoint can be stored");
        assert!(progress.acknowledged(&target, &receipt(&target, 15)));

        assert!(progress.complete_through(1, 11));
        assert_eq!(
            progress.unstored(),
            None,
            "15 is stored, so 11 needs no acknowledgment"
        );
    }

    #[test]
    fn given_a_receipt_above_the_target_when_acknowledged_then_should_cover_only_what_it_reports() {
        let mut progress = PartitionProgress::new(FilteredStart::First);
        progress.record(1, &page(Some(20), StopReason::Filled), [11, 15, 18]);
        assert!(progress.complete_through(1, 11));
        let target = progress.unstored().cloned().expect("the checkpoint");

        assert!(
            progress.acknowledged(&target, &receipt(&target, 15)),
            "a higher durable offset answers the target"
        );
        assert_eq!(
            progress.stored_offset(),
            Some(11),
            "the reader's own progress stays its own"
        );
        assert_eq!(
            progress.in_flight(),
            1,
            "another consumer's progress marks no local record handled"
        );
        assert!(progress.complete_through(1, 15));
        assert_eq!(progress.unstored(), None, "the durable offset covers 15");
        progress.complete_record(1, 18);
        assert_eq!(progress.unstored().map(|target| target.offset), Some(19));
    }

    #[test]
    fn given_a_receipt_below_the_target_or_for_another_generation_when_acknowledged_then_should_refuse_it()
     {
        let mut progress = PartitionProgress::new(FilteredStart::First);
        progress.record(1, &page(Some(20), StopReason::Filled), [11, 15]);
        assert!(progress.complete_through(1, 11));
        let target = progress.unstored().cloned().expect("the checkpoint");

        assert!(!progress.acknowledged(&target, &receipt(&target, 10)));
        let recreated = AckReceipt {
            generation: SourceGeneration {
                partition_incarnation: 31,
                ..generation()
            },
            ..receipt(&target, 11)
        };
        assert!(!progress.acknowledged(&target, &recreated));
        assert_eq!(progress.unstored(), Some(&target));
        assert_eq!(progress.stored_offset(), None);
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

    // A group page that stopped in front of a malformed record at `offset`,
    // read under another owner than `page`.
    fn fault_page(next_scan_offset: Option<u64>, offset: u64) -> FilteredPage {
        let mut faulted = page(next_scan_offset, StopReason::Fault);
        faulted.policy.group_id = Some(3);
        faulted.policy.policy_generation = 2;
        faulted.ack_context = AckContext {
            owner_generation: 5,
            metadata_op: 9,
        };
        faulted.fault = Some(RecordFault {
            offset,
            reason: FaultReason::Malformed,
        });
        faulted
    }

    fn receipt(target: &AckTarget, offset: u64) -> AckReceipt {
        AckReceipt {
            partition_id: 0,
            offset,
            generation: target.generation,
        }
    }
}
