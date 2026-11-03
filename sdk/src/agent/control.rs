use crate::types::ConversationId;
use laser_wire::agent::{
    AgentEnvelope, AgentId, AgentKind, RecordId, SessionPauseRequest, TaskState,
};
use laser_wire::dispatch::{
    OPERATION_SESSION_CANCEL, OPERATION_SESSION_PAUSE, OPERATION_SESSION_RESUME,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// The operator requests a session has on `agent.control`, as this process
/// observed them. A handler reads them through
/// [`Session::pending_control`](crate::agent::Session::pending_control) and
/// decides itself when to stop: the runtime never interrupts a handler.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PendingControl {
    /// A pause was requested and no later resume lifted it.
    pub pause_requested: bool,
    /// A cancel was requested, or an operator forced the session canceled.
    pub cancel_requested: bool,
    /// A control request for the session arrived on another partition of
    /// `agent.control` than the one its conversation id routes to. Such a
    /// request is never applied, here or by the managed backend.
    pub control_conflict: bool,
}

impl PendingControl {
    fn apply(&mut self, action: ControlAction) {
        match action {
            ControlAction::Pause => self.pause_requested = true,
            ControlAction::Resume => self.pause_requested = false,
            ControlAction::Cancel => self.cancel_requested = true,
        }
    }
}

/// One control request a session record carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlAction {
    Pause,
    Resume,
    Cancel,
}

/// One control request as one agent reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ControlRequest {
    pub(crate) action: ControlAction,
    /// The request names this agent: it is addressed to it, or it is a pause
    /// that lists it among the participants whose acknowledgments it needs.
    pub(crate) named: bool,
    /// The request's record id, for the causal link of an acknowledgment.
    pub(crate) record: Option<RecordId>,
}

/// The control request `envelope` read from `agent.control` makes, when it is
/// addressed to `me` or to every agent. A forced cancel is a canceled session
/// status on the control topic. A pause applies to every agent it reaches,
/// and its participant list only decides which agents it names.
pub(crate) fn control_request(
    envelope: &AgentEnvelope,
    me: Option<&AgentId>,
) -> Option<ControlRequest> {
    if let (Some(target), Some(me)) = (&envelope.target, me)
        && target != me
    {
        return None;
    }
    let action = match envelope.kind {
        AgentKind::Command => match envelope.operation.as_deref()? {
            OPERATION_SESSION_PAUSE => ControlAction::Pause,
            OPERATION_SESSION_RESUME => ControlAction::Resume,
            OPERATION_SESSION_CANCEL => ControlAction::Cancel,
            _ => return None,
        },
        AgentKind::Status if envelope.task_state == Some(TaskState::Canceled) => {
            ControlAction::Cancel
        }
        _ => return None,
    };
    let addressed = envelope.target.is_some() && me.is_some();
    let listed = action == ControlAction::Pause
        && me.is_some_and(|me| {
            serde_json::from_slice::<SessionPauseRequest>(&envelope.body)
                .is_ok_and(|request| request.participants.contains(me))
        });
    Some(ControlRequest {
        action,
        named: addressed || listed,
        record: envelope.record,
    })
}

/// Where a control record sits: its partition and offset on `agent.control`.
pub(crate) type ControlPosition = (u32, u64);

/// The `agent.control` partition a session's control records belong on: the
/// partition Iggy routes the conversation id key to, XXH32 with seed zero
/// over the conversation string modulo the partition count. The managed backend applies
/// the same rule, so both refuse a record on any other partition.
pub(crate) fn control_partition(conversation: ConversationId, partitions: u32) -> Option<u32> {
    (partitions > 0).then(|| xxh32(conversation.to_string().as_bytes()) % partitions)
}

const PRIME_1: u32 = 0x9E37_79B1;
const PRIME_2: u32 = 0x85EB_CA77;
const PRIME_3: u32 = 0xC2B2_AE3D;
const PRIME_4: u32 = 0x27D4_EB2F;
const PRIME_5: u32 = 0x1656_67B1;

/// XXH32 with seed zero.
fn xxh32(input: &[u8]) -> u32 {
    let lane =
        |at: usize| u32::from_le_bytes([input[at], input[at + 1], input[at + 2], input[at + 3]]);
    let round = |acc: u32, value: u32| {
        acc.wrapping_add(value.wrapping_mul(PRIME_2))
            .rotate_left(13)
            .wrapping_mul(PRIME_1)
    };
    let mut at = 0;
    let mut hash = if input.len() >= 16 {
        let mut acc = [
            PRIME_1.wrapping_add(PRIME_2),
            PRIME_2,
            0,
            0u32.wrapping_sub(PRIME_1),
        ];
        while at + 16 <= input.len() {
            for (index, slot) in acc.iter_mut().enumerate() {
                *slot = round(*slot, lane(at + index * 4));
            }
            at += 16;
        }
        acc[0]
            .rotate_left(1)
            .wrapping_add(acc[1].rotate_left(7))
            .wrapping_add(acc[2].rotate_left(12))
            .wrapping_add(acc[3].rotate_left(18))
    } else {
        PRIME_5
    };
    hash = hash.wrapping_add(input.len() as u32);
    while at + 4 <= input.len() {
        hash = hash
            .wrapping_add(lane(at).wrapping_mul(PRIME_3))
            .rotate_left(17)
            .wrapping_mul(PRIME_4);
        at += 4;
    }
    while at < input.len() {
        hash = hash
            .wrapping_add(u32::from(input[at]).wrapping_mul(PRIME_5))
            .rotate_left(11)
            .wrapping_mul(PRIME_1);
        at += 1;
    }
    hash ^= hash >> 15;
    hash = hash.wrapping_mul(PRIME_2);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(PRIME_3);
    hash ^ (hash >> 16)
}

/// A control request at its position on `agent.control`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlacedRequest {
    pub(crate) at: ControlPosition,
    pub(crate) named: bool,
    pub(crate) record: Option<RecordId>,
}

/// The control state of one session: its flags, the pause request in force,
/// and the latest resume request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ControlState {
    pub(crate) flags: PendingControl,
    /// The pause request in force, set while a pause is requested.
    pub(crate) pause: Option<PlacedRequest>,
    /// The latest resume request.
    pub(crate) resume: Option<PlacedRequest>,
}

impl ControlState {
    fn apply(&mut self, at: ControlPosition, request: ControlRequest) {
        self.flags.apply(request.action);
        let placed = PlacedRequest {
            at,
            named: request.named,
            record: request.record,
        };
        match request.action {
            ControlAction::Pause => self.pause = Some(placed),
            ControlAction::Resume => {
                self.pause = None;
                self.resume = Some(placed);
            }
            ControlAction::Cancel => {}
        }
    }

    /// The pause request in force, `None` when no pause is requested.
    pub(crate) fn paused(&self) -> Option<PlacedRequest> {
        self.pause.filter(|_| self.flags.pause_requested)
    }
}

/// The control state of the sessions one runtime has seen. The runtime loads
/// it from a bounded read of `agent.control` at startup, and the control
/// follower records every later request, so a process that joined after a
/// request still sees it. A book that was never loaded folds a session on its
/// first read, and live records that arrive before that fold are kept and
/// replayed on top of it.
#[derive(Default)]
pub(crate) struct ControlBook {
    entries: Mutex<HashMap<ConversationId, Entry>>,
    // Whether a follower keeps folded entries current. Without one every
    // read folds the log again.
    live: AtomicBool,
    // Whether the startup read loaded every session, so a session without an
    // entry has no requests.
    loaded: AtomicBool,
    // The partition count of agent.control, zero while unknown.
    partitions: AtomicU32,
}

#[derive(Default)]
struct Entry {
    state: ControlState,
    last: Option<ControlPosition>,
    folded: bool,
    pending: Vec<(ControlPosition, ControlRequest)>,
    // A request arrived off the session's control route.
    conflict: bool,
}

impl Entry {
    fn with_conflict(&self) -> ControlState {
        let mut state = self.state;
        state.flags.control_conflict |= self.conflict;
        state
    }

    fn apply(&mut self, at: ControlPosition, request: ControlRequest) {
        if self.last.is_some_and(|last| !newer(at, last)) {
            return;
        }
        self.state.apply(at, request);
        self.last = Some(at);
    }
}

// Offsets order records within one partition. A session's control records
// share one partition, so a record on another partition is taken as newer.
fn newer(at: ControlPosition, last: ControlPosition) -> bool {
    at.0 != last.0 || at.1 > last.1
}

impl ControlBook {
    /// The partition count of `agent.control`, which fixes each session's
    /// control route.
    pub(crate) fn set_partitions(&self, partitions: u32) {
        self.partitions.store(partitions, Ordering::Release);
    }

    // Whether `partition` is the control route of `conversation`. Every
    // partition is while the count is unknown.
    fn on_route(&self, conversation: ConversationId, partition: u32) -> bool {
        control_partition(conversation, self.partitions.load(Ordering::Acquire))
            .is_none_or(|route| route == partition)
    }

    /// Forget every session, for a recreated `agent.control` whose history
    /// starts again.
    pub(crate) fn reset(&self) {
        self.lock().clear();
        self.loaded.store(false, Ordering::Release);
    }

    /// Mark whether a follower is keeping the book current.
    pub(crate) fn set_live(&self, live: bool) {
        self.live.store(live, Ordering::Release);
    }

    /// Whether a follower is keeping the book current.
    pub(crate) fn is_live(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }

    /// Load the requests a bounded read of `agent.control` found, in log
    /// order. Every session is folded afterwards: one without an entry has
    /// no requests.
    pub(crate) fn load(
        &self,
        records: impl IntoIterator<Item = (ConversationId, ControlPosition, ControlRequest)>,
    ) {
        let mut entries = self.lock();
        for (conversation, at, request) in records {
            let on_route = self.on_route(conversation, at.0);
            let entry = entries.entry(conversation).or_default();
            entry.folded = true;
            if on_route {
                entry.apply(at, request);
            } else {
                entry.conflict = true;
            }
        }
        self.loaded.store(true, Ordering::Release);
    }

    /// Record one live control request.
    pub(crate) fn observe(
        &self,
        conversation: ConversationId,
        at: ControlPosition,
        request: ControlRequest,
    ) {
        let loaded = self.loaded.load(Ordering::Acquire);
        let on_route = self.on_route(conversation, at.0);
        let mut entries = self.lock();
        let entry = entries.entry(conversation).or_default();
        if !on_route {
            entry.conflict = true;
        } else if entry.folded || loaded {
            entry.folded = true;
            entry.apply(at, request);
        } else {
            entry.pending.push((at, request));
        }
    }

    /// The folded control state of `conversation`, `None` when it must be
    /// folded from the log first.
    pub(crate) fn state(&self, conversation: ConversationId) -> Option<ControlState> {
        if !self.is_live() {
            return None;
        }
        let loaded = self.loaded.load(Ordering::Acquire);
        match self.lock().get(&conversation) {
            Some(entry) if entry.folded => Some(entry.with_conflict()),
            Some(_) => None,
            None => loaded.then(ControlState::default),
        }
    }

    /// Whether any session has a pause or resume request, so the runtime has
    /// held work to look for.
    pub(crate) fn has_pause_history(&self) -> bool {
        self.lock()
            .values()
            .any(|entry| entry.state.pause.is_some() || entry.state.resume.is_some())
    }

    /// The sessions with a pause or resume request.
    pub(crate) fn paused_or_resumed(&self) -> Vec<ConversationId> {
        self.lock()
            .iter()
            .filter(|(_, entry)| entry.state.pause.is_some() || entry.state.resume.is_some())
            .map(|(conversation, _)| *conversation)
            .collect()
    }

    /// Install the state folded from the log through `last`, replay the live
    /// requests that arrived after it, and return the result. An entry a
    /// live follower keeps current is kept as it is.
    pub(crate) fn install(
        &self,
        conversation: ConversationId,
        state: ControlState,
        last: Option<ControlPosition>,
    ) -> ControlState {
        let live = self.is_live();
        let mut entries = self.lock();
        let entry = entries.entry(conversation).or_default();
        if entry.folded && live {
            return entry.with_conflict();
        }
        let pending = std::mem::take(&mut entry.pending);
        entry.state = state;
        entry.last = last;
        entry.folded = true;
        for (at, request) in pending {
            entry.apply(at, request);
        }
        entry.with_conflict()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ConversationId, Entry>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Fold control records in log order into a state and the position of the
/// last one. A record off `route`, the session's control partition, is not
/// applied and flags the state.
pub(crate) fn fold(
    records: impl IntoIterator<Item = (ControlPosition, ControlRequest)>,
    route: Option<u32>,
) -> (ControlState, Option<ControlPosition>) {
    let mut entry = Entry::default();
    for (at, request) in records {
        if route.is_none_or(|route| route == at.0) {
            entry.apply(at, request);
        } else {
            entry.conflict = true;
        }
    }
    (entry.with_conflict(), entry.last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_known_inputs_when_hashed_then_should_match_the_xxh32_reference() {
        assert_eq!(xxh32(b""), 0x02CC_5D05);
        assert_eq!(xxh32(b"abc"), 0x32D1_53FF);
        assert_eq!(
            xxh32(b"Nobody inspects the spammish repetition"),
            0xE229_3B2F
        );
    }

    #[test]
    fn given_a_request_off_the_control_route_when_recorded_then_should_flag_and_never_apply() {
        let session: ConversationId = "01KWM3K3XEP3NP5TN850J17YBP".parse().unwrap();
        let route = control_partition(session, 2).expect("a partition count");
        let book = ControlBook::default();
        book.set_partitions(2);
        book.load([
            (session, (route, 1), request(ControlAction::Pause)),
            (session, (1 - route, 2), request(ControlAction::Cancel)),
        ]);
        book.set_live(true);
        book.observe(session, (1 - route, 3), request(ControlAction::Resume));
        let state = book.state(session).expect("folded");
        assert!(state.flags.pause_requested, "the routed pause applies");
        assert!(
            !state.flags.cancel_requested,
            "the misrouted cancel never applies"
        );
        assert!(state.flags.control_conflict);

        let (folded, _) = fold(
            [
                ((1 - route, 5), request(ControlAction::Cancel)),
                ((route, 6), request(ControlAction::Pause)),
            ],
            Some(route),
        );
        assert_eq!(
            folded.flags, state.flags,
            "the log fold agrees with the book"
        );

        book.reset();
        assert_eq!(
            book.state(session),
            None,
            "a reset book folds again from the log"
        );
    }
    use laser_wire::agent::CorrelationId;

    fn command(operation: &str, target: Option<&str>) -> AgentEnvelope {
        let envelope = AgentEnvelope::command(
            RecordId::from_u128(1),
            laser_wire::agent::ConversationId::from_u128(2),
            "operator".parse().expect("operator parses"),
            CorrelationId::from_u128(3),
            b"{}".to_vec(),
        )
        .with_operation(operation);
        match target {
            Some(target) => envelope.with_target(target.parse().expect("target parses")),
            None => envelope,
        }
    }

    fn flags(book: &ControlBook, session: ConversationId) -> Option<PendingControl> {
        book.state(session).map(|state| state.flags)
    }

    fn request(action: ControlAction) -> ControlRequest {
        ControlRequest {
            action,
            named: false,
            record: None,
        }
    }

    #[test]
    fn given_control_commands_when_classified_then_should_map_each_request_and_skip_other_agents() {
        let me: AgentId = "worker".parse().expect("worker parses");
        let action = |envelope: &AgentEnvelope, me: Option<&AgentId>| {
            control_request(envelope, me).map(|request| request.action)
        };
        assert_eq!(
            action(&command(OPERATION_SESSION_PAUSE, Some("worker")), Some(&me)),
            Some(ControlAction::Pause)
        );
        assert_eq!(
            action(&command(OPERATION_SESSION_RESUME, None), Some(&me)),
            Some(ControlAction::Resume)
        );
        assert_eq!(
            action(&command(OPERATION_SESSION_CANCEL, None), None),
            Some(ControlAction::Cancel)
        );
        assert_eq!(
            action(
                &command(OPERATION_SESSION_CANCEL, Some("critic")),
                Some(&me)
            ),
            None
        );
        assert_eq!(action(&command("chat", None), Some(&me)), None);
    }

    #[test]
    fn given_a_pause_with_participants_when_classified_then_should_name_only_listed_agents() {
        let me: AgentId = "worker".parse().expect("worker parses");
        let other: AgentId = "critic".parse().expect("critic parses");
        let mut pause = command(OPERATION_SESSION_PAUSE, None);
        pause.body = br#"{"participants":["worker"]}"#.to_vec();
        let named = control_request(&pause, Some(&me)).expect("the pause applies");
        assert!(named.named);
        assert_eq!(named.record, Some(RecordId::from_u128(1)));
        let unnamed = control_request(&pause, Some(&other)).expect("the pause still applies");
        assert!(!unnamed.named, "an unlisted agent still pauses");
        let addressed =
            control_request(&command(OPERATION_SESSION_PAUSE, Some("worker")), Some(&me))
                .expect("the pause applies");
        assert!(addressed.named);
        let broadcast = control_request(&command(OPERATION_SESSION_PAUSE, None), Some(&me))
            .expect("the pause applies");
        assert!(!broadcast.named);
    }

    #[test]
    fn given_pause_resume_and_cancel_when_folded_then_should_keep_only_the_latest_pause_state() {
        let (state, last) = fold(
            [
                ((0, 1), request(ControlAction::Pause)),
                ((0, 2), request(ControlAction::Resume)),
                ((0, 3), request(ControlAction::Cancel)),
            ],
            None,
        );
        assert_eq!(
            state.flags,
            PendingControl {
                pause_requested: false,
                cancel_requested: true,
                control_conflict: false,
            }
        );
        assert_eq!(state.paused(), None);
        assert_eq!(state.resume.map(|resume| resume.at), Some((0, 2)));
        assert_eq!(last, Some((0, 3)));
    }

    #[test]
    fn given_live_requests_before_the_fold_when_installed_then_should_replay_only_newer_ones() {
        let book = ControlBook::default();
        book.set_live(true);
        let session = ConversationId::new();
        book.observe(session, (0, 4), request(ControlAction::Pause));
        book.observe(session, (0, 6), request(ControlAction::Cancel));
        assert_eq!(flags(&book, session), None, "unfolded until the first read");
        let (state, last) = fold(
            [
                ((0, 4), request(ControlAction::Pause)),
                ((0, 5), request(ControlAction::Resume)),
            ],
            None,
        );
        let installed = book.install(session, state, last);
        assert_eq!(
            installed.flags,
            PendingControl {
                pause_requested: false,
                cancel_requested: true,
                control_conflict: false,
            }
        );
        book.observe(session, (0, 7), request(ControlAction::Pause));
        assert_eq!(
            flags(&book, session),
            Some(PendingControl {
                pause_requested: true,
                cancel_requested: true,
                control_conflict: false,
            })
        );
        assert_eq!(
            book.state(session)
                .and_then(|state| state.paused())
                .map(|pause| pause.at),
            Some((0, 7))
        );
        book.set_live(false);
        assert_eq!(
            flags(&book, session),
            None,
            "a stopped follower forces a fold"
        );
        let refolded = book.install(session, ControlState::default(), None);
        assert_eq!(
            refolded,
            ControlState::default(),
            "a stopped follower trusts the fold"
        );
    }

    #[test]
    fn given_a_loaded_book_when_reading_an_unseen_session_then_should_report_no_requests() {
        let book = ControlBook::default();
        let paused = ConversationId::new();
        book.load([(paused, (1, 3), request(ControlAction::Pause))]);
        book.set_live(true);
        assert!(book.has_pause_history());
        assert_eq!(book.paused_or_resumed(), vec![paused]);
        assert_eq!(
            flags(&book, ConversationId::new()),
            Some(PendingControl::default())
        );
        let later = ConversationId::new();
        book.observe(later, (2, 9), request(ControlAction::Cancel));
        assert_eq!(
            flags(&book, later),
            Some(PendingControl {
                pause_requested: false,
                cancel_requested: true,
                control_conflict: false,
            })
        );
    }
}
