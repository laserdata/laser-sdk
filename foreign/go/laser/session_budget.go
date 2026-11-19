package laser

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// BudgetEndReason is the SessionEnd reason of a session ended failed for
// passing its budget.
const BudgetEndReason = "budget"

// The budget dimensions a session can pass.
const (
	// BudgetTokens is the summed input and output tokens of a session's
	// records.
	BudgetTokens = "tokens"
	// BudgetCostMicros is their summed cost.
	BudgetCostMicros = "cost_micros"
)

// BudgetBreach is how a session passed its budget.
//
// A budget is a cooperative limit rather than a hard spending cap: the answer
// is folded from records that are already written, so it is eventually
// consistent and a session can always pass its ceiling by a little before
// anybody notices.
type BudgetBreach struct {
	// Dimension is the ceiling that was passed, empty when the breach is
	// known only by its numbers.
	Dimension string
	Ceiling   uint64
	Spent     uint64
}

// Error reports a session stopped by its budget.
type BudgetExceededError struct{ Breach BudgetBreach }

func (e *BudgetExceededError) Error() string {
	return "the session spent " + strconv.FormatUint(e.Breach.Spent, 10) +
		" of its ceiling " + strconv.FormatUint(e.Breach.Ceiling, 10)
}

// ErrorBody is the structured failure a session ended on this breach carries.
func (breach BudgetBreach) ErrorBody() wire.AgentErrorBody {
	var message string
	switch breach.Dimension {
	case BudgetTokens:
		message = "the session went over its token budget: " +
			strconv.FormatUint(breach.Spent, 10) + " of " +
			strconv.FormatUint(breach.Ceiling, 10) + " tokens"
	case BudgetCostMicros:
		message = "the session went over its cost budget: " +
			strconv.FormatUint(breach.Spent, 10) + " of " +
			strconv.FormatUint(breach.Ceiling, 10) + " micro-units"
	default:
		message = "the session went over its budget: spent " +
			strconv.FormatUint(breach.Spent, 10) + " of ceiling " +
			strconv.FormatUint(breach.Ceiling, 10)
	}
	detail := map[string]wire.Value{
		"ceiling": wire.UintValue(breach.Ceiling),
		"spent":   wire.UintValue(breach.Spent),
	}
	if breach.Dimension != "" {
		detail["budget"] = wire.StrValue(breach.Dimension)
	}
	// Not retryable: the same work against the same ceiling spends the same
	// again, so a retry is only a second way to fail.
	return wire.AgentErrorBody{
		Code: wire.ErrorInternal, Message: &message,
		Retryable: false, Detail: detail,
	}
}

// BudgetOf reports how a session's usage passed its budget, and false while
// it is within it.
//
// The token ceiling is checked before the cost ceiling, which is the order
// the session index applies, so a session over both is reported the same way
// by a client folding the lane and by one reading the index.
func BudgetOf(budget *wire.Budget, tokens, costMicros uint64) (BudgetBreach, bool) {
	if budget == nil {
		return BudgetBreach{}, false
	}
	if budget.Tokens != nil && tokens > *budget.Tokens {
		return BudgetBreach{
			Dimension: BudgetTokens, Ceiling: *budget.Tokens, Spent: tokens}, true
	}
	if budget.CostMicros != nil && costMicros > *budget.CostMicros {
		return BudgetBreach{
			Dimension: BudgetCostMicros,
			Ceiling:   *budget.CostMicros, Spent: costMicros}, true
	}
	return BudgetBreach{}, false
}

// budgetOfInfo is the breach the session index reports.
//
// The index's own flag decides. Its counters only name which ceiling was
// passed, and when they do not line up the breach is reported by its numbers
// rather than denied: the index said the session is over.
func budgetOfInfo(info *wire.SessionInfo) (BudgetBreach, bool) {
	if info == nil || !info.OverBudget {
		return BudgetBreach{}, false
	}
	tokens := info.TokensIn + info.TokensOut
	if breach, over := BudgetOf(info.Budget, tokens, info.CostMicros); over {
		return breach, true
	}
	return BudgetBreach{Ceiling: 0, Spent: tokens}, true
}

// OverBudget reports whether this session's usage has passed its budget.
//
// A deployment that indexes sessions answers from its index. On open Apache
// Iggy, or for a session the index does not know yet, the retained lane is
// folded instead: the budget of the first start record against the usage of
// every record. A session without a budget is never over it.
func (session *Session) OverBudget(ctx context.Context) (bool, error) {
	_, over, err := session.BudgetBreach(ctx)
	return over, err
}

// BudgetBreach is how this session passed its budget, and false while it is
// within it.
func (session *Session) BudgetBreach(
	ctx context.Context,
) (BudgetBreach, bool, error) {
	if session.laser.capabilities.Sessions {
		info, err := session.laser.SessionsIn(session.config.Stream).
			Get(ctx, session.conversation)
		switch {
		case err == nil:
			breach, over := budgetOfInfo(info)
			return breach, over, nil
		case !indexUnusable(err):
			return BudgetBreach{}, false, err
		}
		// The index cannot answer yet, so the lane is folded instead. A
		// session it has not caught up to is not a session with no budget.
	}
	breach, over, err := session.laneBudgetBreach(ctx)
	return breach, over, err
}

// laneBudgetBreach folds the retained lane: the budget of the start record
// the index would apply, against the usage of every record.
//
// A lane whose start record retention has removed reads as within budget,
// because the ceiling it was given is no longer on the log and inventing one
// would end sessions nobody limited.
func (session *Session) laneBudgetBreach(
	ctx context.Context,
) (BudgetBreach, bool, error) {
	envelopes, err := session.laneEnvelopes(ctx)
	if err != nil {
		return BudgetBreach{}, false, err
	}
	lifecycle, tokens, cost := FoldLaneUsage(envelopes)
	breach, over := BudgetOf(lifecycle.Budget, tokens, cost)
	return breach, over, nil
}

// FoldLaneUsage folds a session's lane into its lifecycle and the usage its
// records spent.
//
// Usage is summed over every record that carries any, while the budget comes
// from the lifecycle fold, which is the one the session index applies.
func FoldLaneUsage(
	envelopes []wire.AgentEnvelope,
) (lifecycle LaneLifecycle, tokens, costMicros uint64) {
	for _, envelope := range envelopes {
		if envelope.Usage == nil {
			continue
		}
		tokens = saturatingAdd(tokens, envelope.Usage.InputTokens)
		tokens = saturatingAdd(tokens, envelope.Usage.OutputTokens)
		if envelope.Usage.CostMicros != nil {
			costMicros = saturatingAdd(costMicros, *envelope.Usage.CostMicros)
		}
	}
	return FoldLaneLifecycle(envelopes), tokens, costMicros
}

// saturatingAdd keeps a running total at the ceiling rather than wrapping to
// nothing, because a session that wrapped would read as under budget.
func saturatingAdd(total, more uint64) uint64 {
	if total+more < total {
		return ^uint64(0)
	}
	return total + more
}

// FailOverBudget ends this session failed with reason budget.
func (session *Session) FailOverBudget(
	ctx context.Context, breach BudgetBreach,
) (*AgdxReceipt, error) {
	body := breach.ErrorBody()
	reason := BudgetEndReason
	return session.End(ctx, wire.TaskFailed, wire.SessionEnd{
		Reason: &reason, Error: &body,
	})
}

// indexUnusable reports a failed index read that leaves the lane fold to
// answer: the index does not know the session, is catching up, or is out of
// reach for now.
//
// Anything else surfaces. A read refused for want of a grant is not a reason
// to fold the lane and act as though the deployment agreed.
func indexUnusable(err error) bool {
	var failure *SessionError
	if errors.As(err, &failure) {
		switch failure.Failure.Kind {
		case wire.SessionNotFound, wire.SessionNotRegistered, wire.SessionStale,
			wire.SessionUnavailable:
			return true
		}
	}
	var command *CommandFailure
	if errors.As(err, &command) {
		switch command.Failure.Code {
		case wire.ResultUnavailable, wire.ResultStale:
			return true
		}
	}
	var unsupported *UnsupportedError
	return errors.As(err, &unsupported)
}

// BudgetGate decides whether a session's work may reach a handler.
//
// It reads the session index once per session per poll batch, so a deployment
// without the index never enforces here at all. The Rust client drives this
// from its own consumer runtime; that runtime is not ported, so a caller
// driving its own loop calls Admit before handing a record to its handler.
type BudgetGate struct {
	mutex sync.Mutex
	// batches holds, per partition, the head offset the verdicts came from
	// and each session's verdict. Records of one poll share their
	// partition's head, so a later poll that moved the head reads again.
	batches map[uint32]*budgetBatch
	// ending holds the sessions this process is failing for their budget,
	// kept until the index shows them ended, so a retried failure repeats
	// the same terminal record and a confirmed one is not written twice.
	ending map[wire.ConversationID]bool
}

type budgetBatch struct {
	head     uint64
	verdicts map[wire.ConversationID]budgetVerdict
}

type budgetVerdict struct {
	breach BudgetBreach
	over   bool
	ended  bool
}

// NewBudgetGate opens an empty gate.
func NewBudgetGate() *BudgetGate {
	return &BudgetGate{
		batches: map[uint32]*budgetBatch{},
		ending:  map[wire.ConversationID]bool{},
	}
}

// Admit reports whether a work record of session, delivered from partition by
// a poll that saw head, may reach the handler.
//
// A session over its budget is ended failed with reason budget, once, and its
// record is not handled. A failed index read lets the record through: a
// budget nobody could check is not a reason to drop work. A failed terminal
// write is left for the session's next record to retry.
func (gate *BudgetGate) Admit(
	ctx context.Context, session *Session, partition uint32, head uint64,
) bool {
	if session == nil {
		return true
	}
	// A deployment without the session index refuses the read below as an
	// unserved surface, which this treats as "cannot say" and admits. There
	// is no separate check for it here.
	verdict, cached := gate.cached(partition, head, session.conversation)
	if !cached {
		info, err := session.laser.SessionsIn(session.config.Stream).
			Get(ctx, session.conversation)
		switch {
		case err == nil:
			breach, over := budgetOfInfo(info)
			verdict = budgetVerdict{breach: breach, over: over,
				ended: endedStatus(info.Status)}
		case isUnknownSession(err):
			// A session the index has not seen is not a session over its
			// budget.
			verdict = budgetVerdict{}
		default:
			return true
		}
		gate.store(partition, head, session.conversation, verdict)
	}
	if !verdict.over {
		return true
	}
	if verdict.ended {
		gate.mutex.Lock()
		delete(gate.ending, session.conversation)
		gate.mutex.Unlock()
		return false
	}
	gate.mutex.Lock()
	written := gate.ending[session.conversation]
	if !written {
		gate.ending[session.conversation] = false
	}
	gate.mutex.Unlock()
	if !written {
		if _, err := session.FailOverBudget(ctx, verdict.breach); err == nil {
			gate.mutex.Lock()
			gate.ending[session.conversation] = true
			gate.mutex.Unlock()
		}
	}
	return false
}

func (gate *BudgetGate) cached(
	partition uint32, head uint64, session wire.ConversationID,
) (budgetVerdict, bool) {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	batch, known := gate.batches[partition]
	if !known || batch.head != head {
		return budgetVerdict{}, false
	}
	verdict, held := batch.verdicts[session]
	return verdict, held
}

func (gate *BudgetGate) store(
	partition uint32, head uint64,
	session wire.ConversationID, verdict budgetVerdict,
) {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	if gate.batches == nil {
		gate.batches = map[uint32]*budgetBatch{}
	}
	batch, known := gate.batches[partition]
	if !known || batch.head != head {
		batch = &budgetBatch{
			head: head, verdicts: map[wire.ConversationID]budgetVerdict{}}
		gate.batches[partition] = batch
	}
	batch.verdicts[session] = verdict
}

// endedStatus reports a session the index shows as finished.
func endedStatus(status wire.SessionStatus) bool {
	switch status {
	case wire.SessionCompleted, wire.SessionFailed, wire.SessionCanceled:
		return true
	default:
		return false
	}
}

// isUnknownSession reports an index that does not know the session.
func isUnknownSession(err error) bool {
	var failure *SessionError
	if !errors.As(err, &failure) {
		return false
	}
	return failure.Failure.Kind == wire.SessionNotFound ||
		failure.Failure.Kind == wire.SessionNotRegistered
}
