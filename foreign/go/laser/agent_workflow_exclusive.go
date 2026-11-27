package laser

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// workflowFenceNamespace is the key-value namespace holding a workflow run's
// fenced lease, the orchestrator-fence an exclusive step claims, when the
// step names none of its own with ExclusiveIn.
const workflowFenceNamespace = "agdx.workflow.fence"

// workflowLeaseTTL is how long an exclusive step's fenced lease is held
// before it must be renewed. Shorter than the default step deadline, so a
// resumed run waits out the lease a crashed run left behind instead of
// failing the step.
const workflowLeaseTTL = 15 * time.Second

// leaseRetryInterval is how long an exclusive step waits before it asks
// again for a lease another holder still has.
const leaseRetryInterval = 500 * time.Millisecond

// maxReassignments is the most times an exclusive step is reassigned on
// timeout before it fails.
const maxReassignments uint32 = 2

// errExclusiveTimedOut is exclusiveAttempt's signal that the attempt's
// contract timed out while its lease was held, naming the agent so a
// reassignment prefers someone else. Carries no effect: no terminal reply
// landed, so nothing ran to completion worth compensating.
type errExclusiveTimedOut struct{ agent wire.AgentID }

func (e *errExclusiveTimedOut) Error() string {
	return "exclusive workflow step attempt timed out"
}

// runExclusive dispatches a directed step whose target claims a fenced
// lease before running, pinning a task conversation stable across
// re-dispatch so the fence gate buckets every attempt together, and on a
// timeout may reassign under a new holder whose acquisition bumps the fence
// and gates the stale holder out.
func (workflow *Workflow) runExclusive(
	ctx context.Context, runID wire.ConversationID, step workflowStep, payload []byte,
	attempt workflowAttempt, started time.Time, deadlineAt time.Time,
) ([]byte, uint64, []wire.AgentID, error) {
	taskConversation := DeriveConversationID(fmt.Sprintf("%s/%s", runID, step.label))
	last := attempt.number
	if step.onTimeout == OnTimeoutReassign {
		last = attempt.number + maxReassignments
	}
	var avoid []wire.AgentID
	for {
		number := attempt.number
		output, tokens, agents, err := workflow.exclusiveAttempt(
			ctx, runID, step, payload, taskConversation, attempt, avoid, deadlineAt)
		var timedOut *errExclusiveTimedOut
		switch {
		case err == nil:
			return output, tokens, agents, nil
		case errors.As(err, &timedOut) && number < last:
			avoid = append(avoid, timedOut.agent)
			attempt = workflowAttempt{number: number + 1}
			deadlineAt = time.Now().Add(workflow.stepDeadline(step, started))
		case errors.As(err, &timedOut):
			return nil, 0, nil, workflow.stepFailedError(runID, step, StepTimedOut)
		default:
			return nil, 0, agents, err
		}
	}
}

// exclusiveCompletion is one attempt's dispatch outcome, reported on a
// channel so the caller can race it against the lease's own renewal clock.
type exclusiveCompletion struct {
	timedOut      bool
	timedOutAgent wire.AgentID
	output        []byte
	tokens        uint64
	agents        []wire.AgentID
	err           error
}

// exclusiveAttempt claims, maintains, and releases one assignment's lease. A
// live lease of an earlier holder, a crashed run being resumed, is waited
// out inside the step deadline. Renewal runs at half of the granted TTL
// while the contract, verification, and completion journal are active, and
// stops at the step deadline. Every exit releases the lease, so a timeout
// reassignment can acquire immediately under a fresh holder and bump the
// fence sequence before the stale task can commit.
//
// Unlike Rust's Kv::lease/renew (a dedicated coordination client that
// retries an ambiguous send under the operation's own identity until the
// store's recorded outcome resolves it), Go's Kv.LeaseGuard/Renew take the
// plain managed round trip every other kv operation already uses: an
// ambiguous send surfaces as a plain transport error here, the same
// documented deviation laser/kv_lease.go's own lease/renew/release already
// carry. Go also does not race the renewal RPC against the in-flight
// contract the way Rust's tokio::select! does (both still finish, since a
// renew call is one short round trip): a renewal failure always abandons the
// attempt's own completion outright, rather than Rust's one narrower case of
// waiting for a completion that was already winning the race before
// deciding. Both are documented simplifications, not silent gaps.
func (workflow *Workflow) exclusiveAttempt(
	ctx context.Context, runID wire.ConversationID, step workflowStep, payload []byte,
	taskConversation wire.ConversationID, attempt workflowAttempt, avoid []wire.AgentID,
	deadlineAt time.Time,
) ([]byte, uint64, []wire.AgentID, error) {
	namespace := step.fenceNamespace
	if namespace == "" {
		namespace = workflowFenceNamespace
	}
	key := runID.String()
	holder := workflowLeaseHolder(runID, attempt.number)
	kv := workflow.laser.Kv(namespace)
	guard, err := acquireLease(ctx, kv, key, holder, deadlineAt)
	if err != nil {
		return nil, 0, nil, err
	}
	if guard == nil {
		return nil, 0, nil, workflow.stepFailedError(runID, step, StepTimedOut)
	}

	dispatch, err := newWorkflowDispatch(runID, step.label, attempt.number)
	if err != nil {
		_, _ = guard.Release(ctx)
		return nil, 0, nil, err
	}

	completionCtx, cancelCompletion := context.WithCancel(ctx)
	defer cancelCompletion()
	done := make(chan exclusiveCompletion, 1)
	go func() {
		replyFrom, err := workflow.dispatchPoint(completionCtx, runID, step, attempt)
		if err != nil {
			done <- exclusiveCompletion{err: err}
			return
		}
		target, contract, err := workflow.laser.Contract(*step.router, workflow.topic).
			From(workflow.source).
			Payload(payload).
			Registry(workflow.registry).
			InboxRoute(workflow.inboxRoute()).
			ReplyOn(workflow.topic).
			Fence(guard.Lease().Token).
			Conversation(taskConversation).
			Parent(runID, runID).
			Correlation(dispatch.correlation).
			IdempotencyKey(dispatch.key).
			ResumeFrom(replyFrom).
			Excluding(avoid).
			Deadline(time.Until(deadlineAt)).
			SendResolved(completionCtx)
		if err != nil {
			done <- exclusiveCompletion{err: err}
			return
		}
		if contract.State == ContractTimedOut {
			done <- exclusiveCompletion{timedOut: true, timedOutAgent: target}
			return
		}
		output, tokens, failErr := workflow.completion(runID, step, contract)
		if failErr != nil {
			done <- exclusiveCompletion{err: failErr}
			return
		}
		agents := []wire.AgentID{target}
		finished, tokens, err := workflow.finishStep(
			completionCtx, runID, step, output, tokens, agents, deadlineAt)
		if err != nil {
			done <- exclusiveCompletion{err: err, agents: agents}
			return
		}
		done <- exclusiveCompletion{output: finished, tokens: tokens, agents: agents}
	}()

	lease := *guard.Lease()
	var result exclusiveCompletion
maintain:
	for {
		renewalDelay, ok := workflowRenewalDelay(lease.GrantedTTL)
		if !ok {
			cancelCompletion()
			<-done
			result = exclusiveCompletion{err: leaseLostError()}
			break maintain
		}
		select {
		case result = <-done:
			break maintain
		case <-time.After(renewalDelay):
			renewalDeadline := lease.ValidUntil
			if deadlineAt.Before(renewalDeadline) {
				renewalDeadline = deadlineAt
			}
			if !time.Now().Before(renewalDeadline) {
				cancelCompletion()
				<-done
				result = exclusiveCompletion{err: leaseLostError()}
				break maintain
			}
			renewed, err := guard.Renew(ctx, workflowLeaseTTL)
			if err != nil {
				cancelCompletion()
				<-done
				result = exclusiveCompletion{err: err}
				break maintain
			}
			lease = *renewed
		}
	}

	released, releaseErr := guard.Release(ctx)
	if result.err != nil {
		return nil, 0, result.agents, result.err
	}
	if result.timedOut {
		switch {
		case releaseErr != nil:
			return nil, 0, nil, releaseErr
		case !released:
			return nil, 0, nil, leaseLostError()
		default:
			return nil, 0, nil, &errExclusiveTimedOut{agent: result.timedOutAgent}
		}
	}
	switch {
	case releaseErr != nil:
		return nil, 0, result.agents, releaseErr
	case !released:
		return nil, 0, result.agents, leaseLostError()
	default:
		return result.output, result.tokens, result.agents, nil
	}
}

// acquireLease claims the step's lease, waiting out a live lease of an
// earlier holder until deadlineAt. nil, nil when the deadline passed first.
func acquireLease(
	ctx context.Context, kv *Kv, key, holder string, deadlineAt time.Time,
) (*LeaseGuard, error) {
	for {
		guard, err := kv.LeaseGuard(ctx, []byte(key), holder, workflowLeaseTTL)
		if err == nil {
			return guard, nil
		}
		var kvErr *KvError
		if !errors.As(err, &kvErr) || kvErr.Kind() != wire.KvLeaseHeld {
			return nil, err
		}
		if !time.Now().Add(leaseRetryInterval).Before(deadlineAt) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(leaseRetryInterval):
		}
	}
}

// leaseLostError is the error an exclusive step's attempt ends with when it
// cannot confirm it still held its lease through to release.
func leaseLostError() error {
	return &KvError{Failure: wire.KvError{Kind: wire.KvLeaseLost}}
}

func workflowLeaseHolder(runID wire.ConversationID, attempt uint32) string {
	return fmt.Sprintf("workflow:%s:%d", runID, attempt)
}

// workflowRenewalDelay is half of the granted TTL, or false when that is
// zero (a renewal can never keep up, so the lease is already unusable).
func workflowRenewalDelay(grantedTTL time.Duration) (time.Duration, bool) {
	delay := grantedTTL / 2
	if delay <= 0 {
		return 0, false
	}
	return delay, true
}
