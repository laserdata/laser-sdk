package laser

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// mutationAttempts is what the attempts of one managed mutation know about
// its outcome.
//
// An attempt whose outcome is unknown makes every later refusal ambiguous
// too, because that earlier attempt may still apply: a refusal after it says
// this attempt was not admitted, not that nothing happened.
type mutationAttempts struct {
	uncertain *string
	// slots are the unresolved items of a batch frame, by index, and empty
	// for a single mutation.
	slots []*string
	batch bool
}

// mutationAttemptsFor is the bookkeeping of one managed request. A batch
// frame settles item by item.
func mutationAttemptsFor(code uint32) *mutationAttempts {
	return &mutationAttempts{batch: code == wire.AgdxBatchCode}
}

type attemptKind uint8

const (
	// attemptAnswered is a reply from a node, which may still say the
	// outcome is unknown.
	attemptAnswered attemptKind = iota
	// attemptLost is a request that may have left, with no outcome back.
	attemptLost
	// attemptRefused is a failure the transport decided.
	attemptRefused
)

// mutationAttempt is what one try of a managed mutation came back with.
type mutationAttempt struct {
	kind   attemptKind
	reply  []byte
	reason string
	err    error
}

// settle reports the reply that settles the mutation, that it is to be sent
// again, or the error the caller gets.
func (attempts *mutationAttempts) settle(made mutationAttempt) ([]byte, bool, error) {
	switch made.kind {
	case attemptAnswered:
		if attempts.batch {
			var batch wire.BatchReply
			if err := wire.Unmarshal(made.reply, &batch); err == nil {
				return attempts.settleBatch(made.reply, batch)
			}
		}
		if unknown, is := unknownOutcome(made.reply); is {
			attempts.uncertain = &unknown
			return nil, false, nil
		}
		if attempts.uncertain != nil {
			if refusal, admission := admissionRefusal(made.reply); admission {
				return nil, false, ambiguousAfter(*attempts.uncertain, refusal)
			}
		}
		return made.reply, true, nil
	case attemptLost:
		attempts.uncertain = &made.reason
		return nil, false, nil
	default:
		if attempts.uncertain != nil {
			return nil, false, ambiguousAfter(*attempts.uncertain, made.err.Error())
		}
		return nil, false, made.err
	}
}

// settleBatch settles a batch frame item by item.
//
// An item whose outcome is unknown leaves the whole frame unresolved, so the
// same frame is sent again and every item answers under its own derived
// identity: a mutation with its recorded outcome, a read with a fresh result.
// An item that was unknown and is then refused by admission may still apply,
// exactly like a single mutation.
func (attempts *mutationAttempts) settleBatch(
	reply []byte, batch wire.BatchReply,
) ([]byte, bool, error) {
	frameUncertain := attempts.uncertain
	attempts.uncertain = nil
	for len(attempts.slots) < len(batch.Results) {
		attempts.slots = append(attempts.slots, nil)
	}
	attempts.slots = attempts.slots[:len(batch.Results)]
	unresolved := make([]int, 0, len(batch.Results))
	for index, slot := range batch.Results {
		earlier := attempts.slots[index]
		attempts.slots[index] = nil
		if earlier == nil {
			earlier = frameUncertain
		}
		if unknown, is := unknownOutcome(slot); is {
			attempts.slots[index] = &unknown
			unresolved = append(unresolved, index)
			continue
		}
		if earlier == nil {
			continue
		}
		if refusal, admission := admissionRefusal(slot); admission {
			return nil, false, ambiguousAfter(
				fmt.Sprintf("item %d: %s", index, *earlier), refusal)
		}
	}
	if len(unresolved) == 0 {
		return reply, true, nil
	}
	first := ""
	if held := attempts.slots[unresolved[0]]; held != nil {
		first = *held
	}
	held := fmt.Sprintf("items %v are unresolved: %s", unresolved, first)
	attempts.uncertain = &held
	return nil, false, nil
}

// ambiguousAfter is the refusal that followed an attempt whose outcome was
// never known. The caller's loop names the identity to recover under.
func ambiguousAfter(earlier, refusal string) *AmbiguousMutationError {
	return &AmbiguousMutationError{Reason: fmt.Sprintf(
		"an earlier attempt's outcome is unknown (%s), then the request was "+
			"refused (%s)", earlier, refusal)}
}

// unknownOutcome reports a reply that says the mutation may yet apply.
func unknownOutcome(reply []byte) (string, bool) {
	var failure wire.CommandError
	if err := wire.Unmarshal(reply, &failure); err != nil {
		return "", false
	}
	if failure.Code.IsPending() || failure.Code.IsBackendUnavailable() ||
		failure.Code == wire.ResultUnavailable {
		return failure.Message, true
	}
	return "", false
}

// admissionRefusal reports a refusal that names no outcome of the mutation,
// only why this attempt was not admitted.
func admissionRefusal(reply []byte) (string, bool) {
	var failure wire.CommandError
	if err := wire.Unmarshal(reply, &failure); err != nil {
		return "", false
	}
	switch failure.Code {
	case wire.ResultForbidden, wire.ResultUnauthenticated, wire.ResultStepUpRequired:
		return fmt.Sprintf("%v: %s", failure.Code, failure.Message), true
	default:
		return "", false
	}
}

// sendManagedMutation repeats one managed mutation under its own identity
// while its outcome is unknown.
//
// A mutation whose reply never came back may have applied, so stopping at the
// first lost link reports a failure the deployment may have carried out. The
// identity is what makes the repeat safe: the backend records the outcome
// against it and answers the recorded one rather than applying twice.
//
// Each attempt is bounded on its own, the first by ManagedAttemptFirst and
// each later one by twice that, never past the whole deadline, so a dead node
// costs one bound rather than the entire deadline. Between attempts the
// managed backoff spreads the repeats.
func (laser *Laser) sendManagedMutation(
	ctx context.Context, code uint32, operationID wire.U128, frame []byte,
) ([]byte, error) {
	if err := laser.ensureScopable(code); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(ManagedDeadline)
	delay := ManagedRetry.Base
	bound := ManagedAttemptFirst
	attempts := mutationAttemptsFor(code)
	for {
		attemptDeadline := time.Now().Add(bound)
		if attemptDeadline.After(deadline) {
			attemptDeadline = deadline
		}
		made := laser.attemptMutation(ctx, attemptDeadline, code, frame)
		reply, settled, err := attempts.settle(made)
		if err != nil {
			return nil, namingOperation(err, operationID)
		}
		if settled {
			return reply, nil
		}
		if ctx.Err() != nil {
			return nil, laser.unknownOutcomeError(attempts, operationID, ctx.Err())
		}
		wait := Jittered(delay)
		if !time.Now().Add(wait).Before(deadline) {
			return nil, &AmbiguousMutationError{OperationID: operationID, Reason: fmt.Sprintf(
				"still unknown after %s: %s", ManagedDeadline, held(attempts.uncertain))}
		}
		select {
		case <-ctx.Done():
			return nil, laser.unknownOutcomeError(attempts, operationID, ctx.Err())
		case <-time.After(wait):
		}
		delay = min(delay*2, ManagedRetry.Max)
		bound *= 2
	}
}

// attemptMutation makes one bounded attempt and says what came back.
//
// An attempt that ran out its own bound is lost rather than refused: the
// request may have reached the node and applied there, and the next attempt
// under the same identity is what finds out.
func (laser *Laser) attemptMutation(
	ctx context.Context, until time.Time, code uint32, frame []byte,
) mutationAttempt {
	bounded, cancel := context.WithDeadline(ctx, until)
	defer cancel()
	reply, err := laser.sendManagedFrame(bounded, code, frame, walkingOnce())
	switch {
	case err == nil:
		return mutationAttempt{kind: attemptAnswered, reply: reply}
	case ctx.Err() != nil:
		// The caller's own context ended, not this attempt's bound.
		return mutationAttempt{kind: attemptLost, reason: err.Error()}
	case transientTransport(err) || errors.Is(err, context.DeadlineExceeded):
		return mutationAttempt{kind: attemptLost, reason: err.Error()}
	default:
		return mutationAttempt{kind: attemptRefused, err: err}
	}
}

// unknownOutcomeError reports a mutation the caller stopped waiting for while
// its outcome was unknown, which is ambiguous rather than cancelled.
func (laser *Laser) unknownOutcomeError(
	attempts *mutationAttempts, operationID wire.U128, cause error,
) error {
	if attempts.uncertain == nil {
		return cause
	}
	return &AmbiguousMutationError{OperationID: operationID, Reason: fmt.Sprintf(
		"%s, and the caller stopped waiting (%v)", *attempts.uncertain, cause)}
}

// namingOperation puts the identity to recover under on an ambiguous outcome.
func namingOperation(err error, operationID wire.U128) error {
	var ambiguous *AmbiguousMutationError
	if errors.As(err, &ambiguous) && ambiguous.OperationID.IsZero() {
		ambiguous.OperationID = operationID
	}
	return err
}

func held(reason *string) string {
	if reason == nil {
		return "no attempt reported why"
	}
	return *reason
}
