package laser

import (
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAnUnknownOutcomeFrameWhenABatchSettlesThenShouldRepeatIt(t *testing.T) {
	for _, code := range []wire.ResultCode{wire.ResultUnavailable, wire.ResultPending} {
		attempts := mutationAttemptsFor(wire.AgdxBatchCode)

		reply, settled, err := attempts.settle(answeredWith(replyOf(t, code)))

		if reply != nil || settled || err != nil || attempts.uncertain == nil {
			t.Fatalf("%v: a frame-level unknown outcome must be sent again: settled=%v err=%v", code, settled, err)
		}
	}
}

func TestGivenAnUnknownBatchFrameWhenALaterAttemptIsForbiddenThenShouldStayAmbiguous(t *testing.T) {
	attempts := mutationAttemptsFor(wire.AgdxBatchCode)
	if _, settled, err := attempts.settle(answeredWith(replyOf(t, wire.ResultUnavailable))); settled || err != nil {
		t.Fatalf("an unavailable frame is not settled: settled=%v err=%v", settled, err)
	}

	_, _, err := attempts.settle(answeredWith(replyOf(t, wire.ResultForbidden)))

	var ambiguous *AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an ambiguous outcome, got %T: %v", err, err)
	}
}
