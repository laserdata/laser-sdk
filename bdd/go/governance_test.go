package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedGovernanceScenariosWhenRunThenShouldHold runs action
// governance at the effect boundary.
//
// The point is that enforcement happens before the publish: a blocked action
// must leave nothing on the log at all, and every non-allow decision must
// leave evidence whether or not the effect ran.
func TestGivenTheSharedGovernanceScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "governance.feature", time.Minute, governanceStep)
}

// blockingGovernor refuses one payload and allows everything else, which is
// the policy every scenario here describes.
type blockingGovernor struct{ blocked string }

func (governor blockingGovernor) Decide(
	_ context.Context, action *laser.GovernedAction,
) (*laser.ActionDecision, error) {
	if string(action.Payload) == governor.blocked {
		return laser.Block("the policy refuses " + governor.blocked), nil
	}
	return laser.Allow(), nil
}

func governanceStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "the laser is governed by a policy that blocks"):
		return installGovernor(ctx, state, arguments)

	case strings.HasPrefix(text, "I send a governed agent command"):
		return sendGoverned(ctx, state, arguments)

	case strings.HasPrefix(text, "I publish a governed business record"):
		return publishGoverned(ctx, state, arguments)

	case text == "the send is rejected by policy":
		var refused *laser.PolicyBlockedError
		if !errors.As(state.failure, &refused) {
			return fmt.Errorf("want a policy refusal, got %v", state.failure)
		}
		return nil

	case strings.HasPrefix(text, "the audit topic records"):
		return auditRecords(ctx, state, arguments)

	// Observe mode lets the send through, and the scenario then reads it back
	// the same way the provenance feature does.
	case text == "I assemble the conversation":
		return assemble(ctx, state)

	case strings.HasPrefix(text, "the assembled message"):
		return checkAssembled(state, text, arguments)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// installGovernor puts the policy in front of every effect this handle runs.
//
// The audit topic is created first: a blocked action must still leave
// evidence, and a missing topic would make the absence of evidence look like
// a policy that never ran.
func installGovernor(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want a payload and a mode, got %v", arguments)
	}
	if err := state.topic(wire.AgentAudit).Ensure(ctx, 1, nil); err != nil {
		return err
	}
	state.client = state.client.WithGovernor(
		blockingGovernor{blocked: arguments[0]}, laser.GovernorMode(arguments[1]))
	return nil
}

func sendGoverned(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one payload, got %v", arguments)
	}
	agent, err := wire.NewAgentID("planner")
	if err != nil {
		return err
	}
	state.failure = nil
	_, state.failure = state.client.SendAgent(
		ctx, wire.AgentSessions, []byte(arguments[0]),
		&laser.Provenance{Conversation: state.conversation, Agent: &agent})
	return nil
}

// publishGoverned writes a plain business record rather than an agent one, so
// the same policy is proved to sit in front of both paths.
func publishGoverned(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one payload, got %v", arguments)
	}
	if err := state.topic("payments").Ensure(ctx, 1, nil); err != nil {
		return err
	}
	state.failure = nil
	_, state.failure = state.topic("payments").
		Publish().Payload([]byte(arguments[0])).Send(ctx)
	return nil
}

// auditRecords reads the evidence back off the audit topic.
func auditRecords(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want a decision and an outcome, got %v", arguments)
	}
	decision, outcome := arguments[0], arguments[1]
	deadline := time.Now().Add(20 * time.Second)
	for {
		records, err := state.replay(ctx, wire.AgentAudit)
		if err != nil {
			return err
		}
		var seen []string
		for _, record := range records {
			message, openErr := laser.OpenAgdx(record)
			if openErr != nil {
				continue
			}
			evidence, decodeErr := laser.DecodePolicyEvidence(message.Envelope.Body)
			if decodeErr != nil {
				continue
			}
			seen = append(seen, evidence.Decision+"/"+evidence.Outcome)
			if evidence.Decision != decision || evidence.Outcome != outcome {
				continue
			}
			// Evidence is a chain, so one record must also be sealed: a
			// receipt digest is what makes dropping or reordering detectable.
			if evidence.ReceiptDigest == "" {
				return fmt.Errorf("the evidence carries no receipt digest")
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want a %q decision with outcome %q, the audit "+
				"topic holds %v", decision, outcome, seen)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}
