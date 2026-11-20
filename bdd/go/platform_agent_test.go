package bdd_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The steps provenance.feature and agent.feature share. Both describe the same
// spine on the same lane, one from the headers and one from the threading, so
// they read the log the same way and the steps live together.

// platformAgentStep runs one step of either feature.
func platformAgentStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "I send agent commands"):
		for _, body := range arguments {
			if err := sendCommand(ctx, state, body, "agent", nil); err != nil {
				return err
			}
		}
		return nil

	case strings.HasPrefix(text, "I send an agent command"):
		return sendNamedCommand(ctx, state, text, arguments)

	case text == "I start another conversation":
		state.conversation = laser.NewConversationID()
		return nil

	case strings.HasPrefix(text, "I publish an AGDX command"):
		return publishTyped(ctx, state, arguments)

	case text == "I assemble the conversation":
		return assemble(ctx, state)

	case strings.HasPrefix(text, "the assembled payloads are"):
		return checkPayloads(state, arguments)

	case strings.HasPrefix(text, "the AGDX command body is"):
		return checkEnvelopeBody(state, arguments)

	case strings.HasPrefix(text, "the assembled message"):
		return checkAssembled(state, text, arguments)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// sendNamedCommand reads the agent and the one extra field the step carries.
//
// The idempotency key and the correlation id are kept apart on the spine, so
// the step that names one must not set the other: crossing them is exactly
// what the two fields exist to prevent.
func sendNamedCommand(
	ctx context.Context, state *world, text string, arguments []string,
) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want a payload and an agent, got %v", arguments)
	}
	body, agentName := arguments[0], arguments[1]
	var extra func(*laser.Provenance)
	switch {
	case strings.Contains(text, "idempotency key"):
		if len(arguments) < 3 {
			return fmt.Errorf("want an idempotency key, got %v", arguments)
		}
		key := arguments[2]
		extra = func(provenance *laser.Provenance) {
			provenance.IdempotencyKey = &key
		}
	case strings.Contains(text, "correlation id"):
		if len(arguments) < 3 {
			return fmt.Errorf("want a correlation id, got %v", arguments)
		}
		correlation := arguments[2]
		extra = func(provenance *laser.Provenance) {
			provenance.CorrelationID = &correlation
		}
	}
	return sendCommand(ctx, state, body, agentName, extra)
}

// sendCommand appends one agent record with the spine the scenario asked for.
func sendCommand(
	ctx context.Context, state *world, body, agentName string,
	extra func(*laser.Provenance),
) error {
	agent, err := wire.NewAgentID(agentName)
	if err != nil {
		return err
	}
	provenance := &laser.Provenance{
		Conversation: state.conversation,
		Agent:        &agent,
	}
	if extra != nil {
		extra(provenance)
	}
	_, err = state.client.SendAgent(
		ctx, wire.AgentSessions, []byte(body), provenance)
	return err
}

// publishTyped sends through the typed AGDX producer rather than by hand, so
// the envelope and the spine are derived from one description of the send.
func publishTyped(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a body, got %v", arguments)
	}
	agent, err := wire.NewAgentID("producer")
	if err != nil {
		return err
	}
	agdx := state.client.AgdxOn(
		state.stream, wire.AgentSessions, agent, state.conversation)
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
	_, err = agdx.Command(correlation, []byte(arguments[0])).Send(ctx)
	return err
}

// assemble reads the conversation back off the lane.
//
// The read repeats until something lands, because an append is acknowledged
// before every replica serves it and a single empty read would otherwise fail
// a scenario for a race rather than for the contract.
func assemble(ctx context.Context, state *world) error {
	scope := state.client.Context(state.conversation)
	deadline := time.Now().Add(20 * time.Second)
	for {
		messages, err := scope.Fetch(ctx, []string{wire.AgentSessions}, 100, 0)
		if err == nil && len(messages) > 0 {
			state.assembled = messages
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("the conversation assembled nothing")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func checkPayloads(state *world, want []string) error {
	var got []string
	for _, message := range state.assembled {
		got = append(got, string(message.Body()))
	}
	if len(got) != len(want) {
		return fmt.Errorf("want %v in order, got %v", want, got)
	}
	for index, body := range want {
		if got[index] != body {
			return fmt.Errorf("want %v in order, got %v", want, got)
		}
	}
	return nil
}

func checkEnvelopeBody(state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a body, got %v", arguments)
	}
	for _, message := range state.assembled {
		if message.Envelope == nil {
			continue
		}
		if bytes.Equal(message.Envelope.Body, []byte(arguments[0])) {
			return nil
		}
	}
	return fmt.Errorf("no assembled envelope carries the body %q", arguments[0])
}

func checkAssembled(state *world, text string, arguments []string) error {
	if len(state.assembled) == 0 {
		return fmt.Errorf("nothing was assembled")
	}
	if len(arguments) < 1 && !strings.Contains(text, "belongs to the conversation") {
		return fmt.Errorf("want an expected value, got none")
	}
	// The scenarios that assert on one message send exactly one, so the check
	// reads the first rather than the last: reading the last would pass even
	// where a send leaked a record the scenario never asked for.
	message := state.assembled[0]
	spine := message.Record.Provenance

	switch {
	case strings.Contains(text, "payload is"):
		if len(state.assembled) != 1 {
			return fmt.Errorf("want one assembled message, got %d",
				len(state.assembled))
		}
		if got := string(message.Body()); got != arguments[0] {
			return fmt.Errorf("want payload %q, got %q", arguments[0], got)
		}
	case strings.Contains(text, "agent is"):
		agent := message.Agent()
		if agent == nil || agent.String() != arguments[0] {
			return fmt.Errorf("want agent %q, got %v", arguments[0], agent)
		}
	case strings.Contains(text, "idempotency key is"):
		if spine == nil || spine.IdempotencyKey == nil ||
			*spine.IdempotencyKey != arguments[0] {
			return fmt.Errorf("want idempotency key %q, got %v",
				arguments[0], idempotencyOf(spine))
		}
	case strings.Contains(text, "correlation id is"):
		if spine == nil || spine.CorrelationID == nil ||
			*spine.CorrelationID != arguments[0] {
			return fmt.Errorf("want correlation id %q, got %v",
				arguments[0], correlationOf(spine))
		}
	case strings.Contains(text, "belongs to the conversation"):
		if spine == nil || spine.Conversation != state.conversation {
			return fmt.Errorf("the message belongs to another conversation")
		}
	default:
		return fmt.Errorf("this runner does not implement the check")
	}
	return nil
}

func idempotencyOf(spine *laser.Provenance) any {
	if spine == nil || spine.IdempotencyKey == nil {
		return nil
	}
	return *spine.IdempotencyKey
}

func correlationOf(spine *laser.Provenance) any {
	if spine == nil || spine.CorrelationID == nil {
		return nil
	}
	return *spine.CorrelationID
}
