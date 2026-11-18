package bdd_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The scenarios live beside the other runners' so every client is held to one
// set rather than each keeping its own.
const scenarios = "../scenarios"

// deployment is the address the scenarios run against.
//
// Without one the suite skips rather than passing: a conformance run that
// reported success without reaching a server would be worse than no run, since
// it would be read as evidence.
func deployment(t *testing.T) string {
	t.Helper()
	address := os.Getenv("LASER_BDD_ADDR")
	if address == "" {
		address = os.Getenv("LASER_CONNECTION_STRING")
	}
	if address == "" {
		t.Skip("set LASER_BDD_ADDR or LASER_CONNECTION_STRING to run the " +
			"conformance scenarios against a deployment")
	}
	return address
}

// world is the state one scenario builds up as its steps run.
type world struct {
	client       *laser.Laser
	stream       string
	conversation wire.ConversationID
	assembled    []laser.ContextMessage
}

// quoted pulls the quoted arguments out of a step, which is how the shared
// scenarios carry their values.
var quoted = regexp.MustCompile(`"([^"]*)"`)

func argumentsOf(step bdd.Step) []string {
	var found []string
	for _, match := range quoted.FindAllStringSubmatch(step.Text, -1) {
		found = append(found, match[1])
	}
	return found
}

// TestGivenTheSharedProvenanceScenariosWhenRunThenShouldHold runs the
// provenance feature, which exercises the spine this client writes and reads.
func TestGivenTheSharedProvenanceScenariosWhenRunThenShouldHold(t *testing.T) {
	address := deployment(t)
	feature, err := bdd.ParseFeature(filepath.Join(scenarios, "provenance.feature"))
	if err != nil {
		t.Fatalf("read the scenarios: %v", err)
	}
	if len(feature.Scenarios) == 0 {
		t.Fatal("the feature file holds no scenarios, so this would pass vacuously")
	}

	for _, scenario := range feature.Scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			state := &world{}
			for _, step := range scenario.Steps {
				if err := run(ctx, t, state, address, step); err != nil {
					t.Fatalf("%s %s: %v", step.Keyword, step.Text, err)
				}
			}
			if state.client != nil {
				state.client.Close()
			}
		})
	}
}

// run executes one step.
//
// A step this runner does not implement fails rather than being skipped: a
// scenario that silently dropped half its steps would report a pass it did not
// earn.
func run(
	ctx context.Context, t *testing.T, state *world, address string, step bdd.Step,
) error {
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case text == "a running data platform":
		client, err := laser.Connect(ctx, address)
		if err != nil {
			return err
		}
		state.client = client
		return nil

	case strings.HasPrefix(text, "a fresh stream"):
		state.stream = fmt.Sprintf("bdd-go-%d", time.Now().UnixNano())
		stream := state.client.Stream(state.stream)
		if err := stream.Ensure(ctx); err != nil {
			return err
		}
		t.Cleanup(func() {
			// The scenarios name a fresh stream each run, so each cleans up
			// after itself rather than leaving the deployment to fill.
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := stream.Delete(cleanup); err != nil {
				t.Logf("could not delete %s: %v", state.stream, err)
			}
		})
		return state.client.Stream(state.stream).
			Topic(wire.AgentSessions).Ensure(ctx, 4, nil)

	case text == "a new conversation":
		state.conversation = laser.NewConversationID()
		return nil

	case strings.HasPrefix(text, "I send an agent command"):
		return sendCommand(ctx, state, step, arguments)

	case text == "I assemble the conversation":
		return assemble(ctx, state)

	case strings.HasPrefix(text, "the assembled message"):
		return checkAssembled(state, text, arguments)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

func sendCommand(
	ctx context.Context, state *world, step bdd.Step, arguments []string,
) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want a payload and an agent, got %v", arguments)
	}
	payload, agentName := arguments[0], arguments[1]
	agent, err := wire.NewAgentID(agentName)
	if err != nil {
		return err
	}
	agdx := state.client.AgdxOn(state.stream, wire.AgentSessions, agent, state.conversation)
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
	send := agdx.Command(correlation, []byte(payload))

	if strings.Contains(step.Text, "idempotency key") && len(arguments) > 2 {
		key, err := wire.NewIdempotencyKey(arguments[2])
		if err != nil {
			return err
		}
		send = send.WithIdempotencyKey(key)
	}
	_, err = send.Send(ctx)
	return err
}

func assemble(ctx context.Context, state *world) error {
	cursor, err := state.client.Stream(state.stream).Topic(wire.AgentSessions).Replay()
	if err != nil {
		return err
	}
	records, err := cursor.Poll(ctx)
	if err != nil {
		return err
	}
	state.assembled = laser.Assemble(state.conversation).
		Add(wire.AgentSessions, records...).Messages()
	return nil
}

func checkAssembled(state *world, text string, arguments []string) error {
	if len(state.assembled) == 0 {
		return fmt.Errorf("nothing was assembled")
	}
	message := state.assembled[len(state.assembled)-1]

	switch {
	case strings.Contains(text, "payload is"):
		if got := string(message.Body()); got != arguments[0] {
			return fmt.Errorf("want payload %q, got %q", arguments[0], got)
		}
	case strings.Contains(text, "agent is"):
		agent := message.Agent()
		if agent == nil || agent.String() != arguments[0] {
			return fmt.Errorf("want agent %q, got %v", arguments[0], agent)
		}
	case strings.Contains(text, "idempotency key is"):
		spine := message.Record.Provenance
		if spine == nil || spine.IdempotencyKey == nil ||
			*spine.IdempotencyKey != arguments[0] {
			return fmt.Errorf("want idempotency key %q", arguments[0])
		}
	case strings.Contains(text, "belongs to the conversation"):
		spine := message.Record.Provenance
		if spine == nil || spine.Conversation != state.conversation {
			return fmt.Errorf("the message belongs to another conversation")
		}
	default:
		return fmt.Errorf("this runner does not implement the check")
	}
	return nil
}
