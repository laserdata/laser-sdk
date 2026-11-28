package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedInjectedCapabilityScenariosWhenRunThenShouldHold runs the
// read-consistency pre-gate.
//
// A read-your-writes query against a surface that advertises only the weakest
// consistency must fail before any round trip: a server that did not
// understand the ask would drop the additive field and answer a wider
// question than the caller posed, and the caller would never know.
func TestGivenTheSharedInjectedCapabilityScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "capabilities_injected.feature", time.Minute, injectedStep)
}

func injectedStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case text == "a managed-query connection that does not advertise read-your-writes":
		return injectWeakQuery(state)

	case strings.HasPrefix(text, "I run a read-your-writes query against topic"):
		return runQuery(ctx, state, arguments, true)

	case text == "the call fails as unsupported":
		return capabilityStep(ctx, state, step)

	case text == "the unified result code is unsupported":
		return unifiedCode(state)

	case strings.HasPrefix(text, "a prepared release receives"):
		code, err := strconv.ParseUint(text[strings.LastIndexByte(text, ' ')+1:], 10, 16)
		if err != nil {
			return err
		}
		return injectReleaseReply(ctx, state, wire.ResultCode(code))

	case strings.HasPrefix(text, "the command error keeps code"):
		var code uint16
		var retryable bool
		if _, err := fmt.Sscanf(text, "the command error keeps code %d and retryability %t", &code, &retryable); err != nil {
			return err
		}
		var failure *laser.CommandFailure
		if !errors.As(state.failure, &failure) || failure.ResultCode().Code() != code || failure.Retryable() != retryable {
			return fmt.Errorf("expected code %d and retryability %t, got %v", code, retryable, state.failure)
		}
		return nil

	case text == "the release fails as an ambiguous mutation":
		var ambiguous *laser.AmbiguousMutationError
		id, _ := state.get("release-id")
		if !errors.As(state.failure, &ambiguous) || ambiguous.OperationID != id {
			return fmt.Errorf("expected ambiguous release %v, got %v", id, state.failure)
		}
		return nil

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// injectWeakQuery hands the client a capability set the live server never
// reported: a managed query surface serving only eventual consistency.
//
// The point is to reach the local pre-gate on a deployment that has no
// managed backend at all, so the gate is proved rather than the server's
// refusal.
func injectWeakQuery(state *world) error {
	capabilities := state.client.Capabilities()
	capabilities.Managed = true
	capabilities.Query.Available = true
	capabilities.Query.Consistency = wire.ConsistencyEventual
	state.client = state.client.WithCapabilities(capabilities)
	if state.client.Capabilities().ServesConsistency(wire.ConsistencyReadYourWrites) {
		return fmt.Errorf("the injected surface still serves read-your-writes")
	}
	return nil
}

type releaseReply struct {
	iggcon.Client
	code wire.ResultCode
}

func (r releaseReply) GetClusterMetadata(context.Context) (*iggcon.ClusterMetadata, error) {
	return &iggcon.ClusterMetadata{Nodes: []iggcon.ClusterNode{{ReplicaID: 0}}}, nil
}

func (r releaseReply) SendBinaryRequest(_ context.Context, code uint32, _ []byte) ([]byte, error) {
	if code != wire.AgdxKvReleaseCode {
		return nil, fmt.Errorf("unexpected command %d", code)
	}
	return wire.Marshal(wire.NewCommandError(r.code, "refused"))
}

func injectReleaseReply(ctx context.Context, state *world, code wire.ResultCode) error {
	caps := laser.OpenCapabilities()
	caps.Managed, caps.Kv.Available, caps.Kv.FencedLeases = true, true, true
	client := laser.FromClient(releaseReply{code: code}, laser.Options{Capabilities: &caps})
	prepared, err := client.Kv("coord").PrepareRelease([]byte("lease"), "worker", 1)
	if err != nil {
		return err
	}
	state.put("release-id", prepared.OperationID())
	bounded, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	_, state.failure = prepared.Execute(bounded)
	return nil
}
