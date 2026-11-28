package bdd_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedMaterializationScenariosWhenRunThenShouldHold runs the
// materialization readiness feature.
//
// Progress is measured against the heads captured when the binding was
// applied, not against a source that keeps growing, and completeness is a
// claim a binding may only make when it can prove it. Both are decided from a
// status the client reads, so the readiness scenarios run with no deployment
// and only the managed ones below them wait for a backend.
func TestGivenTheSharedMaterializationScenariosWhenRunThenShouldHold(t *testing.T) {
	runLocalFeature(t, "materialization.feature", 2*time.Minute, materializationStep)
}

// source is the one binding every scenario here describes.
var materializedSource = wire.NewSourceSelector("telemetry", "readings")

func offset(value uint64) *uint64 { return &value }

// capturedTarget is the status the readiness wait captured when it applied
// the binding: head 5 on partition 0.
func capturedTarget(head uint64) wire.BindingStatus {
	incarnation := uint64(7)
	return wire.BindingStatus{
		History:           wire.AppendHistory(),
		Source:            materializedSource,
		Index:             "readings",
		Realization:       wire.RealizationRealized,
		SourceIncarnation: &incarnation,
		Partitions: []wire.PartitionProgress{
			{PartitionID: 0, HeadOffset: offset(head), FirstOffset: offset(0)},
		},
	}
}

func materializationStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	counts := numbersOf(step)
	arguments := argumentsOf(step)

	switch {
	case text == "I materialize whole records twice under one operation identity", text == "I publish two arbitrary records to the materialized topic", text == "materialization reaches the captured source head":
		return operationalSQLStep(ctx, state, step)
	case text == "the materialized query returns both original bodies exactly once":
		return originalBodies(ctx, state, sqlIndex, []string{"not-json", string([]byte{0, 255, 1, 2})})
	case strings.HasPrefix(text, "I materialize keyed records"):
		var deleteHeader *string
		if text == "I materialize keyed records" {
			header := "delete"
			deleteHeader = &header
		}
		topic := state.topic("keyed_records")
		if err := topic.Ensure(ctx, 1, nil); err != nil {
			return err
		}
		projection := wire.Projection{ID: "keyed_records", Kind: wire.ProjectionRecord, ContentType: wire.ContentAny, History: wire.KeyedHistory(wire.HeaderProjectionKey("key"), deleteHeader)}
		sqlFlowOf(state).materialization, err = topic.Materialize().Typed(projection).Apply(ctx)
		return err
	case strings.HasPrefix(text, "I publish keyed value"):
		request := state.topic("keyed_records").Publish().Payload([]byte(arguments[0])).Header("key", "one")
		if len(arguments) == 2 {
			request.Header(arguments[1], "true")
		}
		_, err := request.Send(ctx)
		return err
	case text == "I delete the materialized key":
		_, err := state.topic("keyed_records").Publish().Payload([]byte{0}).Header("key", "one").Header("delete", "true").Send(ctx)
		return err
	case strings.HasPrefix(text, "the keyed view contains only"), text == "the keyed view is empty":
		if err := materializationCaughtUp(ctx, sqlFlowOf(state)); err != nil {
			return err
		}
		return originalBodies(ctx, state, "keyed_records", arguments)
	case strings.HasPrefix(text, "a realized binding with captured head"):
		if len(counts) != 2 {
			return fmt.Errorf("want a captured head and an applied offset, got %v",
				counts)
		}
		state.put("captured", capturedTarget(counts[0]))
		state.put("status", liveStatus(counts[0], counts[1]))
		return nil

	case strings.HasPrefix(text, "the binding advances to offset"):
		if len(counts) != 2 {
			return fmt.Errorf("want an applied offset and a source head, got %v",
				counts)
		}
		// The source head grows past what was captured, which is the point:
		// catch-up must be measured against the captured head rather than
		// against a source that never stops moving.
		state.put("status", liveStatus(counts[1], counts[0]))
		return nil

	case text == "the binding has reached the captured head":
		return caughtUp(state, true)

	case text == "the binding has no known first offset":
		return amendStatus(state, func(status *wire.BindingStatus) {
			status.Partitions[0].FirstOffset = nil
		})

	case text == "another partition lost its records before the binding read them":
		return amendStatus(state, func(status *wire.BindingStatus) {
			// A first offset above zero says the earlier records expired
			// before this binding ever read them, so the history it holds has
			// a hole it can never fill.
			status.Partitions = append(status.Partitions, wire.PartitionProgress{
				PartitionID:   1,
				HeadOffset:    offset(9),
				AppliedOffset: offset(9),
				FirstOffset:   offset(4),
			})
		})

	case text == "the binding cannot claim complete history":
		return completeHistory(state, false)

	case strings.HasPrefix(text, "a pending binding whose table creation failed with class"):
		if len(arguments) != 1 {
			return fmt.Errorf("want a failure class, got %v", arguments)
		}
		state.put("status", failedStatus(wire.FailureClass(arguments[0])))
		return nil

	case text == "the binding has failed for good":
		return failedForGood(state, true)

	case strings.HasPrefix(text, "its failure class becomes"):
		if len(arguments) != 1 {
			return fmt.Errorf("want a failure class, got %v", arguments)
		}
		class := wire.FailureClass(arguments[0])
		return amendStatus(state, func(status *wire.BindingStatus) {
			status.Failure = &class
		})

	case text == "the binding has not failed for good":
		return failedForGood(state, false)

	default:
		// The managed scenarios of this feature need a backend that folds
		// materializations, and the harness skips them before they reach here.
		return fmt.Errorf("this runner does not implement the step")
	}
}

// liveStatus is the binding as the answering node reports it now.
func liveStatus(head, applied uint64) wire.BindingStatus {
	status := capturedTarget(head)
	status.Partitions[0].AppliedOffset = offset(applied)
	return status
}

func failedStatus(class wire.FailureClass) wire.BindingStatus {
	status := capturedTarget(5)
	status.Realization = wire.RealizationPending
	status.Failure = &class
	return status
}

func statusOf(state *world) (wire.BindingStatus, error) {
	value, found := state.get("status")
	if !found {
		return wire.BindingStatus{}, fmt.Errorf("the scenario declared no binding")
	}
	return value.(wire.BindingStatus), nil
}

func amendStatus(state *world, amend func(*wire.BindingStatus)) error {
	status, err := statusOf(state)
	if err != nil {
		return err
	}
	amend(&status)
	state.put("status", status)
	return nil
}

func caughtUp(state *world, want bool) error {
	status, err := statusOf(state)
	if err != nil {
		return err
	}
	target, found := state.get("captured")
	if !found {
		return fmt.Errorf("the scenario captured no target")
	}
	if got := status.CaughtUpTo(target.(wire.BindingStatus)); got != want {
		return fmt.Errorf("want caught up %v, got %v", want, got)
	}
	return nil
}

func completeHistory(state *world, want bool) error {
	status, err := statusOf(state)
	if err != nil {
		return err
	}
	// Blocking is the default oversize policy, so this asks whether the
	// history is complete for a binding that keeps every body.
	if got := status.HistoryComplete(wire.OversizeBlock); got != want {
		return fmt.Errorf("want complete history %v, got %v", want, got)
	}
	return nil
}

func failedForGood(state *world, want bool) error {
	status, err := statusOf(state)
	if err != nil {
		return err
	}
	if got := status.IsFailed(); got != want {
		return fmt.Errorf("want failed for good %v, got %v", want, got)
	}
	return nil
}
