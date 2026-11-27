package bdd_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedBindingScenariosWhenRunThenShouldHold runs the projection
// binding feature.
//
// It is about the wire form rather than about a deployment: a default that
// encodes itself would change the bytes of every binding written before the
// field existed, so the default must be absent from the encoding.
func TestGivenTheSharedBindingScenariosWhenRunThenShouldHold(t *testing.T) {
	runLocalFeature(t, "bindings.feature", time.Minute, bindingStep)
}

func bindingStep(_ context.Context, state *world, step bdd.Step) error {
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "a binding from stream"):
		if len(arguments) != 2 {
			return fmt.Errorf("want a stream and a topic, got %v", arguments)
		}
		state.put("binding", wire.ProjectionBinding{
			Source: wire.NewSourceSelector(arguments[0], arguments[1]),
			Index:  "readings",
		})
		return nil

	case text == "the binding lets the backend skip rejected records":
		return amendBinding(state, func(binding *wire.ProjectionBinding) {
			binding.InvalidRecords = wire.InvalidRecordDeadLetter
		})

	case text == "the binding keeps references for oversized records":
		return amendBinding(state, func(binding *wire.ProjectionBinding) {
			binding.Oversize = wire.OversizeReference
		})

	case text == "I build the binding":
		return buildBinding(state)

	case text == "the binding's wire form omits invalid_records":
		return carriesKey(state, "invalid_records", false)

	case text == "the binding's wire form carries invalid_records dead_letter":
		if err := carriesKey(state, "invalid_records", true); err != nil {
			return err
		}
		return decodedBinding(state, func(binding wire.ProjectionBinding) error {
			if binding.InvalidRecords != wire.InvalidRecordDeadLetter {
				return fmt.Errorf("want dead_letter, got %q", binding.InvalidRecords)
			}
			return nil
		})

	case text == "the binding's wire form carries oversize reference":
		if err := carriesKey(state, "oversize", true); err != nil {
			return err
		}
		return decodedBinding(state, func(binding wire.ProjectionBinding) error {
			if binding.Oversize != wire.OversizeReference {
				return fmt.Errorf("want reference, got %q", binding.Oversize)
			}
			return nil
		})

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

func amendBinding(state *world, amend func(*wire.ProjectionBinding)) error {
	value, found := state.get("binding")
	if !found {
		return fmt.Errorf("the scenario declared no binding")
	}
	binding := value.(wire.ProjectionBinding)
	amend(&binding)
	state.put("binding", binding)
	return nil
}

func buildBinding(state *world) error {
	value, found := state.get("binding")
	if !found {
		return fmt.Errorf("the scenario declared no binding")
	}
	encoded, err := wire.Marshal(value.(wire.ProjectionBinding))
	if err != nil {
		return err
	}
	state.put("binding wire", encoded)
	return nil
}

// carriesKey reports whether the encoded map holds a field at all.
//
// The check is on the encoded bytes rather than on a decoded value, because
// the question the scenario asks is exactly whether the key was written: a
// decoded default and an omitted field are indistinguishable once decoded. A
// field name appears in a flat CBOR map only as its key, so finding it is
// finding the field.
func carriesKey(state *world, key string, want bool) error {
	value, found := state.get("binding wire")
	if !found {
		return fmt.Errorf("the binding was never built")
	}
	carried := bytes.Contains(value.([]byte), []byte(key))
	if carried != want {
		return fmt.Errorf("want the wire form to carry %q: %v, got %v",
			key, want, carried)
	}
	return nil
}

// decodedBinding checks the value a carried field decodes back to, so a
// present key is confirmed to mean what the scenario said rather than only to
// be there.
func decodedBinding(state *world, check func(wire.ProjectionBinding) error) error {
	value, found := state.get("binding wire")
	if !found {
		return fmt.Errorf("the binding was never built")
	}
	var back wire.ProjectionBinding
	if err := wire.Unmarshal(value.([]byte), &back); err != nil {
		return err
	}
	return check(back)
}
