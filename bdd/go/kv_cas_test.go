package bdd_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
)

// TestGivenTheSharedKvCasScenariosWhenRunThenShouldHold runs the
// compare-and-swap feature against this module's reference engine.
//
// A local feature: the scenarios pin the race semantics every backend and
// every client must reproduce, so they run against the pure engine rather
// than a deployment. Making them need a server would test the transport
// instead of the contract.
func TestGivenTheSharedKvCasScenariosWhenRunThenShouldHold(t *testing.T) {
	runLocalFeature(t, "kv_cas.feature", time.Minute, kvCasStep)
}

// kvCasNow is the logical clock the scenarios that do not name one read at.
// Steps that care about expiry pass an absolute value instead, so expiry is
// deterministic with no wall clock anywhere.
const kvCasNow uint64 = 1_000

// casBench is the scenario's engine and its last swap.
type casBench struct {
	engine  *bdd.KvEngine
	outcome bdd.CasOutcome
}

func benchOf(state *world) *casBench {
	if existing, found := state.get("kv-cas"); found {
		return existing.(*casBench)
	}
	bench := &casBench{engine: bdd.NewKvEngine()}
	state.put("kv-cas", bench)
	return bench
}

func kvCasStep(_ context.Context, state *world, step bdd.Step) error {
	bench := benchOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case text == "an empty KV store":
		bench.engine = bdd.NewKvEngine()
		return nil

	case strings.HasPrefix(text, "key ") && strings.Contains(text, " expiring at "):
		expiry, named := after(text, " expiring at ")
		if len(arguments) != 2 || !named {
			return fmt.Errorf("want a key, a value and an expiry, got %v", arguments)
		}
		bench.engine.Set(arguments[0], []byte(arguments[1]), expiry, true, kvCasNow)
		return nil

	case strings.HasPrefix(text, "key ") && strings.Contains(text, " holds "):
		if len(arguments) != 2 {
			return fmt.Errorf("want a key and a value, got %v", arguments)
		}
		bench.engine.Set(arguments[0], []byte(arguments[1]), 0, false, kvCasNow)
		return nil

	case strings.HasPrefix(text, "I create ") && strings.Contains(text, " if absent"):
		if len(arguments) != 2 {
			return fmt.Errorf("want a key and a value, got %v", arguments)
		}
		bench.outcome = bench.engine.Cas(
			arguments[0], []byte(arguments[1]), bdd.ExpectAbsent(),
			0, false, clockOf(text))
		return nil

	case strings.HasPrefix(text, "I swap ") && strings.Contains(text, " expecting version "):
		version, named := after(text, " expecting version ")
		if len(arguments) != 2 || !named {
			return fmt.Errorf("want a key, a value and a version, got %v", arguments)
		}
		bench.outcome = bench.engine.Cas(
			arguments[0], []byte(arguments[1]), bdd.ExpectVersion(version),
			0, false, clockOf(text))
		return nil

	case strings.HasPrefix(text, "the swap commits version"):
		version, named := after(text, "the swap commits version ")
		if !named {
			return fmt.Errorf("want one expected version in %q", text)
		}
		return swapCommitted(bench, version)

	case text == "the swap conflicts because the key is absent":
		return swapConflictedAbsent(bench)

	case strings.HasPrefix(text, "the swap conflicts with current version"):
		current, named := after(text, "current version ")
		if !named {
			return fmt.Errorf("want one current version in %q", text)
		}
		return swapConflictedAt(bench, current)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// after reads the integer that follows a phrase in the step text.
//
// Positional, not "the nth number in the step": a value like "2" is itself a
// number, so counting numbers reads the value as the version. Only the words
// before it say which is which.
func after(text, phrase string) (uint64, bool) {
	at := strings.Index(text, phrase)
	if at < 0 {
		return 0, false
	}
	rest := strings.TrimSpace(text[at+len(phrase):])
	digits := 0
	for digits < len(rest) && rest[digits] >= '0' && rest[digits] <= '9' {
		digits++
	}
	if digits == 0 {
		return 0, false
	}
	value, err := strconv.ParseUint(rest[:digits], 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

// clockOf is the logical clock a step named with "at <n>", or the default.
func clockOf(text string) uint64 {
	if now, named := after(text, " at "); named {
		return now
	}
	return kvCasNow
}

func swapCommitted(bench *casBench, version uint64) error {
	if bench.outcome.Conflict {
		return fmt.Errorf("want a commit at version %d, got a conflict", version)
	}
	if bench.outcome.Committed != version {
		return fmt.Errorf("want version %d committed, got %d",
			version, bench.outcome.Committed)
	}
	return nil
}

func swapConflictedAbsent(bench *casBench) error {
	if !bench.outcome.Conflict {
		return fmt.Errorf("want a conflict, got a commit at version %d",
			bench.outcome.Committed)
	}
	// Absent is a different fact from version zero, which is why the
	// outcome carries the distinction rather than a bare number.
	if bench.outcome.HasCurrent {
		return fmt.Errorf("want no current version, got %d", bench.outcome.Current)
	}
	return nil
}

func swapConflictedAt(bench *casBench, current uint64) error {
	if !bench.outcome.Conflict {
		return fmt.Errorf("want a conflict, got a commit at version %d",
			bench.outcome.Committed)
	}
	if !bench.outcome.HasCurrent || bench.outcome.Current != current {
		return fmt.Errorf("want the conflict to name current version %d, got %d (present=%v)",
			current, bench.outcome.Current, bench.outcome.HasCurrent)
	}
	return nil
}
