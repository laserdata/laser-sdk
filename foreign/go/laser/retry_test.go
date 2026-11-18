package laser_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// The schedule is shared with the Rust and TypeScript clients. A client that
// paced itself on its own numbers would retry a deployment at a rate it was
// never sized for, so every value is pinned to the one shared table.
func TestGivenTheSharedRetryTableWhenComparedThenShouldMatchEveryGoValue(t *testing.T) {
	path := filepath.Join("..", "..", "..", "sdk", "tests", "fixtures", "retry-table.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the shared table: %v", err)
	}
	var table map[string]map[string]float64
	if err := json.Unmarshal(body, &table); err != nil {
		t.Fatalf("parse the shared table: %v", err)
	}
	millis := func(section, field string) time.Duration {
		value, named := table[section][field]
		if !named {
			t.Fatalf("the table names no %s.%s", section, field)
		}
		return time.Duration(value) * time.Millisecond
	}

	for _, want := range []struct {
		name  string
		got   time.Duration
		table time.Duration
	}{
		{"managed_mutation.base_ms", laser.ManagedMutation.Base,
			millis("managed_mutation", "base_ms")},
		{"managed_mutation.max_ms", laser.ManagedMutation.Max,
			millis("managed_mutation", "max_ms")},
		{"managed_mutation.deadline_ms", laser.ManagedMutationDeadline,
			millis("managed_mutation", "deadline_ms")},
		{"managed_mutation.attempt_first_ms", laser.ManagedAttemptFirst,
			millis("managed_mutation", "attempt_first_ms")},
		{"consumer.base_ms", laser.ConsumerBackoff.Base, millis("consumer", "base_ms")},
		{"consumer.max_ms", laser.ConsumerBackoff.Max, millis("consumer", "max_ms")},
		{"control_discovery.max_ms", laser.ControlDiscoveryMax,
			millis("control_discovery", "max_ms")},
		{"filter_mutation.backoff_ms", laser.FilterMutationBackoff,
			millis("filter_mutation", "backoff_ms")},
		{"filter_mutation.outcome_wait_ms", laser.FilterOutcomeWait,
			millis("filter_mutation", "outcome_wait_ms")},
		{"query.deadline_ms", laser.QueryDeadline, millis("query", "deadline_ms")},
		{"query.reply_grace_ms", laser.QueryReplyGrace, millis("query", "reply_grace_ms")},
		{"presence.attempt_ms", laser.PresenceAttempt, millis("presence", "attempt_ms")},
	} {
		if want.got != want.table {
			t.Errorf("%s: Go has %v, the table has %v", want.name, want.got, want.table)
		}
	}

	for _, want := range []struct {
		name  string
		got   float64
		table float64
	}{
		{"consumer.failure_budget", float64(laser.ConsumerFailureBudget),
			table["consumer"]["failure_budget"]},
		{"filter_mutation.attempts", float64(laser.FilterMutationAttempts),
			table["filter_mutation"]["attempts"]},
		{"create_race.attempts", float64(laser.CreateRaceAttempts),
			table["create_race"]["attempts"]},
	} {
		if want.got != want.table {
			t.Errorf("%s: Go has %v, the table has %v", want.name, want.got, want.table)
		}
	}
}

// The delay doubles per failure and stops at the ceiling, which is what keeps
// a long outage from pacing retries hours apart.
func TestGivenConsecutiveFailuresWhenBackingOffThenShouldDoubleUpToTheCeiling(t *testing.T) {
	backoff := laser.Backoff{Base: 100 * time.Millisecond, Max: time.Second}

	for attempt, want := range map[uint32]time.Duration{
		1: 100 * time.Millisecond,
		2: 200 * time.Millisecond,
		3: 400 * time.Millisecond,
		4: 800 * time.Millisecond,
		5: time.Second,
		9: time.Second,
	} {
		if got := backoff.Ceiling(attempt); got != want {
			t.Errorf("attempt %d: want %v, got %v", attempt, want, got)
		}
	}
	// An attempt count high enough to overflow the doubling is still capped
	// rather than wrapping to a short delay.
	if got := backoff.Ceiling(1 << 30); got != time.Second {
		t.Errorf("a runaway attempt count must stay at the ceiling, got %v", got)
	}
}

// Clients that failed together must not retry together, so the delay is spread
// over the upper half of its ceiling rather than used exactly.
func TestGivenAJitteredDelayWhenTakenThenShouldStayInTheUpperHalf(t *testing.T) {
	const ceiling = time.Second
	spread := map[time.Duration]struct{}{}

	for range 200 {
		delay := laser.Jittered(ceiling)
		if delay < ceiling/2 || delay > ceiling {
			t.Fatalf("want a delay in [%v, %v], got %v", ceiling/2, ceiling, delay)
		}
		spread[delay] = struct{}{}
	}
	if len(spread) < 2 {
		t.Error("a jittered delay that is always the same spreads nothing")
	}
}
