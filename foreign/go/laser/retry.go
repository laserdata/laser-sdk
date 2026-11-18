package laser

import (
	"math/rand/v2"
	"time"
)

// The retry schedule, shared with the Rust `src/retry.rs` and the TypeScript
// `src/retry.ts`. All three are tested against
// `sdk/tests/fixtures/retry-table.json`, so a client that backs off on its own
// numbers cannot drift from the deployment's expectations.

// Backoff is a delay that doubles per consecutive failure from Base up to Max.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// Ceiling is the delay before retry attempt, counting the first retry as 1,
// before jitter.
func (backoff Backoff) Ceiling(attempt uint32) time.Duration {
	shift := attempt
	if shift > 0 {
		shift--
	}
	if shift > 16 {
		shift = 16
	}
	delay := backoff.Base * time.Duration(1<<shift)
	// A doubling that overflows is already past the ceiling.
	if delay <= 0 || delay > backoff.Max {
		return backoff.Max
	}
	return delay
}

// Delay is the jittered delay before retry attempt.
func (backoff Backoff) Delay(attempt uint32) time.Duration {
	return Jittered(backoff.Ceiling(attempt))
}

// Jittered spreads delay over its upper half, so clients that failed together
// do not retry together and beat the server back down.
func Jittered(delay time.Duration) time.Duration {
	return time.Duration(float64(delay) *
		(jitterLow + rand.Float64()*(jitterHigh-jitterLow)))
}

const (
	jitterLow  = 0.5
	jitterHigh = 1.0
)

// The schedule itself.
var (
	// ManagedMutation is how a managed mutation or control append whose
	// outcome is unknown repeats under the same identity.
	ManagedMutation = Backoff{Base: 100 * time.Millisecond, Max: 2 * time.Second}
	// ConsumerBackoff is how a reliable consumer reopens.
	ConsumerBackoff = Backoff{Base: 50 * time.Millisecond, Max: time.Second}
)

const (
	// ManagedMutationDeadline bounds how long a mutation is repeated for.
	ManagedMutationDeadline = 30 * time.Second
	// ManagedAttemptFirst is how long the first attempt of a managed mutation
	// gets before it counts as lost, each later one twice as long and never
	// past the operation deadline. A dead node then costs one bound rather
	// than the whole deadline.
	ManagedAttemptFirst = time.Second
	// ConsumerFailureBudget is how many consecutive failures a consumer gives
	// up after.
	ConsumerFailureBudget uint32 = 10
	// ControlDiscoveryMax is the longest wait between looks for a control
	// topic that does not exist yet.
	ControlDiscoveryMax = 30 * time.Second
	// FilterMutationAttempts is how many times a filter catalog change is
	// tried.
	FilterMutationAttempts uint32 = 3
	// FilterMutationBackoff is how far apart those attempts are.
	FilterMutationBackoff = 200 * time.Millisecond
	// FilterOutcomeWait is how long a mutation's authoritative outcome is
	// awaited before it is reported ambiguous.
	FilterOutcomeWait = 30 * time.Second
	// QueryDeadline is the deadline a query without its own gets, and the
	// latest any query may ask for.
	QueryDeadline = 25 * time.Second
	// QueryReplyGrace is how long past its deadline a query reply is awaited.
	QueryReplyGrace = 2 * time.Second
	// CreateRaceAttempts is how often producers racing to create the same
	// stream or topic retry.
	CreateRaceAttempts = 10
	// PresenceAttempt is how long a presence advertise or clear waits for the
	// replica that holds it before reporting the replica unavailable.
	PresenceAttempt = 5 * time.Second
)
