package laser

import (
	"context"
	"sync"
)

// SwappableGovernor lets a policy be hot-swapped without restarting the
// client: Decide always runs under whichever ActionGovernor Swap most
// recently installed. A swap only changes which policy the *next* Decide
// call runs under. A decide already in flight finishes under whichever
// policy it read, and nothing already evaluated is reinterpreted, the same
// non-retroactive rule a durable intent's policy version gives. Ported from
// Rust's SwappableGovernor (sdk/src/govern.rs:1165-1199).
type SwappableGovernor struct {
	mutex  sync.RWMutex
	active ActionGovernor
}

// NewSwappableGovernor opens a swappable governor starting from initial.
func NewSwappableGovernor(initial ActionGovernor) *SwappableGovernor {
	return &SwappableGovernor{active: initial}
}

// Swap replaces the active policy with next, returning the one just
// replaced.
func (governor *SwappableGovernor) Swap(next ActionGovernor) ActionGovernor {
	governor.mutex.Lock()
	defer governor.mutex.Unlock()
	previous := governor.active
	governor.active = next
	return previous
}

// Current is the currently active policy.
func (governor *SwappableGovernor) Current() ActionGovernor {
	governor.mutex.RLock()
	defer governor.mutex.RUnlock()
	return governor.active
}

// Decide runs the currently active policy.
func (governor *SwappableGovernor) Decide(
	ctx context.Context, action *GovernedAction,
) (*ActionDecision, error) {
	return governor.Current().Decide(ctx, action)
}
