package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// agentConsolidation is a spawned agent's background consolidation tick,
// stopped alongside the agent.
type agentConsolidation struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// startConsolidation runs consolidator.Consolidate every *every, off the
// handler loop (its own goroutine), scoped to just identity's own memory,
// until abort. Both every and consolidator must be given for a tick to
// exist: either missing (nil) means no background consolidation, ever, and
// returns nil rather than a no-op consolidation.
//
// The first pass runs at once rather than waiting out the first period,
// matching Rust's tokio::time::interval (whose first tick resolves
// immediately): Go's time.Ticker never does, so this calls consolidator
// once before entering the ticker loop.
func startConsolidation(
	identity wire.AgentID, every *time.Duration, consolidator Consolidator,
) *agentConsolidation {
	if every == nil || consolidator == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	scope := MemoryScope{Agent: &identity}
	consolidation := &agentConsolidation{cancel: cancel, done: make(chan struct{})}
	go consolidation.tickLoop(ctx, scope, *every, consolidator)
	return consolidation
}

// tickLoop runs one pass at once, then one per every until ctx ends.
func (consolidation *agentConsolidation) tickLoop(
	ctx context.Context, scope MemoryScope, every time.Duration, consolidator Consolidator,
) {
	defer close(consolidation.done)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	consolidation.pass(ctx, scope, every, consolidator)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			consolidation.pass(ctx, scope, every, consolidator)
		}
	}
}

// pass runs one consolidation, bounded by every so a slow pass does not run
// indefinitely, and ignored on failure: a logged warning in Rust, and this
// client has no logging facility of its own to report one through, the same
// best-effort shape the capability card refresh already carries.
func (*agentConsolidation) pass(
	ctx context.Context, scope MemoryScope, every time.Duration, consolidator Consolidator,
) {
	attempt, cancel := context.WithTimeout(ctx, every)
	defer cancel()
	_, _ = consolidator.Consolidate(attempt, &scope)
}

// abort stops the tick and waits for its goroutine to exit, canceling an
// in-flight pass's context so a consolidator that respects ctx cancellation
// returns at once rather than running out its own timeout.
func (consolidation *agentConsolidation) abort() {
	consolidation.cancel()
	<-consolidation.done
}
