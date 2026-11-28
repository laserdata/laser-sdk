package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// StartAgentHandleForTest exposes startAgentHandle to the external test
// package, which already carries the fakes (queuedSource, runtimeServer,
// delivered, handling) its lifecycle tests need. It exists for tests only:
// it is not part of the public API, and this file is never part of a real
// build. It always starts with no advertisement and no consolidation, since
// agentAdvertisement/agentConsolidation are unexported and these lifecycle
// tests are not about card/presence advertising or background consolidation.
func StartAgentHandleForTest(
	identity wire.AgentID, runtime *AgentRuntime, source RecordSource, handler AgentHandler,
) *AgentHandle {
	return startAgentHandle(identity, runtime, source, handler, nil, nil)
}

// WarmUpDedupForTest exposes warmUpDedup, the same step Run takes before it
// starts reading, so a WarmDedup test can prove what it seeded without
// driving a whole Run loop to do it.
func WarmUpDedupForTest(runtime *AgentRuntime, ctx context.Context) error {
	return runtime.warmUpDedup(ctx)
}

// RuntimeAgentForTest is the Agent a runtime applies its gates through, so a
// WarmDedup test can inspect what its dedup window holds afterward.
func RuntimeAgentForTest(runtime *AgentRuntime) *Agent { return runtime.agent }

// CompensationRouteForTest exposes the rollback route a compensation takes,
// so a test can check what it keeps from the step's own route without
// running a whole workflow to observe it indirectly.
var CompensationRouteForTest = compensationRoute

// SharedRegistriesForTest reports how many shared agent registries this handle's connection holds.
func SharedRegistriesForTest(laser *Laser) int {
	shared := laser.state()
	shared.mutex.RLock()
	defer shared.mutex.RUnlock()
	return len(shared.registries)
}

// ReplyDispatchersForTest reports how many shared reply dispatchers this
// handle's connection currently holds.
func ReplyDispatchersForTest(laser *Laser) int {
	shared := laser.state()
	shared.mutex.RLock()
	defer shared.mutex.RUnlock()
	return len(shared.replyHubs)
}

// ReplyDispatchersOpenedForTest reports how many dispatchers were actually
// started, which is the number of pollers run over reply topics.
//
// This is the count that proves sharing. The number currently held cannot:
// re-opening a dispatcher under a key it already had leaves the map the
// same size, so a cache that never hit would read as one that always did.
func ReplyDispatchersOpenedForTest(laser *Laser) int {
	shared := laser.state()
	shared.mutex.RLock()
	defer shared.mutex.RUnlock()
	return shared.replyHubsOpened
}

// ReplyDispatcherDoneForTest hands back each open dispatcher's done channel,
// so a test can prove Close actually stops the goroutines rather than only
// forgetting them.
func ReplyDispatcherDoneForTest(laser *Laser) []<-chan struct{} {
	shared := laser.state()
	shared.mutex.RLock()
	defer shared.mutex.RUnlock()
	done := make([]<-chan struct{}, 0, len(shared.replyHubs))
	for _, dispatcher := range shared.replyHubs {
		done = append(done, dispatcher.done)
	}
	return done
}

// FoldParkedForTest exposes the pure fold of a session's lane into the work
// still held, so the rules can be tested without a log.
var FoldParkedForTest = foldParked

// ApprovalCorrelationForTest exposes approvalCorrelation to the external
// test package, so an approval-parking test can build the decision the
// runtime will recognize rather than guessing at a derived correlation.
// Tests only. This file is never part of a real build.
func ApprovalCorrelationForTest(at wire.LogPosition) wire.CorrelationID {
	return approvalCorrelation(at)
}

// SourcePositionForTest is where the runtime thinks a record sits, which is
// what an approval correlation is derived from.
func (runtime *AgentRuntime) SourcePositionForTest(record *Record) wire.LogPosition {
	return runtime.position(record)
}
