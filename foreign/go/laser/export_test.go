package laser

import "context"

// StartAgentHandleForTest exposes startAgentHandle to the external test
// package, which already carries the fakes (queuedSource, runtimeServer,
// delivered, handling) its lifecycle tests need. It exists for tests only:
// it is not part of the public API, and this file is never part of a real
// build.
var StartAgentHandleForTest = startAgentHandle

// WarmUpDedupForTest exposes warmUpDedup, the same step Run takes before it
// starts reading, so a WarmDedup test can prove what it seeded without
// driving a whole Run loop to do it.
func WarmUpDedupForTest(runtime *AgentRuntime, ctx context.Context) error {
	return runtime.warmUpDedup(ctx)
}

// RuntimeAgentForTest is the Agent a runtime applies its gates through, so a
// WarmDedup test can inspect what its dedup window holds afterward.
func RuntimeAgentForTest(runtime *AgentRuntime) *Agent { return runtime.agent }
