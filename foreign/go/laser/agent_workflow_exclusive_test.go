package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// fakeLeaseStore is a minimal in-memory lease/fence backend: a bumping token
// per acquisition, held until its granted expiry, idempotent release. Good
// enough to prove the workflow engine's own acquire/renew/release/reassign
// logic. It is not a model of the real managed backend's fence semantics beyond
// that.
type fakeLeaseStore struct {
	mutex  sync.Mutex
	leases map[string]*fakeLease
	tokens uint64
	// grantedTTL, when set, is granted in place of the requested lifetime.
	grantedTTL time.Duration
	// unknownRenewals answers that many renewals with an unknown outcome first.
	unknownRenewals int
	renewals        []wire.U128
}

type fakeLease struct {
	holder    string
	token     uint64
	expiresAt time.Time
}

// held is how many leases the store holds now.
func (store *fakeLeaseStore) held() int {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return len(store.leases)
}

func newFakeLeaseStore() *fakeLeaseStore {
	return &fakeLeaseStore{leases: map[string]*fakeLease{}}
}

func leaseStoreKey(namespace string, key []byte) string {
	return namespace + "\x00" + string(key)
}

func (store *fakeLeaseStore) handle(code uint32, payload []byte) ([]byte, error) {
	var envelope wire.ManagedRequestEnvelope
	if err := wire.Unmarshal(payload, &envelope); err != nil {
		return nil, err
	}
	switch code {
	case wire.AgdxKvLeaseCode:
		return store.lease(envelope.Payload)
	case wire.AgdxKvLeaseRenewCode:
		store.mutex.Lock()
		store.renewals = append(store.renewals, envelope.OperationID)
		unknown := store.unknownRenewals > 0
		if unknown {
			store.unknownRenewals--
		}
		store.mutex.Unlock()
		if unknown {
			return wire.Marshal(wire.NewCommandError(wire.ResultPending, "the outcome is not known yet"))
		}
		return store.renew(envelope.Payload)
	case wire.AgdxKvReleaseCode:
		return store.release(envelope.Payload)
	default:
		return wire.Marshal(wire.ErrReply(wire.KvError{
			Kind: wire.KvUnsupported, Message: "fakeLeaseStore serves only lease ops",
		}))
	}
}

func (store *fakeLeaseStore) lease(payload []byte) ([]byte, error) {
	var request wire.KvLease
	if err := wire.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	key := leaseStoreKey(request.Namespace, request.Key)
	now := time.Now()
	if existing, held := store.leases[key]; held && existing.holder != request.HolderID &&
		existing.expiresAt.After(now) {
		expires := uint64(existing.expiresAt.UnixMicro())
		return wire.Marshal(wire.ErrReply(wire.KvError{
			Kind: wire.KvLeaseHeld, ExpiresAtMicros: &expires,
		}))
	}
	store.tokens++
	ttl := store.ttl(request.LeaseTTLMicros)
	granted := &fakeLease{
		holder: request.HolderID, token: store.tokens, expiresAt: now.Add(ttl),
	}
	store.leases[key] = granted
	return wire.Marshal(wire.OkReply(wire.KvOutcome{Kind: wire.KvLeased, Lease: &wire.KvLeaseGrant{
		LeaseToken: granted.token, GrantedTTLMicros: uint64(ttl.Microseconds()),
	}}))
}

func (store *fakeLeaseStore) renew(payload []byte) ([]byte, error) {
	var request wire.KvLeaseRenew
	if err := wire.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	key := leaseStoreKey(request.Namespace, request.Key)
	existing, held := store.leases[key]
	if !held || existing.token != request.LeaseToken || existing.holder != request.HolderID {
		return wire.Marshal(wire.ErrReply(wire.KvError{Kind: wire.KvLeaseLost}))
	}
	ttl := store.ttl(request.LeaseTTLMicros)
	existing.expiresAt = time.Now().Add(ttl)
	return wire.Marshal(wire.OkReply(wire.KvOutcome{Kind: wire.KvRenewed, Lease: &wire.KvLeaseGrant{
		LeaseToken: existing.token, GrantedTTLMicros: uint64(ttl.Microseconds()),
	}}))
}

// ttl is the lifetime the store grants for a requested one. The caller holds the lock.
func (store *fakeLeaseStore) ttl(requestedMicros uint64) time.Duration {
	if store.grantedTTL > 0 {
		return store.grantedTTL
	}
	return time.Duration(requestedMicros) * time.Microsecond
}

func (store *fakeLeaseStore) release(payload []byte) ([]byte, error) {
	var request wire.KvRelease
	if err := wire.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()
	key := leaseStoreKey(request.Namespace, request.Key)
	existing, held := store.leases[key]
	if !held || existing.token != request.LeaseToken || existing.holder != request.HolderID {
		return wire.Marshal(wire.OkReply(wire.KvOutcome{Kind: wire.KvReleased, Released: false}))
	}
	delete(store.leases, key)
	return wire.Marshal(wire.OkReply(wire.KvOutcome{Kind: wire.KvReleased, Released: true}))
}

// exclusiveWorkflowServer composes workflowServer's topic-aware log with a
// fakeLeaseStore, so one fake serves both the managed command band an
// exclusive step's lease calls ride and the streaming log its session,
// contract, and journal records ride. Neither file alone does this.
type exclusiveWorkflowServer struct {
	*workflowServer
	leases *fakeLeaseStore
}

func newExclusiveWorkflowServer() *exclusiveWorkflowServer {
	return &exclusiveWorkflowServer{workflowServer: newWorkflowServer(), leases: newFakeLeaseStore()}
}

func (server *exclusiveWorkflowServer) SendBinaryRequest(
	ctx context.Context, code uint32, payload []byte,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return server.leases.handle(code, payload)
}

// commandsOn decodes every KindCommand envelope published to topic, in log
// order.
func commandsOn(t *testing.T, server *workflowServer, topic string) []wire.AgentEnvelope {
	t.Helper()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	var commands []wire.AgentEnvelope
	for _, message := range server.records[topic] {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			continue
		}
		if envelope.Kind == wire.KindCommand {
			commands = append(commands, envelope)
		}
	}
	return commands
}

// fenceOf reads a command's fence header. A small positive UintValue decodes
// back as a ValueInt (the untagged Value's own documented round trip: a
// small or non-negative integer stays an Int), so this reads either
// representation rather than assuming the one the sender used.
func fenceOf(t *testing.T, envelope wire.AgentEnvelope) (uint64, bool) {
	t.Helper()
	value, named := envelope.Metadata[wire.HeaderFence]
	if !named {
		return 0, false
	}
	switch value.Kind {
	case wire.ValueUint:
		return value.Uint, true
	case wire.ValueInt:
		return uint64(value.Int), value.Int >= 0
	default:
		return 0, false
	}
}

func TestGivenAnExclusiveStepWhenRunThenShouldDispatchUnderAFenceToken(t *testing.T) {
	server := newExclusiveWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("done")
	server.replySource = agentID(t, "worker")
	handle := laser.CoordinateOverClientForTest(laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: fencedCapabilities(),
	}))

	outcome, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Step("task", laser.To(agentID(t, "worker")), func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("payload"), nil
		}).
		Exclusive().
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if text, ok := outcome.Text("task"); !ok || text != "done" {
		t.Fatalf("want step output `done`, got %q (ok=%v)", text, ok)
	}

	commands := commandsOn(t, server.workflowServer, "agent.sessions")
	if len(commands) != 1 {
		t.Fatalf("want exactly one command, got %d", len(commands))
	}
	fence, named := fenceOf(t, commands[0])
	if !named || fence != 1 {
		t.Errorf("want the command to carry fence token 1 (the only lease granted), got %d (named=%v)",
			fence, named)
	}
}

func TestGivenALocalStepMarkedExclusiveWhenRunThenShouldRefuse(t *testing.T) {
	server := newExclusiveWorkflowServer()
	handle := laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: fencedCapabilities(),
	})
	_, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		StepLocal("task", func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("x"), nil
		}).
		Exclusive().
		Run(context.Background())
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %v", err)
	}
}

func TestGivenAnAllCapableStepMarkedExclusiveWhenRunThenShouldRefuse(t *testing.T) {
	server := newExclusiveWorkflowServer()
	handle := laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: fencedCapabilities(),
	})
	_, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Step("task", laser.AllCapable("review", laser.RouteAny),
			func(context.Context, map[string][]byte) ([]byte, error) { return []byte("x"), nil }).
		Exclusive().
		Run(context.Background())
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %v", err)
	}
}

func TestGivenReassignWithoutExclusiveWhenRunThenShouldRefuse(t *testing.T) {
	server := newExclusiveWorkflowServer()
	handle := laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: fencedCapabilities(),
	})
	_, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Step("task", laser.To(agentID(t, "worker")),
			func(context.Context, map[string][]byte) ([]byte, error) { return []byte("x"), nil }).
		OnTimeout(laser.OnTimeoutReassign).
		Run(context.Background())
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %v", err)
	}
}

func TestGivenNoFencedLeaseCapabilityWhenAnExclusiveStepIsRunThenShouldRefuse(t *testing.T) {
	server := newExclusiveWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	_, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Step("task", laser.To(agentID(t, "worker")),
			func(context.Context, map[string][]byte) ([]byte, error) { return []byte("x"), nil }).
		Exclusive().
		Run(context.Background())
	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %v", err)
	}
}

func TestGivenACancelledRunWhenAnExclusiveStepHoldsItsLeaseThenShouldReleaseTheLease(t *testing.T) {
	server := newExclusiveWorkflowServer()
	server.silent = map[string]bool{"worker": true}
	handle := laser.CoordinateOverClientForTest(laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: fencedCapabilities(),
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
			Step("task", laser.To(agentID(t, "worker")),
				func(context.Context, map[string][]byte) ([]byte, error) { return []byte("x"), nil }).
			Exclusive().
			Deadline(30 * time.Second).
			Run(ctx)
		finished <- err
	}()
	waitUntil(t, "the step to hold its lease", func() bool { return server.leases.held() == 1 })

	cancel()

	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("want the cancelled run to fail")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled run did not end")
	}
	if held := server.leases.held(); held != 0 {
		t.Fatalf("want the lease released after the cancel, %d still held", held)
	}
}

func TestGivenOnTimeoutFailWhenTheTargetNeverRepliesThenShouldFailWithoutReassigning(t *testing.T) {
	server := newExclusiveWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("done")
	server.replySource = agentID(t, "worker")
	server.silent = map[string]bool{"worker": true}
	handle := laser.CoordinateOverClientForTest(laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: fencedCapabilities(),
	}))

	_, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Step("task", laser.To(agentID(t, "worker")),
			func(context.Context, map[string][]byte) ([]byte, error) { return []byte("x"), nil }).
		Exclusive().
		Deadline(200 * time.Millisecond).
		Run(context.Background())
	var failed *laser.StepFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("want a StepFailedError, got %v", err)
	}
	if failed.Reason != laser.StepTimedOut {
		t.Errorf("want StepTimedOut, got %v", failed.Reason)
	}
	if commands := commandsOn(t, server.workflowServer, "agent.sessions"); len(commands) != 1 {
		t.Errorf("want exactly one command (no reassignment under OnTimeoutFail), got %d", len(commands))
	}
}

func TestGivenARenewalWithAnUnknownOutcomeWhenAnExclusiveStepIsInFlightThenShouldRepeatItUnderOneIdentity(t *testing.T) {
	server := newExclusiveWorkflowServer()
	server.silent = map[string]bool{"worker": true}
	// The lease renews at half its lifetime, so the step's deadline falls between two renewals.
	server.leases.grantedTTL = 800 * time.Millisecond
	server.leases.unknownRenewals = 1
	handle := laser.CoordinateOverClientForTest(laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: fencedCapabilities(),
	}))

	_, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Step("task", laser.To(agentID(t, "worker")),
			func(context.Context, map[string][]byte) ([]byte, error) { return []byte("x"), nil }).
		Exclusive().
		Deadline(700 * time.Millisecond).
		Run(context.Background())

	var failed *laser.StepFailedError
	if !errors.As(err, &failed) || failed.Reason != laser.StepTimedOut {
		t.Fatalf("want the step to time out with its lease kept, got %v", err)
	}
	server.leases.mutex.Lock()
	renewals := append([]wire.U128(nil), server.leases.renewals...)
	server.leases.mutex.Unlock()
	if len(renewals) < 2 {
		t.Fatalf("want the unknown renewal repeated, got %d renewals", len(renewals))
	}
	if renewals[0] != renewals[1] {
		t.Errorf("want the repeat under the first renewal's identity, got %v then %v", renewals[0], renewals[1])
	}
}

func TestGivenOnTimeoutReassignWhenTheFirstPickNeverRepliesThenShouldReassignUnderABumpedFence(t *testing.T) {
	server := newExclusiveWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("done")
	server.replySource = agentID(t, "worker-b")
	server.silent = map[string]bool{"worker-a": true}
	handle := laser.CoordinateOverClientForTest(laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: fencedCapabilities(),
	}))
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker-a": cardFor("task", nil, nil, nil, nil),
		"worker-b": cardFor("task", nil, nil, nil, nil),
	})

	outcome, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		Registry(registry).
		Step("task", laser.ToCapable("task", laser.RouteAny),
			func(context.Context, map[string][]byte) ([]byte, error) { return []byte("x"), nil }).
		Exclusive().
		OnTimeout(laser.OnTimeoutReassign).
		Deadline(200 * time.Millisecond).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if text, ok := outcome.Text("task"); !ok || text != "done" {
		t.Fatalf("want step output `done`, got %q (ok=%v)", text, ok)
	}

	commands := commandsOn(t, server.workflowServer, "agent.sessions")
	if len(commands) != 2 {
		t.Fatalf("want two commands (the timed-out attempt and the reassignment), got %d", len(commands))
	}
	first, second := commands[0], commands[1]
	if first.Target == nil || first.Target.String() != "worker-a" {
		t.Errorf("want the first attempt addressed to worker-a, got %v", first.Target)
	}
	if second.Target == nil || second.Target.String() != "worker-b" {
		t.Errorf("want the reassignment addressed to worker-b (avoiding worker-a), got %v", second.Target)
	}
	firstFence, _ := fenceOf(t, first)
	secondFence, _ := fenceOf(t, second)
	if firstFence != 1 || secondFence != 2 {
		t.Errorf("want fence tokens 1 then 2 (bumped on reassignment), got %d then %d",
			firstFence, secondFence)
	}
}

func TestGivenALiveLeaseOfAnEarlierHolderWhenAnExclusiveStepRunsThenShouldWaitThenAcquire(t *testing.T) {
	server := newExclusiveWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("done")
	server.replySource = agentID(t, "worker")
	handle := laser.CoordinateOverClientForTest(laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: fencedCapabilities(),
	}))
	runID := laser.NewConversationID()
	// The namespace a Kv handle sends is scoped to the default stream
	// (laser.ResourceName), not the bare name the workflow engine names.
	scopedNamespace := wire.ScopedResource("agents", "agdx.workflow.fence")
	server.leases.leases[leaseStoreKey(scopedNamespace, []byte(runID.String()))] = &fakeLease{
		holder: "a-crashed-run", token: 99, expiresAt: time.Now().Add(300 * time.Millisecond),
	}

	started := time.Now()
	outcome, err := handle.Workflow(agentID(t, "orchestrator"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).
		RunID(runID).
		Step("task", laser.To(agentID(t, "worker")),
			func(context.Context, map[string][]byte) ([]byte, error) { return []byte("x"), nil }).
		Exclusive().
		Deadline(3 * time.Second).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 300*time.Millisecond {
		t.Errorf("want the run to wait out the earlier holder's lease, took only %s", elapsed)
	}
	if text, ok := outcome.Text("task"); !ok || text != "done" {
		t.Fatalf("want step output `done`, got %q (ok=%v)", text, ok)
	}
}

// waitUntil polls condition until it holds or the test's patience ends.
func waitUntil(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
