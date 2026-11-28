package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedWarmDedupScenariosWhenRunThenShouldHold runs the
// warm-duplicate-suppression feature: a restarted worker refills its
// duplicate window from what its group already committed, and which copies
// refill it is the policy's decision.
func TestGivenTheSharedWarmDedupScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "warm_dedup.feature", 3*time.Minute, warmDedupStep)
}

// warmWorker is the agent under test across its stop and restart: the keys
// it handled each time, and what it needs to start again under the same
// group and policy.
type warmWorker struct {
	mutex   sync.Mutex
	handled []string

	name   string
	group  string
	policy laser.WarmDedupPolicy
	handle *laser.AgentHandle
	// handledBeforeRestart is what the first run handled, kept so the
	// restarted run's own list is read on its own.
	handledBeforeRestart []string
}

func (worker *warmWorker) record(key string) {
	worker.mutex.Lock()
	worker.handled = append(worker.handled, key)
	worker.mutex.Unlock()
}

func (worker *warmWorker) seen() []string {
	worker.mutex.Lock()
	defer worker.mutex.Unlock()
	return append([]string(nil), worker.handled...)
}

func workerOf(state *world) (*warmWorker, error) {
	value, found := state.get("warm-worker")
	if !found {
		return nil, fmt.Errorf("the scenario started no warm-dedup worker")
	}
	return value.(*warmWorker), nil
}

func warmDedupStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "a warm-dedup worker"):
		if len(arguments) != 2 {
			return fmt.Errorf("want a worker name and a policy, got %v", arguments)
		}
		return startWarmWorker(ctx, state, arguments[0], arguments[1])

	case strings.Contains(text, "is sent a command under key") &&
		strings.Contains(text, "due in"):
		return sendKeyed(ctx, state, step, arguments, keyedDueIn)

	case strings.Contains(text, "is sent a command under key") &&
		strings.Contains(text, "already past its deadline"):
		return sendKeyed(ctx, state, step, arguments, keyedExpired)

	case strings.Contains(text, "is sent a command under key") &&
		strings.Contains(text, "without a deadline"):
		return sendKeyed(ctx, state, step, arguments, keyedNoDeadline)

	case strings.HasPrefix(text, "the worker handled the keys"):
		return workerHandledKeys(state, keyList(arguments), false)

	case strings.HasPrefix(text, "the restarted worker handled the keys"):
		return workerHandledKeys(state, keyList(arguments), true)

	case strings.Contains(text, "stops and the deadline of key"):
		return stopWorkerAndWait(ctx, state)

	case strings.HasPrefix(text, "resends arrive under the keys"):
		return resendKeys(ctx, state, keyList(arguments))

	case strings.Contains(text, "restarts"):
		return restartWarmWorker(ctx, state)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// keyList flattens the quoted arguments into the keys they name.
//
// A step writes several keys inside one pair of quotes ("expired, ran"), so
// each argument is itself a comma-separated list rather than one key.
func keyList(arguments []string) []string {
	var keys []string
	for _, argument := range arguments {
		keys = append(keys, list(argument)...)
	}
	return keys
}

// keyedShape is how a sent command carries (or does not carry) a deadline.
type keyedShape int

const (
	keyedDueIn keyedShape = iota
	keyedExpired
	keyedNoDeadline
)

// startWarmWorker opens the worker and runs it for the first time.
func startWarmWorker(ctx context.Context, state *world, name, policy string) error {
	parsed, err := warmPolicyOf(policy)
	if err != nil {
		return err
	}
	worker := &warmWorker{
		name:   name,
		group:  fmt.Sprintf("bdd-warm-%s-%d", name, time.Now().UnixNano()),
		policy: parsed,
	}
	state.put("warm-worker", worker)
	return runWarmWorker(ctx, state, worker)
}

// warmPolicyOf reads the policy the scenario names.
func warmPolicyOf(name string) (laser.WarmDedupPolicy, error) {
	switch name {
	case "dead_letters":
		return laser.WarmDedupDeadLetters, nil
	case "unexpired":
		return laser.WarmDedupUnexpired, nil
	case "all":
		return laser.WarmDedupAll, nil
	default:
		return 0, fmt.Errorf("unknown warm dedup policy %q", name)
	}
}

// runWarmWorker spawns the agent under the worker's own group, which is what
// a restart rejoins and what the refill reads the committed offsets of.
func runWarmWorker(ctx context.Context, state *world, worker *warmWorker) error {
	identity, err := bridgeAgent(worker.name)
	if err != nil {
		return err
	}
	handle, err := state.client.SpawnAgent(ctx, identity, laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{
			Topic:           wire.AgentSessions,
			RespondOn:       wire.AgentSessions,
			Group:           worker.group,
			WarmDedup:       true,
			WarmDedupPolicy: worker.policy,
		},
	}, laser.HandlerFunc(func(
		ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		if key := message.Envelope.IdempotencyKey; key != nil {
			worker.record(key.String())
		}
		return nil
	}))
	if err != nil {
		return err
	}
	ready, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := handle.Ready(ready); err != nil {
		return err
	}
	worker.handle = handle
	state.t.Cleanup(func() {
		shutdown, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		if err := handle.Shutdown(shutdown); err != nil {
			state.t.Errorf("warm worker did not release its group membership: %v", err)
		}
	})
	return nil
}

// sendKeyed sends one command under an idempotency key, shaped by how the
// scenario described its deadline.
func sendKeyed(
	ctx context.Context, state *world, step bdd.Step, arguments []string, shape keyedShape,
) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a key, got %v", arguments)
	}
	worker, err := workerOf(state)
	if err != nil {
		return err
	}
	deadline := time.Duration(0)
	switch shape {
	case keyedDueIn:
		seconds := numbersOf(step)
		if len(seconds) != 1 {
			return fmt.Errorf("want one deadline in seconds, got %v", seconds)
		}
		deadline = time.Duration(seconds[0]) * time.Second
	case keyedExpired:
		// Already past, so the target's own deadline gate drops it before
		// any handler runs, which is what makes it a dead letter.
		deadline = -time.Second
	}
	return sendWarmCommand(ctx, state, worker, arguments[0], shape, deadline)
}

// sendWarmCommand publishes one keyed command to the worker's topic.
func sendWarmCommand(
	ctx context.Context, state *world, worker *warmWorker,
	key string, shape keyedShape, deadline time.Duration,
) error {
	target, err := bridgeAgent(worker.name)
	if err != nil {
		return err
	}
	source, err := bridgeAgent("operator")
	if err != nil {
		return err
	}
	idempotency, err := wire.NewIdempotencyKey(key)
	if err != nil {
		return err
	}
	send := state.client.
		AgdxOn(state.stream, wire.AgentSessions, source, state.conversation).
		Command(wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())},
			[]byte("warm "+key)).
		WithTarget(target).
		WithIdempotencyKey(idempotency)
	if shape != keyedNoDeadline {
		send = send.WithDeadline(time.Now().Add(deadline))
	}
	_, err = send.Send(ctx)
	return err
}

// workerHandledKeys asserts which keys the worker handled, on the run the
// step is asking about.
func workerHandledKeys(state *world, expected []string, restarted bool) error {
	worker, err := workerOf(state)
	if err != nil {
		return err
	}
	// The worker handles asynchronously, so the assertion waits for the
	// expected set rather than reading once and racing the loop.
	deadline := time.Now().Add(20 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = worker.seen()
		if restarted {
			got = got[min(len(worker.handledBeforeRestart), len(got)):]
		}
		if equalStrings(got, expected) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	// A worker that already stopped explains an empty list better than the list does.
	probe, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if stopped := worker.handle.Join(probe); !errors.Is(stopped, context.DeadlineExceeded) {
		return fmt.Errorf("want the keys %v handled, got %v, the worker stopped with %v", expected, got, stopped)
	}
	return fmt.Errorf("want the keys %v handled, got %v", expected, got)
}

// stopWorkerAndWait stops the loop and waits out the deadline of the key the
// scenario named, so the restart's refill sees it expired.
func stopWorkerAndWait(ctx context.Context, state *world) error {
	worker, err := workerOf(state)
	if err != nil {
		return err
	}
	worker.handledBeforeRestart = worker.seen()
	// Settle before stopping: the loop commits a record *after* its handler
	// returns, and the assertion that got us here fires as soon as the
	// handler ran. Cancelling straight away can cut off the last commit, and
	// the refill then never sees that record -- which looked like a policy
	// bug until the missing key turned out to be the last one handled every
	// time.
	select {
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}
	shutdown, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := worker.handle.Shutdown(shutdown); err != nil {
		return fmt.Errorf("the warm-dedup worker did not stop: %w", err)
	}
	// The scenario's own "due in 2 seconds" is what has to pass.
	select {
	case <-time.After(2500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// resendKeys sends the resends the scenario lists, all without deadlines, so
// what suppresses them is the refilled window rather than their own expiry.
func resendKeys(ctx context.Context, state *world, keys []string) error {
	worker, err := workerOf(state)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := sendWarmCommand(
			ctx, state, worker, key, keyedNoDeadline, 0); err != nil {
			return err
		}
	}
	return nil
}

// restartWarmWorker starts the loop again under the same group, which is
// what makes the refill read what the first run committed.
func restartWarmWorker(ctx context.Context, state *world) error {
	worker, err := workerOf(state)
	if err != nil {
		return err
	}
	return runWarmWorker(ctx, state, worker)
}
