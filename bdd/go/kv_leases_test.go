package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedKvLeaseScenariosWhenRunThenShouldHold runs the fenced
// lease feature against the managed store: renewal keeps the token, release
// frees it, a write from a holder whose lease ended is refused, and a
// takeover read at the new grant waits for the earlier holder's writes.
func TestGivenTheSharedKvLeaseScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "kv_leases.feature", 2*time.Minute, kvLeaseStep)
}

const (
	// leaseCoordination and leaseState are the namespaces the feature uses:
	// one for the lease keys, one for the values a fenced write guards.
	leaseCoordination = "coordination"
	leaseState        = "state"
	// leaseTakeoverDeadline bounds how long a takeover polls for the
	// earlier grant to run out, rather than sleeping a fixed time.
	leaseTakeoverDeadline = 15 * time.Second
)

// leaseFlow is what each holder holds, and the outcome of the last fenced
// write and release.
type leaseFlow struct {
	held     map[string]heldLease
	writeErr error
	wrote    bool
	released bool
	renewed  *laser.Lease
	// lastEntry is the guarded key as last read, which the next fenced write
	// states as its compare-and-swap precondition.
	lastEntry *wire.KvEntry
}

// heldLease is one holder's lease and the key it took.
type heldLease struct {
	key   string
	lease *laser.Lease
}

func leaseFlowOf(state *world) *leaseFlow {
	if existing, found := state.get("kv-leases"); found {
		return existing.(*leaseFlow)
	}
	flow := &leaseFlow{held: map[string]heldLease{}}
	state.put("kv-leases", flow)
	return flow
}

func kvLeaseStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	if step.Text == "a fresh stream bootstrapped on a connection-string client" {
		// The fenced-lease verbs need the managed band, and the namespaces
		// are scoped to a stream, so the stream has to exist first.
		if err := state.freshStream(ctx); err != nil {
			return err
		}
		return state.client.Bootstrap(ctx, 1, laser.ExpireAfter(24*time.Hour))
	}
	flow := leaseFlowOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.Contains(text, "acquires the lease") && strings.Contains(text, "once it is free"):
		if len(arguments) != 2 {
			return fmt.Errorf("want a holder and a key, got %v", arguments)
		}
		return acquireWhenFree(ctx, state, flow, arguments[0], arguments[1])

	case strings.Contains(text, "acquires the lease"):
		ttl, named := after(text, " for ")
		if len(arguments) != 2 || !named {
			return fmt.Errorf("want a holder, a key and a ttl in %q", text)
		}
		return acquireLease(ctx, state, flow, arguments[0], arguments[1],
			time.Duration(ttl)*time.Millisecond)

	case strings.Contains(text, "renews the lease"):
		if len(arguments) != 2 {
			return fmt.Errorf("want a holder and a key, got %v", arguments)
		}
		return renewLease(ctx, state, flow, arguments[0])

	case strings.Contains(text, "releases the lease"):
		if len(arguments) != 2 {
			return fmt.Errorf("want a holder and a key, got %v", arguments)
		}
		return releaseHeldLease(ctx, state, flow, arguments[0])

	case strings.Contains(text, "fenced by its lease"):
		if len(arguments) != 3 {
			return fmt.Errorf("want a holder, a value and a key, got %v", arguments)
		}
		return fencedWrite(ctx, state, flow, arguments[0], arguments[1], arguments[2])

	case strings.HasPrefix(text, "the renewal of"):
		return renewalKeptToken(flow, arguments)

	case text == "the release reports the lease was held":
		if !flow.released {
			return fmt.Errorf("want the release to report the lease was held")
		}
		return nil

	case strings.HasPrefix(text, "a read of") && strings.Contains(text, "at the lease grant of"):
		if len(arguments) != 3 {
			return fmt.Errorf("want a key, a holder and a value, got %v", arguments)
		}
		return readAtGrant(ctx, state, flow, arguments[0], arguments[1], arguments[2])

	case text == "the fenced write fails as lease lost":
		return writeFailedAsLeaseLost(flow)

	case strings.HasPrefix(text, "reading ") && strings.Contains(text, " returns "):
		if len(arguments) != 2 {
			return fmt.Errorf("want a key and a value, got %v", arguments)
		}
		return stateKeyReads(ctx, state, arguments[0], arguments[1])

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

func acquireLease(
	ctx context.Context, state *world, flow *leaseFlow,
	holder, key string, ttl time.Duration,
) error {
	lease, err := state.client.Kv(leaseCoordination).
		Lease(ctx, []byte(key), holder, ttl)
	if err != nil {
		return err
	}
	flow.held[holder] = heldLease{key: key, lease: lease}
	return nil
}

// acquireWhenFree polls until the store grants the key, rather than sleeping
// out the earlier holder's ttl: the grant is what the scenario waits for.
func acquireWhenFree(
	ctx context.Context, state *world, flow *leaseFlow, holder, key string,
) error {
	deadline := time.Now().Add(leaseTakeoverDeadline)
	for {
		lease, err := state.client.Kv(leaseCoordination).
			TryLease(ctx, []byte(key), holder, 5*time.Second)
		if err != nil {
			return err
		}
		if lease != nil {
			flow.held[holder] = heldLease{key: key, lease: lease}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the lease %q was still held after %v", key, leaseTakeoverDeadline)
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func renewLease(ctx context.Context, state *world, flow *leaseFlow, holder string) error {
	held, taken := flow.held[holder]
	if !taken {
		return fmt.Errorf("%q holds no lease to renew", holder)
	}
	renewed, err := state.client.Kv(leaseCoordination).
		RenewLease(ctx, []byte(held.key), holder, held.lease.Token, 5*time.Second)
	if err != nil {
		return err
	}
	flow.renewed = renewed
	return nil
}

func releaseHeldLease(ctx context.Context, state *world, flow *leaseFlow, holder string) error {
	held, taken := flow.held[holder]
	if !taken {
		return fmt.Errorf("%q holds no lease to release", holder)
	}
	wasHeld, err := state.client.Kv(leaseCoordination).
		ReleaseLease(ctx, []byte(held.key), holder, held.lease.Token)
	if err != nil {
		return err
	}
	flow.released = wasHeld
	return nil
}

// renewalKeptToken asserts a renewal extended the lease without minting a
// new token, which is what lets an in-flight fenced write stay valid.
func renewalKeptToken(flow *leaseFlow, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one holder, got %v", arguments)
	}
	held, taken := flow.held[arguments[0]]
	if !taken || flow.renewed == nil {
		return fmt.Errorf("%q renewed no lease", arguments[0])
	}
	if flow.renewed.Token != held.lease.Token {
		return fmt.Errorf("want the renewal to keep token %d, got %d",
			held.lease.Token, flow.renewed.Token)
	}
	return nil
}

// fencedWrite writes under a holder's lease token, so the store refuses it
// once a newer holder owns the key.
func fencedWrite(
	ctx context.Context, state *world, flow *leaseFlow, holder, value, key string,
) error {
	held, taken := flow.held[holder]
	if !taken {
		return fmt.Errorf("%q holds no lease to write under", holder)
	}
	request := state.client.Kv(leaseState).
		CasFenced([]byte(key), leaseCoordination, []byte(held.key), held.lease.Token).
		Bytes([]byte(value))
	// The fence guards *who* may write; the compare-and-swap precondition is
	// separate and still required. This runner used to send neither, so even
	// the first write was refused KvVersionConflict and the guarded key was
	// never written at all -- which then read back as "absent" and looked
	// like a lost write. Absent for the first write, the version last read
	// after that, exactly as the Rust runner does.
	if flow.lastEntry != nil {
		request = request.ExpectVersion(flow.lastEntry.Version)
	} else {
		request = request.ExpectAbsent()
	}
	_, err := request.Commit(ctx)
	flow.writeErr, flow.wrote = err, true
	// Kept rather than returned: the scenario decides whether this write was
	// meant to succeed.
	return nil
}

// writeFailedAsLeaseLost asserts the stale holder's write was refused as a
// lost fence rather than any other failure.
func writeFailedAsLeaseLost(flow *leaseFlow) error {
	if !flow.wrote {
		return fmt.Errorf("the scenario made no fenced write")
	}
	if flow.writeErr == nil {
		return fmt.Errorf("want the stale holder's write refused, it committed")
	}
	var kvError *laser.KvError
	if !errors.As(flow.writeErr, &kvError) {
		return fmt.Errorf("want a store refusal, got %v", flow.writeErr)
	}
	// Named rather than "any refusal": a write refused for some other
	// reason would otherwise read as the fence doing its job.
	if kvError.Failure.Kind != wire.KvLeaseLost {
		return fmt.Errorf("want the write refused as a lost lease, got %v",
			kvError.Failure.Kind)
	}
	return nil
}

// readAtGrant reads the guarded key at the new holder's grant position, so
// the read waits for every write the earlier holder made.
func readAtGrant(
	ctx context.Context, state *world, flow *leaseFlow, key, holder, expected string,
) error {
	held, taken := flow.held[holder]
	if !taken {
		return fmt.Errorf("%q holds no lease to read at", holder)
	}
	// The grant's own position is what the read is barriered on, which is
	// what makes a takeover see the predecessor's writes rather than racing
	// them. This step used to discard the position and read plainly, which
	// passed against in-process fakes and failed against a real deployment
	// for the only reason that matters: the read landed before the earlier
	// holder's write was applied.
	entry, err := state.client.Kv(leaseState).
		GetEntryAtLeast(ctx, []byte(key), held.lease.Position)
	if err != nil {
		return err
	}
	if entry == nil {
		return fmt.Errorf("want %q to hold %q at the grant position, it is absent",
			key, expected)
	}
	if string(entry.Value) != expected {
		return fmt.Errorf("want %q to read %q at the grant position, got %q",
			key, expected, entry.Value)
	}
	flow.lastEntry = entry
	return nil
}

func stateKeyReads(ctx context.Context, state *world, key, expected string) error {
	value, found, err := state.client.Kv(leaseState).Get(ctx, []byte(key))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("want %q to hold %q, it is absent", key, expected)
	}
	if string(value) != expected {
		return fmt.Errorf("want %q to read %q, got %q", key, expected, value)
	}
	return nil
}
