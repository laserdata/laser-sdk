package bdd

// Reference key-value engine: the executable specification of the
// compare-and-swap semantics every LaserData SDK and the managed backend must
// reproduce. Pure and transport-free (no Apache Iggy, no client), so the
// shared Gherkin pins one cross-language contract. The Rust counterpart lives
// in the Rust BDD crate and the Python one in bdd/python/reference.py. This
// port must return the same answers for the same inputs.

// CasExpectation is what a compare-and-swap requires of the live key.
type CasExpectation struct {
	// Absent requires the key not to be there, which is create-if-absent.
	Absent bool
	// Version is the version the swap expects when Absent is false.
	Version uint64
}

// ExpectAbsent requires the key to be absent or expired.
func ExpectAbsent() CasExpectation { return CasExpectation{Absent: true} }

// ExpectVersion requires the key to be live at this exact version.
func ExpectVersion(version uint64) CasExpectation {
	return CasExpectation{Version: version}
}

// CasOutcome is a compare-and-swap result: a committed version, or a conflict
// carrying the live version.
type CasOutcome struct {
	// Committed is the version the swap wrote, when it committed.
	Committed uint64
	// Conflict reports that the expectation did not hold.
	Conflict bool
	// Current is the live version a conflict found, and false when the key
	// was absent or expired, which is a different fact from version zero.
	Current    uint64
	HasCurrent bool
}

// kvStored is one key's value, version and expiry.
type kvStored struct {
	value     []byte
	version   uint64
	expiresAt uint64
	expires   bool
}

// KvEngine is an in-memory namespace.
//
// Each key carries a version that increases by one on every successful
// mutation, resetting to 1 over an absent or expired key. Expiry reads as
// absence. Time is an explicit argument, so expiry is deterministic with no
// wall clock.
type KvEngine struct {
	entries map[string]kvStored
}

// NewKvEngine opens an empty namespace.
func NewKvEngine() *KvEngine {
	return &KvEngine{entries: map[string]kvStored{}}
}

// liveVersion is the key's version when it is there and unexpired.
func (engine *KvEngine) liveVersion(key string, now uint64) (uint64, bool) {
	stored, held := engine.entries[key]
	if !held {
		return 0, false
	}
	if stored.expires && now >= stored.expiresAt {
		return 0, false
	}
	return stored.version, true
}

// Set writes a value, bumping the version over a live key and restarting at
// one over an absent or expired one.
func (engine *KvEngine) Set(
	key string, value []byte, expiresAt uint64, expires bool, now uint64,
) uint64 {
	version := uint64(1)
	if live, held := engine.liveVersion(key, now); held {
		version = live + 1
	}
	engine.entries[key] = kvStored{
		value: value, version: version, expiresAt: expiresAt, expires: expires,
	}
	return version
}

// Cas commits the swap when the expectation holds, and otherwise reports the
// conflict with whatever version is live.
func (engine *KvEngine) Cas(
	key string, value []byte, expect CasExpectation,
	expiresAt uint64, expires bool, now uint64,
) CasOutcome {
	live, held := engine.liveVersion(key, now)
	committable := held && live == expect.Version
	if expect.Absent {
		committable = !held
	}
	if !committable {
		return CasOutcome{Conflict: true, Current: live, HasCurrent: held}
	}
	return CasOutcome{Committed: engine.Set(key, value, expiresAt, expires, now)}
}
