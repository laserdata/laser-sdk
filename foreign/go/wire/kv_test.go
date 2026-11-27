package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// roundTripFixture decodes a fixture into T, re-encodes it, and requires the
// bytes to match the Rust encoder exactly.
func roundTripFixture[T any](t *testing.T, name string) T {
	t.Helper()
	raw := readFixture(t, name)
	var value T
	if err := wire.Unmarshal(raw, &value); err != nil {
		t.Fatalf("%s: decode: %v", name, err)
	}
	again, err := wire.Marshal(value)
	if err != nil {
		t.Fatalf("%s: encode: %v", name, err)
	}
	if string(again) != string(raw) {
		t.Fatalf("%s: re-encode differs (%d in, %d out)%s",
			name, len(raw), len(again), describeDiff(raw, again))
	}
	return value
}

// Every key-value request shape in the corpus, so the whole surface is pinned
// rather than one representative per family.
func TestGivenKeyValueRequestFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("kv_get_barriered.bin", func(t *testing.T) {
		request := roundTripFixture[wire.KvGet](t, "kv_get_barriered.bin")
		if request.MinPosition == nil {
			t.Error("the barriered get carries a minimum position")
		}
	})
	t.Run("kv_set.bin", func(t *testing.T) {
		roundTripFixture[wire.KvSet](t, "kv_set.bin")
	})
	t.Run("kv_set_ttl.bin", func(t *testing.T) {
		// A relative lifetime is resolved once at fold time, so every replica
		// stores the same expiry rather than each reading its own clock.
		request := roundTripFixture[wire.KvSet](t, "kv_set_ttl.bin")
		if request.TTLMicros == nil {
			t.Error("the lifetime fixture names a relative lifetime")
		}
		if request.ExpiresAtMicros != nil {
			t.Error("the two expiry forms are mutually exclusive")
		}
	})
	t.Run("kv_set_session.bin", func(t *testing.T) {
		request := roundTripFixture[wire.KvSet](t, "kv_set_session.bin")
		if request.Session == nil {
			t.Error("the session fixture carries a session reference")
		}
	})
	t.Run("kv_cas.bin", func(t *testing.T) {
		request := roundTripFixture[wire.KvCas](t, "kv_cas.bin")
		if request.Expect.Kind != wire.CasMatch || request.Expect.Version != 7 {
			t.Errorf("want a version-7 precondition, got %+v", request.Expect)
		}
	})
	t.Run("kv_cas_session.bin", func(t *testing.T) {
		roundTripFixture[wire.KvCas](t, "kv_cas_session.bin")
	})
	t.Run("kv_cas_fenced.bin", func(t *testing.T) {
		request := roundTripFixture[wire.KvCasFenced](t, "kv_cas_fenced.bin")
		if request.FenceToken == 0 {
			t.Error("a fenced write presents a fence token")
		}
	})
	t.Run("kv_cas_fenced_session.bin", func(t *testing.T) {
		request := roundTripFixture[wire.KvCasFenced](t, "kv_cas_fenced_session.bin")
		if request.Session == nil {
			t.Error("the session fixture carries a session reference")
		}
	})
	t.Run("kv_delete_session.bin", func(t *testing.T) {
		roundTripFixture[wire.KvDelete](t, "kv_delete_session.bin")
	})
	t.Run("kv_patch_session.bin", func(t *testing.T) {
		roundTripFixture[wire.KvPatch](t, "kv_patch_session.bin")
	})
	t.Run("kv_copy.bin", func(t *testing.T) {
		roundTripFixture[wire.KvCopy](t, "kv_copy.bin")
	})
	t.Run("kv_move.bin", func(t *testing.T) {
		roundTripFixture[wire.KvMove](t, "kv_move.bin")
	})
	t.Run("kv_lease.bin", func(t *testing.T) {
		request := roundTripFixture[wire.KvLease](t, "kv_lease.bin")
		if request.HolderID == "" {
			t.Error("a lease names its holder")
		}
	})
	t.Run("kv_lease_renew.bin", func(t *testing.T) {
		roundTripFixture[wire.KvLeaseRenew](t, "kv_lease_renew.bin")
	})
	t.Run("kv_release.bin", func(t *testing.T) {
		roundTripFixture[wire.KvRelease](t, "kv_release.bin")
	})
	t.Run("kv_namespaces.bin", func(t *testing.T) {
		roundTripFixture[wire.KvNamespaces](t, "kv_namespaces.bin")
	})
	t.Run("kv_scan.bin", func(t *testing.T) {
		roundTripFixture[wire.KvScan](t, "kv_scan.bin")
	})
	t.Run("kv_entry_memory_scope.bin", func(t *testing.T) {
		entry := roundTripFixture[wire.KvEntry](t, "kv_entry_memory_scope.bin")
		if entry.Scope == nil {
			t.Fatal("the memory-scope fixture carries a scope")
		}
		if entry.Scope.Source == nil {
			t.Error("the scope points back at its source record")
		}
	})
}

// Every reply shape, including the three failures, since a client dispatches
// real behaviour on them.
func TestGivenKeyValueReplyFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("kv_reply_committed.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.KvReply](t, "kv_reply_committed.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.KvCommitted {
			t.Fatalf("want a committed outcome, got %+v", reply)
		}
		if reply.Outcome.Version != 8 {
			t.Errorf("want version 8, got %d", reply.Outcome.Version)
		}
	})
	t.Run("kv_reply_page.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.KvReply](t, "kv_reply_page.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.KvPageOutcome {
			t.Fatalf("want a page outcome, got %+v", reply)
		}
	})
	t.Run("kv_reply_namespaces.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.KvReply](t, "kv_reply_namespaces.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.KvNamespacesOutcome {
			t.Fatalf("want a namespaces outcome, got %+v", reply)
		}
	})
	t.Run("kv_reply_leased.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.KvReply](t, "kv_reply_leased.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.KvLeased {
			t.Fatalf("want a leased outcome, got %+v", reply)
		}
		if reply.Outcome.Lease.GrantedTTLMicros == 0 {
			t.Error("a granted lease TTL is never zero")
		}
	})
	t.Run("kv_reply_renewed.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.KvReply](t, "kv_reply_renewed.bin")
		if !reply.IsOk() || reply.Outcome.Kind != wire.KvRenewed {
			t.Fatalf("want a renewed outcome, got %+v", reply)
		}
	})
	t.Run("kv_reply_lease_held.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.KvReply](t, "kv_reply_lease_held.bin")
		if reply.IsOk() || reply.Err.Kind != wire.KvLeaseHeld {
			t.Fatalf("want a held lease, got %+v", reply)
		}
		// The expiry is the earliest a fresh acquisition can succeed, so a
		// caller waits for it rather than spinning.
		if reply.Err.ExpiresAtMicros == nil {
			t.Error("a held lease reports when the holder's claim expires")
		}
		if reply.Err.ResultCode() != wire.ResultConflict {
			t.Errorf("a held lease projects onto conflict, got %s", reply.Err.ResultCode())
		}
	})
	t.Run("kv_reply_barrier_unreachable.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.KvReply](t, "kv_reply_barrier_unreachable.bin")
		if reply.IsOk() || reply.Err.Kind != wire.KvBarrierUnreachable {
			t.Fatalf("want an unreachable barrier, got %+v", reply)
		}
		// Unreachable is not stale: the fold will never reach the position,
		// so a caller re-reads the generation rather than retrying the wait.
		if reply.Err.ResultCode() != wire.ResultStaleGeneration {
			t.Errorf("an unreachable barrier projects onto a stale generation, got %s",
				reply.Err.ResultCode())
		}
	})
	t.Run("kv_reply_version_conflict.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.KvReply](t, "kv_reply_version_conflict.bin")
		if reply.IsOk() || reply.Err.Kind != wire.KvVersionConflict {
			t.Fatalf("want a version conflict, got %+v", reply)
		}
		if reply.Err.Current == nil || *reply.Err.Current != 7 {
			t.Errorf("want current version 7, got %v", reply.Err.Current)
		}
		if reply.Err.ResultCode() != wire.ResultConflict {
			t.Errorf("a conflict projects onto conflict, got %s", reply.Err.ResultCode())
		}
	})
	t.Run("kv_reply_stale.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.KvReply](t, "kv_reply_stale.bin")
		if reply.IsOk() || reply.Err.Kind != wire.KvStale {
			t.Fatalf("want a stale barrier, got %+v", reply)
		}
		// A stale barrier must never be read as an absent value.
		if reply.Err.Required.Offset == 0 {
			t.Error("a stale error names the position it waited for")
		}
		if !reply.Err.ResultCode().IsRetryable() {
			t.Error("a stale read is retryable against a caught-up fold")
		}
	})
}

// A lease outcome decoded as success when it is actually something else would
// have a caller trust an unextended deadline, so the reply enums decode
// fail-closed on an unknown variant.
func TestGivenAnUnknownReplyVariantWhenDecodedThenShouldFailClosed(t *testing.T) {
	// {"Ok": {"Teleported": 1}}
	unknown := []byte{0xa1, 0x62, 'O', 'k', 0xa1, 0x6a}
	unknown = append(unknown, []byte("Teleported")...)
	unknown = append(unknown, 0x01)

	var reply wire.KvReply
	if err := wire.Unmarshal(unknown, &reply); err == nil {
		t.Fatal("an unknown outcome variant must not decode as success")
	}

	// A unit encoding where a payload is required is equally refused.
	committedWithoutPayload := []byte{0xa1, 0x62, 'O', 'k', 0x69}
	committedWithoutPayload = append(committedWithoutPayload, []byte("Committed")...)
	var second wire.KvReply
	if err := wire.Unmarshal(committedWithoutPayload, &second); err == nil {
		t.Fatal("a payload-carrying variant must not decode from its bare name")
	}
}

// The precondition is the compare half of optimistic concurrency, so both
// forms must survive the wire.
func TestGivenACasPreconditionWhenRoundTrippedThenShouldKeepItsForm(t *testing.T) {
	for name, expect := range map[string]wire.CasExpect{
		"match":  wire.MatchVersion(42),
		"absent": wire.AbsentKey(),
	} {
		encoded, err := wire.Marshal(expect)
		if err != nil {
			t.Errorf("%s: marshal: %v", name, err)
			continue
		}
		var back wire.CasExpect
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", name, err)
			continue
		}
		if back != expect {
			t.Errorf("%s: round trip changed the precondition: %+v", name, back)
		}
	}
}

// The namespace rule is shared by the client edge and every serving tier, so a
// name one accepts the others accept.
func TestGivenANamespaceWhenValidatedThenShouldApplyTheSharedRule(t *testing.T) {
	if err := wire.ValidateKvNamespace("orders/eu-west"); err != nil {
		t.Errorf("slash hierarchy stays legal: %v", err)
	}
	if err := wire.ValidateKvNamespace(""); err == nil {
		t.Error("an empty namespace must be refused")
	}
	if err := wire.ValidateKvNamespace("bad\tname"); err == nil {
		t.Error("a control character must be refused")
	}
}

// A zero version is reserved for "unversioned" and must stay off the wire, so
// entries written before versioning stay byte-identical.
func TestGivenAnUnversionedEntryWhenEncodedThenShouldOmitTheVersion(t *testing.T) {
	entry := wire.KvEntry{Key: []byte("k"), Value: []byte("v")}

	encoded, err := wire.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.KvEntry
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Version != 0 {
		t.Fatalf("want an unversioned entry, got version %d", back.Version)
	}
	// Two fields only: key and value.
	if encoded[0] != 0xa2 {
		t.Fatalf("want a two-entry map, got head 0x%02x", encoded[0])
	}
}
