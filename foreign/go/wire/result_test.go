package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The numeric codes are a pinned cross-repo contract, so each is asserted
// against its literal rather than trusting declaration order.
func TestGivenResultCodesWhenNumberedThenShouldMatchThePinnedValues(t *testing.T) {
	for name, pair := range map[string][2]uint16{
		"ok":                  {wire.ResultOk.Code(), 0},
		"unsupported":         {wire.ResultUnsupported.Code(), 1},
		"not found":           {wire.ResultNotFound.Code(), 2},
		"invalid argument":    {wire.ResultInvalidArgument.Code(), 3},
		"too large":           {wire.ResultTooLarge.Code(), 4},
		"conflict":            {wire.ResultConflict.Code(), 5},
		"stale":               {wire.ResultStale.Code(), 6},
		"version skew":        {wire.ResultVersionSkew.Code(), 7},
		"unauthenticated":     {wire.ResultUnauthenticated.Code(), 8},
		"backend":             {wire.ResultBackend.Code(), 9},
		"forbidden":           {wire.ResultForbidden.Code(), 10},
		"step up required":    {wire.ResultStepUpRequired.Code(), 11},
		"unavailable":         {wire.ResultUnavailable.Code(), 12},
		"resource limit":      {wire.ResultResourceLimit.Code(), 13},
		"cancelled":           {wire.ResultCancelled.Code(), 14},
		"deadline exceeded":   {wire.ResultDeadlineExceeded.Code(), 15},
		"expired snapshot":    {wire.ResultExpiredSnapshot.Code(), 16},
		"stale generation":    {wire.ResultStaleGeneration.Code(), 17},
		"target unavailable":  {wire.ResultTargetUnavailable.Code(), 18},
		"pending":             {wire.ResultPending.Code(), 19},
		"backend unavailable": {wire.ResultBackendUnavailable.Code(), 20},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s: want code %d, got %d", name, pair[1], pair[0])
		}
	}
}

// Every named code round-trips as its snake_case name, which is the form the
// HTTP surface and the binary band both carry.
func TestGivenANamedResultCodeWhenEncodedThenShouldRideAsItsName(t *testing.T) {
	for code := wire.ResultOk; code <= wire.ResultBackendUnavailable; code++ {
		encoded, err := wire.Marshal(code)
		if err != nil {
			t.Errorf("%s: marshal: %v", code, err)
			continue
		}
		// Major type 3 is a text string.
		if encoded[0]>>5 != 3 {
			t.Errorf("%s: want a text string, got head 0x%02x", code, encoded[0])
			continue
		}
		var back wire.ResultCode
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", code, err)
			continue
		}
		if back != code {
			t.Errorf("%s: round trip gave %s", code, back)
		}
		if parsed, err := wire.ParseResultCode(code.String()); err != nil || parsed != code {
			t.Errorf("%s: name did not parse back (%v)", code, err)
		}
	}
}

// A code from a newer peer must decode and re-encode unchanged, so an old
// build relays it rather than failing the whole reply.
func TestGivenAnUnrecognizedResultCodeWhenRelayedThenShouldSurviveUnchanged(t *testing.T) {
	const fromNewerPeer = wire.ResultCode(4242)

	if fromNewerPeer.IsRecognized() {
		t.Fatal("4242 is outside the named range")
	}
	encoded, err := wire.Marshal(fromNewerPeer)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.ResultCode
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != fromNewerPeer {
		t.Fatalf("want %d, got %d", fromNewerPeer, back)
	}
	again, err := wire.Marshal(back)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if string(again) != string(encoded) {
		t.Fatal("a relayed code must re-encode byte for byte")
	}
	if fromNewerPeer.HTTPStatus() != 500 {
		t.Errorf("an unknown code maps to 500, got %d", fromNewerPeer.HTTPStatus())
	}
}

// A named code must never arrive in the passthrough form, or two encodings of
// the same code would exist and byte-identical relaying would be ambiguous.
func TestGivenANamedCodeInThePassthroughFormWhenDecodedThenShouldBeRefused(t *testing.T) {
	// {"unrecognized": 5} names conflict, which has its own name.
	disguised := []byte{0xa1, 0x6c}
	disguised = append(disguised, []byte("unrecognized")...)
	disguised = append(disguised, 0x05)

	var code wire.ResultCode
	if err := wire.Unmarshal(disguised, &code); err == nil {
		t.Fatal("a named code must not ride as the passthrough form")
	}
}

// Only the transient classes are retryable. Getting this wrong either wastes
// attempts or gives up on a request that would have succeeded.
func TestGivenAResultCodeWhenClassifiedThenShouldNameOnlyTheTransientOnesRetryable(t *testing.T) {
	retryable := map[wire.ResultCode]bool{
		wire.ResultUnavailable:        true,
		wire.ResultStale:              true,
		wire.ResultTargetUnavailable:  true,
		wire.ResultBackendUnavailable: true,
	}
	for code := wire.ResultOk; code <= wire.ResultBackendUnavailable; code++ {
		if got := code.IsRetryable(); got != retryable[code] {
			t.Errorf("%s: want retryable=%v, got %v", code, retryable[code], got)
		}
	}

	// Pending is accepted-but-undecided: retryable only under the original
	// operation identity, so it must not read as generically retryable.
	if wire.ResultPending.IsRetryable() {
		t.Error("pending must not be generically retryable")
	}
	if !wire.ResultPending.IsPending() {
		t.Error("pending must report itself as pending")
	}
	if !wire.ResultBackendUnavailable.IsBackendUnavailable() {
		t.Error("backend unavailable must report itself")
	}
	if wire.ResultUnavailable.IsBackendUnavailable() {
		t.Error("unavailable names the destination, not the node")
	}
}

// One status mapping is shared by every surface, so a route never decides it.
func TestGivenAResultCodeWhenMappedToHTTPThenShouldUseTheSharedStatuses(t *testing.T) {
	for code, want := range map[wire.ResultCode]uint16{
		wire.ResultOk:                 200,
		wire.ResultUnsupported:        501,
		wire.ResultNotFound:           404,
		wire.ResultInvalidArgument:    400,
		wire.ResultVersionSkew:        400,
		wire.ResultTooLarge:           413,
		wire.ResultConflict:           409,
		wire.ResultCancelled:          409,
		wire.ResultStaleGeneration:    409,
		wire.ResultUnauthenticated:    401,
		wire.ResultBackend:            502,
		wire.ResultForbidden:          403,
		wire.ResultStepUpRequired:     403,
		wire.ResultStale:              503,
		wire.ResultUnavailable:        503,
		wire.ResultTargetUnavailable:  503,
		wire.ResultPending:            503,
		wire.ResultBackendUnavailable: 503,
		wire.ResultResourceLimit:      429,
		wire.ResultDeadlineExceeded:   408,
		wire.ResultExpiredSnapshot:    410,
	} {
		if got := code.HTTPStatus(); got != want {
			t.Errorf("%s: want status %d, got %d", code, want, got)
		}
	}
}

// The fallback reply turns a wrong-surface answer into a typed code.
func TestGivenACommandErrorWhenRoundTrippedThenShouldKeepItsClassification(t *testing.T) {
	original := wire.UnsupportedCommandError("no compare-and-swap on this build")

	encoded, err := wire.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.CommandError
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != original {
		t.Fatalf("round trip changed the error: %+v", back)
	}
	if back.Code != wire.ResultUnsupported {
		t.Errorf("want unsupported, got %s", back.Code)
	}
	if back.Error() == "" {
		t.Error("a command error must render as a Go error")
	}
}
