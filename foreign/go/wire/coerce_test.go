package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Coercion decides how a filter orders values, so every client must read the
// same text as the same instant. These are the Rust vectors.
func TestGivenRFC3339InstantsWhenParsedThenShouldNormalizeToUTCMicros(t *testing.T) {
	utc, ok := wire.FormatRFC3339.MicrosFromText("2026-09-21T18:04:12.331Z")
	if !ok {
		t.Fatal("a UTC instant must parse")
	}
	offset, ok := wire.FormatRFC3339.MicrosFromText("2026-09-21T20:04:12.331000+02:00")
	if !ok {
		t.Fatal("an offset instant must parse")
	}
	if utc != offset {
		t.Fatalf("the same instant in two offsets must compare equal: %d vs %d", utc, offset)
	}

	for text, want := range map[string]int64{
		"1970-01-01T00:00:00Z":           0,
		"1969-12-31T23:59:59.999999Z":    -1,
		"2024-02-29T00:00:00.000000000Z": 1_709_164_800_000_000,
	} {
		got, ok := wire.FormatRFC3339.MicrosFromText(text)
		if !ok {
			t.Errorf("%q must parse", text)
			continue
		}
		if got != want {
			t.Errorf("%q: want %d, got %d", text, want, got)
		}
	}
}

// An ambiguous or lossy instant is refused rather than rounded, so a filter
// never silently compares against a value it changed.
func TestGivenAmbiguousOrLossyRFC3339WhenParsedThenShouldBeRejected(t *testing.T) {
	for _, text := range []string{
		"2026-09-21T18:04:12.331000",   // no offset
		"2026-09-21 18:04:12Z",         // space separator
		"2026-02-29T00:00:00Z",         // not a leap year
		"2026-09-21T24:00:00Z",         // hour out of range
		"2026-09-21T18:04:60Z",         // leap second
		"2026-09-21T18:04:12.0000001Z", // past microseconds, nonzero
		"2026-09-21T18:04:12.Z",        // empty fraction
		"2026-09-21T18:04:12+2:00",     // unpadded offset
	} {
		if _, ok := wire.FormatRFC3339.MicrosFromText(text); ok {
			t.Errorf("%q must be rejected", text)
		}
	}
}

func TestGivenEpochUnitsWhenCoercedThenShouldScaleExactly(t *testing.T) {
	if got, ok := wire.FormatEpochSeconds.MicrosFromInteger(1_758_470_652); !ok ||
		got != 1_758_470_652_000_000 {
		t.Errorf("seconds: want 1758470652000000, got %d (ok=%v)", got, ok)
	}
	if got, ok := wire.FormatEpochMillis.MicrosFromText("1758470652331"); !ok ||
		got != 1_758_470_652_331_000 {
		t.Errorf("millis: want 1758470652331000, got %d (ok=%v)", got, ok)
	}
	if got, ok := wire.FormatEpochMicros.MicrosFromInteger(-5); !ok || got != -5 {
		t.Errorf("micros: want -5, got %d (ok=%v)", got, ok)
	}

	// Scaling that would overflow is refused rather than wrapping.
	if _, ok := wire.FormatEpochSeconds.MicrosFromInteger(1<<62 - 1); ok {
		t.Error("a scale that overflows must be refused")
	}
	// A decimal is not an integer instant.
	if _, ok := wire.FormatEpochMillis.MicrosFromText("12.5"); ok {
		t.Error("a decimal must not parse as an epoch integer")
	}
	// RFC 3339 is text-only.
	if _, ok := wire.FormatRFC3339.MicrosFromInteger(5); ok {
		t.Error("RFC 3339 takes no integer")
	}
}

// Exact decimals compare without floating-point rounding, which is the whole
// reason the contract carries them instead of doubles.
func TestGivenDecimalsWhenComparedThenShouldOrderExactly(t *testing.T) {
	parse := func(text string) wire.ExactDecimal {
		t.Helper()
		value, ok := wire.ParseExactDecimal(text)
		if !ok {
			t.Fatalf("%q must parse", text)
		}
		return value
	}

	if !parse("1.50").Equal(parse("1.5")) {
		t.Error("trailing zeros must normalise away")
	}
	if !parse("-0").Equal(parse("0.000")) {
		t.Error("negative zero is zero")
	}
	// Past the double's exact integer range, where a float comparison fails.
	if parse("9007199254740993").Compare(parse("9007199254740992")) <= 0 {
		t.Error("exact comparison must survive past 2^53")
	}
	if parse("-2").Compare(parse("-1.999")) >= 0 {
		t.Error("a larger negative magnitude is the smaller number")
	}
	if parse("0.1").Compare(parse("0.10001")) >= 0 {
		t.Error("fraction ordering is wrong")
	}
	if parse("10").Compare(parse("9.999999")) <= 0 {
		t.Error("a longer integer part is always larger")
	}
	if !wire.DecimalFromInteger(-42).Equal(parse("-42.0")) {
		t.Error("an integer must equal its decimal spelling")
	}
	half, ok := wire.DecimalFromFloat(0.5)
	if !ok || !half.Equal(parse("0.5")) {
		t.Error("a finite double must render as its shortest exact decimal")
	}
	if _, ok := wire.DecimalFromFloat(mustInf()); ok {
		t.Error("an infinity has no exact decimal")
	}
}

func TestGivenMalformedDecimalsWhenParsedThenShouldBeRejected(t *testing.T) {
	for _, text := range []string{"", "-", ".", "1.", "1e5", " 1", "1,5", "0x10", "--1"} {
		if _, ok := wire.ParseExactDecimal(text); ok {
			t.Errorf("%q must be rejected", text)
		}
	}
	// A bare fraction is legal.
	if _, ok := wire.ParseExactDecimal(".5"); !ok {
		t.Error("`.5` must be accepted")
	}
}

func mustInf() float64 {
	zero := 0.0
	return 1 / zero
}
