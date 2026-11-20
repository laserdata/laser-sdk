package laser_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenAMessageIDWhenRenderedThenShouldBePartitionAndOffset(t *testing.T) {
	if got := laser.NewMessageID(1, 42).String(); got != "1:42" {
		t.Fatalf("got %q", got)
	}
	// Zero is a real partition and a real offset, so it spells out rather
	// than reading as absent.
	if got := laser.NewMessageID(0, 0).String(); got != "0:0" {
		t.Fatalf("got %q", got)
	}
	if got := laser.NewMessageID(4_294_967_295, 18_446_744_073_709_551_615).String(); got !=
		"4294967295:18446744073709551615" {
		t.Fatalf("got %q", got)
	}
}

func TestGivenAMessageIDTextWhenParsedThenShouldRoundTrip(t *testing.T) {
	for _, want := range []laser.MessageID{
		laser.NewMessageID(0, 0),
		laser.NewMessageID(1, 42),
		laser.NewMessageID(4_294_967_295, 18_446_744_073_709_551_615),
	} {
		got, err := laser.ParseMessageID(want.String())
		if err != nil {
			t.Fatalf("parse %s: %v", want, err)
		}
		if got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestGivenATextThatIsNotAMessageIDWhenParsedThenShouldRefuseIt(t *testing.T) {
	for _, probe := range []string{
		"", "42", ":", "1:", ":42", "1:2:3", "a:1", "1:b",
		// Each of these survives a plain integer parse while breaking the
		// round trip with String, so two clients would disagree about
		// whether two ids name the same record.
		"+1:42", "1:+42", "-1:42", "1:-42", " 1:42", "1:42 ", "01:42", "1:042",
		// Past what a partition or an offset can hold.
		"4294967296:0", "0:18446744073709551616",
	} {
		if got, err := laser.ParseMessageID(probe); err == nil {
			t.Fatalf("%q parsed as %v", probe, got)
		}
	}
}

func TestGivenAZeroThatIsNotPaddedWhenParsedThenShouldBeAccepted(t *testing.T) {
	// A single zero is canonical. Only a leading zero before another digit
	// is the spelling that does not round-trip.
	got, err := laser.ParseMessageID("0:0")
	if err != nil || got != laser.NewMessageID(0, 0) {
		t.Fatalf("got %v %v", got, err)
	}
}
