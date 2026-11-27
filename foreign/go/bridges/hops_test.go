package bridges_test

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAHopPathWhenStampedThenShouldBeAListOfStrings(t *testing.T) {
	hops, err := bridges.EnterBridge("mcp-edge", []string{"a2a-edge"})
	if err != nil {
		t.Fatalf("enter: %v", err)
	}
	want := wire.ListValue(wire.StrValue("a2a-edge"), wire.StrValue("mcp-edge"))
	if !reflect.DeepEqual(bridges.HopsMetadata(hops), want) {
		t.Fatalf("got %+v, want %+v", bridges.HopsMetadata(hops), want)
	}
	if empty := bridges.HopsMetadata(nil); empty.Kind != wire.ValueList ||
		len(empty.List) != 0 {
		t.Fatalf("an empty path is an empty list, got %+v", empty)
	}
}

func TestGivenAPathHoldingTheBridgeWhenEnteredThenShouldRejectTheLoop(t *testing.T) {
	if _, err := bridges.EnterBridge("edge", []string{"edge"}); err == nil {
		t.Fatal("a call that has been here before is the loop")
	}
	if _, err := bridges.EnterBridge("", nil); err == nil {
		t.Fatal("a bridge with no id guards nothing")
	}
}

func TestGivenAPreviousPathWhenEnteredThenShouldNotChangeIt(t *testing.T) {
	// Room to spare, so appending in place would write into the caller's
	// slice rather than a copy of it.
	previous := make([]string, 1, 8)
	previous[0] = "a2a-edge"
	hops, err := bridges.EnterBridge("mcp-edge", previous)
	if err != nil {
		t.Fatalf("enter: %v", err)
	}
	hops[0] = "rewritten"
	if previous[0] != "a2a-edge" {
		t.Fatal("entering a bridge must not rewrite the caller's path")
	}
}

func TestGivenInboundHopsWhenACallEntersTheBridgeThenShouldExtendOrRefuseThePath(t *testing.T) {
	configured := []string{"upstream", "a2a-edge"}
	direct, err := bridges.CallHops(configured, nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !reflect.DeepEqual(direct, configured) {
		t.Fatalf("got %v, want %v", direct, configured)
	}
	forwarded, err := bridges.CallHops(configured, []string{"upstream", "mcp-edge"})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	// upstream is already in the configured path, so it is not repeated: a
	// path listing one bridge twice would read as a loop that is not there.
	if !reflect.DeepEqual(forwarded, []string{"upstream", "mcp-edge", "a2a-edge"}) {
		t.Fatalf("got %v", forwarded)
	}
	if _, err := bridges.CallHops(
		configured, []string{"a2a-edge", "mcp-edge"}); err == nil {
		t.Fatal("an inbound path naming this bridge is the loop")
	}
	if _, err := bridges.CallHops(nil, nil); err == nil {
		t.Fatal("a bridge with no configured path guards nothing")
	}
}

func TestGivenAConfiguredPathWhenACallEntersThenShouldNotChangeIt(t *testing.T) {
	configured := make([]string, 2, 8)
	configured[0], configured[1] = "upstream", "a2a-edge"
	direct, err := bridges.CallHops(configured, nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	direct[0] = "rewritten"
	if configured[0] != "upstream" {
		t.Fatal("a call must not rewrite the bridge's configured path")
	}
	// A forwarded call appends its inbound hops, which would land in the
	// configured path's own spare capacity if the path were not copied.
	if _, err := bridges.CallHops(configured, []string{"mcp-edge"}); err != nil {
		t.Fatalf("call: %v", err)
	}
	if configured[1] != "a2a-edge" {
		t.Fatalf("a forwarded call rewrote the configured path to %v", configured)
	}
	// A configured path naming this bridge twice is refused rather than
	// quietly forwarded, because it reads as a loop wherever it lands.
	if _, err := bridges.CallHops([]string{"a2a-edge", "a2a-edge"}, nil); err == nil {
		t.Fatal("a configured path that names this bridge twice must be refused")
	}
}

func TestGivenRequestMetadataWhenReadThenShouldReturnTheHopsOrRefuseABadShape(t *testing.T) {
	hops, err := bridges.InboundHops(nil)
	if err != nil || len(hops) != 0 {
		t.Fatalf("absent metadata carries no hops, got %v %v", hops, err)
	}
	hops, err = bridges.InboundHops(json.RawMessage(`{"trace":"x"}`))
	if err != nil || len(hops) != 0 {
		t.Fatalf("metadata without the key carries no hops, got %v %v", hops, err)
	}
	hops, err = bridges.InboundHops(json.RawMessage(
		`{"laserdata.com/bridge_hops":["a2a-edge","mcp-edge"]}`))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !reflect.DeepEqual(hops, []string{"a2a-edge", "mcp-edge"}) {
		t.Fatalf("got %v", hops)
	}
	for _, bad := range []string{
		`{"laserdata.com/bridge_hops":"a2a-edge"}`,
		`{"laserdata.com/bridge_hops":[1]}`,
		`{"laserdata.com/bridge_hops":{}}`,
		`{"laserdata.com/bridge_hops":null}`,
		`not json`,
		`["a2a-edge"]`,
	} {
		if _, err := bridges.InboundHops(json.RawMessage(bad)); err == nil {
			t.Fatalf("%s must be refused", bad)
		}
	}
}

func TestGivenTheHopKeyWhenSpelledThenShouldMatchEveryClient(t *testing.T) {
	// The key is a wire contract: a bridge spelling it differently would not
	// see another client's guard and would forward the loop.
	if bridges.BridgeHopsMetaKey != "laserdata.com/bridge_hops" {
		t.Fatalf("the hop key is %q", bridges.BridgeHopsMetaKey)
	}
}

func TestGivenAnOriginWhenCheckedThenShouldAllowListedSameOriginAndNonBrowserRequests(t *testing.T) {
	allowed := []string{"https://app.example.com"}
	for _, probe := range []struct {
		name   string
		host   string
		origin string
		want   bool
	}{
		{"no origin at all", "bridge.example:8080", "", true},
		{"an allowlisted origin", "bridge.example:8080", "https://app.example.com", true},
		{"same origin on a v4 literal", "127.0.0.1:8080", "http://127.0.0.1:8080", true},
		{"same origin on a v6 literal", "[::1]:8080", "http://[::1]:8080", true},
		{"same origin on localhost", "LOCALHOST:8080", "http://localhost:8080", true},
		{"same origin on a rebindable name", "evil.example:8080",
			"http://evil.example:8080", false},
		{"a different port", "127.0.0.1:8080", "http://127.0.0.1:9090", false},
		{"a null origin", "127.0.0.1:8080", "null", false},
		{"no host", "", "http://127.0.0.1:8080", false},
		{"an origin that is not the host", "127.0.0.1:8080",
			"https://evil.example", false},
		{"a scheme this check does not read", "127.0.0.1:8080",
			"ws://127.0.0.1:8080", false},
		{"an origin with no scheme at all", "127.0.0.1:8080", "127.0.0.1:8080", false},
		{"a bare v6 literal with no brackets", "::1", "http://::1", false},
		{"an https same origin on a literal", "127.0.0.1", "https://127.0.0.1", true},
	} {
		t.Run(probe.name, func(t *testing.T) {
			request := &http.Request{Host: probe.host, Header: http.Header{}}
			if probe.origin != "" {
				request.Header.Set("Origin", probe.origin)
			}
			if got := bridges.OriginAllowed(request, allowed); got != probe.want {
				t.Fatalf("%q on %q gave %v, want %v",
					probe.origin, probe.host, got, probe.want)
			}
		})
	}
	if bridges.OriginAllowed(nil, allowed) {
		t.Fatal("a request that is not there is not a request to allow")
	}
}
