package bridges

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// BridgeHopsMetaKey is the key an inbound request carries its hop path under:
// in MCP params._meta, in A2A params.metadata.
//
// A bridge that forwards a call to another bridge's HTTP endpoint puts the
// path of the work it handles there, so the next bridge can refuse a loop.
// Two bridges pointed at each other would otherwise pass one call back and
// forth until something ran out.
const BridgeHopsMetaKey = "laserdata.com/bridge_hops"

// HopError reports a hop path a bridge will not forward.
type HopError struct{ Reason string }

func (e *HopError) Error() string { return e.Reason }

// EnterBridge appends this bridge to the ordered loop-guard path.
//
// A bridge must refuse a message whose path already holds its own id: the
// call has been here before, so handling it again is the loop. The result is
// ready to store under the hop metadata.
func EnterBridge(bridge string, previous []string) ([]string, error) {
	if bridge == "" {
		return nil, &HopError{Reason: "bridge id must not be empty"}
	}
	for _, hop := range previous {
		if hop == bridge {
			return nil, &HopError{Reason: "bridge loop detected at `" + bridge + "`"}
		}
	}
	hops := make([]string, 0, len(previous)+1)
	hops = append(hops, previous...)
	return append(hops, bridge), nil
}

// HopsMetadata is the metadata value a bridge stamps on what it publishes:
// the hop path as a list of strings.
func HopsMetadata(hops []string) wire.Value {
	values := make([]wire.Value, 0, len(hops))
	for _, hop := range hops {
		values = append(values, wire.StrValue(hop))
	}
	return wire.ListValue(values...)
}

// CallHops is the path one call carries: this bridge's configured path with
// the call's inbound hops before it, refused when either already holds this
// bridge.
//
// The configured path ends with this bridge. A hop the configured path
// already names is not repeated, because a path that listed one bridge twice
// would read as a loop that is not there.
func CallHops(configured, inbound []string) ([]string, error) {
	if len(configured) == 0 {
		return nil, &HopError{Reason: "bridge id must not be empty"}
	}
	bridge := configured[len(configured)-1]
	previous := configured[:len(configured)-1]
	path := append([]string(nil), previous...)
	for _, hop := range inbound {
		if !contains(previous, hop) {
			path = append(path, hop)
		}
	}
	return EnterBridge(bridge, path)
}

// InboundHops is the hop path under BridgeHopsMetaKey of a request's metadata
// object, empty when absent.
//
// Anything but a list of strings is refused rather than read past: a bridge
// that guessed at a malformed guard would be guarding nothing.
func InboundHops(metadata json.RawMessage) ([]string, error) {
	invalid := &HopError{
		Reason: BridgeHopsMetaKey + " must be a list of strings"}
	if len(metadata) == 0 {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &fields); err != nil {
		return nil, invalid
	}
	raw, carried := fields[BridgeHopsMetaKey]
	if !carried {
		return nil, nil
	}
	// A pointer, so a null under the key is refused rather than read as an
	// empty path: a guard the sender explicitly blanked is not no guard.
	var hops *[]string
	if err := json.Unmarshal(raw, &hops); err != nil || hops == nil {
		return nil, invalid
	}
	return *hops, nil
}

func contains(hops []string, wanted string) bool {
	for _, hop := range hops {
		if hop == wanted {
			return true
		}
	}
	return false
}

// OriginAllowed reports whether a request may reach a bridge HTTP endpoint.
//
// A request with no Origin header comes from a non-browser client and is
// allowed: the header is a browser's statement about itself, and its absence
// is not a claim to check. A browser request passes when its origin is
// allowlisted, or when it is same-origin with a host a DNS rebinding cannot
// name: an IP literal or localhost.
//
// The host restriction is the whole point. A rebound page sends its own name
// as both origin and host, so a same-origin check on any other host would
// wave it straight through.
func OriginAllowed(request *http.Request, allowed []string) bool {
	if request == nil {
		return false
	}
	origin := request.Header.Get("Origin")
	if origin == "" {
		return true
	}
	for _, permitted := range allowed {
		if permitted == origin {
			return true
		}
	}
	authority, scheme := strings.CutPrefix(origin, "http://")
	if !scheme {
		authority, scheme = strings.CutPrefix(origin, "https://")
	}
	if !scheme {
		return false
	}
	// Go keeps the Host header on the request rather than in the header map,
	// so it is read from there. A request that carries one in the map too
	// does not get to override it.
	host := request.Host
	return strings.EqualFold(host, authority) && unrebindable(host)
}

// unrebindable reports a Host value naming an IP literal or localhost, a host
// no DNS answer can point a foreign page at.
func unrebindable(host string) bool {
	name := host
	switch {
	case strings.HasPrefix(host, "["):
		name, _, _ = strings.Cut(host[1:], "]")
	case strings.Contains(host, ":"):
		// The last colon ends the name, so a bare IPv6 literal without
		// brackets does not read as one. It is not a valid Host value, and
		// treating it as an address would admit a host this check exists to
		// keep out.
		name = host[:strings.LastIndex(host, ":")]
	}
	return strings.EqualFold(name, "localhost") || net.ParseIP(name) != nil
}
