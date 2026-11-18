package laser

import (
	"context"
	"errors"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// BinaryRequester sends one managed command over an open connection and returns
// the raw reply. The Apache Iggy Go SDK's client satisfies it, and naming the
// one call the managed band needs keeps the probe testable without a server.
type BinaryRequester interface {
	SendBinaryRequest(ctx context.Context, code uint32, payload []byte) ([]byte, error)
}

// probeManagedHost runs the connect-time AGDX_HELLO probe.
//
// The three outcomes are distinct and the caller must keep them apart:
//
//   - an announcement means the deployment is Laser Streams and exposes the
//     managed bridge, so the surfaces it advertises light up
//   - a nil announcement with a nil error means the server answered without
//     one, which is Apache Iggy refusing the command or an older server's
//     empty body, so the managed surfaces are positively absent
//   - an error means the probe established nothing, because the transport
//     failed or the reply did not decode. Nothing may be concluded from it
//
// The probe is cheap and never fatal: a client that cannot classify the
// deployment still connects, with the open surface alone.
func probeManagedHost(
	ctx context.Context, client BinaryRequester,
) (*wire.BackendAnnounce, error) {
	reply, err := client.SendBinaryRequest(ctx, wire.AgdxHelloCode, nil)
	if err != nil {
		// Raw Apache Iggy does not know the command. That is an answer, not a
		// failure: the managed surfaces are absent rather than unclassified.
		if errors.Is(err, ierror.InvalidCommand{}) {
			return nil, nil
		}
		return nil, err
	}
	// A server from before the announcement existed answers with an empty
	// body, which leaves the versions unadvertised and the surfaces absent.
	if len(reply) == 0 {
		return nil, nil
	}
	var announce wire.BackendAnnounce
	if err := wire.Unmarshal(reply, &announce); err != nil {
		return nil, err
	}
	if err := announce.Validate(); err != nil {
		return nil, err
	}
	return &announce, nil
}

// NegotiateCapabilities probes the server once and folds what it announced into
// the configured set.
//
// The configured set is the starting point and never a ceiling, so a caller
// that pinned a capability keeps it and still gains whatever the deployment
// serves. A probe that fails leaves the set as configured and records that
// nothing was established, which is what stops a consumer reading a deployment
// it could not classify.
func NegotiateCapabilities(
	ctx context.Context, client BinaryRequester, configured Capabilities,
) Capabilities {
	negotiated := configured
	announce, err := probeManagedHost(ctx, client)
	switch {
	case err != nil:
		negotiated.Hello = HelloFailed
	case announce == nil:
		negotiated.Hello = HelloRejected
	default:
		negotiated.mergeAnnouncement(announce)
		negotiated.Hello = HelloAnswered
	}
	return negotiated
}
