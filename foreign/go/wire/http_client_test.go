package wire_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// recordingTransport answers a scripted response and remembers what it was
// asked to send, which is how the typed calls are checked without a server.
type recordingTransport struct {
	requests []wire.HTTPRequest
	reply    wire.HTTPResponse
	failure  error
}

func (transport *recordingTransport) Send(
	ctx context.Context, request wire.HTTPRequest,
) (wire.HTTPResponse, error) {
	// A transport must honor the caller's cancellation.
	if err := ctx.Err(); err != nil {
		return wire.HTTPResponse{}, err
	}
	transport.requests = append(transport.requests, request)
	if transport.failure != nil {
		return wire.HTTPResponse{}, transport.failure
	}
	return transport.reply, nil
}

func (transport *recordingTransport) last() wire.HTTPRequest {
	return transport.requests[len(transport.requests)-1]
}

func jsonReply(t *testing.T, status uint16, value any) wire.HTTPResponse {
	t.Helper()
	body, err := wire.MarshalJSON(value)
	if err != nil {
		t.Fatalf("encode the reply: %v", err)
	}
	return wire.NewHTTPResponse(status, body)
}

// A typed call must build the exact request the route expects: the verb, the
// path with its encoded parameters, and the body.
func TestGivenATypedCallWhenSentThenShouldBuildTheRouteRequest(t *testing.T) {
	capabilities := wire.NewCapabilities(true, wire.OpVersions{Query: 1})
	transport := &recordingTransport{reply: jsonReply(t, 200, capabilities)}
	client := wire.NewHTTPClient(transport)

	got, err := client.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if !got.Managed || !got.Query.Available {
		t.Error("the decoded capabilities must match what was served")
	}
	if request := transport.last(); request.Method != wire.MethodGet ||
		request.Path != "/agdx/capabilities" || request.Body != nil {
		t.Errorf("want a bare GET of the capabilities path, got %+v", request)
	}
}

// A binary key is base64url-encoded into the path, so raw bytes never enter a
// path segment, while the value rides the raw body with no encoding inflation.
func TestGivenABinaryKeyWhenWrittenThenShouldEncodeTheKeyAndNotTheValue(t *testing.T) {
	transport := &recordingTransport{reply: wire.NewHTTPResponse(204, nil)}
	client := wire.NewHTTPClient(transport)

	key := []byte{0xff, 0x00, '/'}
	value := []byte{1, 2, 3}
	expiry := uint64(1_700)
	if err := client.KvSet(context.Background(), "sessions", key, value, &expiry); err != nil {
		t.Fatalf("set: %v", err)
	}

	request := transport.last()
	if request.Method != wire.MethodPut {
		t.Errorf("a write is a PUT, got %s", request.Method)
	}
	encoded := wire.Base64URLEncode(key)
	if !strings.HasPrefix(request.Path, "/agdx/kv/sessions/"+encoded) {
		t.Errorf("the key must be base64url in the path, got %q", request.Path)
	}
	if !strings.Contains(request.Path, "expires_at_micros=1700") {
		t.Errorf("the expiry rides the query string, got %q", request.Path)
	}
	// The value is the raw body: a base64 value would inflate a large write.
	if string(request.Body) != string(value) {
		t.Errorf("the value rides the body raw, got %x", request.Body)
	}
}

// A read of something absent is not an error: the request succeeded and the
// answer is that there is no such thing.
func TestGivenAnAbsentEntityWhenReadThenShouldReportAbsenceNotFailure(t *testing.T) {
	transport := &recordingTransport{reply: wire.NewHTTPResponse(404, nil)}
	client := wire.NewHTTPClient(transport)

	value, found, err := client.KvGet(context.Background(), "sessions", []byte("k"))
	if err != nil {
		t.Fatalf("a missing entry is not a failure: %v", err)
	}
	if found || value.Value != nil {
		t.Error("a missing entry reports absence")
	}

	// So is a delete of something that was already gone, which is what makes
	// a retry safe.
	deleted, err := client.KvDelete(context.Background(), "sessions", []byte("k"))
	if err != nil || deleted {
		t.Errorf("deleting what is absent reports false, got %v %v", deleted, err)
	}
}

// A single-key read carries its expiry out of band, because the body is the
// raw value with nowhere to put it.
func TestGivenAnEntryWithAnExpiryWhenReadThenShouldTakeItFromTheHeader(t *testing.T) {
	transport := &recordingTransport{
		reply: wire.NewHTTPResponse(200, []byte("payload")).
			WithHeader("AGDX-Expires-At-Micros", "1700"),
	}
	client := wire.NewHTTPClient(transport)

	value, found, err := client.KvGet(context.Background(), "sessions", []byte("k"))
	if err != nil || !found {
		t.Fatalf("read: %v %v", found, err)
	}
	if string(value.Value) != "payload" {
		t.Errorf("the body is the value, got %q", value.Value)
	}
	// Header names are case-insensitive, so a transport's own casing must not
	// lose the expiry.
	if value.ExpiresAtMicros == nil || *value.ExpiresAtMicros != 1700 {
		t.Errorf("want the header expiry, got %v", value.ExpiresAtMicros)
	}

	// A transport that surfaces no headers leaves the expiry absent rather
	// than failing the read.
	transport.reply = wire.NewHTTPResponse(200, []byte("payload"))
	value, _, err = client.KvGet(context.Background(), "sessions", []byte("k"))
	if err != nil || value.ExpiresAtMicros != nil {
		t.Errorf("no header means no expiry, got %v %v", value.ExpiresAtMicros, err)
	}
}

// A caller branches on the classified code rather than the message text.
func TestGivenAFailureWhenReceivedThenShouldCarryItsClassifiedCode(t *testing.T) {
	body := wire.NewErrorBody(wire.ResultConflict, "version 4 is current")
	transport := &recordingTransport{reply: jsonReply(t, 409, body)}
	client := wire.NewHTTPClient(transport)

	_, err := client.Capabilities(context.Background())
	if err == nil {
		t.Fatal("a 409 must surface as an error")
	}
	code, classified := wire.ErrorCode(err)
	if !classified || code != wire.ResultConflict {
		t.Errorf("want a conflict, got %v %v", code, classified)
	}
	var failure *wire.ClientError
	if !errors.As(err, &failure) || failure.Kind != wire.ClientAPIFailed {
		t.Errorf("want a classified API failure, got %+v", err)
	}
	if !strings.Contains(err.Error(), "version 4 is current") {
		t.Errorf("the message reaches the caller, got %q", err)
	}
}

// A failure with no classified body still reaches the caller as something it
// can branch on, inferred from the status.
func TestGivenAFailureWithoutABodyWhenReceivedThenShouldInferTheCodeFromTheStatus(
	t *testing.T,
) {
	cases := map[uint16]wire.ResultCode{
		404: wire.ResultNotFound,
		400: wire.ResultInvalidArgument,
		401: wire.ResultUnauthenticated,
		403: wire.ResultForbidden,
		409: wire.ResultConflict,
		413: wire.ResultTooLarge,
		501: wire.ResultUnsupported,
		503: wire.ResultStale,
		502: wire.ResultBackend,
	}
	for status, want := range cases {
		transport := &recordingTransport{
			reply: wire.NewHTTPResponse(status, []byte("bad gateway")),
		}
		client := wire.NewHTTPClient(transport)
		_, err := client.Capabilities(context.Background())
		code, classified := wire.ErrorCode(err)
		if !classified || code != want {
			t.Errorf("status %d is %v, want %v", status, code, want)
		}
		var failure *wire.ClientError
		if errors.As(err, &failure) && failure.Status != status {
			t.Errorf("the status reaches the caller, got %d", failure.Status)
		}
	}
}

// A transport failure is surfaced verbatim, so a caller can match on its own
// error, and carries no result code: the server never saw the request.
func TestGivenATransportFailureWhenSentThenShouldSurfaceItVerbatim(t *testing.T) {
	cause := errors.New("connection refused")
	transport := &recordingTransport{failure: cause}
	client := wire.NewHTTPClient(transport)

	_, err := client.Capabilities(context.Background())
	if !errors.Is(err, cause) {
		t.Errorf("the transport's own failure must reach the caller, got %v", err)
	}
	if _, classified := wire.ErrorCode(err); classified {
		t.Error("a request the server never saw has no result code")
	}
}

// The context reaches the transport, so a cancelled call stops rather than
// waiting on a server that will never answer.
func TestGivenACancelledContextWhenCallingThenShouldStop(t *testing.T) {
	transport := &recordingTransport{reply: wire.NewHTTPResponse(200, []byte("{}"))}
	client := wire.NewHTTPClient(transport)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Capabilities(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("want the cancellation, got %v", err)
	}
}

// A write re-sent under the same operation identity is answered from the
// recorded outcome, so the identity has to reach the query string.
func TestGivenAnOperationIdentityWhenWritingThenShouldCarryItOnTheRequest(t *testing.T) {
	transport := &recordingTransport{reply: wire.NewHTTPResponse(204, nil)}
	client := wire.NewHTTPClient(transport)

	operation, err := wire.ParseU128("00000000000000000000000001")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := client.KvSetAs(context.Background(), "ns",
		[]byte("k"), []byte("v"), nil, operation); err != nil {
		t.Fatalf("set: %v", err)
	}
	if !strings.Contains(transport.last().Path, "operation_id="+operation.String()) {
		t.Errorf("the operation identity must ride the query, got %q",
			transport.last().Path)
	}
}

// A conditional write names exactly one precondition, and the two forms are
// spelled differently on the wire.
func TestGivenAConditionalWriteWhenSentThenShouldCarryOnePrecondition(t *testing.T) {
	transport := &recordingTransport{
		reply: jsonReply(t, 200, wire.CasCommittedView{Version: 5}),
	}
	client := wire.NewHTTPClient(transport)

	version, err := client.KvCas(context.Background(), "ns",
		[]byte("k"), []byte("v"), wire.MatchVersion(4), nil)
	if err != nil {
		t.Fatalf("cas: %v", err)
	}
	if version != 5 {
		t.Errorf("want the committed version, got %d", version)
	}
	if !strings.Contains(transport.last().Path, "expect_version=4") {
		t.Errorf("want the version precondition, got %q", transport.last().Path)
	}
	if !strings.HasSuffix(strings.Split(transport.last().Path, "?")[0], "/cas") {
		t.Errorf("a conditional write has its own route, got %q", transport.last().Path)
	}

	if _, err := client.KvCas(context.Background(), "ns",
		[]byte("k"), []byte("v"), wire.AbsentKey(), nil); err != nil {
		t.Fatalf("cas: %v", err)
	}
	if !strings.Contains(transport.last().Path, "expect_absent=true") {
		t.Errorf("want the absence precondition, got %q", transport.last().Path)
	}
}

// Base64url is the encoding this surface uses for every binary value, so it
// must round-trip exactly, including the lengths that need no padding.
func TestGivenBinaryValuesWhenBase64EncodedThenShouldRoundTrip(t *testing.T) {
	cases := [][]byte{
		{}, {0}, {0, 1}, {0, 1, 2}, {0xff, 0xfe, 0xfd, 0xfc},
		[]byte("hello"), {0xfb, 0xff, 0xbf},
	}
	for _, input := range cases {
		encoded := wire.Base64URLEncode(input)
		if strings.ContainsAny(encoded, "+/=") {
			t.Errorf("%x encoded as %q, which is not URL-safe unpadded", input, encoded)
		}
		decoded, err := wire.Base64URLDecode(encoded)
		if err != nil {
			t.Errorf("%q: %v", encoded, err)
			continue
		}
		if string(decoded) != string(input) {
			t.Errorf("%x round-tripped to %x", input, decoded)
		}
	}
	for _, broken := range []string{"A", "####", "a+b", "=="} {
		if _, err := wire.Base64URLDecode(broken); err == nil {
			t.Errorf("%q must not decode", broken)
		}
	}
}

// A query string carries its values percent-encoded, so a filter term holding
// a reserved character reaches the server as written.
func TestGivenAQueryTermWithReservedCharactersWhenSentThenShouldEncodeIt(t *testing.T) {
	transport := &recordingTransport{
		reply: jsonReply(t, 200, []wire.ProjectionInfo{}),
	}
	client := wire.NewHTTPClient(transport)

	term := "a&b=c"
	if _, err := client.ListProjections(context.Background(),
		wire.ProjectionListQuery{NameContains: &term}); err != nil {
		t.Fatalf("list: %v", err)
	}
	path := transport.last().Path
	if !strings.Contains(path, "name_contains=a%26b%3Dc") {
		t.Errorf("the term must be encoded, got %q", path)
	}
	// An absent term is not sent at all, rather than sent empty.
	if strings.Contains(path, "topic=") {
		t.Errorf("an absent term must be omitted, got %q", path)
	}
}
