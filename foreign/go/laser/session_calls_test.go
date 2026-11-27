package laser_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// recorded reads back the envelopes the session wrote, in lane order.
func recorded(t *testing.T, server *stateServer) []wire.AgentEnvelope {
	t.Helper()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	envelopes := make([]wire.AgentEnvelope, 0, len(server.lane))
	for _, message := range server.lane {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			t.Fatalf("decode the envelope: %v", err)
		}
		envelopes = append(envelopes, envelope)
	}
	return envelopes
}

// numeric reads a metadata number, whichever signed or unsigned form the
// contract decoded it into: a value inside the signed range comes back as an
// Int, and only one past it stays a Uint.
func numeric(t *testing.T, value wire.Value) uint64 {
	t.Helper()
	switch value.Kind {
	case wire.ValueInt:
		if value.Int < 0 {
			t.Fatalf("a duration must not be negative, got %d", value.Int)
		}
		return uint64(value.Int)
	case wire.ValueUint:
		return value.Uint
	default:
		t.Fatalf("want a number, got %+v", value)
		return 0
	}
}

// metadata reads one metadata value off an envelope.
func metadata(
	t *testing.T, envelope wire.AgentEnvelope, key string,
) (wire.Value, bool) {
	t.Helper()
	value, carried := envelope.Metadata[key]
	return value, carried
}

func TestGivenAModelCallWhenRecordedThenShouldWriteItsRequestAndResult(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	call, err := session.Model(context.Background(), laser.ModelRequest{
		Model: "a-model", Provider: "a-provider", Body: []byte(`{"prompt":"hi"}`),
	}, nil)
	if err != nil {
		t.Fatalf("model: %v", err)
	}
	usage := wire.TokenUsage{InputTokens: 11, OutputTokens: 22}
	if _, err := call.Complete(context.Background(), laser.ModelResponse{
		Body: []byte(`{"text":"hello"}`), Model: "a-model-0613",
		FinishReason: "stop", Usage: &usage, Duration: 250 * time.Millisecond,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	written := recorded(t, server)
	if len(written) != 2 {
		t.Fatalf("want a request and a result, got %d records", len(written))
	}
	request, result := written[0], written[1]
	if request.Kind != wire.KindCommand || result.Kind != wire.KindResponse {
		t.Fatalf("got %q and %q", request.Kind, result.Kind)
	}
	// Both carry one correlation, which is what ties a result to the request
	// it answers when a session made several calls.
	if request.Correlation == nil || result.Correlation == nil ||
		*request.Correlation != *result.Correlation {
		t.Fatalf("got %v and %v", request.Correlation, result.Correlation)
	}
	if *request.Correlation != call.Correlation() {
		t.Fatal("the call names the correlation it wrote under")
	}
	if request.Operation == nil || *request.Operation != wire.OperationChat {
		t.Fatalf("a model call with no operation is a chat, got %v", request.Operation)
	}
	// The request is marked recorded, so the agent's own runtime never takes
	// its own timeline entry as new work addressed to it.
	if value, carried := metadata(t, request, wire.MetadataRecorded); !carried ||
		value.Kind != wire.ValueBool || !value.Bool {
		t.Fatalf("got %+v %v", value, carried)
	}
	if value, _ := metadata(t, request, wire.MetadataRequestModel); value.Str != "a-model" {
		t.Fatalf("got %q", value.Str)
	}
	if value, _ := metadata(t, request, wire.MetadataProviderName); value.Str != "a-provider" {
		t.Fatalf("got %q", value.Str)
	}
	// The model that answered can differ from the one asked for, so both are
	// recorded rather than one standing for the other.
	if value, _ := metadata(t, result, wire.MetadataResponseModel); value.Str != "a-model-0613" {
		t.Fatalf("got %q", value.Str)
	}
	if value, _ := metadata(t, result, wire.MetadataDurationMicros); numeric(t, value) != 250_000 {
		t.Fatalf("got %+v", value)
	}
	if result.FinishReason == nil || *result.FinishReason != "stop" {
		t.Fatalf("got %v", result.FinishReason)
	}
	if result.Usage == nil || result.Usage.InputTokens != 11 ||
		result.Usage.OutputTokens != 22 {
		t.Fatalf("got %+v", result.Usage)
	}
}

func TestGivenAModelCallWithNoDurationWhenCompletedThenShouldMeasureIt(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	call, err := session.Model(context.Background(),
		laser.ModelRequest{Model: "a-model", Body: []byte("hi")}, nil)
	if err != nil {
		t.Fatalf("model: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, err := call.Complete(context.Background(),
		laser.ModelResponse{Body: []byte("ok")}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// The application asked this handle to hold the call, so the elapsed
	// time is the only measurement there is.
	result := recorded(t, server)[1]
	value, carried := metadata(t, result, wire.MetadataDurationMicros)
	if !carried || numeric(t, value) == 0 {
		t.Fatalf("got %+v %v", value, carried)
	}
}

func TestGivenARecordedModelCallWithNoDurationThenShouldRecordZero(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	// A call that already happened was not timed by this handle, so zero is
	// the honest answer rather than the time since this call was made.
	if _, err := session.RecordModelCall(context.Background(),
		laser.ModelRequest{Model: "a-model", Body: []byte("hi")},
		laser.ModelResponse{Body: []byte("ok")}, nil); err != nil {
		t.Fatalf("record: %v", err)
	}

	result := recorded(t, server)[1]
	if value, _ := metadata(t, result, wire.MetadataDurationMicros); numeric(t, value) != 0 {
		t.Fatalf("got %+v", value)
	}
}

func TestGivenAModelCallWithAContextWhenRecordedThenShouldWriteItsManifest(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	assembled := &laser.AssembledContext{
		Manifest: wire.ContextManifest{Policy: "last_n(3)", Tokens: 42},
	}
	call, err := session.Model(context.Background(),
		laser.ModelRequest{Model: "a-model", Body: []byte("hi")}, assembled)
	if err != nil {
		t.Fatalf("model: %v", err)
	}

	written := recorded(t, server)
	if len(written) != 2 {
		t.Fatalf("want the request and its manifest, got %d", len(written))
	}
	manifest := written[1]
	if manifest.Operation == nil ||
		*manifest.Operation != wire.OperationContextAssembled {
		t.Fatalf("got %v", manifest.Operation)
	}
	// The manifest carries the call's correlation, so a reader can tell
	// which prompt this context was assembled for.
	if manifest.Correlation == nil || *manifest.Correlation != call.Correlation() {
		t.Fatalf("got %v", manifest.Correlation)
	}
	var decoded wire.ContextManifest
	if err := wire.Unmarshal(manifest.Body, &decoded); err != nil {
		t.Fatalf("decode the manifest: %v", err)
	}
	if decoded.Policy != "last_n(3)" || decoded.Tokens != 42 {
		t.Fatalf("got %+v", decoded)
	}
	if decoded.Correlation == nil || *decoded.Correlation != call.Correlation() {
		t.Fatalf("the manifest body must name the call, got %v", decoded.Correlation)
	}
}

func TestGivenAnAssembledContextWhenRenderedThenShouldJoinItsFragments(t *testing.T) {
	assembled := laser.AssembledContext{Fragments: []laser.ContextMessage{
		{Envelope: &wire.AgentEnvelope{Body: []byte("first")}},
		{Envelope: &wire.AgentEnvelope{Body: []byte("second")}},
	}}
	if got := assembled.Text(); got != "first\nsecond" {
		t.Fatalf("got %q", got)
	}
	if got := (laser.AssembledContext{}).Text(); got != "" {
		t.Fatalf("got %q", got)
	}
}

func TestGivenAToolCallWhenRecordedThenShouldNameTheToolOnBothRecords(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	call, err := session.Tool(context.Background(), "search",
		document("query", text("status")))
	if err != nil {
		t.Fatalf("tool: %v", err)
	}
	if _, err := call.Complete(context.Background(), []byte("found")); err != nil {
		t.Fatalf("complete: %v", err)
	}

	written := recorded(t, server)
	if len(written) != 2 {
		t.Fatalf("got %d records", len(written))
	}
	for index, envelope := range written {
		if envelope.Tool == nil || *envelope.Tool != "search" {
			t.Fatalf("record %d names tool %v", index, envelope.Tool)
		}
		if envelope.Operation == nil ||
			*envelope.Operation != wire.OperationExecuteTool {
			t.Fatalf("record %d names operation %v", index, envelope.Operation)
		}
	}
	if *written[0].Correlation != call.Correlation() {
		t.Fatal("the call names the correlation it wrote under")
	}
	// Marked recorded, so the agent's own runtime never picks its own
	// timeline entry up as a tool call addressed to it.
	if value, carried := metadata(t, written[0], wire.MetadataRecorded); !carried ||
		value.Kind != wire.ValueBool || !value.Bool {
		t.Fatalf("got %+v %v", value, carried)
	}
	if string(written[1].Body) != "found" {
		t.Fatalf("got %q", written[1].Body)
	}
}

func TestGivenSecretArgumentsWhenRecordedThenShouldDropTheirValues(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	// A tool's arguments land on the log, where anyone reading the session
	// sees them. The common secret-bearing keys are dropped by name.
	nested := wire.NewDynValue(cborform.Map{
		{Key: cborform.Text("api_key"), Value: cborform.Text("k")},
		{Key: cborform.Text("keep"), Value: cborform.Text("v")},
	})
	args := wire.NewDynValue(cborform.Map{
		{Key: cborform.Text("query"), Value: cborform.Text("status")},
		{Key: cborform.Text("Authorization"), Value: cborform.Text("Bearer x")},
		{Key: cborform.Text("nested"), Value: nested.Item()},
		{Key: cborform.Text("list"), Value: cborform.Array{cborform.Map{
			{Key: cborform.Text("password"), Value: cborform.Text("p")},
		}}},
	})
	if _, err := session.Tool(context.Background(), "search", args); err != nil {
		t.Fatalf("tool: %v", err)
	}

	body := recorded(t, server)[0].Body
	for _, secret := range []string{"Bearer x", `"k"`, `"p"`} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("the recorded arguments still carry %s: %s", secret, body)
		}
	}
	// Only the values go. The keys stay, so a reader still sees what was
	// passed, and the values that are not secret are untouched.
	for _, kept := range []string{"status", `"keep"`, `"v"`, "Authorization", "api_key"} {
		if !strings.Contains(string(body), kept) {
			t.Fatalf("the recorded arguments lost %s: %s", kept, body)
		}
	}
	if strings.Count(string(body), laser.RedactedPlaceholder) != 3 {
		t.Fatalf("want three redactions, got %s", body)
	}
}

func TestGivenAJSONPromptWhenRecordedThenShouldRedactIt(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	if _, err := session.Model(context.Background(), laser.ModelRequest{
		Model: "a-model", Body: []byte(`{"prompt":"hi","api_key":"k"}`),
	}, nil); err != nil {
		t.Fatalf("model: %v", err)
	}

	body := string(recorded(t, server)[0].Body)
	if strings.Contains(body, `"k"`) {
		t.Fatalf("the recorded prompt still carries the key: %s", body)
	}
	if !strings.Contains(body, "hi") {
		t.Fatalf("the prompt itself must stand: %s", body)
	}
}

func TestGivenAnOpaquePromptWhenRecordedThenShouldLeaveItAlone(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	// A body that is not JSON cannot be read key by key, so this client
	// cannot tell a secret from the rest of it and records it as it stands.
	if _, err := session.Model(context.Background(), laser.ModelRequest{
		Model: "a-model", Body: []byte("not json at all"),
	}, nil); err != nil {
		t.Fatalf("model: %v", err)
	}
	if body := string(recorded(t, server)[0].Body); body != "not json at all" {
		t.Fatalf("got %q", body)
	}
}

func TestGivenAnotherRedactorWhenSetThenShouldUseIt(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	// A caller that redacts upstream, or differently, replaces the default
	// rather than having both applied.
	plain := session.Redact(nil)
	if _, err := plain.Tool(context.Background(), "search",
		document("api_key", text("k"))); err != nil {
		t.Fatalf("tool: %v", err)
	}
	if body := string(recorded(t, server)[0].Body); !strings.Contains(body, `"k"`) {
		t.Fatalf("a nil redactor records the arguments as they stand: %s", body)
	}

	everything := session.Redact(func(wire.DynValue) wire.DynValue {
		return text("gone")
	})
	if _, err := everything.Tool(context.Background(), "search",
		document("query", text("status"))); err != nil {
		t.Fatalf("tool: %v", err)
	}
	if body := string(recorded(t, server)[1].Body); !strings.Contains(body, "gone") {
		t.Fatalf("got %s", body)
	}
	// Deriving a handle does not change the one it came from.
	if _, err := session.Tool(context.Background(), "search",
		document("api_key", text("k"))); err != nil {
		t.Fatalf("tool: %v", err)
	}
	body := string(recorded(t, server)[2].Body)
	if strings.Contains(body, `"k"`) {
		t.Fatalf("the original handle must still redact: %s", body)
	}
	// Still the default redactor rather than either derived one: the key
	// name survives and only its value is dropped.
	if !strings.Contains(body, `"api_key"`) ||
		!strings.Contains(body, laser.RedactedPlaceholder) {
		t.Fatalf("the original handle must still redact by default: %s", body)
	}
}

func TestGivenAFailedCallWhenRecordedThenShouldCarryTheStructuredError(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)
	message := "the provider timed out"
	failure := wire.AgentErrorBody{
		Code: wire.ErrorToolFailure, Message: &message, Retryable: true,
	}

	call, err := session.Model(context.Background(),
		laser.ModelRequest{Model: "a-model", Body: []byte("hi")}, nil)
	if err != nil {
		t.Fatalf("model: %v", err)
	}
	if _, err := call.Fail(context.Background(), failure); err != nil {
		t.Fatalf("fail: %v", err)
	}
	tool, err := session.Tool(context.Background(), "search", document())
	if err != nil {
		t.Fatalf("tool: %v", err)
	}
	if _, err := tool.Fail(context.Background(), failure); err != nil {
		t.Fatalf("fail: %v", err)
	}

	written := recorded(t, server)
	modelFailure, toolFailure := written[1], written[3]
	for index, envelope := range []wire.AgentEnvelope{modelFailure, toolFailure} {
		if envelope.Kind != wire.KindError {
			t.Fatalf("failure %d is %q", index, envelope.Kind)
		}
		var decoded wire.AgentErrorBody
		if err := wire.Unmarshal(envelope.Body, &decoded); err != nil {
			t.Fatalf("decode failure %d: %v", index, err)
		}
		// A classified code is what a reader acts on. A message it would
		// have to parse is not.
		if decoded.Code != wire.ErrorToolFailure || !decoded.Retryable {
			t.Fatalf("failure %d decoded as %+v", index, decoded)
		}
	}
	if toolFailure.Tool == nil || *toolFailure.Tool != "search" {
		t.Fatalf("a failed tool call names its tool, got %v", toolFailure.Tool)
	}
	if modelFailure.Tool != nil {
		t.Fatalf("a model call names no tool, got %v", modelFailure.Tool)
	}
}

func TestGivenAnIncompleteCallWhenRecordedThenShouldRefuseIt(t *testing.T) {
	server := &stateServer{}
	session := stateSession(t, server)

	if _, err := session.Model(context.Background(),
		laser.ModelRequest{Body: []byte("hi")}, nil); err == nil {
		t.Fatal("a model call names the model it asked")
	}
	if _, err := session.Tool(context.Background(), "", document()); err == nil {
		t.Fatal("a tool call names its tool")
	}
	if server.written() != 0 {
		t.Fatal("nothing must be written")
	}
}

func TestGivenNoAgentWhenACallIsRecordedThenShouldSaySo(t *testing.T) {
	server := &stateServer{}
	server.session = conversation(1)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	session := handle.Sessions().Open(server.session, "")

	if _, err := session.Model(context.Background(),
		laser.ModelRequest{Model: "a-model", Body: []byte("hi")}, nil); err == nil {
		t.Fatal("a session writes as an agent")
	}
	if _, err := session.Tool(context.Background(), "search", document()); err == nil {
		t.Fatal("a session writes as an agent")
	}
}
