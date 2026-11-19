package laser

import (
	"context"
	"strings"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// redactedKeys are the keys whose values DefaultRedact drops.
var redactedKeys = map[string]struct{}{
	"authorization": {}, "api_key": {}, "token": {},
	"password": {}, "secret": {}, "cookie": {},
}

// RedactedPlaceholder is what a redacted value is replaced with.
const RedactedPlaceholder = "[redacted]"

// Redactor rewrites a value before it is recorded.
type Redactor func(wire.DynValue) wire.DynValue

// DefaultRedact drops the values of keys such as authorization, api_key,
// token, password, secret and cookie, at any depth.
//
// It is a convenience and not a guarantee: a secret under another key is
// published as it is. A caller that needs more replaces the redactor.
func DefaultRedact(value wire.DynValue) wire.DynValue {
	return wire.NewDynValue(redactItem(value.Item()))
}

func redactItem(item cborform.Value) cborform.Value {
	switch typed := item.(type) {
	case cborform.Map:
		redacted := make(cborform.Map, 0, len(typed))
		for _, entry := range typed {
			key, text := entry.Key.(cborform.Text)
			if text {
				if _, secret := redactedKeys[strings.ToLower(string(key))]; secret {
					redacted = append(redacted, cborform.Entry{
						Key: entry.Key, Value: cborform.Text(RedactedPlaceholder)})
					continue
				}
			}
			redacted = append(redacted, cborform.Entry{
				Key: entry.Key, Value: redactItem(entry.Value)})
		}
		return redacted
	case cborform.Array:
		redacted := make(cborform.Array, 0, len(typed))
		for _, element := range typed {
			redacted = append(redacted, redactItem(element))
		}
		return redacted
	default:
		return item
	}
}

// Redact derives a session handle that rewrites recorded arguments and
// prompts with redact instead of the default.
//
// A nil redactor records them unchanged, which is what a caller that has
// already redacted upstream wants.
func (session *Session) Redact(redact Redactor) *Session {
	derived := *session
	derived.redactor = redact
	derived.redactorSet = true
	return &derived
}

// redact applies this handle's redactor.
func (session *Session) redact(value wire.DynValue) wire.DynValue {
	switch {
	case !session.redactorSet:
		return DefaultRedact(value)
	case session.redactor == nil:
		return value
	default:
		return session.redactor(value)
	}
}

// ModelRequest is one model call request.
//
// This client never calls a model. The application calls its provider and
// records the call through the session, so the log holds what was asked and
// what came back without the SDK being in the path.
type ModelRequest struct {
	// Model is the model asked for.
	Model string
	// Provider serves the call, empty when the application does not name one.
	Provider string
	// Operation is the model operation. Empty records a chat.
	Operation string
	// Body is the prompt, as the application sent it.
	Body []byte
}

// ModelResponse is one model call result.
type ModelResponse struct {
	Body []byte
	// Model is the model that answered, which can differ from the one asked
	// for.
	Model string
	// FinishReason names why generation stopped, in the OTel vocabulary.
	FinishReason string
	// Usage is the tokens and cost the call spent.
	Usage *wire.TokenUsage
	// Duration is the call's measured time. Left unset, a recorded call
	// carries a duration of zero rather than a guess.
	Duration time.Duration
}

// AssembledContext is the context a model call received, assembled without
// writing anything.
type AssembledContext struct {
	Fragments []ContextMessage
	Manifest  wire.ContextManifest
}

// Text is the fragments' bodies as UTF-8, one per line, the prompt-ready
// form.
func (assembled AssembledContext) Text() string {
	lines := make([]string, 0, len(assembled.Fragments))
	for _, fragment := range assembled.Fragments {
		lines = append(lines, string(fragment.Body()))
	}
	return strings.Join(lines, "\n")
}

// ModelCall is a model call in flight. Finish it with Complete or Fail.
type ModelCall struct {
	session     *Session
	correlation wire.CorrelationID
	operation   string
	started     time.Time
}

// Correlation is the call's correlation, shared by its request, its manifest
// and its result.
func (call *ModelCall) Correlation() wire.CorrelationID { return call.correlation }

// Complete records the model's response.
//
// A response that names no duration is timed from when the call was opened,
// since the application asked this handle to hold the call and the elapsed
// time is the only measurement it has.
func (call *ModelCall) Complete(
	ctx context.Context, response ModelResponse,
) (*AgdxReceipt, error) {
	duration := response.Duration
	if duration == 0 {
		duration = time.Since(call.started)
	}
	return call.session.writeResult(ctx, callResult{
		correlation:  call.correlation,
		operation:    call.operation,
		body:         response.Body,
		usage:        response.Usage,
		duration:     duration,
		model:        response.Model,
		finishReason: response.FinishReason,
	})
}

// Fail records that the call failed.
func (call *ModelCall) Fail(
	ctx context.Context, failure wire.AgentErrorBody,
) (*AgdxReceipt, error) {
	return call.session.writeFailure(
		ctx, call.correlation, call.operation, "", failure)
}

// ToolCall is a tool call in flight. Finish it with Complete or Fail.
type ToolCall struct {
	session     *Session
	correlation wire.CorrelationID
	tool        string
	started     time.Time
}

// Correlation is the call's correlation, shared by its command and its
// result.
func (call *ToolCall) Correlation() wire.CorrelationID { return call.correlation }

// Complete records the tool's result and the measured duration.
func (call *ToolCall) Complete(
	ctx context.Context, result []byte,
) (*AgdxReceipt, error) {
	return call.session.writeResult(ctx, callResult{
		correlation: call.correlation,
		operation:   wire.OperationExecuteTool,
		tool:        call.tool,
		body:        result,
		duration:    time.Since(call.started),
	})
}

// Fail records that the tool failed.
func (call *ToolCall) Fail(
	ctx context.Context, failure wire.AgentErrorBody,
) (*AgdxReceipt, error) {
	return call.session.writeFailure(
		ctx, call.correlation, wire.OperationExecuteTool, call.tool, failure)
}

// Model records a model request addressed to this agent, and the context it
// received when one is given.
//
// The request is marked recorded, so the agent's own runtime never takes it
// as new work: it is a timeline entry, not a command waiting for somebody.
// Finish the returned call when the application's provider answers.
func (session *Session) Model(
	ctx context.Context, request ModelRequest, assembled *AssembledContext,
) (*ModelCall, error) {
	operation := request.Operation
	if operation == "" {
		operation = wire.OperationChat
	}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(NewOperationID())}
	if err := session.writeModelRequest(
		ctx, correlation, request, operation, assembled); err != nil {
		return nil, err
	}
	return &ModelCall{
		session: session, correlation: correlation,
		operation: operation, started: time.Now(),
	}, nil
}

// Tool records a tool call addressed to this agent, with its arguments
// redacted.
func (session *Session) Tool(
	ctx context.Context, name string, args wire.DynValue,
) (*ToolCall, error) {
	if name == "" {
		return nil, &InvalidError{Reason: "a tool call needs the tool's name"}
	}
	lane, err := session.Agdx()
	if err != nil {
		return nil, err
	}
	body, err := wire.MarshalJSON(session.redact(args))
	if err != nil {
		return nil, &CodecError{Reason: "encode the tool arguments", Cause: err}
	}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(NewOperationID())}
	if _, err := lane.Command(correlation, body).
		WithOperation(wire.OperationExecuteTool).
		WithTool(name).
		WithTarget(*session.agent).
		WithMetadata(wire.MetadataRecorded, wire.BoolValue(true)).
		ContentType(wire.ContentJSON).
		Send(ctx); err != nil {
		return nil, err
	}
	return &ToolCall{
		session: session, correlation: correlation,
		tool: name, started: time.Now(),
	}, nil
}

// RecordModelCall records a model call that already happened: the request,
// the context when given, and the response.
//
// A response with no duration records zero rather than a measurement this
// client did not take.
func (session *Session) RecordModelCall(
	ctx context.Context, request ModelRequest, response ModelResponse,
	assembled *AssembledContext,
) (*AgdxReceipt, error) {
	operation := request.Operation
	if operation == "" {
		operation = wire.OperationChat
	}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(NewOperationID())}
	if err := session.writeModelRequest(
		ctx, correlation, request, operation, assembled); err != nil {
		return nil, err
	}
	return session.writeResult(ctx, callResult{
		correlation:  correlation,
		operation:    operation,
		body:         response.Body,
		usage:        response.Usage,
		duration:     response.Duration,
		model:        response.Model,
		finishReason: response.FinishReason,
	})
}

// writeModelRequest records the request and, when one is given, the context
// it was assembled from.
func (session *Session) writeModelRequest(
	ctx context.Context, correlation wire.CorrelationID,
	request ModelRequest, operation string, assembled *AssembledContext,
) error {
	if request.Model == "" {
		return &InvalidError{Reason: "a model call needs the model it asked"}
	}
	lane, err := session.Agdx()
	if err != nil {
		return err
	}
	// A prompt that is JSON is redacted the way tool arguments are. One that
	// is not is recorded as it stands, because this client cannot tell a
	// secret from the rest of an opaque body.
	body := request.Body
	var prompt wire.DynValue
	if wire.UnmarshalJSON(body, &prompt) == nil {
		redacted, err := wire.MarshalJSON(session.redact(prompt))
		if err != nil {
			return &CodecError{Reason: "encode the model request", Cause: err}
		}
		body = redacted
	}
	send := lane.Command(correlation, body).
		WithOperation(operation).
		WithTarget(*session.agent).
		WithMetadata(wire.MetadataRecorded, wire.BoolValue(true)).
		WithMetadata(wire.MetadataRequestModel, wire.StrValue(request.Model))
	if request.Provider != "" {
		send = send.WithMetadata(
			wire.MetadataProviderName, wire.StrValue(request.Provider))
	}
	if _, err := send.Send(ctx); err != nil {
		return err
	}
	if assembled == nil {
		return nil
	}
	manifest := assembled.Manifest
	manifest.Correlation = &correlation
	encoded, err := wire.Marshal(manifest)
	if err != nil {
		return &CodecError{Reason: "encode the context manifest", Cause: err}
	}
	_, err = session.writeCorrelatedEvent(
		ctx, wire.OperationContextAssembled, encoded, correlation)
	return err
}

// callResult is one finished call as it is recorded.
type callResult struct {
	correlation  wire.CorrelationID
	operation    string
	tool         string
	body         []byte
	usage        *wire.TokenUsage
	duration     time.Duration
	model        string
	finishReason string
}

func (session *Session) writeResult(
	ctx context.Context, result callResult,
) (*AgdxReceipt, error) {
	lane, err := session.Agdx()
	if err != nil {
		return nil, err
	}
	micros := result.duration.Microseconds()
	if micros < 0 {
		micros = 0
	}
	send := lane.Respond(result.correlation, result.body).
		WithOperation(result.operation).
		WithTarget(*session.agent).
		WithMetadata(wire.MetadataDurationMicros, wire.UintValue(uint64(micros)))
	if result.tool != "" {
		send = send.WithTool(result.tool)
	}
	if result.usage != nil {
		send = send.WithUsage(*result.usage)
	}
	if result.model != "" {
		send = send.WithMetadata(
			wire.MetadataResponseModel, wire.StrValue(result.model))
	}
	if result.finishReason != "" {
		send = send.WithFinishReason(result.finishReason)
	}
	return send.Send(ctx)
}

func (session *Session) writeFailure(
	ctx context.Context, correlation wire.CorrelationID,
	operation, tool string, failure wire.AgentErrorBody,
) (*AgdxReceipt, error) {
	lane, err := session.Agdx()
	if err != nil {
		return nil, err
	}
	send := lane.Fail(correlation, failure).
		WithOperation(operation).
		WithTarget(*session.agent)
	if tool != "" {
		send = send.WithTool(tool)
	}
	return send.Send(ctx)
}

// writeCorrelatedEvent appends one session event tied to a call.
func (session *Session) writeCorrelatedEvent(
	ctx context.Context, operation string, body []byte,
	correlation wire.CorrelationID,
) (*AgdxReceipt, error) {
	lane, err := session.Agdx()
	if err != nil {
		return nil, err
	}
	return lane.Emit(body).WithOperation(operation).
		WithCorrelation(correlation).
		ContentType(wire.ContentCBOR).Send(ctx)
}
