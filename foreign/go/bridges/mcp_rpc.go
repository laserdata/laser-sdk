package bridges

import (
	"context"
	"encoding/json"
	"errors"
)

// CodeResourceNotFound is a resource or prompt no advertisement names.
const CodeResourceNotFound = -32002

// mcpFault is a refusal on its way to a JSON-RPC error.
type mcpFault struct {
	code    int
	message string
}

func (fault *mcpFault) Error() string { return fault.message }

func mcpFaultOf(code int, message string) *mcpFault {
	return &mcpFault{code: code, message: message}
}

// HandleRPC answers one JSON-RPC message, without any transport around it.
//
// A notification, a message carrying no id, is never answered: the bool
// reports whether there is an answer to send, so the Streamable HTTP router
// above it answers 202 with no body rather than inventing a response.
func (bridge *MCPBridge) HandleRPC(
	ctx context.Context, request []byte,
) (JSONRPCResponse, bool) {
	if isJSONObject(request) {
		var probe JSONRPCRequest
		if json.Unmarshal(request, &probe) == nil && probe.ID == nil {
			return JSONRPCResponse{}, false
		}
	}
	id, result, fault := bridge.dispatch(ctx, request)
	answer := JSONRPCResponse{JSONRPC: JSONRPCVersion, ID: id}
	if fault != nil {
		answer.Error = &JSONRPCError{Code: fault.code, Message: fault.message}
		return answer, true
	}
	answer.Result = result
	return answer, true
}

func (bridge *MCPBridge) dispatch(
	ctx context.Context, request []byte,
) (json.RawMessage, any, *mcpFault) {
	// The id is read first and separately, so a request this bridge refuses
	// is still answered under the id its caller used.
	var envelope JSONRPCRequest
	decodeErr := json.Unmarshal(request, &envelope)
	id := envelope.ID
	if id == nil {
		id = json.RawMessage("null")
	}
	if !isJSONObject(request) || decodeErr != nil {
		return id, nil, mcpFaultOf(CodeInvalidRequest, "invalid request")
	}
	raw := envelope.Params
	if len(raw) == 0 || string(raw) == "null" {
		raw = json.RawMessage("{}")
	}
	params, ok := jsonObjectFields(raw)
	if !ok {
		return id, nil, mcpFaultOf(CodeInvalidParams, "invalid params")
	}
	switch MCPMethod(envelope.Method) {
	case MCPInitialize:
		version, present, valid := optionalString(params, "protocolVersion")
		if present && !valid {
			return id, nil, mcpFaultOf(CodeInvalidParams, "invalid params")
		}
		return id, bridge.Initialize(version), nil
	case MCPPing:
		return id, map[string]any{}, nil
	case MCPToolsList:
		return id, bridge.ListTools(), nil
	case MCPResourcesList:
		return id, bridge.ListResources(), nil
	case MCPResourcesRead:
		uri, ok := requiredString(params, "uri")
		if !ok {
			return id, nil, mcpFaultOf(CodeInvalidParams, "invalid params")
		}
		result, err := bridge.ReadResource(uri)
		if err != nil {
			return id, nil, mcpFaultOf(CodeResourceNotFound, "resource not found")
		}
		return id, result, nil
	case MCPPromptsList:
		return id, bridge.ListPrompts(), nil
	case MCPPromptsGet:
		name, ok := requiredString(params, "name")
		if !ok {
			return id, nil, mcpFaultOf(CodeInvalidParams, "invalid params")
		}
		result, err := bridge.GetPrompt(name)
		if err != nil {
			return id, nil, mcpFaultOf(CodeInvalidParams, "unknown prompt")
		}
		return id, result, nil
	case MCPToolsCall:
		return bridge.dispatchToolsCall(ctx, id, params, raw)
	default:
		return id, nil, mcpFaultOf(CodeMethodNotFound, "method not found")
	}
}

func (bridge *MCPBridge) dispatchToolsCall(
	ctx context.Context, id json.RawMessage,
	params map[string]json.RawMessage, paramsJSON json.RawMessage,
) (json.RawMessage, any, *mcpFault) {
	name, ok := requiredString(params, "name")
	if !ok {
		return id, nil, mcpFaultOf(CodeInvalidParams, "invalid params")
	}
	// Only an advertised tool is published, so a guessed name costs no wait.
	if !bridge.hasTool(name) {
		return id, nil, mcpFaultOf(CodeInvalidParams, "unknown tool")
	}
	previous, err := InboundHops(params["_meta"])
	if err != nil {
		return id, nil, mcpFaultOf(CodeInvalidParams, err.Error())
	}
	// The whole params object tunnels byte-identical as the command body, so
	// an agent reads exactly what the MCP client sent.
	result, err := bridge.CallToolWithHops(ctx, previous, nil, name, paramsJSON)
	if err != nil {
		return id, nil, internalMCPFault(err)
	}
	return id, result, nil
}

// internalMCPFault reports a failure of this bridge or its caller.
//
// A hop loop is the caller's path to fix, so it keeps the invalid-params
// code rather than reading as a fault of this bridge.
func internalMCPFault(err error) *mcpFault {
	var loop *HopError
	if errors.As(err, &loop) {
		return mcpFaultOf(CodeInvalidParams, loop.Reason)
	}
	return mcpFaultOf(CodeInternalError, err.Error())
}

// jsonObjectFields decodes raw as a JSON object's fields, refusing every
// other JSON shape.
func jsonObjectFields(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	var fields map[string]json.RawMessage
	if len(raw) == 0 {
		return nil, false
	}
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, false
	}
	return fields, true
}

// jsonString reads raw as a JSON string, refusing null and every other type.
// Unmarshaling null or a number into a Go string succeeds silently, so the
// leading byte is checked too.
func jsonString(raw json.RawMessage) (string, bool) {
	var value string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

// requiredString reads a required string field, failing when it is absent or
// not a string.
func requiredString(params map[string]json.RawMessage, key string) (string, bool) {
	raw, present := params[key]
	if !present {
		return "", false
	}
	return jsonString(raw)
}

// optionalString reads an optional string field. present reports whether the
// key was there at all, and valid reports whether it was a JSON string when
// it was: an explicit null or another type is refused rather than silently
// defaulted.
func optionalString(
	params map[string]json.RawMessage, key string,
) (value string, present, valid bool) {
	raw, there := params[key]
	if !there {
		return "", false, true
	}
	value, valid = jsonString(raw)
	return value, true, valid
}
