package bridges

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// The JSON-RPC 2.0 envelope and the codes this bridge answers with. The
// first four are the protocol's own and the last three are A2A's.
const (
	JSONRPCVersion = "2.0"

	// CodeInvalidRequest is a request that is not a JSON-RPC call.
	CodeInvalidRequest = -32600
	// CodeMethodNotFound is a method this bridge does not serve.
	CodeMethodNotFound = -32601
	// CodeInvalidParams is a call whose params do not fit its method.
	CodeInvalidParams = -32602
	// CodeInternalError is a failure on this side of the call.
	CodeInternalError = -32603
	// CodeTaskNotFound is an id no task answers to.
	CodeTaskNotFound = -32001
	// CodeTaskNotCancelable is a task that already ended.
	CodeTaskNotCancelable = -32002
	// CodeUnsupportedOperation is a method served by the protocol but not by
	// this bridge.
	CodeUnsupportedOperation = -32004
)

// JSONRPCRequest is one inbound call.
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// JSONRPCError is a refusal with its code.
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// JSONRPCResponse is one answer, carrying either a result or an error.
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *JSONRPCError   `json:"error,omitempty"`
}

// a2aFault is a refusal on its way to a JSON-RPC error.
type a2aFault struct {
	code    int
	message string
}

func (fault *a2aFault) Error() string { return fault.message }

func faultOf(code int, message string) *a2aFault {
	return &a2aFault{code: code, message: message}
}

// HandleRPC answers one JSON-RPC request without any HTTP around it.
//
// Every refusal comes back as a JSON-RPC error rather than as a Go error:
// a caller on the other side of a transport is owed an answer in the
// protocol it spoke, and an HTTP shell over this is then a shell rather than
// a second place where faults are decided.
func (bridge *A2ABridge) HandleRPC(
	ctx context.Context, request []byte,
) JSONRPCResponse {
	return bridge.handleRPCFor(ctx, request, "")
}

func (bridge *A2ABridge) handleRPCFor(
	ctx context.Context, request []byte, caller string,
) JSONRPCResponse {
	id, result, fault := bridge.dispatch(ctx, request, caller)
	answer := JSONRPCResponse{JSONRPC: JSONRPCVersion, ID: id}
	if fault != nil {
		answer.Error = &JSONRPCError{Code: fault.code, Message: fault.message}
		return answer
	}
	answer.Result = result
	return answer
}

// dispatch runs one call, returning its id, its result and the fault that
// replaced it.
func (bridge *A2ABridge) dispatch(
	ctx context.Context, request []byte, caller string,
) (json.RawMessage, any, *a2aFault) {
	// The id is read first and separately, so a request this bridge refuses
	// is still answered under the id its caller used.
	var envelope JSONRPCRequest
	if err := json.Unmarshal(request, &envelope); err != nil {
		return nil, nil, faultOf(CodeInvalidRequest, "invalid request")
	}
	id := envelope.ID
	if !isJSONObject(request) {
		return id, nil, faultOf(CodeInvalidRequest, "invalid request")
	}
	params := envelope.Params
	if len(params) == 0 || string(params) == "null" {
		params = json.RawMessage("{}")
	}
	if !isJSONObject(params) {
		return id, nil, faultOf(CodeInvalidParams, "invalid params")
	}
	switch A2AMethod(envelope.Method) {
	case A2AMessageSend:
		task, fault := bridge.sendMessage(ctx, params, caller)
		if fault != nil {
			return id, nil, fault
		}
		return id, map[string]any{"task": task}, nil
	case A2AMessageStream:
		return id, nil, faultOf(CodeUnsupportedOperation, "streaming is not supported")
	case A2ATasksGet, A2ATasksCancel:
		task, fault := bridge.taskCall(ctx, A2AMethod(envelope.Method), params, caller)
		if fault != nil {
			return id, nil, fault
		}
		return id, task, nil
	default:
		return id, nil, faultOf(CodeMethodNotFound, "method not found")
	}
}

// taskCall serves GetTask and CancelTask, which differ only in what they do
// once the task is read.
func (bridge *A2ABridge) taskCall(
	ctx context.Context, method A2AMethod, params json.RawMessage, caller string,
) (*A2ATask, *a2aFault) {
	var named struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(params, &named); err != nil || named.ID == "" {
		return nil, faultOf(CodeInvalidParams, "invalid params")
	}
	conversation, err := bridge.openTaskID(ctx, named.ID, caller)
	if err != nil {
		// An id this caller cannot open names no task it can answer for,
		// which is what keeps one caller's ids useless to another.
		return nil, faultOf(CodeTaskNotFound, "task not found")
	}
	view, err := bridge.view(ctx, conversation, named.ID)
	if err != nil {
		return nil, internalFault(err)
	}
	if view == nil {
		return nil, faultOf(CodeTaskNotFound, "task not found")
	}
	if method == A2ATasksGet {
		return view.task, nil
	}
	if view.terminal {
		return nil, faultOf(CodeTaskNotCancelable, "task cannot be canceled")
	}
	canceled, err := bridge.publishCancel(ctx, conversation, named.ID, view.task.ContextID)
	if err != nil {
		return nil, internalFault(err)
	}
	return canceled, nil
}

// sendMessage starts a task, or continues the one a message names.
//
// A message naming a task rides its conversation with the same correlation
// and the original target, so the agent already handling it sees the
// continuation rather than a second task nobody connected to the first.
func (bridge *A2ABridge) sendMessage(
	ctx context.Context, params json.RawMessage, caller string,
) (*A2ATask, *a2aFault) {
	var call struct {
		Message  json.RawMessage `json:"message"`
		Metadata json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(params, &call); err != nil {
		return nil, faultOf(CodeInvalidParams, "invalid params")
	}
	if !isJSONObject(call.Message) {
		return nil, faultOf(CodeInvalidParams, "invalid params")
	}
	previous, err := InboundHops(call.Metadata)
	if err != nil {
		return nil, faultOf(CodeInvalidParams, "invalid params")
	}
	hops, err := CallHops(bridge.hops, previous)
	if err != nil {
		return nil, faultOf(CodeInvalidParams, err.Error())
	}
	var named struct {
		TaskID *string `json:"taskId"`
	}
	if err := json.Unmarshal(call.Message, &named); err != nil {
		return nil, faultOf(CodeInvalidParams, "invalid params")
	}
	if named.TaskID == nil {
		task, err := bridge.submitFor(ctx, params, bridge.target, hops, caller)
		if err != nil {
			return nil, internalFault(err)
		}
		return task, nil
	}
	return bridge.continueTask(ctx, *named.TaskID, params, hops, caller)
}

// continueTask adds a message to a task already running.
func (bridge *A2ABridge) continueTask(
	ctx context.Context, id string, params json.RawMessage, hops []string, caller string,
) (*A2ATask, *a2aFault) {
	conversation, err := bridge.openTaskID(ctx, id, caller)
	if err != nil {
		return nil, faultOf(CodeTaskNotFound, "task not found")
	}
	view, err := bridge.view(ctx, conversation, id)
	if err != nil {
		return nil, internalFault(err)
	}
	if view == nil {
		return nil, faultOf(CodeTaskNotFound, "task not found")
	}
	if view.terminal {
		return nil, faultOf(CodeUnsupportedOperation,
			"the task ended and takes no further message")
	}
	// The continuation goes to whoever the task went to, so a shared lane
	// does not hand it to a second agent.
	target := bridge.target
	if view.command != nil && view.command.Target != nil {
		target = view.command.Target
	}
	if err := bridge.publishCommand(ctx, conversation, params, target, hops, nil, nil); err != nil {
		return nil, internalFault(err)
	}
	return &A2ATask{ID: id, ContextID: view.task.ContextID,
		Status: A2ATaskStatus{State: a2aWorking}}, nil
}

// internalFault reports a failure of this bridge rather than of the request.
//
// A refusal the caller caused keeps its own code: an invalid argument is the
// caller's to fix, and reporting it as an internal error would send them
// looking in the wrong place.
func internalFault(err error) *a2aFault {
	var invalid *BridgeError
	if errors.As(err, &invalid) {
		return faultOf(CodeInvalidParams, invalid.Reason)
	}
	var rejected *A2ARejectedError
	if errors.As(err, &rejected) {
		return faultOf(CodeTaskNotCancelable, rejected.Reason)
	}
	return faultOf(CodeInternalError, fmt.Sprintf("the bridge could not serve the request: %v", err))
}

// isJSONObject reports a value that is a JSON object, which several of these
// shapes require: a JSON array also decodes into a struct, so a check on the
// decode alone would accept one.
func isJSONObject(raw []byte) bool {
	var probe map[string]json.RawMessage
	return len(raw) > 0 && json.Unmarshal(raw, &probe) == nil && probe != nil
}
