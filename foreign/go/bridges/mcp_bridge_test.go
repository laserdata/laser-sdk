package bridges_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func mcpBridgeOver(t *testing.T, log iggcon.Client) *bridges.MCPBridge {
	t.Helper()
	handle := laser.FromClient(log, laser.Options{DefaultStream: "agents"})
	return bridges.NewMCPBridge(handle, agent(t, "mcp-edge"), "agent.sessions", "agent.sessions", "tools")
}

// replyingLog answers every published command with a correlated response on
// the next poll, so CallTool's wait has something to find without a real
// agent on the other end.
type replyingLog struct {
	bridgeLog
	source wire.AgentID
}

func (log *replyingLog) SendMessages(
	ctx context.Context, stream, topic iggcon.Identifier,
	partitioning iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	response, err := log.bridgeLog.SendMessages(ctx, stream, topic, partitioning, messages)
	if err != nil {
		return nil, err
	}
	for _, message := range messages {
		var command wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &command); err != nil || command.Correlation == nil {
			continue
		}
		replyRecord := wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())}
		reply := wire.AgentEnvelope{
			Kind:         wire.KindResponse,
			Record:       &replyRecord,
			Conversation: command.Conversation,
			Source:       log.source,
			Target:       &command.Source,
			Correlation:  command.Correlation,
			Body:         []byte("42"),
		}
		payload, err := wire.Marshal(&reply)
		if err != nil {
			continue
		}
		headers, err := laser.EncodeHeaderBlock(wire.HeadersForEnvelope(&reply, wire.ContentJSON, false))
		if err != nil {
			continue
		}
		replyMessage, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
		if err != nil {
			continue
		}
		if _, err := log.bridgeLog.SendMessages(ctx, stream, topic, partitioning,
			[]iggcon.IggyMessage{replyMessage}); err != nil {
			continue
		}
	}
	return response, nil
}

func withTool(t *testing.T, bridge *bridges.MCPBridge, name string) *bridges.MCPBridge {
	t.Helper()
	bridge, err := bridge.WithTool(name, "", map[string]any{"type": "object"})
	if err != nil {
		t.Fatalf("with tool: %v", err)
	}
	return bridge
}

func TestGivenAnAdvertisedToolWhenCalledThenShouldPublishParamsByteIdentical(t *testing.T) {
	log := &replyingLog{source: agent(t, "worker")}
	bridge := withTool(t, mcpBridgeOver(t, log), "search")
	params := []byte(`{"name":"search","arguments":{"q":"laser"}}`)

	result, err := bridge.CallTool(context.Background(), "search", params)
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if result.IsError {
		t.Fatalf("want a successful result, got %+v", result)
	}
	if len(result.Content) != 1 || result.Content[0].Text != "42" {
		t.Errorf("want the reply body as content, got %+v", result.Content)
	}

	stored := log.stored()
	if len(stored) == 0 {
		t.Fatal("want at least one published record")
	}
	var command wire.AgentEnvelope
	if err := wire.Unmarshal(stored[0].Payload, &command); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	if string(command.Body) != string(params) {
		t.Errorf("want the params byte-identical, got %q", command.Body)
	}
	if command.Tool == nil || *command.Tool != "search" {
		t.Errorf("want the tool named, got %v", command.Tool)
	}
}

func TestGivenNoAnswerWhenCalledThenShouldTimeOut(t *testing.T) {
	bridge := withTool(t, mcpBridgeOver(t, &bridgeLog{}), "search").
		WithTimeout(50 * time.Millisecond)

	_, err := bridge.CallTool(context.Background(), "search", []byte(`{"name":"search"}`))

	var timeout *laser.TimeoutError
	if !asError(err, &timeout) {
		t.Fatalf("want a timeout, got %T: %v", err, err)
	}
}

func TestGivenInitializeWhenCalledThenShouldAnswerTheSupportedVersion(t *testing.T) {
	bridge := mcpBridgeOver(t, &bridgeLog{})

	result := bridge.Initialize("2025-06-18")
	if result["protocolVersion"] != "2025-06-18" {
		t.Errorf("want the asked version echoed, got %v", result["protocolVersion"])
	}

	fallback := bridge.Initialize("1999-01-01")
	if fallback["protocolVersion"] != "2025-11-25" {
		t.Errorf("want the latest version served, got %v", fallback["protocolVersion"])
	}
}

func TestGivenResourcesAndPromptsWhenInitializedThenShouldAdvertiseThem(t *testing.T) {
	bridge := mcpBridgeOver(t, &bridgeLog{}).
		WithResource("file:///readme", "readme", "", "hello").
		WithPrompt(bridges.MCPPrompt{Name: "greet"}, [][2]string{{"user", "hi"}})

	capabilities, ok := bridge.Initialize("")["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("want a capabilities object, got %+v", bridge.Initialize(""))
	}
	if _, has := capabilities["resources"]; !has {
		t.Error("want resources advertised")
	}
	if _, has := capabilities["prompts"]; !has {
		t.Error("want prompts advertised")
	}
}

func rpcDispatch(t *testing.T, bridge *bridges.MCPBridge, body string) (bridges.JSONRPCResponse, bool) {
	t.Helper()
	return bridge.HandleRPC(context.Background(), []byte(body))
}

func TestGivenACatalogRequestWhenDispatchedThenShouldWorkWithoutHTTP(t *testing.T) {
	bridge := mcpBridgeOver(t, &bridgeLog{})

	answer, answered := rpcDispatch(t, bridge, `{"id":"init","method":"initialize"}`)
	if !answered {
		t.Fatal("want an answer")
	}
	if answer.Error != nil {
		t.Fatalf("want a result, got %+v", answer.Error)
	}
	result, ok := answer.Result.(map[string]any)
	if !ok || result["protocolVersion"] != "2025-11-25" {
		t.Fatalf("want the default protocol version, got %+v", answer.Result)
	}

	list, answered := rpcDispatch(t, bridge, `{"id":null,"method":"tools/list","params":null}`)
	if !answered {
		t.Fatal("want an answer")
	}
	if string(list.ID) != "null" {
		t.Errorf("want the null id echoed, got %s", list.ID)
	}

	_, answered = rpcDispatch(t, bridge, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	if answered {
		t.Error("a notification is never answered")
	}
}

func TestGivenMalformedRequestsWhenDispatchedThenShouldEchoIDsAndReturnPublicErrors(t *testing.T) {
	bridge := mcpBridgeOver(t, &bridgeLog{})
	cases := []struct {
		body string
		code int
	}{
		{"null", bridges.CodeInvalidRequest},
		{"[]", bridges.CodeInvalidRequest},
		{`[7,"tools/list",null]`, bridges.CodeInvalidRequest},
		{`{"id":7}`, bridges.CodeMethodNotFound},
		{`{"id":7,"method":"unknown"}`, bridges.CodeMethodNotFound},
		{`{"id":7,"method":"initialize","params":{"protocolVersion":null}}`, bridges.CodeInvalidParams},
		{`{"id":7,"method":"tools/list","params":[]}`, bridges.CodeInvalidParams},
		{`{"id":7,"method":"resources/read"}`, bridges.CodeInvalidParams},
		{`{"id":7,"method":"prompts/get","params":{"name":12}}`, bridges.CodeInvalidParams},
		{`{"id":7,"method":"tools/call","params":{"name":false}}`, bridges.CodeInvalidParams},
	}
	for _, testCase := range cases {
		answer, answered := rpcDispatch(t, bridge, testCase.body)
		if !answered {
			t.Fatalf("%s: want an answer", testCase.body)
		}
		if answer.Error == nil || answer.Error.Code != testCase.code {
			t.Errorf("%s: want code %d, got %+v", testCase.body, testCase.code, answer.Error)
		}
	}
}

func TestGivenProtocolDetailsWhenDispatchedThenShouldFollowTheMCPSchema(t *testing.T) {
	bridge := mcpBridgeOver(t, &bridgeLog{}).
		WithPrompt(bridges.MCPPrompt{Name: "greet"}, [][2]string{{"user", "hello"}})
	bridge = withTool(t, bridge, "search")

	ping, _ := rpcDispatch(t, bridge, `{"id":1,"method":"ping"}`)
	if result, ok := ping.Result.(map[string]any); !ok || len(result) != 0 {
		t.Errorf("want an empty result, got %+v", ping.Result)
	}

	prompt, _ := rpcDispatch(t, bridge, `{"id":2,"method":"prompts/get","params":{"name":"greet"}}`)
	result, ok := prompt.Result.(map[string]any)
	if !ok {
		t.Fatalf("want a result, got %+v", prompt)
	}
	if _, has := result["description"]; has {
		t.Error("a missing description must be omitted, never present")
	}

	for _, testCase := range []struct {
		body string
		code int
	}{
		{`{"id":4,"method":"unknown"}`, bridges.CodeMethodNotFound},
		{`{"id":5,"method":"tools/call","params":{"name":"nope"}}`, bridges.CodeInvalidParams},
		{`{"id":6,"method":"prompts/get","params":{"name":"nope"}}`, bridges.CodeInvalidParams},
		{`{"id":7,"method":"resources/read","params":{"uri":"file:///nope"}}`, bridges.CodeResourceNotFound},
	} {
		answer, _ := rpcDispatch(t, bridge, testCase.body)
		if answer.Error == nil || answer.Error.Code != testCase.code {
			t.Errorf("%s: want code %d, got %+v", testCase.body, testCase.code, answer.Error)
		}
	}
}

func TestGivenAnInboundHopPathHoldingThisBridgeWhenCalledThenShouldRefuseTheLoop(t *testing.T) {
	bridge := withTool(t, mcpBridgeOver(t, &bridgeLog{}), "search")

	body := `{"id":8,"method":"tools/call","params":{"name":"search",` +
		`"_meta":{"laserdata.com/bridge_hops":["a2a-edge","mcp-edge"]}}}`
	answer, _ := rpcDispatch(t, bridge, body)

	if answer.Error == nil || answer.Error.Code != bridges.CodeInvalidParams {
		t.Fatalf("want the loop refused as invalid params, got %+v", answer.Error)
	}
}

func TestGivenANewBridgeWhenBuiltThenShouldStampItsOwnIDAsTheHopPath(t *testing.T) {
	bridge, err := mcpBridgeOver(t, &bridgeLog{}).WithBridgeHops(nil)
	if err != nil {
		t.Fatalf("with bridge hops: %v", err)
	}
	_ = bridge
}

func TestGivenAnUpstreamHopPathWhenContinuedThenShouldAppendThisBridge(t *testing.T) {
	bridge, err := mcpBridgeOver(t, &bridgeLog{}).WithBridgeHops([]string{"a2a-edge"})
	if err != nil {
		t.Fatalf("with bridge hops: %v", err)
	}
	if _, err := bridge.WithBridgeHops([]string{"mcp-edge"}); err == nil {
		t.Error("a path already holding this bridge must be refused")
	}
}

func TestGivenAnEmptyInputSchemaWhenAdvertisedThenShouldRefuseIt(t *testing.T) {
	_, err := mcpBridgeOver(t, &bridgeLog{}).WithTool("search", "", "not an object")
	if err == nil {
		t.Error("a non-object input schema must be refused")
	}
}

var _ = json.Marshal
