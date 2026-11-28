package bridges_test

import (
	"reflect"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAMessageSendWhenMappedThenShouldBeAChatCommandCarryingTheParamsUntouched(t *testing.T) {
	record, conversation, source, correlation := mappingIdentity(t)
	params := []byte(`{"message":{"role":"user","parts":[{"kind":"text","text":"hi"}]},"extension":7}`)

	envelope := bridges.CommandFromMessageSend(record, conversation, source, correlation, params)

	operation := wire.OperationChat
	want := wire.AgentEnvelope{
		Kind: wire.KindCommand, Record: &record, Conversation: conversation, Source: source,
		Correlation: &correlation, Operation: &operation, Body: params,
	}
	if !reflect.DeepEqual(envelope, want) {
		t.Fatalf("got %+v, want %+v", envelope, want)
	}
	params[0] = '['
	if envelope.Body[0] != '{' {
		t.Fatal("the envelope body must not alias the caller's params")
	}
}

func TestGivenAToolCallWhenMappedThenShouldBeACommandNamingTheTool(t *testing.T) {
	record, conversation, source, correlation := mappingIdentity(t)
	params := []byte(`{"name":"search","arguments":{"query":"node-7"},"_meta":{"trace":"abc"}}`)

	envelope := bridges.ToolCallFromRequest(record, conversation, source, correlation, "search", params)

	tool := "search"
	want := wire.AgentEnvelope{
		Kind: wire.KindCommand, Record: &record, Conversation: conversation, Source: source,
		Correlation: &correlation, Tool: &tool, Body: params,
	}
	if !reflect.DeepEqual(envelope, want) {
		t.Fatalf("got %+v, want %+v", envelope, want)
	}
	params[0] = '['
	if envelope.Body[0] != '{' {
		t.Fatal("the envelope body must not alias the caller's params")
	}
}

func mappingIdentity(t *testing.T) (wire.RecordID, wire.ConversationID, wire.AgentID, wire.CorrelationID) {
	t.Helper()
	source, err := wire.NewAgentID("edge")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	id := wire.IDFromBytes(wire.U128{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	return wire.RecordID{ID: id}, wire.ConversationID{ID: id}, source, wire.CorrelationID{ID: id}
}
