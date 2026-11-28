package laser_test

import (
	"context"
	"encoding/json"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenACollectionOfBodiesWhenExtendingABatchThenShouldAddOneRecordPerBodyInOrder(t *testing.T) {
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "agents"}).Topic("readings")
	batch := topic.PublishBatch()
	if !batch.IsEmpty() {
		t.Fatal("a new batch holds no record")
	}

	batch.ExtendJSON(map[string]int{"cpu": 1}, map[string]int{"cpu": 2}).
		ExtendMsgpack(map[string]int{"cpu": 3}).
		ExtendEncoded(wire.CBORCodec, map[string]int{"cpu": 4}, map[string]int{"cpu": 5})

	if batch.IsEmpty() || batch.Len() != 5 {
		t.Fatalf("want five records queued, got %d", batch.Len())
	}
	if _, err := batch.Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(server.sentMessages) != 5 {
		t.Fatalf("want five records sent, got %d", len(server.sentMessages))
	}
	var first, second map[string]int
	if json.Unmarshal(server.sentMessages[0].Payload, &first) != nil || json.Unmarshal(server.sentMessages[1].Payload, &second) != nil ||
		first["cpu"] != 1 || second["cpu"] != 2 {
		t.Fatalf("want the JSON bodies in order, got %s and %s", server.sentMessages[0].Payload, server.sentMessages[1].Payload)
	}
	want := []wire.ContentType{wire.ContentJSON, wire.ContentJSON, wire.ContentMsgpack, wire.ContentCBOR, wire.ContentCBOR}
	for index, message := range server.sentMessages {
		if kind := batchContentType(t, message); kind != want[index] {
			t.Fatalf("record %d: want content type %v, got %v", index, want[index], kind)
		}
	}
}

func TestGivenABodyThatDoesNotEncodeWhenExtendingABatchThenShouldSendNothing(t *testing.T) {
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "agents"}).Topic("readings")

	_, err := topic.PublishBatch().ExtendJSON(map[string]int{"cpu": 1}, make(chan int)).Send(context.Background())

	if err == nil || len(server.sentMessages) != 0 {
		t.Fatalf("want the batch refused with nothing sent, got %v and %d records", err, len(server.sentMessages))
	}
}

func batchContentType(t *testing.T, message iggcon.IggyMessage) wire.ContentType {
	t.Helper()
	headers, err := iggcon.DeserializeHeaders(message.UserHeaders)
	if err != nil {
		t.Fatalf("read the headers: %v", err)
	}
	for _, header := range headers {
		if string(header.Key.Value) == wire.HeaderContentType {
			kind, known := wire.ContentTypeFromCode(header.Value.Value[0])
			if !known {
				t.Fatalf("unknown content type code %d", header.Value.Value[0])
			}
			return kind
		}
	}
	t.Fatal("the record carries no content type")
	return ""
}
