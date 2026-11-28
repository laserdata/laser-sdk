package laser_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenTheSharedJSONSchemaCasesWhenCompiledThenShouldValidateEveryValue(t *testing.T) {
	raw, err := os.ReadFile("../../../wire/fixtures/schema_validation_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name               string
		Schema             json.RawMessage
		Accepted, Rejected []json.RawMessage
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("empty validation corpus")
	}
	for _, sample := range cases {
		t.Run(sample.Name, func(t *testing.T) {
			schema, err := laser.CompileSchema(wire.SchemaDef{ID: 7, Source: wire.JSONSchema(string(sample.Schema))})
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range sample.Accepted {
				if !schema.ValidateValue(value) || !schema.Validate(value) {
					t.Fatalf("rejected %s", value)
				}
			}
			for _, value := range sample.Rejected {
				if schema.ValidateValue(value) || schema.Validate(value) {
					t.Fatalf("accepted %s", value)
				}
			}
		})
	}
}

func TestGivenAnExternalJSONSchemaReferenceWhenCompilingThenShouldRefuseIt(t *testing.T) {
	_, err := laser.CompileSchema(wire.SchemaDef{Source: wire.JSONSchema(`{"$ref":"https://example.invalid/definitions"}`)})
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("external reference: %v", err)
	}
	schema, err := laser.CompileSchema(wire.SchemaDef{Source: wire.JSONSchema(`{"$defs":{"integer":{"type":"integer"}},"$ref":"#/$defs/integer"}`)})
	if err != nil || !schema.Validate([]byte("7")) || schema.Validate([]byte("7 8")) || schema.Validate([]byte{'"', 0xff, '"'}) {
		t.Fatalf("internal reference or JSON parsing failed: %v", err)
	}
}

func TestGivenATypedAvroPublishWhenSentThenShouldValidateAndStampTheWireHeaders(t *testing.T) {
	const id = 400
	def := wire.SchemaDef{ID: id, Source: wire.AvroSchema(`{"type":"record","name":"Reading","fields":[{"name":"n","type":"long"}]}`)}
	schema, err := laser.CompileSchema(def)
	if err != nil {
		t.Fatal(err)
	}
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "telemetry"}).Topic("readings")
	typed := laser.TypedWith[map[string]int64](topic, schema)
	const n = 9_007_199_254_740_993
	if _, err := typed.Publish(map[string]int64{"n": n}).Send(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := iggcon.DeserializeHeaders(server.sentMessages[0].UserHeaders)
	if err != nil {
		t.Fatal(err)
	}
	headers := make(map[string]iggcon.HeaderValue)
	for _, entry := range entries {
		headers[string(entry.Key.Value)] = entry.Value
	}
	kind := headers[wire.HeaderContentType]
	schemaID := headers[wire.HeaderSchemaID]
	code, _ := wire.ContentAvro.Code()
	if kind.Kind != iggcon.Uint8 || !bytes.Equal(kind.Value, []byte{code}) || schemaID.Kind != iggcon.Uint32 || !bytes.Equal(schemaID.Value, binary.LittleEndian.AppendUint32(nil, id)) {
		t.Fatalf("unexpected schema stamps: %+v", headers)
	}
	result, err := typed.Decode(&laser.Record{Message: server.sentMessages[0]})
	if err != nil || result.Body["n"] != n {
		t.Fatalf("round trip: %v, %v", result, err)
	}
	if _, err := topic.Publish().Avro(schema, map[string]string{"n": "invalid"}).Send(context.Background()); err == nil {
		t.Fatal("accepted invalid datum")
	}
	if len(server.sentMessages) != 1 {
		t.Fatal("sent invalid datum")
	}
}

func TestGivenABatchWithMixedContentWhenSentThenShouldPreserveTypedRecordOverrides(t *testing.T) {
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "agents"}).Topic("mixed")
	fingerprint := wire.NewSchemaFingerprint([wire.DigestBytes]byte{1})
	metadata := wire.ArrowIpcMessageMetadata{ContractVersion: wire.ArrowIpcContractVersion, SchemaFingerprint: fingerprint, EncodedBytes: 2, FieldCount: 1, RecordBatchCount: 1, RowCount: 1}
	batch := topic.PublishBatch().SchemaID(17).InlinePayload().ProjectionRef("default.v1").Index("host", "node-7")
	batch.AddRecord(topic.Publish().JSON(map[string]int{"cpu": 42}).ProjectionRef("cpu.v1")).AddArrowIPC([]byte{1, 2}, metadata)
	if _, err := batch.Send(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(server.sentMessages) != 2 {
		t.Fatalf("sent %d records", len(server.sentMessages))
	}
	for i, msg := range server.sentMessages {
		headers, err := iggcon.DeserializeHeaders(msg.UserHeaders)
		if err != nil {
			t.Fatal(err)
		}
		found := map[string]iggcon.HeaderValue{}
		for _, header := range headers {
			found[string(header.Key.Value)] = header.Value
		}
		if found[wire.HeaderInlinePayload].Kind != iggcon.Bool || string(found[wire.HeaderIdxPrefix+"host"].Value) != "node-7" || found[wire.HeaderSchemaID].Kind != iggcon.Uint32 {
			t.Fatalf("record %d headers: %v", i, found)
		}
		if i == 0 && string(found[wire.HeaderProjectionRef].Value) != "cpu.v1" {
			t.Fatal("lost per-record projection")
		}
		if i == 1 && (!bytes.Equal(found[wire.HeaderLogicalSchemaFingerprint].Value, fingerprint.Bytes) || found[wire.HeaderLogicalSchemaFingerprint].Kind != iggcon.Raw) {
			t.Fatal("lost Arrow fingerprint")
		}
	}
}

func TestGivenAMalformedRecordWhenFinishingABatchThenShouldSendNothing(t *testing.T) {
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "demo"}).Topic("orders")
	batch := topic.PublishBatch().AddPayload([]byte("good")).AddRawBytes([]byte("no fingerprint"), wire.ContentArrow)
	if _, err := batch.Send(context.Background()); err == nil || len(server.calls) != 0 {
		t.Fatalf("invalid batch: %v, calls %v", err, server.calls)
	}
	invalid := topic.Publish().Header(wire.HeaderSchemaID, "1").JSON(1)
	if _, err := invalid.Send(context.Background()); err == nil || len(server.calls) != 0 {
		t.Fatalf("reserved header: %v, calls %v", err, server.calls)
	}
}

func TestGivenECMAScriptPatternsWhenValidatingThenShouldHandleLookaheadAndBackreferences(t *testing.T) {
	schema, err := laser.CompileSchema(wire.SchemaDef{Source: wire.JSONSchema(`{"type":"string","pattern":"^(?=[A-Z]{2})([A-Z])\\1$"}`)})
	if err != nil {
		t.Fatal(err)
	}
	for value, want := range map[string]bool{"AA": true, "AB": false, "aa": false} {
		if schema.ValidateValue(value) != want {
			t.Fatalf("unexpected decision for %q", value)
		}
	}
}
