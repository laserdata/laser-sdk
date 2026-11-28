package laser_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

type fleetReading struct {
	Satellite string `json:"satellite"`
	Mode      string `json:"mode"`
	Battery   int32  `json:"battery"`
}

func TestGivenATypedProtobufPublishWhenSentThenShouldEncodeTheBodyAndStampTheWireHeaders(t *testing.T) {
	const id = 401
	schema, err := laser.CompileSchema(wire.SchemaDef{ID: id, Source: wire.ProtobufSchema(fleetReadingDescriptorSet(t), "fleet.Reading")})
	if err != nil {
		t.Fatal(err)
	}
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "telemetry"}).Topic("readings")
	typed := laser.TypedWith[fleetReading](topic, schema)
	reading := fleetReading{Satellite: "sat-7", Mode: "safe", Battery: 41}

	if _, err := typed.Publish(reading).Send(context.Background()); err != nil {
		t.Fatal(err)
	}

	sent := server.sentMessages[0]
	if got := hex.EncodeToString(sent.Payload); got != "0a057361742d37120473616665"+"1829" {
		t.Fatalf("want the message bytes of the three fields, got %s", got)
	}
	entries, err := iggcon.DeserializeHeaders(sent.UserHeaders)
	if err != nil {
		t.Fatal(err)
	}
	headers := make(map[string]iggcon.HeaderValue)
	for _, entry := range entries {
		headers[string(entry.Key.Value)] = entry.Value
	}
	code, _ := wire.ContentProtobuf.Code()
	kind, schemaID := headers[wire.HeaderContentType], headers[wire.HeaderSchemaID]
	if !bytes.Equal(kind.Value, []byte{code}) || !bytes.Equal(schemaID.Value, binary.LittleEndian.AppendUint32(nil, id)) {
		t.Fatalf("unexpected schema stamps: %+v", headers)
	}
	back, err := typed.Decode(&laser.Record{Message: sent})
	if err != nil || back.Body != reading {
		t.Fatalf("round trip: %+v, %v", back, err)
	}
}

func TestGivenABodyThatDoesNotFitWhenPublishedUnderAProtobufSchemaThenShouldSendNothing(t *testing.T) {
	schema, err := laser.CompileSchema(wire.SchemaDef{ID: 401, Source: wire.ProtobufSchema(fleetReadingDescriptorSet(t), "fleet.Reading")})
	if err != nil {
		t.Fatal(err)
	}
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "telemetry"}).Topic("readings")

	_, err = laser.TypedWith[map[string]string](topic, schema).
		Publish(map[string]string{"battery": "full"}).Send(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) || len(server.sentMessages) != 0 {
		t.Fatalf("want a codec refusal before sending, got %v after %d sends", err, len(server.sentMessages))
	}
}

func TestGivenAnAvroSchemaWhenEncodingProtobufThenShouldRefuseIt(t *testing.T) {
	schema, err := laser.CompileSchema(wire.SchemaDef{ID: 400, Source: wire.AvroSchema(`{"type":"record","name":"Reading","fields":[{"name":"n","type":"long"}]}`)})
	if err != nil {
		t.Fatal(err)
	}

	_, err = schema.EncodeProtobuf(map[string]int64{"n": 1})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid error, got %v", err)
	}
}

func fleetReadingDescriptorSet(t *testing.T) []byte {
	t.Helper()
	field := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name: proto.String(name), Number: proto.Int32(number), Type: kind.Enum(),
			Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		}
	}
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name: proto.String("fleet-reading.proto"), Package: proto.String("fleet"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Reading"), Field: []*descriptorpb.FieldDescriptorProto{
			field("satellite", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("mode", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("battery", 3, descriptorpb.FieldDescriptorProto_TYPE_INT32),
		}}},
	}}}
	raw, err := proto.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
