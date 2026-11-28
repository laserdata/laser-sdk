package laser_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenACodecWhenAValueIsWrittenThenShouldStoreTheBytesThatCodecEncodes(t *testing.T) {
	reading := storedReading{Host: "node-7", CPU: 91}
	for name, write := range map[string]struct {
		codec wire.Codec
		set   func(*laser.KvSetRequest) *laser.KvSetRequest
	}{
		"any codec": {wire.CBORCodec, func(request *laser.KvSetRequest) *laser.KvSetRequest {
			return request.EncodeWith(wire.CBORCodec, reading)
		}},
		"MessagePack": {wire.MsgpackCodec, func(request *laser.KvSetRequest) *laser.KvSetRequest { return request.Msgpack(reading) }},
	} {
		server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvWritten}}}
		kv := kvHandle(server, laser.Options{}).Kv("ns")

		if err := write.set(kv.Set([]byte("k"))).Send(context.Background()); err != nil {
			t.Fatalf("%s: set: %v", name, err)
		}

		want, err := write.codec.Encode(reading)
		if err != nil {
			t.Fatalf("%s: encode: %v", name, err)
		}
		var request wire.KvSet
		decodeRequest(t, server, 0, &request)
		if string(request.Value) != string(want) {
			t.Fatalf("%s: stored %x, want %x", name, request.Value, want)
		}
		var stored storedReading
		if err := write.codec.(wire.Decoder).Decode(request.Value, &stored); err != nil || stored != reading {
			t.Fatalf("%s: read back %+v, %v", name, stored, err)
		}
	}
}

func TestGivenAValueThatDoesNotEncodeWhenWrittenThenShouldRefuseBeforeSending(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvWritten}}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	err := kv.Set([]byte("k")).Msgpack(make(chan int)).Send(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("got %v, want a codec refusal", err)
	}
	if len(server.requests) != 0 {
		t.Fatalf("%d requests left for a value that never encoded", len(server.requests))
	}
}

func TestGivenNoCodecWhenAValueIsWrittenThenShouldRefuseBeforeSending(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvWritten}}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	err := kv.Set([]byte("k")).EncodeWith(nil, storedReading{}).Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("got %v, want an invalid refusal", err)
	}
	if len(server.requests) != 0 {
		t.Fatalf("%d requests left without a codec", len(server.requests))
	}
}

func TestGivenACodecWhenAFencedValueIsCommittedThenShouldStoreTheBytesThatCodecEncodes(t *testing.T) {
	reading := storedReading{Host: "node-7", CPU: 91}
	for name, write := range map[string]struct {
		codec wire.Codec
		set   func(*laser.KvCasFencedRequest) *laser.KvCasFencedRequest
	}{
		"any codec": {wire.CBORCodec, func(request *laser.KvCasFencedRequest) *laser.KvCasFencedRequest {
			return request.EncodeWith(wire.CBORCodec, reading)
		}},
		"MessagePack": {wire.MsgpackCodec, func(request *laser.KvCasFencedRequest) *laser.KvCasFencedRequest {
			return request.Msgpack(reading)
		}},
	} {
		server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvCommitted, Version: 3}}}
		kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

		_, err := write.set(kv.CasFenced([]byte("job-1"), "fence-ns", []byte("run-1"), 7)).
			ExpectAbsent().Commit(context.Background())
		if err != nil {
			t.Fatalf("%s: commit: %v", name, err)
		}

		want, err := write.codec.Encode(reading)
		if err != nil {
			t.Fatalf("%s: encode: %v", name, err)
		}
		var request wire.KvCasFenced
		decodeRequest(t, server, 0, &request)
		if string(request.Value) != string(want) {
			t.Fatalf("%s: stored %x, want %x", name, request.Value, want)
		}
	}
}

func TestGivenNoCodecWhenAFencedValueIsCommittedThenShouldRefuseBeforeSending(t *testing.T) {
	server := &kvServer{}
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")

	_, err := kv.CasFenced([]byte("job-1"), "fence-ns", []byte("run-1"), 7).
		EncodeWith(nil, 1).ExpectAbsent().Commit(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) || len(server.requests) != 0 {
		t.Fatalf("want a refusal before sending, got %v after %d requests", err, len(server.requests))
	}
}

type storedReading struct {
	Host string `json:"host" msgpack:"host"`
	CPU  uint32 `json:"cpu" msgpack:"cpu"`
}
