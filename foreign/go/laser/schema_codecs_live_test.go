//go:build integration

package laser_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALiveTopicWhenAProtobufBodyIsPublishedUnderItsSchemaThenShouldReplayTheSameBody(t *testing.T) {
	ctx := liveContext(t)
	topic := liveTopic(t, dialLive(t), ctx)
	schema, err := laser.CompileSchema(wire.SchemaDef{ID: 401, Source: wire.ProtobufSchema(fleetReadingDescriptorSet(t), "fleet.Reading")})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	typed := laser.TypedWith[fleetReading](topic, schema)
	reading := fleetReading{Satellite: "sat-7", Mode: "safe", Battery: 41}

	if _, err := typed.Publish(reading).Send(ctx); err != nil {
		t.Fatalf("publish: %v", err)
	}

	cursor, err := typed.Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	var read []fleetReading
	for len(read) == 0 {
		if ctx.Err() != nil {
			t.Fatal("the record never came back")
		}
		err = typed.Records(ctx, cursor, func(record *laser.TypedRecord[fleetReading]) bool {
			read = append(read, record.Body)
			return true
		}, func(failure *laser.TypedDecodeError) { t.Fatalf("decode: %v", failure) })
		if err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	if len(read) != 1 || read[0] != reading {
		t.Fatalf("want %+v back, got %+v", reading, read)
	}
}
