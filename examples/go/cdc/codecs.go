package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	schemaTimeout = 15 * time.Second
	schemaPoll    = 50 * time.Millisecond
)

// satelliteReading is the typed record every codec carries.
type satelliteReading struct {
	Satellite string `json:"satellite" cbor:"satellite"`
	Mode      string `json:"mode" cbor:"mode"`
	Battery   int32  `json:"battery" cbor:"battery"`
}

// runCodecs publishes the same three readings as CBOR, Avro and Protobuf and
// lets a group filter select the safe-mode one inside each payload.
func runCodecs(ctx context.Context, client *laser.Laser) (err error) {
	examples.Phase("select typed records inside CBOR, Avro, and Protobuf payloads")
	var registered []uint32
	defer func() {
		for _, id := range registered {
			err = errors.Join(err, client.Schemas().Drop(ctx, id))
		}
	}()
	for _, codec := range []wire.FilterCodec{wire.CodecCBOR, wire.CodecAvro, wire.CodecProtobuf} {
		source, err := schemaSource(codec)
		if err != nil {
			return err
		}
		var (
			compiled *laser.CompiledSchema
			schemaID uint32
		)
		if source != nil {
			schemaID, err = client.Schemas().Register(*source).Name("fleet-" + string(codec)).Send(ctx)
			if err != nil {
				return fmt.Errorf("register the %s schema: %w", codec, err)
			}
			registered = append(registered, schemaID)
			if err := waitVisible(ctx, client, schemaID); err != nil {
				return err
			}
			if compiled, err = laser.CompileSchema(wire.SchemaDef{ID: schemaID, Source: *source}); err != nil {
				return fmt.Errorf("compile the %s schema: %w", codec, err)
			}
		}
		if err := selectSafeMode(ctx, client, codec, compiled, schemaID); err != nil {
			return err
		}
	}
	return nil
}

func selectSafeMode(
	ctx context.Context, client *laser.Laser, codec wire.FilterCodec, compiled *laser.CompiledSchema, schemaID uint32,
) (err error) {
	topicName := "fleet_" + string(codec)
	topic := client.Topic(topicName)
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", topicName, err)
	}
	for _, value := range []satelliteReading{
		{Satellite: "sat-042", Mode: "nominal", Battery: 80},
		{Satellite: "sat-042", Mode: "safe", Battery: 20},
		{Satellite: "sat-042", Mode: "nominal", Battery: 60},
	} {
		if err := publishReading(ctx, topic, codec, compiled, value); err != nil {
			return fmt.Errorf("publish a %s reading: %w", codec, err)
		}
	}
	filter := wire.NewConsumerFilter(wire.ExprPredicate("mode", wire.CmpEq, wire.StringTyped("safe")), codec)
	if compiled != nil {
		filter.SchemaRefs = []uint32{schemaID}
	}
	group := topic.Group("safe-" + string(codec))
	if _, err := group.Create().Filter("safe-mode-"+string(codec), filter).Build(ctx); err != nil {
		return fmt.Errorf("create the %s group: %w", codec, err)
	}
	defer func() {
		_, deleteErr := group.Filter().Delete(ctx)
		err = errors.Join(err, deleteErr)
	}()
	first := wire.StartAtFirst()
	reader, err := group.OpenReaderWith(ctx, laser.GroupReaderOptions{Start: &first, LocalGuard: true})
	if err != nil {
		return fmt.Errorf("open the %s reader: %w", codec, err)
	}
	defer func() { err = errors.Join(err, reader.Close(ctx)) }()
	record, err := nextRecord(ctx, reader)
	if err != nil {
		return err
	}
	var selected satelliteReading
	if compiled == nil {
		err = wire.Unmarshal(record.Payload(), &selected)
	} else {
		err = compiled.Decode(record.Payload(), &selected)
	}
	if err != nil {
		return fmt.Errorf("decode the %s record: %w", codec, err)
	}
	fmt.Printf("  %s: %s entered %s mode at %d%% battery, 1 of 3 records delivered\n",
		codec, selected.Satellite, selected.Mode, selected.Battery)
	return reader.AckRecord(ctx, record.Page(), record.Offset())
}

// publishReading encodes one reading in the topic's codec. A registered
// schema encodes the body and stamps its id.
func publishReading(
	ctx context.Context, topic *laser.Topic, codec wire.FilterCodec, compiled *laser.CompiledSchema,
	value satelliteReading,
) error {
	request := topic.Publish()
	switch codec {
	case wire.CodecCBOR:
		request.CBOR(value)
	case wire.CodecAvro:
		request.Avro(compiled, value)
	default:
		request.EncodeWith(compiled, value)
	}
	_, err := request.Send(ctx)
	return err
}

func schemaSource(codec wire.FilterCodec) (*wire.SchemaSource, error) {
	switch codec {
	case wire.CodecAvro:
		schema, err := os.ReadFile(sharedAsset("fleet-reading.avsc"))
		if err != nil {
			return nil, fmt.Errorf("read the Avro schema: %w", err)
		}
		source := wire.AvroSchema(string(schema))
		return &source, nil
	case wire.CodecProtobuf:
		descriptorSet, err := os.ReadFile(sharedAsset("fleet-reading.desc"))
		if err != nil {
			return nil, fmt.Errorf("read the Protobuf descriptor set: %w", err)
		}
		source := wire.ProtobufSchema(descriptorSet, "fleet.Reading")
		return &source, nil
	default:
		return nil, nil
	}
}

// sharedAsset is a schema file every language's example registers, kept beside
// the example directories of this checkout.
func sharedAsset(name string) string {
	_, source, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(source), "..", "..", "shared", name)
}

func waitVisible(ctx context.Context, client *laser.Laser, id uint32) error {
	deadline := time.Now().Add(schemaTimeout)
	for {
		info, err := client.Schemas().Get(ctx, id)
		if err != nil {
			return fmt.Errorf("read schema %d: %w", id, err)
		}
		if info != nil {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("writer schema did not become visible")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(schemaPoll):
		}
	}
}
