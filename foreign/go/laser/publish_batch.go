package laser

import (
	"context"
	"fmt"
	"slices"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// BatchPublishRequest collects records for one send on the batch's topic and
// key. Per-record metadata takes precedence over batch defaults.
type BatchPublishRequest struct {
	defaults *PublishRequest
	records  []PublishRequest
}

// PublishBatch begins a batch of records sent together on this topic.
func (topic *Topic) PublishBatch() *BatchPublishRequest {
	return &BatchPublishRequest{defaults: topic.Publish()}
}

// Key routes every record of the batch to the partition of one key.
func (batch *BatchPublishRequest) Key(key string) *BatchPublishRequest {
	batch.defaults.Key(key)
	return batch
}

// ContentType stamps a content type on every record that names none.
func (batch *BatchPublishRequest) ContentType(kind wire.ContentType) *BatchPublishRequest {
	batch.defaults.ContentType(kind)
	return batch
}

// SchemaID stamps a writer schema on every record that names none.
func (batch *BatchPublishRequest) SchemaID(id uint32) *BatchPublishRequest {
	batch.defaults.SchemaID(id)
	return batch
}

// ProjectionRef selects a projection for every record that names none.
func (batch *BatchPublishRequest) ProjectionRef(ref string) *BatchPublishRequest {
	batch.defaults.ProjectionRef(ref)
	return batch
}

// Index adds a scalar field to the materialized row of every record.
func (batch *BatchPublishRequest) Index(field, value string) *BatchPublishRequest {
	batch.defaults.Index(field, value)
	return batch
}

// InlinePayload asks the projector to retain each record body with its row.
func (batch *BatchPublishRequest) InlinePayload() *BatchPublishRequest {
	batch.defaults.InlinePayload()
	return batch
}

// Header adds a user header to every record that does not set it.
func (batch *BatchPublishRequest) Header(key, value string) *BatchPublishRequest {
	batch.defaults.Header(key, value)
	return batch
}

// Len is the number of records added so far.
func (batch *BatchPublishRequest) Len() int { return len(batch.records) }

// IsEmpty reports a batch that holds no record yet.
func (batch *BatchPublishRequest) IsEmpty() bool { return len(batch.records) == 0 }

// AddRecord takes a snapshot of one publish request. The batch supplies the
// partition key for every record in the send.
func (batch *BatchPublishRequest) AddRecord(record *PublishRequest) *BatchPublishRequest {
	if record == nil || record.topic == nil {
		batch.defaults.fail(&InvalidError{Reason: "missing batch record"})
		return batch
	}
	topic := batch.defaults.topic
	if record.topic.name != topic.name || record.topic.stream != topic.stream || record.topic.laser != topic.laser || record.topic.encryption != topic.encryption || record.hasKey {
		batch.defaults.fail(&InvalidError{Reason: "a batch record must use the batch topic and partition key"})
		return batch
	}
	entry := *record
	entry.headers = slices.Clone(entry.headers)
	batch.records = append(batch.records, entry)
	return batch
}

// AddPayload adds a record with an opaque payload.
func (batch *BatchPublishRequest) AddPayload(payload []byte) *BatchPublishRequest {
	return batch.add(batch.defaults.topic.Publish().Payload(payload))
}

// AddRawBytes adds a record with encoded bytes under a content type.
func (batch *BatchPublishRequest) AddRawBytes(payload []byte, kind wire.ContentType) *BatchPublishRequest {
	return batch.add(batch.defaults.topic.Publish().RawBytes(payload, kind))
}

// AddJSON adds a record with a JSON body.
func (batch *BatchPublishRequest) AddJSON(body any) *BatchPublishRequest {
	return batch.add(batch.defaults.topic.Publish().JSON(body))
}

// AddEncoded adds a record encoded through a codec.
func (batch *BatchPublishRequest) AddEncoded(codec wire.Codec, value any) *BatchPublishRequest {
	return batch.add(batch.defaults.topic.Publish().EncodeWith(codec, value))
}

// ExtendEncoded adds one record per value, each encoded through the codec.
func (batch *BatchPublishRequest) ExtendEncoded(codec wire.Codec, values ...any) *BatchPublishRequest {
	for _, value := range values {
		batch.AddEncoded(codec, value)
	}
	return batch
}

// ExtendJSON adds one record with a JSON body per value.
func (batch *BatchPublishRequest) ExtendJSON(bodies ...any) *BatchPublishRequest {
	for _, body := range bodies {
		batch.AddJSON(body)
	}
	return batch
}

// ExtendMsgpack adds one record with a MessagePack body per value.
func (batch *BatchPublishRequest) ExtendMsgpack(values ...any) *BatchPublishRequest {
	return batch.ExtendEncoded(wire.MsgpackCodec, values...)
}

// AddMsgpack adds a record with a MessagePack body.
func (batch *BatchPublishRequest) AddMsgpack(value any) *BatchPublishRequest {
	return batch.AddEncoded(wire.MsgpackCodec, value)
}

// AddBSON adds a record with a BSON body.
func (batch *BatchPublishRequest) AddBSON(value any) *BatchPublishRequest {
	return batch.AddEncoded(wire.BSONCodec, value)
}

// AddCBOR adds a record with a CBOR body.
func (batch *BatchPublishRequest) AddCBOR(body any) *BatchPublishRequest {
	return batch.add(batch.defaults.topic.Publish().CBOR(body))
}

// AddAvro adds a record encoded under a registered Avro writer schema.
func (batch *BatchPublishRequest) AddAvro(schema *CompiledSchema, body any) *BatchPublishRequest {
	return batch.add(batch.defaults.topic.Publish().Avro(schema, body))
}

// AddArrowIPC adds an encoded Arrow IPC stream under its logical schema fingerprint.
func (batch *BatchPublishRequest) AddArrowIPC(payload []byte, metadata wire.ArrowIpcMessageMetadata) *BatchPublishRequest {
	return batch.add(batch.defaults.topic.Publish().ArrowIPC(payload, metadata))
}

func (batch *BatchPublishRequest) add(record *PublishRequest) *BatchPublishRequest {
	batch.records = append(batch.records, *record)
	return batch
}

// Send validates every record before submitting the batch.
func (batch *BatchPublishRequest) Send(ctx context.Context) (*iggcon.SendMessagesResponse, error) {
	if batch.defaults.err != nil {
		return nil, batch.defaults.err
	}
	if batch.Len() == 0 {
		return &iggcon.SendMessagesResponse{}, nil
	}
	messages := make([]iggcon.IggyMessage, 0, batch.Len())
	for i, record := range batch.records {
		if record.err != nil {
			return nil, fmt.Errorf("record #%d: %w", i, record.err)
		}
		if len(batch.defaults.headers) > 0 {
			merged := PublishRequest{headers: slices.Clone(batch.defaults.headers)}
			for _, header := range record.headers {
				merged.setHeader(string(header.Key.Value), header.Value)
			}
			record.headers = merged.headers
		}
		message, err := record.message()
		if err != nil {
			return nil, fmt.Errorf("record #%d: %w", i, err)
		}
		messages = append(messages, message)
	}
	return batch.defaults.topic.governedSend(ctx, messages, batch.defaults.key, batch.defaults.hasKey)
}
