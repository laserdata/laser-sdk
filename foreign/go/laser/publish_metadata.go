package laser

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ContentType sets the payload's codec without replacing the payload.
func (request *PublishRequest) ContentType(kind wire.ContentType) *PublishRequest {
	if code, ok := kind.Code(); ok {
		return request.setHeader(wire.HeaderContentType, iggcon.HeaderValue{Kind: iggcon.Uint8, Value: []byte{code}})
	}
	return request.textHeader(wire.HeaderContentType, string(kind))
}

// ProjectionRef selects a projection allowed by the topic's binding.
func (request *PublishRequest) ProjectionRef(ref string) *PublishRequest {
	return request.textHeader(wire.HeaderProjectionRef, ref)
}

// Index adds a scalar field to the materialized row.
func (request *PublishRequest) Index(field, value string) *PublishRequest {
	if field == "" {
		return request.fail(&InvalidError{Reason: "index key must not be empty"})
	}
	if strings.HasPrefix(field, wire.HeaderIdxPrefix) {
		return request.fail(&InvalidError{Reason: "index key `" + field + "` must not start with the reserved `" + wire.HeaderIdxPrefix + "` prefix"})
	}
	return request.textHeader(wire.HeaderIdxPrefix+field, value)
}

// InlinePayload asks the projector to retain the record body with its row.
func (request *PublishRequest) InlinePayload() *PublishRequest {
	return request.setHeader(wire.HeaderInlinePayload, iggcon.HeaderValue{Kind: iggcon.Bool, Value: []byte{1}})
}

// ArrowIPC publishes an encoded Arrow IPC stream under its logical schema fingerprint.
func (request *PublishRequest) ArrowIPC(payload []byte, metadata wire.ArrowIpcMessageMetadata) *PublishRequest {
	if err := metadata.Validate(); err != nil {
		return request.fail(&InvalidError{Reason: err.Error()})
	}
	if uint64(len(payload)) != metadata.EncodedBytes {
		return request.fail(&InvalidError{Reason: fmt.Sprintf("Arrow IPC payload is %d bytes, metadata declares %d", len(payload), metadata.EncodedBytes)})
	}
	return request.RawBytes(payload, wire.ContentArrow).setHeader(wire.HeaderLogicalSchemaFingerprint, iggcon.HeaderValue{Kind: iggcon.Raw, Value: bytes.Clone(metadata.SchemaFingerprint.Bytes)})
}

func (request *PublishRequest) textHeader(name, value string) *PublishRequest {
	if len(value) == 0 || len(value) > wire.HeaderValueMax || !utf8.ValidString(value) {
		return request.fail(&InvalidError{Reason: fmt.Sprintf("header %s requires UTF-8 text of 1 to %d bytes", name, wire.HeaderValueMax)})
	}
	return request.setHeader(name, iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)})
}

func reservedMetadata(name string) bool {
	switch name {
	case wire.HeaderContentType, wire.HeaderSchemaID, wire.HeaderLogicalSchemaFingerprint, wire.HeaderProjectionRef, wire.HeaderInlinePayload:
		return true
	default:
		return strings.HasPrefix(name, wire.HeaderIdxPrefix)
	}
}

func validatePublishHeaders(headers []iggcon.HeaderEntry) error {
	size, indexed := 0, 0
	var arrow, fingerprint bool
	code, _ := wire.ContentArrow.Code()
	for _, header := range headers {
		size += len(header.Key.Value) + len(header.Value.Value) + wire.HeaderFramingBytes
		name := string(header.Key.Value)
		switch name {
		case wire.HeaderContentType:
			arrow = header.Value.Kind == iggcon.Uint8 && len(header.Value.Value) == 1 && header.Value.Value[0] == code
		case wire.HeaderLogicalSchemaFingerprint:
			fingerprint = true
		default:
			if strings.HasPrefix(name, wire.HeaderIdxPrefix) {
				indexed++
			}
		}
	}
	if arrow != fingerprint {
		return &InvalidError{Reason: "Arrow IPC content and its logical schema fingerprint must be set together"}
	}
	if indexed > wire.MaxIndexEntriesPerRecord {
		return &InvalidError{Reason: fmt.Sprintf("record exceeds %d indexed fields", wire.MaxIndexEntriesPerRecord)}
	}
	if size > wire.HeaderSoftCap {
		return &InvalidError{Reason: fmt.Sprintf("record headers exceed %d bytes", wire.HeaderSoftCap)}
	}
	return nil
}
