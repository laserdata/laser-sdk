package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// ContentType is the wire codec tag stamped on the agdx.ct header.
//
// It rides CBOR as its snake_case name and the header as the compact u8 code.
// The codes are a growable dictionary: a server must treat an unknown code as
// opaque and pass it through rather than reject the record.
type ContentType string

const (
	// ContentAny is the best-effort sentinel: try JSON, else raw.
	ContentAny ContentType = "any"
	// ContentRaw is the default codec.
	ContentRaw      ContentType = "raw"
	ContentJSON     ContentType = "json"
	ContentAvro     ContentType = "avro"
	ContentProtobuf ContentType = "protobuf"
	ContentMsgpack  ContentType = "msgpack"
	ContentCBOR     ContentType = "cbor"
	ContentBSON     ContentType = "bson"
	ContentArrow    ContentType = "arrow"
	// ContentBodyRef marks a body that is a BodyRef capsule pointing at content
	// stored elsewhere: the claim-check form.
	ContentBodyRef ContentType = "ref"
)

// IsRaw reports the default codec, which is omitted on the wire.
func (contentType ContentType) IsRaw() bool { return contentType == ContentRaw }

// Code is the compact u8 wire code stamped as the agdx.ct header value. The
// dictionary is shared with LaserData Cloud and its display layers.
func (contentType ContentType) Code() (uint8, bool) {
	switch contentType {
	case ContentRaw:
		return 0, true
	case ContentJSON:
		return 1, true
	case ContentMsgpack:
		return 2, true
	case ContentCBOR:
		return 3, true
	case ContentBSON:
		return 4, true
	case ContentAvro:
		return 5, true
	case ContentProtobuf:
		return 6, true
	case ContentArrow:
		return 7, true
	case ContentBodyRef:
		return 8, true
	case ContentAny:
		return 255, true
	default:
		return 0, false
	}
}

// ContentTypeFromCode decodes a compact agdx.ct code. The second result is
// false for a code this build does not name, which a consumer must treat as
// opaque rather than an error.
func ContentTypeFromCode(code uint8) (ContentType, bool) {
	switch code {
	case 0:
		return ContentRaw, true
	case 1:
		return ContentJSON, true
	case 2:
		return ContentMsgpack, true
	case 3:
		return ContentCBOR, true
	case 4:
		return ContentBSON, true
	case 5:
		return ContentAvro, true
	case 6:
		return ContentProtobuf, true
	case 7:
		return ContentArrow, true
	case 8:
		return ContentBodyRef, true
	case 255:
		return ContentAny, true
	default:
		return "", false
	}
}

// MarshalCBOR writes the snake_case name.
func (contentType ContentType) MarshalCBOR() (cborform.Value, error) {
	if _, known := contentType.Code(); !known {
		return nil, fmt.Errorf("wire: unknown content type %q", string(contentType))
	}
	return cborform.Text(string(contentType)), nil
}

// UnmarshalCBOR reads the snake_case name.
func (contentType *ContentType) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a content type", item)
	}
	candidate := ContentType(text)
	if _, known := candidate.Code(); !known {
		return fmt.Errorf("wire: unknown content type %q", string(text))
	}
	*contentType = candidate
	return nil
}
