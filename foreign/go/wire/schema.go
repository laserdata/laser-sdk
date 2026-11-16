package wire

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// The logical-schema bounds. A schema is read before anything is allocated
// for it, so each one is checked up front rather than discovered by failing.
const (
	// MaxLogicalSchemaFields caps how many fields one logical schema
	// declares.
	MaxLogicalSchemaFields = 4_096
	// MaxLogicalSchemaDepth caps how deeply a schema nests.
	MaxLogicalSchemaDepth = 16
	// MaxLogicalSchemaBytes caps one schema's encoded size.
	MaxLogicalSchemaBytes = 1_048_576
	// MaxFieldNameBytes caps one field name.
	MaxFieldNameBytes = 255
	// MaxFieldDocBytes caps one field's documentation.
	MaxFieldDocBytes = 4_096
	// MaxFixedBytes caps a fixed-width binary field.
	MaxFixedBytes uint32 = 1_048_576
	// MaxDecimalPrecision caps a decimal's significant digits.
	MaxDecimalPrecision uint8 = 38
)

// MicrosPerDay converts a date field, which counts days, to the epoch
// microseconds every other temporal type carries.
const MicrosPerDay int64 = 86_400_000_000

// ProvenanceFieldIDStart is the first field id reserved for provenance. A
// projection's own fields never reach it, so a stamped row cannot collide
// with a declared one.
const ProvenanceFieldIDStart uint32 = 2_000_000_000

// ProvenanceField is one reserved provenance column: its pinned field id and
// the name a materialized row carries it under.
type ProvenanceField struct {
	ID   uint32
	Name string
}

// ProvenanceFields are the reserved provenance columns, in id order. A
// whole-record projection writes the last four, which carry the record back
// in full.
var ProvenanceFields = []ProvenanceField{
	{2_000_000_001, "__laser_cluster_incarnation"},
	{2_000_000_002, "__laser_source_incarnation"},
	{2_000_000_003, "__laser_stream_id"},
	{2_000_000_004, "__laser_topic_id"},
	{2_000_000_005, "__laser_partition_id"},
	{2_000_000_006, "__laser_offset"},
	{2_000_000_007, "__laser_row_ordinal"},
	{2_000_000_008, "__laser_projection_id"},
	{2_000_000_009, "__laser_projection_version"},
	{2_000_000_010, "__laser_destination_id"},
	{2_000_000_011, "__laser_destination_generation"},
	{OriginalPayloadFieldID, OriginalPayloadFieldName},
	{OriginalContentTypeFieldID, OriginalContentTypeFieldName},
	{OriginalSchemaIDFieldID, OriginalSchemaIDFieldName},
	{OriginalHeadersFieldID, OriginalHeadersFieldName},
}

// The whole-record columns, named on their own because a record projection
// reads and writes them directly rather than walking the reserved set.
const (
	OriginalPayloadFieldID       uint32 = 2_000_000_012
	OriginalPayloadFieldName            = "__laser_original_payload"
	OriginalContentTypeFieldID   uint32 = 2_000_000_013
	OriginalContentTypeFieldName        = "__laser_original_content_type"
	OriginalSchemaIDFieldID      uint32 = 2_000_000_014
	OriginalSchemaIDFieldName           = "__laser_original_schema_id"
	OriginalHeadersFieldID       uint32 = 2_000_000_015
	OriginalHeadersFieldName            = "__laser_original_headers"
	// PartitionIDFieldName and OffsetFieldName are the two provenance
	// columns a reader orders and groups a partition's rows by.
	PartitionIDFieldName = "__laser_partition_id"
	OffsetFieldName      = "__laser_offset"
)

// LogicalSchemaID is the immutable identity of one logical schema family.
type LogicalSchemaID struct{ ID }

// MarshalCBOR writes the id as its bytes.
//
// Unlike the record and routing ids, a logical schema id is bytes on every
// surface, including a human-readable one, which is what the contract's own
// serializer does.
func (id LogicalSchemaID) MarshalCBOR() (cborform.Value, error) {
	payload := id.Bytes()
	return cborform.Bytes(payload[:]), nil
}

// DigestBytes is the width of a schema fingerprint and of a content digest.
const DigestBytes = 32

// SchemaFingerprint is the canonical hash of a logical schema.
type SchemaFingerprint struct{ Bytes []byte }

// NewSchemaFingerprint wraps a 32-byte digest.
func NewSchemaFingerprint(digest [DigestBytes]byte) SchemaFingerprint {
	return SchemaFingerprint{Bytes: digest[:]}
}

// Validate checks the fingerprint width.
func (fingerprint SchemaFingerprint) Validate() error {
	if len(fingerprint.Bytes) != DigestBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"schema fingerprint must be %d bytes, got %d", DigestBytes, len(fingerprint.Bytes))}
	}
	return nil
}

// Equal reports whether two fingerprints hold the same digest.
func (fingerprint SchemaFingerprint) Equal(other SchemaFingerprint) bool {
	if len(fingerprint.Bytes) != len(other.Bytes) {
		return false
	}
	for i := range fingerprint.Bytes {
		if fingerprint.Bytes[i] != other.Bytes[i] {
			return false
		}
	}
	return true
}

// MarshalCBOR writes the digest as a byte string, the newtype's wire form.
func (fingerprint SchemaFingerprint) MarshalCBOR() (cborform.Value, error) {
	return cborform.Bytes(fingerprint.Bytes), nil
}

// UnmarshalCBOR reads the digest.
func (fingerprint *SchemaFingerprint) UnmarshalCBOR(item cborform.Value) error {
	var payload []byte
	if err := fromValueInto(item, &payload); err != nil {
		return err
	}
	fingerprint.Bytes = payload
	return nil
}

// Digest32 is a 32-byte content digest.
type Digest32 struct{ Bytes []byte }

// NewDigest32 wraps a 32-byte digest.
func NewDigest32(digest [DigestBytes]byte) Digest32 { return Digest32{Bytes: digest[:]} }

// Validate checks the digest width.
func (digest Digest32) Validate() error {
	if len(digest.Bytes) != DigestBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"digest must be %d bytes, got %d", DigestBytes, len(digest.Bytes))}
	}
	return nil
}

// MarshalCBOR writes the digest as a byte string.
func (digest Digest32) MarshalCBOR() (cborform.Value, error) {
	return cborform.Bytes(digest.Bytes), nil
}

// UnmarshalCBOR reads the digest.
func (digest *Digest32) UnmarshalCBOR(item cborform.Value) error {
	var payload []byte
	if err := fromValueInto(item, &payload); err != nil {
		return err
	}
	digest.Bytes = payload
	return nil
}

// UuidValue is a 16-byte UUID.
type UuidValue struct{ Bytes []byte }

// Validate checks the UUID width.
func (value UuidValue) Validate() error {
	if len(value.Bytes) != UUIDBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"uuid must be %d bytes, got %d", UUIDBytes, len(value.Bytes))}
	}
	return nil
}

// MarshalCBOR writes the UUID as a byte string.
func (value UuidValue) MarshalCBOR() (cborform.Value, error) {
	return cborform.Bytes(value.Bytes), nil
}

// UnmarshalCBOR reads the UUID.
func (value *UuidValue) UnmarshalCBOR(item cborform.Value) error {
	var payload []byte
	if err := fromValueInto(item, &payload); err != nil {
		return err
	}
	value.Bytes = payload
	return nil
}

// LogicalTypeKind is a non-parameterized logical type identity, used in
// capability descriptors.
type LogicalTypeKind string

const (
	KindBoolean           LogicalTypeKind = "boolean"
	KindInt               LogicalTypeKind = "int"
	KindLong              LogicalTypeKind = "long"
	KindFloat             LogicalTypeKind = "float"
	KindDouble            LogicalTypeKind = "double"
	KindDecimal           LogicalTypeKind = "decimal"
	KindDate              LogicalTypeKind = "date"
	KindTimeMicros        LogicalTypeKind = "time_micros"
	KindTimestampMicros   LogicalTypeKind = "timestamp_micros"
	KindTimestampTzMicros LogicalTypeKind = "timestamp_tz_micros"
	KindString            LogicalTypeKind = "string"
	KindUUID              LogicalTypeKind = "uuid"
	KindFixed             LogicalTypeKind = "fixed"
	KindBinary            LogicalTypeKind = "binary"
	KindStruct            LogicalTypeKind = "struct"
	KindList              LogicalTypeKind = "list"
	KindMap               LogicalTypeKind = "map"
	// KindVariant accepts any canonical value, each carrying its own tag,
	// for a field declared without a type.
	KindVariant LogicalTypeKind = "variant"
)

// LogicalType is a field's type. It is internally tagged, so a parameterized
// type carries its parameters alongside the kind.
type LogicalType struct {
	Kind LogicalTypeKind
	// Decimal parameters.
	Precision uint8
	Scale     uint8
	// Fixed parameter.
	Length uint32
	// Struct parameter.
	Fields []LogicalField
	// List parameters.
	ElementID       uint32
	ElementRequired bool
	Element         *LogicalType
	// Map parameters.
	KeyID         uint32
	Key           *LogicalType
	ValueID       uint32
	ValueRequired bool
	Value         *LogicalType
}

// The scalar logical types.
func BooleanType() LogicalType { return LogicalType{Kind: KindBoolean} }
func IntType() LogicalType     { return LogicalType{Kind: KindInt} }
func LongType() LogicalType    { return LogicalType{Kind: KindLong} }
func FloatType() LogicalType   { return LogicalType{Kind: KindFloat} }
func DoubleType() LogicalType  { return LogicalType{Kind: KindDouble} }
func DateType() LogicalType    { return LogicalType{Kind: KindDate} }
func StringType() LogicalType  { return LogicalType{Kind: KindString} }
func UUIDType() LogicalType    { return LogicalType{Kind: KindUUID} }
func BinaryType() LogicalType  { return LogicalType{Kind: KindBinary} }
func TimeType() LogicalType    { return LogicalType{Kind: KindTimeMicros} }
func TimestampType() LogicalType {
	return LogicalType{Kind: KindTimestampMicros}
}
func TimestampTzType() LogicalType {
	return LogicalType{Kind: KindTimestampTzMicros}
}

// DecimalType is an exact decimal of a precision and scale.
func DecimalType(precision, scale uint8) LogicalType {
	return LogicalType{Kind: KindDecimal, Precision: precision, Scale: scale}
}

// FixedType is a fixed-width binary type.
func FixedType(length uint32) LogicalType {
	return LogicalType{Kind: KindFixed, Length: length}
}

// StructType nests a field set.
func StructType(fields ...LogicalField) LogicalType {
	return LogicalType{Kind: KindStruct, Fields: fields}
}

// ListType is a list of an element type.
func ListType(elementID uint32, elementRequired bool, element LogicalType) LogicalType {
	return LogicalType{
		Kind: KindList, ElementID: elementID,
		ElementRequired: elementRequired, Element: &element,
	}
}

// MapType is a map from a key type to a value type.
func MapType(keyID uint32, key LogicalType, valueID uint32, valueRequired bool, value LogicalType) LogicalType {
	return LogicalType{
		Kind: KindMap, KeyID: keyID, Key: &key,
		ValueID: valueID, ValueRequired: valueRequired, Value: &value,
	}
}

type decimalTypeFields struct {
	Precision uint8 `cbor:"precision"`
	Scale     uint8 `cbor:"scale"`
}

type fixedTypeFields struct {
	Length uint32 `cbor:"length"`
}

type structTypeFields struct {
	Fields []LogicalField `cbor:"fields"`
}

type listTypeFields struct {
	ElementID       uint32       `cbor:"element_id"`
	ElementRequired bool         `cbor:"element_required"`
	Element         *LogicalType `cbor:"element"`
}

type mapTypeFields struct {
	KeyID         uint32       `cbor:"key_id"`
	Key           *LogicalType `cbor:"key"`
	ValueID       uint32       `cbor:"value_id"`
	ValueRequired bool         `cbor:"value_required"`
	Value         *LogicalType `cbor:"value"`
}

// MarshalCBOR writes the internally tagged type.
func (logicalType LogicalType) MarshalCBOR() (cborform.Value, error) {
	kind := string(logicalType.Kind)
	switch logicalType.Kind {
	case KindBoolean, KindInt, KindLong, KindFloat, KindDouble, KindDate,
		KindTimeMicros, KindTimestampMicros, KindTimestampTzMicros,
		KindString, KindUUID, KindBinary, KindVariant:
		return cborform.Map{{
			Key:   cborform.Text(internalTagKey),
			Value: cborform.Text(kind),
		}}, nil
	case KindDecimal:
		return marshalInternallyTagged(kind, decimalTypeFields{
			Precision: logicalType.Precision, Scale: logicalType.Scale,
		})
	case KindFixed:
		return marshalInternallyTagged(kind, fixedTypeFields{Length: logicalType.Length})
	case KindStruct:
		return marshalInternallyTagged(kind, structTypeFields{Fields: logicalType.Fields})
	case KindList:
		if logicalType.Element == nil {
			return nil, fmt.Errorf("wire: a list type names no element")
		}
		return marshalInternallyTagged(kind, listTypeFields{
			ElementID:       logicalType.ElementID,
			ElementRequired: logicalType.ElementRequired,
			Element:         logicalType.Element,
		})
	case KindMap:
		if logicalType.Key == nil || logicalType.Value == nil {
			return nil, fmt.Errorf("wire: a map type names no key or value")
		}
		return marshalInternallyTagged(kind, mapTypeFields{
			KeyID: logicalType.KeyID, Key: logicalType.Key,
			ValueID: logicalType.ValueID, ValueRequired: logicalType.ValueRequired,
			Value: logicalType.Value,
		})
	default:
		return nil, fmt.Errorf("wire: unknown logical type %q", kind)
	}
}

// UnmarshalCBOR reads the internally tagged type.
func (logicalType *LogicalType) UnmarshalCBOR(item cborform.Value) error {
	kind, entries, err := readInternalTag(item)
	if err != nil {
		return err
	}
	decoded := LogicalType{Kind: LogicalTypeKind(kind)}

	switch decoded.Kind {
	case KindBoolean, KindInt, KindLong, KindFloat, KindDouble, KindDate,
		KindTimeMicros, KindTimestampMicros, KindTimestampTzMicros,
		KindString, KindUUID, KindBinary, KindVariant:
	case KindDecimal:
		var fields decimalTypeFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		decoded.Precision, decoded.Scale = fields.Precision, fields.Scale
	case KindFixed:
		var fields fixedTypeFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		decoded.Length = fields.Length
	case KindStruct:
		var fields structTypeFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		decoded.Fields = fields.Fields
	case KindList:
		var fields listTypeFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		decoded.ElementID = fields.ElementID
		decoded.ElementRequired = fields.ElementRequired
		decoded.Element = fields.Element
	case KindMap:
		var fields mapTypeFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		decoded.KeyID, decoded.Key = fields.KeyID, fields.Key
		decoded.ValueID, decoded.ValueRequired = fields.ValueID, fields.ValueRequired
		decoded.Value = fields.Value
	default:
		return fmt.Errorf("wire: unknown logical type %q", kind)
	}

	*logicalType = decoded
	return nil
}

// LogicalField is one field of a logical schema.
type LogicalField struct {
	ID        uint32      `cbor:"id"`
	Name      string      `cbor:"name"`
	Required  bool        `cbor:"required"`
	FieldType LogicalType `cbor:"field_type"`
	Doc       *string     `cbor:"doc,omitempty"`
}

// LogicalSchemaRef names a schema family, version and fingerprint.
type LogicalSchemaRef struct {
	ID          LogicalSchemaID   `cbor:"id"`
	Version     uint32            `cbor:"version"`
	Fingerprint SchemaFingerprint `cbor:"fingerprint"`
}

// Validate checks the reference's identity and fingerprint width.
func (ref LogicalSchemaRef) Validate() error {
	if ref.ID.IsZero() {
		return &InvalidError{Reason: "logical schema id must be nonzero"}
	}
	if ref.Version == 0 {
		return &InvalidError{Reason: "logical schema version must be nonzero"}
	}
	return ref.Fingerprint.Validate()
}

// LogicalSchema is a schema reference with its field set.
type LogicalSchema struct {
	Schema LogicalSchemaRef `cbor:"schema"`
	Fields []LogicalField   `cbor:"fields"`
}

// schemaFingerprintDomain prefixes the canonical bytes, so a fingerprint can
// never be confused with another structure's digest.
var schemaFingerprintDomain = []byte("AGDX-SCHEMA-V1\x00")

// CanonicalFingerprintBytes is the exact byte sequence a schema's fingerprint
// is taken over.
//
// Every field is written big-endian with explicit lengths, so the digest is
// independent of the CBOR encoding and identical in every port.
func (schema LogicalSchema) CanonicalFingerprintBytes() ([]byte, error) {
	if err := schema.validateShape(); err != nil {
		return nil, err
	}
	bytes := make([]byte, 0, 64)
	bytes = append(bytes, schemaFingerprintDomain...)
	id := schema.Schema.ID.Bytes()
	bytes = append(bytes, id[:]...)
	bytes = binary.BigEndian.AppendUint32(bytes, schema.Schema.Version)
	bytes, err := appendCanonicalFields(bytes, schema.Fields)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

// ComputeFingerprint is the SHA-256 of the canonical bytes.
func (schema LogicalSchema) ComputeFingerprint() (SchemaFingerprint, error) {
	canonical, err := schema.CanonicalFingerprintBytes()
	if err != nil {
		return SchemaFingerprint{}, err
	}
	digest := sha256.Sum256(canonical)
	return NewSchemaFingerprint(digest), nil
}

// Validate checks the schema's shape and that its stored fingerprint matches
// its canonical bytes, so a schema cannot claim an identity it does not have.
func (schema LogicalSchema) Validate() error {
	if err := schema.validateShape(); err != nil {
		return err
	}
	computed, err := schema.ComputeFingerprint()
	if err != nil {
		return err
	}
	if !schema.Schema.Fingerprint.Equal(computed) {
		return &InvalidError{
			Reason: "logical schema fingerprint does not match its canonical bytes",
		}
	}
	return nil
}

func (schema LogicalSchema) validateShape() error {
	if schema.Schema.ID.IsZero() {
		return &InvalidError{Reason: "logical schema id must be nonzero"}
	}
	if schema.Schema.Version == 0 {
		return &InvalidError{Reason: "logical schema version must be nonzero"}
	}
	if len(schema.Fields) == 0 {
		return &InvalidError{Reason: "logical schema must contain a field"}
	}
	return nil
}

// appendCanonicalFields writes a field set: a big-endian count, then each
// field's id, required flag, name, optional doc and type.
func appendCanonicalFields(bytes []byte, fields []LogicalField) ([]byte, error) {
	if uint64(len(fields)) > 0xffff_ffff {
		return nil, &InvalidError{Reason: "logical schema field count does not fit u32"}
	}
	bytes = binary.BigEndian.AppendUint32(bytes, uint32(len(fields)))
	for _, field := range fields {
		bytes = binary.BigEndian.AppendUint32(bytes, field.ID)
		bytes = append(bytes, boolByte(field.Required))
		var err error
		if bytes, err = appendCanonicalText(bytes, field.Name); err != nil {
			return nil, err
		}
		if field.Doc != nil {
			bytes = append(bytes, 1)
			if bytes, err = appendCanonicalText(bytes, *field.Doc); err != nil {
				return nil, err
			}
		} else {
			bytes = append(bytes, 0)
		}
		if bytes, err = appendCanonicalType(bytes, field.FieldType); err != nil {
			return nil, err
		}
	}
	return bytes, nil
}

func appendCanonicalText(bytes []byte, value string) ([]byte, error) {
	if uint64(len(value)) > 0xffff_ffff {
		return nil, &InvalidError{Reason: "logical schema text length does not fit u32"}
	}
	bytes = binary.BigEndian.AppendUint32(bytes, uint32(len(value)))
	return append(bytes, value...), nil
}

func boolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}

// canonicalTypeCodes are the pinned discriminators of the canonical encoding.
// They are independent of the wire names and must never be renumbered.
var canonicalTypeCodes = map[LogicalTypeKind]byte{
	KindBoolean:           0,
	KindInt:               1,
	KindLong:              2,
	KindFloat:             3,
	KindDouble:            4,
	KindDecimal:           5,
	KindDate:              6,
	KindTimeMicros:        7,
	KindTimestampMicros:   8,
	KindTimestampTzMicros: 9,
	KindString:            10,
	KindUUID:              11,
	KindFixed:             12,
	KindBinary:            13,
	KindStruct:            14,
	KindList:              15,
	KindMap:               16,
	KindVariant:           17,
}

func appendCanonicalType(bytes []byte, logicalType LogicalType) ([]byte, error) {
	code, known := canonicalTypeCodes[logicalType.Kind]
	if !known {
		return nil, &InvalidError{Reason: fmt.Sprintf(
			"unknown logical type %q", string(logicalType.Kind))}
	}

	switch logicalType.Kind {
	case KindDecimal:
		return append(bytes, code, logicalType.Precision, logicalType.Scale), nil
	case KindFixed:
		bytes = append(bytes, code)
		return binary.BigEndian.AppendUint32(bytes, logicalType.Length), nil
	case KindStruct:
		bytes = append(bytes, code)
		return appendCanonicalFields(bytes, logicalType.Fields)
	case KindList:
		if logicalType.Element == nil {
			return nil, &InvalidError{Reason: "a list type names no element"}
		}
		bytes = append(bytes, code)
		bytes = binary.BigEndian.AppendUint32(bytes, logicalType.ElementID)
		bytes = append(bytes, boolByte(logicalType.ElementRequired))
		return appendCanonicalType(bytes, *logicalType.Element)
	case KindMap:
		if logicalType.Key == nil || logicalType.Value == nil {
			return nil, &InvalidError{Reason: "a map type names no key or value"}
		}
		bytes = append(bytes, code)
		bytes = binary.BigEndian.AppendUint32(bytes, logicalType.KeyID)
		var err error
		if bytes, err = appendCanonicalType(bytes, *logicalType.Key); err != nil {
			return nil, err
		}
		bytes = binary.BigEndian.AppendUint32(bytes, logicalType.ValueID)
		bytes = append(bytes, boolByte(logicalType.ValueRequired))
		return appendCanonicalType(bytes, *logicalType.Value)
	default:
		return append(bytes, code), nil
	}
}

// BinaryValue is a byte payload that rides the wire as a byte string.
//
// It is a named type rather than a bare slice so a binary field keeps its
// shape through the structural encoder, which would otherwise have to guess
// between a byte string and an array of integers.
type BinaryValue []byte

// MarshalCBOR writes the bytes as a byte string.
func (value BinaryValue) MarshalCBOR() (cborform.Value, error) {
	return cborform.Bytes(value), nil
}

// UnmarshalCBOR reads the byte string, accepting the array form a
// human-readable encoding writes.
func (value *BinaryValue) UnmarshalCBOR(item cborform.Value) error {
	var payload []byte
	if err := fromValueInto(item, &payload); err != nil {
		return err
	}
	*value = payload
	return nil
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *LogicalTypeKind) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "logical type kind", KindBoolean, KindInt, KindLong, KindFloat, KindDouble, KindDecimal, KindDate, KindTimeMicros, KindTimestampMicros, KindTimestampTzMicros, KindString, KindUUID, KindFixed, KindBinary, KindStruct, KindList, KindMap, KindVariant)
}
