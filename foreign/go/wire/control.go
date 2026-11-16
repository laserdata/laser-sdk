package wire

import (
	"fmt"
	"math"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// FieldType is a storage-type hint for an indexed field.
//
// It is a hint, not a constraint: the projector stores whatever scalar the
// payload carries either way, and columnar backends use it for real column
// definitions instead of a text fallback.
type FieldType string

const (
	FieldText  FieldType = "text"
	FieldInt   FieldType = "int"
	FieldFloat FieldType = "float"
	FieldBool  FieldType = "bool"
)

// IndexField is one indexed field extracted from the payload at a pointer.
type IndexField struct {
	Name    string `cbor:"name"`
	Pointer string `cbor:"pointer"`
	// FieldType is absent on the wire when unset, so a pre-hint registry
	// decodes unchanged.
	FieldType *FieldType `cbor:"field_type,omitempty"`
}

// NewIndexField declares an indexed field extracted at a pointer.
func NewIndexField(name, pointer string) IndexField {
	return IndexField{Name: name, Pointer: pointer}
}

// TypedIndexField declares an indexed field with a storage-type hint.
func TypedIndexField(name, pointer string, fieldType FieldType) IndexField {
	return IndexField{Name: name, Pointer: pointer, FieldType: &fieldType}
}

// IndexSchema is the indexed fields, and optional vector field, a projection
// extracts.
type IndexSchema struct {
	Fields []IndexField `cbor:"fields"`
	// VectorField is a pointer to a float vector inside the payload.
	VectorField *string `cbor:"vector_field,omitempty"`
	// InlinePayload is whether the projector inlines the payload bytes
	// alongside the row by default. A producer can still override per record,
	// so batch publishers need not stamp the directive on every message.
	InlinePayload bool `cbor:"inline_payload"`
}

// ReservedProjectionIDPrefix is the prefix no projection id may start with.
// Activity keys an index-only binding under it, so it must never name a
// projection.
const ReservedProjectionIDPrefix = "index:"

// JSONSchemaDialect is the one JSON Schema dialect the registry accepts, named
// or implied.
const JSONSchemaDialect = "https://json-schema.org/draft/2020-12/schema"

// ProjectionID is an opaque projection identifier: the stable string a
// producer stamps on the wire and the worker keys its catalog by.
//
// The recommended shape is "<name>.v<version>", such as "reading.v1". It is
// distinct from a schema id, which selects a codec's writer schema rather than
// a materialization rule.
type ProjectionID string

// Validate checks the id is bounded text outside the reserved prefix.
func (id ProjectionID) Validate() error {
	if err := ValidateBoundedText("projection id", string(id), MaxProjectionIDBytes); err != nil {
		return err
	}
	if strings.HasPrefix(string(id), ReservedProjectionIDPrefix) {
		return &InvalidError{Reason: fmt.Sprintf(
			"projection id must not start with %q", ReservedProjectionIDPrefix)}
	}
	return nil
}

// ProjectionKind is what a projection materializes.
//
// It rides as a u8 code, so a future kind flows through an older reader as an
// unrecognized value rather than failing the listing.
type ProjectionKind uint8

const (
	// ProjectionRow materializes queryable rows. The default.
	ProjectionRow ProjectionKind = 0
	// ProjectionGraph materializes a knowledge graph.
	ProjectionGraph ProjectionKind = 1
	// ProjectionRecord materializes every routed record whole: its body,
	// headers and source position, with no extraction.
	ProjectionRecord ProjectionKind = 2
)

// Code is the pinned wire code.
func (kind ProjectionKind) Code() uint8 { return uint8(kind) }

// IsRow reports the default kind, which is omitted on the wire.
func (kind ProjectionKind) IsRow() bool { return kind == ProjectionRow }

// IsRecognized reports whether this build names the code.
func (kind ProjectionKind) IsRecognized() bool {
	return kind == ProjectionRow || kind == ProjectionGraph || kind == ProjectionRecord
}

// MarshalCBOR writes the u8 code.
func (kind ProjectionKind) MarshalCBOR() (cborform.Value, error) {
	return cborform.Uint(uint64(kind)), nil
}

// UnmarshalCBOR reads the u8 code, carrying an unknown one through.
func (kind *ProjectionKind) UnmarshalCBOR(item cborform.Value) error {
	number, ok := item.(cborform.Uint)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a projection kind", item)
	}
	if number > math.MaxUint8 {
		return fmt.Errorf("wire: projection kind %d exceeds a u8", number)
	}
	*kind = ProjectionKind(number)
	return nil
}

// NodeExtract is one node-extraction rule: the node's label and the pointer to
// its canonical value, which content-addresses its id so the same entity
// converges.
type NodeExtract struct {
	Label            string  `cbor:"label"`
	ValuePointer     string  `cbor:"value_pointer"`
	EmbeddingPointer *string `cbor:"embedding_pointer,omitempty"`
}

// EdgeExtract is one edge-extraction rule: the edge type, the pointers to its
// endpoint values, and optional pointers to the valid-time window.
type EdgeExtract struct {
	EdgeType    string `cbor:"edge_type"`
	FromPointer string `cbor:"from_pointer"`
	ToPointer   string `cbor:"to_pointer"`
	// ValidFromPointer points at the valid-time start, making the extracted
	// edge a bitemporal fact. Absent leaves the window open.
	ValidFromPointer *string `cbor:"valid_from_pointer,omitempty"`
	// ValidToPointer points at the valid-time end. Absent leaves the edge
	// open-ended, so still valid.
	ValidToPointer *string `cbor:"valid_to_pointer,omitempty"`
}

// EntitySchema is the node and edge extraction plan of a graph projection.
type EntitySchema struct {
	Nodes []NodeExtract `cbor:"nodes"`
	Edges []EdgeExtract `cbor:"edges,omitempty"`
}

// ProjectionKeyKind discriminates where a keyed projection reads its key.
type ProjectionKeyKind string

const (
	// ProjectionKeyHeader reads the key from a named record header.
	ProjectionKeyHeader ProjectionKeyKind = "header"
	// ProjectionKeyPointer reads the key from the payload at a JSON pointer.
	ProjectionKeyPointer ProjectionKeyKind = "pointer"
)

// ProjectionKey says where a keyed projection reads the key that identifies a
// row. It is internally tagged, so the kind rides alongside its one field.
type ProjectionKey struct {
	Kind ProjectionKeyKind
	// Name is the header name, set when the kind reads a header.
	Name string
	// Pointer is the RFC 6901 pointer, set when the kind reads the payload.
	Pointer string
}

// HeaderProjectionKey reads the key from a named header.
func HeaderProjectionKey(name string) ProjectionKey {
	return ProjectionKey{Kind: ProjectionKeyHeader, Name: name}
}

// PointerProjectionKey reads the key from the payload at a pointer.
func PointerProjectionKey(pointer string) ProjectionKey {
	return ProjectionKey{Kind: ProjectionKeyPointer, Pointer: pointer}
}

type headerProjectionKeyFields struct {
	Name string `cbor:"name"`
}

type pointerProjectionKeyFields struct {
	Pointer string `cbor:"pointer"`
}

// MarshalCBOR writes the internally tagged key.
func (key ProjectionKey) MarshalCBOR() (cborform.Value, error) {
	switch key.Kind {
	case ProjectionKeyHeader:
		return marshalInternallyTagged(string(ProjectionKeyHeader),
			headerProjectionKeyFields{Name: key.Name})
	case ProjectionKeyPointer:
		return marshalInternallyTagged(string(ProjectionKeyPointer),
			pointerProjectionKeyFields{Pointer: key.Pointer})
	default:
		return nil, fmt.Errorf("wire: unknown projection key kind %q", string(key.Kind))
	}
}

// UnmarshalCBOR reads the internally tagged key.
func (key *ProjectionKey) UnmarshalCBOR(item cborform.Value) error {
	kind, entries, err := readInternalTag(item)
	if err != nil {
		return err
	}
	switch ProjectionKeyKind(kind) {
	case ProjectionKeyHeader:
		var fields headerProjectionKeyFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*key = HeaderProjectionKey(fields.Name)
		return nil
	case ProjectionKeyPointer:
		var fields pointerProjectionKeyFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*key = PointerProjectionKey(fields.Pointer)
		return nil
	default:
		return fmt.Errorf("wire: unknown projection key kind %q", kind)
	}
}

// ProjectionHistoryKind discriminates how a projection treats repeated keys.
type ProjectionHistoryKind string

const (
	// HistoryAppend keeps every record as its own row. The default.
	HistoryAppend ProjectionHistoryKind = "append"
	// HistoryKeyed keeps one row per key, replaced as new records arrive.
	HistoryKeyed ProjectionHistoryKind = "keyed"
)

// ProjectionHistory says whether a projection appends every record or keeps
// one row per key. It is internally tagged.
type ProjectionHistory struct {
	Kind ProjectionHistoryKind
	// Key says where a keyed projection reads its key.
	Key ProjectionKey
	// DeleteHeader is the header whose presence deletes the record's key.
	// Absent, the view never deletes.
	DeleteHeader *string
}

// AppendHistory keeps every record as its own row.
func AppendHistory() ProjectionHistory {
	return ProjectionHistory{Kind: HistoryAppend}
}

// KeyedHistory keeps one row per key, optionally deleting on a header.
func KeyedHistory(key ProjectionKey, deleteHeader *string) ProjectionHistory {
	return ProjectionHistory{Kind: HistoryKeyed, Key: key, DeleteHeader: deleteHeader}
}

// IsAppend reports the default history, which is omitted on the wire.
func (history ProjectionHistory) IsAppend() bool {
	return history.Kind == HistoryAppend || history.Kind == ""
}

// IsDefaultValue reports the named default, which omitempty cannot see.
func (history ProjectionHistory) IsDefaultValue() bool { return history.IsAppend() }

type keyedHistoryFields struct {
	Key          ProjectionKey `cbor:"key"`
	DeleteHeader *string       `cbor:"delete_header,omitempty"`
}

// MarshalCBOR writes the internally tagged history.
func (history ProjectionHistory) MarshalCBOR() (cborform.Value, error) {
	switch history.Kind {
	case HistoryAppend, "":
		return cborform.Map{{
			Key:   cborform.Text(internalTagKey),
			Value: cborform.Text(string(HistoryAppend)),
		}}, nil
	case HistoryKeyed:
		return marshalInternallyTagged(string(HistoryKeyed), keyedHistoryFields{
			Key:          history.Key,
			DeleteHeader: history.DeleteHeader,
		})
	default:
		return nil, fmt.Errorf("wire: unknown projection history kind %q", string(history.Kind))
	}
}

// UnmarshalCBOR reads the internally tagged history.
func (history *ProjectionHistory) UnmarshalCBOR(item cborform.Value) error {
	kind, entries, err := readInternalTag(item)
	if err != nil {
		return err
	}
	switch ProjectionHistoryKind(kind) {
	case HistoryAppend:
		*history = AppendHistory()
		return nil
	case HistoryKeyed:
		var fields keyedHistoryFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*history = KeyedHistory(fields.Key, fields.DeleteHeader)
		return nil
	default:
		return fmt.Errorf("wire: unknown projection history kind %q", kind)
	}
}

// Projection is a reusable projection definition: the extraction rules that
// turn a payload into a queryable row.
//
// It is not attached to a topic on its own. Bindings declare where
// projections may apply.
type Projection struct {
	// ID is the stable id used on the wire as the projection reference.
	ID ProjectionID `cbor:"id"`
	// Name is human-readable, distinct from the id so the same logical
	// projection can rename without breaking the wire reference.
	Name string `cbor:"name"`
	// Version is the schema-evolution version, bumped on incompatible
	// changes.
	Version uint32         `cbor:"version"`
	Kind    ProjectionKind `cbor:"kind,omitempty"`
	// History says whether the projection appends every record or keeps one
	// row per key.
	History ProjectionHistory `cbor:"history,omitempty"`
	// ContentType is the expected payload codec. The any codec means
	// best-effort decode, so the plan tolerates an opaque payload.
	ContentType ContentType `cbor:"content_type"`
	Extraction  IndexSchema `cbor:"extraction"`
	// EntitySchema is the node and edge plan of a graph projection, absent
	// for a row projection.
	EntitySchema *EntitySchema `cbor:"entity_schema,omitempty"`
	// InlinePayloadDefault is the default for records routed through this
	// projection, overridable per record.
	InlinePayloadDefault bool `cbor:"inline_payload_default"`
}

// RetentionPolicyKind is how long a binding's rows live.
type RetentionPolicyKind string

const (
	// RetentionMirrorLog follows the log: rows are pruned once the messages
	// that produced them are dropped, and the projection is deleted when the
	// source topic is. The default.
	RetentionMirrorLog RetentionPolicyKind = "mirror_log"
	// RetentionKeep keeps rows forever, regardless of the source log or its
	// deletion.
	RetentionKeep RetentionPolicyKind = "keep"
	// RetentionKeepUntilSourceDeleted keeps rows while the source topic
	// exists but drops the projection when that topic is deleted.
	RetentionKeepUntilSourceDeleted RetentionPolicyKind = "keep_until_source_deleted"
	// RetentionTimeToLive keeps rows for a time after they were
	// materialized, independent of the log.
	RetentionTimeToLive RetentionPolicyKind = "time_to_live"
	// RetentionMaxRows keeps the newest rows for the target table,
	// independent of the log.
	RetentionMaxRows RetentionPolicyKind = "max_rows"
	// RetentionUnknown is a policy a newer server named that this build does
	// not know.
	//
	// The decode degrades here rather than failing the whole reply, so an
	// older client keeps reading bindings it cannot fully interpret. It must
	// not re-apply one: re-serializing loses the original kind and its
	// fields.
	RetentionUnknown RetentionPolicyKind = "unknown"
)

// RetentionPolicy is a binding's row lifetime. It is internally tagged, so a
// parameterized policy carries its bound alongside the kind.
type RetentionPolicy struct {
	Kind      RetentionPolicyKind
	TTLMicros uint64
	Rows      uint64
}

// The retention constructors.
func MirrorLogRetention() RetentionPolicy {
	return RetentionPolicy{Kind: RetentionMirrorLog}
}
func KeepRetention() RetentionPolicy { return RetentionPolicy{Kind: RetentionKeep} }
func KeepUntilSourceDeletedRetention() RetentionPolicy {
	return RetentionPolicy{Kind: RetentionKeepUntilSourceDeleted}
}
func TimeToLiveRetention(ttlMicros uint64) RetentionPolicy {
	return RetentionPolicy{Kind: RetentionTimeToLive, TTLMicros: ttlMicros}
}
func MaxRowsRetention(rows uint64) RetentionPolicy {
	return RetentionPolicy{Kind: RetentionMaxRows, Rows: rows}
}

type ttlRetentionFields struct {
	TTLMicros uint64 `cbor:"ttl_micros"`
}

type maxRowsRetentionFields struct {
	Rows uint64 `cbor:"rows"`
}

// MarshalCBOR writes the internally tagged policy.
func (policy RetentionPolicy) MarshalCBOR() (cborform.Value, error) {
	switch policy.Kind {
	case RetentionMirrorLog, RetentionKeep, RetentionKeepUntilSourceDeleted:
		return cborform.Map{{
			Key:   cborform.Text(internalTagKey),
			Value: cborform.Text(string(policy.Kind)),
		}}, nil
	case RetentionTimeToLive:
		return marshalInternallyTagged(string(RetentionTimeToLive),
			ttlRetentionFields{TTLMicros: policy.TTLMicros})
	case RetentionMaxRows:
		return marshalInternallyTagged(string(RetentionMaxRows),
			maxRowsRetentionFields{Rows: policy.Rows})
	default:
		return nil, fmt.Errorf(
			"wire: a retention policy this build does not know must not be re-applied, got %q",
			string(policy.Kind))
	}
}

// UnmarshalCBOR reads the internally tagged policy, degrading an unknown kind
// to the passthrough rather than failing the whole reply.
func (policy *RetentionPolicy) UnmarshalCBOR(item cborform.Value) error {
	kind, entries, err := readInternalTag(item)
	if err != nil {
		return err
	}
	switch RetentionPolicyKind(kind) {
	case RetentionMirrorLog, RetentionKeep, RetentionKeepUntilSourceDeleted:
		*policy = RetentionPolicy{Kind: RetentionPolicyKind(kind)}
		return nil
	case RetentionTimeToLive:
		var fields ttlRetentionFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*policy = TimeToLiveRetention(fields.TTLMicros)
		return nil
	case RetentionMaxRows:
		var fields maxRowsRetentionFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*policy = MaxRowsRetention(fields.Rows)
		return nil
	default:
		*policy = RetentionPolicy{Kind: RetentionUnknown}
		return nil
	}
}

// SourceSelector names the stream and topic a binding consumes.
type SourceSelector struct {
	Stream string `cbor:"stream"`
	Topic  string `cbor:"topic"`
}

// NewSourceSelector names a binding's source.
func NewSourceSelector(stream, topic string) SourceSelector {
	return SourceSelector{Stream: stream, Topic: topic}
}

// ProjectionBinding declares where a projection may materialize.
//
// Bindings, not projections, tell the worker what to consume: a binding pairs
// a source selector with the projections allowed to fire on it, an optional
// default, and one operational index.
type ProjectionBinding struct {
	Source SourceSelector `cbor:"source"`
	// AllowedProjections are the projections allowed to fire on records from
	// this source. A record stamped with a reference outside this set is
	// dead-lettered or skipped, per worker policy.
	AllowedProjections []ProjectionID `cbor:"allowed_projections"`
	// DefaultProjection applies to records carrying no reference. Absent
	// means such records are skipped.
	DefaultProjection *ProjectionID `cbor:"default_projection"`
	// Backend pins an exact operational backend generation. Absent selects
	// the replica-local backend.
	Backend *BackendBinding `cbor:"backend,omitempty"`
	// Index is the operational index name an operational query target serves.
	Index string `cbor:"index"`
	// Notify opts this binding into the change feed, so each committed
	// projector batch publishes one change record. Default off and skipped on
	// the wire, so a pre-feed binding encodes byte-identically.
	Notify bool `cbor:"notify,omitempty"`
	// Retention is the row lifetime, independent of the source topic's own
	// retention. Absent inherits the deployment default.
	Retention *RetentionPolicy `cbor:"retention,omitempty"`
	// InvalidRecords says what happens to a record the destination or the
	// extraction deterministically rejects. Blocking, the default, holds the
	// partition at the record until an operator skips it. Dead-lettering
	// skips it through an ordered control record, so every replica skips the
	// same record, and sends it to the dead-letter topic. A destination
	// rejection counts only after the destination accepted other records of
	// the batch and refused this one twice.
	InvalidRecords InvalidRecordPolicy `cbor:"invalid_records,omitempty"`
	// Oversize says what a whole-record binding does with a body larger than
	// its backend stores. Blocking, the default, holds the partition at the
	// record. Referencing keeps the row without the body, which the source
	// log still holds, and requires mirror-log retention.
	Oversize OversizePolicy `cbor:"oversize,omitempty"`
}

// InvalidRecordPolicy is what a binding does with a record it proved invalid.
type InvalidRecordPolicy string

const (
	// InvalidRecordBlock holds the partition at the record until an operator
	// skips it. The default.
	InvalidRecordBlock InvalidRecordPolicy = "block"
	// InvalidRecordDeadLetter skips the record and sends it to the
	// dead-letter topic.
	InvalidRecordDeadLetter InvalidRecordPolicy = "dead_letter"
	// InvalidRecordUnknown is a policy a newer server named that this build
	// does not know.
	InvalidRecordUnknown InvalidRecordPolicy = "unknown"
)

// IsBlock reports the default policy, which is omitted on the wire.
func (policy InvalidRecordPolicy) IsBlock() bool {
	return policy == InvalidRecordBlock || policy == ""
}

// IsDefaultValue reports the named default, which omitempty cannot see.
func (policy InvalidRecordPolicy) IsDefaultValue() bool { return policy.IsBlock() }

// UnmarshalCBOR reads the policy, degrading an unknown name to the
// passthrough rather than failing the whole reply.
func (policy *InvalidRecordPolicy) UnmarshalCBOR(item cborform.Value) error {
	name, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as an invalid-record policy", item)
	}
	switch InvalidRecordPolicy(name) {
	case InvalidRecordBlock, InvalidRecordDeadLetter:
		*policy = InvalidRecordPolicy(name)
	default:
		*policy = InvalidRecordUnknown
	}
	return nil
}

// OversizePolicy is what a whole-record binding does with a body its backend
// cannot store.
type OversizePolicy string

const (
	// OversizeBlock holds the partition at the record. The default.
	OversizeBlock OversizePolicy = "block"
	// OversizeReference keeps the row without its body, which the source log
	// still holds.
	OversizeReference OversizePolicy = "reference"
	// OversizeUnknown is a policy a newer server named that this build does
	// not know.
	OversizeUnknown OversizePolicy = "unknown"
)

// IsBlock reports the default policy, which is omitted on the wire.
func (policy OversizePolicy) IsBlock() bool {
	return policy == OversizeBlock || policy == ""
}

// IsDefaultValue reports the named default, which omitempty cannot see.
func (policy OversizePolicy) IsDefaultValue() bool { return policy.IsBlock() }

// UnmarshalCBOR reads the policy, degrading an unknown name to the
// passthrough rather than failing the whole reply.
func (policy *OversizePolicy) UnmarshalCBOR(item cborform.Value) error {
	name, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as an oversize policy", item)
	}
	switch OversizePolicy(name) {
	case OversizeBlock, OversizeReference:
		*policy = OversizePolicy(name)
	default:
		*policy = OversizeUnknown
	}
	return nil
}

// SchemaSourceKind discriminates a registered writer schema's payload.
type SchemaSourceKind string

const (
	// SchemaAvro is an Avro writer schema as its canonical JSON text.
	SchemaAvro SchemaSourceKind = "avro"
	// SchemaProtobuf is a descriptor set plus the message type to decode.
	SchemaProtobuf SchemaSourceKind = "protobuf"
	// SchemaJSON is a JSON Schema, validation-only: a self-describing record
	// stamping this id has its decoded payload validated.
	SchemaJSON SchemaSourceKind = "json_schema"
	// SchemaUnknown is a kind a newer server named that this build does not
	// know. The decode degrades here rather than failing, so an older client
	// can still read a registry holding a source it cannot decode against. It
	// must not re-register one: the original kind and its fields are lost.
	SchemaUnknown SchemaSourceKind = "unknown"
)

// SchemaSource is the schema payload for a schema-first codec.
type SchemaSource struct {
	Kind SchemaSourceKind
	// Schema is the Avro or JSON Schema text.
	Schema string
	// DescriptorSet and MessageType carry the Protobuf form.
	DescriptorSet []byte
	MessageType   string
}

// The schema source constructors.
func AvroSchema(schema string) SchemaSource {
	return SchemaSource{Kind: SchemaAvro, Schema: schema}
}
func ProtobufSchema(descriptorSet []byte, messageType string) SchemaSource {
	return SchemaSource{
		Kind: SchemaProtobuf, DescriptorSet: descriptorSet, MessageType: messageType,
	}
}
func JSONSchema(schema string) SchemaSource {
	return SchemaSource{Kind: SchemaJSON, Schema: schema}
}

type avroSchemaFields struct {
	Schema string `cbor:"schema"`
}

type protobufSchemaFields struct {
	DescriptorSet []byte `cbor:"descriptor_set"`
	MessageType   string `cbor:"message_type"`
}

// MarshalCBOR writes the internally tagged source.
func (source SchemaSource) MarshalCBOR() (cborform.Value, error) {
	switch source.Kind {
	case SchemaAvro, SchemaJSON:
		return marshalInternallyTagged(string(source.Kind),
			avroSchemaFields{Schema: source.Schema})
	case SchemaProtobuf:
		return marshalInternallyTagged(string(SchemaProtobuf), protobufSchemaFields{
			DescriptorSet: source.DescriptorSet, MessageType: source.MessageType,
		})
	default:
		return nil, fmt.Errorf(
			"wire: a schema source this build does not know must not be re-registered, got %q",
			string(source.Kind))
	}
}

// UnmarshalCBOR reads the internally tagged source, degrading an unknown kind
// to the passthrough.
func (source *SchemaSource) UnmarshalCBOR(item cborform.Value) error {
	kind, entries, err := readInternalTag(item)
	if err != nil {
		return err
	}
	switch SchemaSourceKind(kind) {
	case SchemaAvro, SchemaJSON:
		var fields avroSchemaFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*source = SchemaSource{Kind: SchemaSourceKind(kind), Schema: fields.Schema}
		return nil
	case SchemaProtobuf:
		var fields protobufSchemaFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*source = ProtobufSchema(fields.DescriptorSet, fields.MessageType)
		return nil
	default:
		*source = SchemaSource{Kind: SchemaUnknown}
		return nil
	}
}

// SchemaDef is a registered writer schema, keyed by the id a producer stamps
// on the schema header.
//
// Avro and Protobuf schemas decode their schema-first bodies, where a JSON
// Schema validates the decoded payload of a self-describing codec. Ids are permanent:
// registering an occupied id with a different definition is rejected, and
// dropping tombstones the definition.
type SchemaDef struct {
	ID     uint32       `cbor:"id"`
	Source SchemaSource `cbor:"source"`
	// Name is pure metadata: stored and returned, never dispatched on.
	Name *string `cbor:"name,omitempty"`
	// Version is caller-tracked metadata, and the id alone selects the decoder.
	Version *uint32 `cbor:"version,omitempty"`
}

// ContentType is the codec this schema applies to. A JSON Schema validates any
// self-describing codec and reports as JSON.
func (definition SchemaDef) ContentType() ContentType {
	switch definition.Source.Kind {
	case SchemaAvro:
		return ContentAvro
	case SchemaProtobuf:
		return ContentProtobuf
	case SchemaJSON:
		return ContentJSON
	default:
		// An unknown source kind from a newer server: best-effort.
		return ContentAny
	}
}

// SchemaAllocation registers a writer schema under the id the control fold
// chooses, rather than one the caller picked.
//
// The record's operation identity keeps the chosen id, so a retry returns it
// rather than allocating a second.
type SchemaAllocation struct {
	Source  SchemaSource `cbor:"source"`
	Name    *string      `cbor:"name,omitempty"`
	Version *uint32      `cbor:"version,omitempty"`
}

// SourceRetention is the backend's observation that one source partition
// incarnation retains no record below a floor.
//
// Skips and accepted gaps below it can never be reached again and leave the
// registry.
type SourceRetention struct {
	Source      SourceSelector `cbor:"source"`
	PartitionID uint32         `cbor:"partition_id"`
	// TopicCreatedAtMicros is the source topic incarnation the floor belongs
	// to.
	TopicCreatedAtMicros uint64 `cbor:"topic_created_at_micros"`
	// FirstOffset is the offset below which no record can be read again. It
	// is the maximum u64 when the incarnation is gone.
	FirstOffset uint64 `cbor:"first_offset"`
}

// RecordSkip is one record a binding skips, on one topic incarnation.
type RecordSkip struct {
	Source      SourceSelector `cbor:"source"`
	PartitionID uint32         `cbor:"partition_id"`
	// TopicCreatedAtMicros is the source topic incarnation. A recreated topic
	// never inherits a skip.
	TopicCreatedAtMicros uint64 `cbor:"topic_created_at_micros"`
	Offset               uint64 `cbor:"offset"`
	// Target is the operational index or graph the record failed to reach.
	// The record's other effects still apply.
	Target string `cbor:"target"`
	// Reason is what the destination or the extraction said about it.
	Reason string `cbor:"reason"`
}

// Covers reports whether this skip covers an exact record of a topic
// incarnation for a target.
func (skip RecordSkip) Covers(stream, topic string, partitionID uint32, generation, offset uint64, target string) bool {
	return skip.Source.Stream == stream &&
		skip.Source.Topic == topic &&
		skip.PartitionID == partitionID &&
		skip.TopicCreatedAtMicros == generation &&
		skip.Offset == offset &&
		skip.Target == target
}

// GapAcceptance is the exact missing range an operator accepted for one source
// partition.
type GapAcceptance struct {
	Source      SourceSelector `cbor:"source"`
	PartitionID uint32         `cbor:"partition_id"`
	// TopicCreatedAtMicros is the incarnation the gap belongs to. A recreated
	// topic needs its own acceptance.
	TopicCreatedAtMicros uint64 `cbor:"topic_created_at_micros"`
	// FromOffset is the first missing offset, the resume point the projection
	// expected.
	FromOffset uint64 `cbor:"from_offset"`
	// ToOffset is the first offset the log could still serve when the gap was
	// accepted.
	ToOffset uint64 `cbor:"to_offset"`
}

// Covers reports whether this acceptance covers a resume at a cursor whose
// first readable record is given, on a topic incarnation.
func (acceptance GapAcceptance) Covers(generation, cursor, firstAvailable uint64) bool {
	return acceptance.TopicCreatedAtMicros == generation &&
		acceptance.FromOffset <= cursor &&
		acceptance.ToOffset >= firstAvailable
}

// SessionTopicsKind discriminates which topics a session-source registration
// covers.
type SessionTopicsKind uint8

const (
	// SessionTopicsAll covers every session topic in the stream.
	SessionTopicsAll SessionTopicsKind = iota
	// SessionTopicsNamed covers the named topics only.
	SessionTopicsNamed
)

// SessionTopics is the topics a session-source registration covers.
type SessionTopics struct {
	Kind   SessionTopicsKind
	Topics []string
}

// AllSessionTopics covers every session topic in the stream.
func AllSessionTopics() SessionTopics { return SessionTopics{Kind: SessionTopicsAll} }

// NamedSessionTopics covers the named topics only.
func NamedSessionTopics(topics ...string) SessionTopics {
	return SessionTopics{Kind: SessionTopicsNamed, Topics: topics}
}

const (
	sessionTopicsAllVariant   = "All"
	sessionTopicsNamedVariant = "Named"
)

// MarshalCBOR writes the externally tagged topics.
func (topics SessionTopics) MarshalCBOR() (cborform.Value, error) {
	switch topics.Kind {
	case SessionTopicsAll:
		return unitVariant(sessionTopicsAllVariant), nil
	case SessionTopicsNamed:
		payload, err := toValueOf(topics.Topics)
		if err != nil {
			return nil, err
		}
		return taggedVariant(sessionTopicsNamedVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: unknown session topics %d", topics.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged topics.
func (topics *SessionTopics) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	switch name {
	case sessionTopicsAllVariant:
		*topics = AllSessionTopics()
		return nil
	case sessionTopicsNamedVariant:
		if err := requirePayload("SessionTopics", name, payload); err != nil {
			return err
		}
		var named []string
		if err := fromValueInto(payload, &named); err != nil {
			return err
		}
		*topics = SessionTopics{Kind: SessionTopicsNamed, Topics: named}
		return nil
	default:
		return unknownVariant("SessionTopics", name)
	}
}

// ControlStreamGeneration is one incarnation of a stream: a deleted and
// recreated stream has a new creation time even when it reuses its name and
// id.
type ControlStreamGeneration struct {
	StreamID        uint32 `cbor:"stream_id"`
	CreatedAtMicros uint64 `cbor:"created_at_micros"`
}

// ControlStamp records who appended a control record and for which resource
// generation.
//
// The backend seals the whole encoded envelope, stamp included, so a record
// cannot be moved to another deployment, actor, operation or stream
// generation.
type ControlStamp struct {
	// DeploymentID is zero for a standalone backend with no deployment
	// identity.
	DeploymentID uint64 `cbor:"deployment_id"`
	// OperationID is the stable identity of the logical control operation. A
	// retry keeps it, so a second append of the same operation applies once.
	OperationID U128 `cbor:"operation_id"`
	// StreamGeneration is present when the request was scoped to a stream.
	StreamGeneration *ControlStreamGeneration `cbor:"stream_generation,omitempty"`
	// KeyID identifies the sealing key without revealing it.
	KeyID uint64 `cbor:"key_id"`
	// Caller is the client that asked for the record, the only actor it
	// names. Absent on a record the backend or the streaming server issues on
	// its own, so the root user, id zero, is never mistaken for them.
	Caller *Caller `cbor:"caller,omitempty"`
}

// validateSchemaText is the shared bound on a registered schema's JSON text.
func validateSchemaText(label, text string) error {
	if len(text) > MaxFilterSchemaBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s exceeds %d bytes", label, MaxFilterSchemaBytes)}
	}
	if !jsonDepthWithin([]byte(text), MaxFilterSchemaDepth) {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s nests deeper than %d", label, MaxFilterSchemaDepth)}
	}
	return nil
}

// Validate checks a registered writer schema's source.
//
// A JSON Schema must also name the one dialect the registry accepts, or none
// at all: a schema written for another draft would validate differently on
// every client that read it.
func (source SchemaSource) Validate() error {
	switch source.Kind {
	case SchemaAvro:
		return validateSchemaText("Avro schema", source.Schema)

	case SchemaJSON:
		if err := validateSchemaText("JSON Schema", source.Schema); err != nil {
			return err
		}
		decoded, fault := decodeJSONPayload([]byte(source.Schema), DefaultDecodeLimits())
		if fault != "" {
			return &InvalidError{Reason: "JSON Schema is not valid JSON"}
		}
		dialect, named := decoded.object["$schema"]
		if !named {
			return nil
		}
		if dialect.kind != payloadText ||
			strings.TrimRight(dialect.text, "#") != JSONSchemaDialect {
			return &InvalidError{Reason: fmt.Sprintf(
				"a JSON Schema uses draft 2020-12, not %s", renderSchemaDialect(dialect))}
		}
		return nil

	case SchemaProtobuf:
		if len(source.DescriptorSet) > MaxFilterSchemaBytes {
			return &InvalidError{Reason: fmt.Sprintf(
				"Protobuf descriptor set exceeds %d bytes", MaxFilterSchemaBytes)}
		}
		return ValidateBoundedText(
			"Protobuf message type", source.MessageType, MaxSchemaNameBytes)

	default:
		return &InvalidError{
			Reason: "a schema source of an unknown kind cannot be registered",
		}
	}
}

// renderSchemaDialect renders the refused dialect the way the message reads
// it, which is as the JSON value it was written as.
func renderSchemaDialect(dialect payloadValue) string {
	return DecodedValue{value: dialect}.JSON()
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *FieldType) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "field type", FieldText, FieldInt, FieldFloat, FieldBool)
}
