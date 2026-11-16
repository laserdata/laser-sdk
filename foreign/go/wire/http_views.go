package wire

import (
	"errors"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// The managed backend's HTTP bodies. Each one is the JSON face of a contract
// type the binary band already carries, so a browser client and an agent see
// the same shapes.

// KvEntryView is one key-value entry on the HTTP surface.
//
// The key and value are URL-safe unpadded base64, because they are arbitrary
// bytes and a JSON string cannot carry those raw.
type KvEntryView struct {
	Key             string  `cbor:"key"`
	Value           string  `cbor:"value"`
	ExpiresAtMicros *uint64 `cbor:"expires_at_micros"`
	// Version is the value a conditional write expects.
	Version uint64 `cbor:"version"`
	// Scope is the memory scope, present only on a memory read-view row, so a
	// client can fold a recalled item back to the message it came from.
	Scope *MemoryRowScope `cbor:"scope,omitempty"`
	// Source is the origin log record this entry was folded from. Every
	// managed write is log-first, so a stored entry points back at the record
	// that wrote it.
	Source *SourceRef `cbor:"source,omitempty"`
}

// KvPageView is one key-value scan page, in key order. Pages count from one.
type KvPageView struct {
	Items        []KvEntryView `cbor:"items"`
	Page         uint32        `cbor:"page"`
	TotalPages   uint32        `cbor:"total_pages"`
	TotalResults uint32        `cbor:"total_results"`
}

// DeletedManyView is how many entries a bulk delete removed.
type DeletedManyView struct {
	Deleted uint64 `cbor:"deleted"`
}

// CasCommittedView is the version a conditional write committed at.
type CasCommittedView struct {
	Version uint64 `cbor:"version"`
}

// PromotedView is how many rows a fork promotion applied.
type PromotedView struct {
	Rows uint64 `cbor:"rows"`
}

// ClientMetadataView is one live connection on the discovery surface.
//
// The metadata is URL-safe unpadded base64 of the opaque advertised bytes, or
// absent when the connection advertised none. A console decodes it per
// producer kind: an agent card, or an application's own blob.
type ClientMetadataView struct {
	ClientID            uint32  `cbor:"client_id"`
	UserID              *uint32 `cbor:"user_id"`
	Transport           uint8   `cbor:"transport"`
	Address             string  `cbor:"address"`
	ConsumerGroupsCount uint32  `cbor:"consumer_groups_count"`
	Metadata            *string `cbor:"metadata,omitempty"`
}

// ClientMetadataListView is one page of the discovery read.
type ClientMetadataListView struct {
	Clients []ClientMetadataView `cbor:"clients"`
	// NextCursor is the continuation for the next page, absent on the last.
	NextCursor *uint32 `cbor:"next_cursor,omitempty"`
}

// GraphNodeView is one graph node on the HTTP surface, with its attributes
// rendered as strings for a client that has no access to the typed values.
type GraphNodeView struct {
	ID     string      `cbor:"id"`
	Labels []string    `cbor:"labels,omitempty"`
	Attrs  []NamedText `cbor:"attrs,omitempty"`
	// Source is where the node was first observed, when that is known.
	Source *SourceRef `cbor:"source,omitempty"`
}

// GraphEdgeView is one graph edge on the HTTP surface.
type GraphEdgeView struct {
	ID       string  `cbor:"id"`
	From     string  `cbor:"from"`
	To       string  `cbor:"to"`
	EdgeType string  `cbor:"edge_type"`
	Weight   float32 `cbor:"weight"`
	// ValidFrom and ValidTo are the valid-time window of a bitemporal edge,
	// omitted when the window is open.
	ValidFrom *uint64 `cbor:"valid_from,omitempty"`
	ValidTo   *uint64 `cbor:"valid_to,omitempty"`
	// Source is what most recently asserted the relationship, when known.
	Source *SourceRef `cbor:"source,omitempty"`
}

// PathView is one path of a graph result: parallel node and edge id
// sequences.
type PathView struct {
	Nodes []string `cbor:"nodes"`
	Edges []string `cbor:"edges"`
}

// GraphResultView is a traversal's reachable nodes, the edges it crossed, and
// the reconstructed paths when the query asked for them.
type GraphResultView struct {
	Nodes []GraphNodeView `cbor:"nodes,omitempty"`
	Edges []GraphEdgeView `cbor:"edges,omitempty"`
	Paths []PathView      `cbor:"paths,omitempty"`
}

// RegisterSchemaBody registers a schema without an id. The backend allocates
// one and the reply carries it back.
type RegisterSchemaBody struct {
	Source  SchemaSource `cbor:"source"`
	Name    *string      `cbor:"name,omitempty"`
	Version *uint32      `cbor:"version,omitempty"`
	// Stream is the registry that allocates the id. Absent uses the
	// deployment-wide registry.
	Stream *string `cbor:"stream,omitempty"`
}

// DecodeRecordBody is a record payload to decode, as URL-safe unpadded
// base64.
type DecodeRecordBody struct {
	Payload string `cbor:"payload"`
}

// ForkCreateBody creates a fork.
type ForkCreateBody struct {
	ForkID string   `cbor:"fork_id"`
	Parent *string  `cbor:"parent,omitempty"`
	Kind   ForkKind `cbor:"kind"`
	Tables []string `cbor:"tables,omitempty"`
}

// ForkPutBody writes one row into a fork. The payload is URL-safe unpadded
// base64, like every binary body on this surface.
type ForkPutBody struct {
	Table             string            `cbor:"table"`
	PartitionID       uint32            `cbor:"partition_id"`
	Offset            uint64            `cbor:"offset"`
	ProjectionID      string            `cbor:"projection_id"`
	ProjectionVersion uint32            `cbor:"projection_version"`
	Fields            map[string]string `cbor:"fields,omitempty"`
	Metadata          map[string]string `cbor:"metadata,omitempty"`
	PayloadBase64     *string           `cbor:"payload_b64,omitempty"`
	Embedding         *string           `cbor:"embedding,omitempty"`
	Tombstone         bool              `cbor:"tombstone"`
}

// RemoveBindingBody names a binding to remove, by its source stream and topic.
//
// A projection reference removes only that projection from the binding and
// leaves the rest. Without one the whole binding for that source goes.
type RemoveBindingBody struct {
	Stream        string  `cbor:"stream"`
	Topic         string  `cbor:"topic"`
	ProjectionRef *string `cbor:"projection_ref,omitempty"`
}

// FilterTestBody evaluates a filter against one supplied sample.
type FilterTestBody struct {
	Filter FilterRef `cbor:"filter"`
	// PayloadBase64 carries a binary sample, which a JSON string cannot hold
	// raw. PayloadText carries one that is already text.
	PayloadBase64 *string        `cbor:"payload_base64,omitempty"`
	PayloadText   *string        `cbor:"payload_text,omitempty"`
	Headers       []FilterHeader `cbor:"headers"`
}

// ErrorBody is what every managed route returns on a failure.
//
// The rule this surface follows is that a success carries the bare payload and
// a failure carries this: the status line gives the coarse class and this body
// gives the machine-dispatchable code plus a human message. A client matches
// on the code rather than grepping the message, which is for people and may
// change.
type ErrorBody struct {
	Code    ResultCode `cbor:"code"`
	Message string     `cbor:"message"`
	// Detail carries optional structured context, such as the conflicting
	// version on a failed conditional write.
	Detail *DynValue `cbor:"detail,omitempty"`
}

// NewErrorBody builds a failure from a classified code and a human message.
func NewErrorBody(code ResultCode, message string) ErrorBody {
	return ErrorBody{Code: code, Message: message}
}

// WithDetail attaches structured context to a failure.
func (body ErrorBody) WithDetail(detail DynValue) ErrorBody {
	body.Detail = &detail
	return body
}

// Error reports the failure, so a decoded body is usable as a Go error.
func (body ErrorBody) Error() string { return body.Message }

// NamedText is one name and its rendered value, the shape the HTTP views carry
// an attribute pair as.
type NamedText struct {
	Name  string
	Value string
}

// MarshalCBOR writes the pair the contract carries.
func (pair NamedText) MarshalCBOR() (cborform.Value, error) {
	return cborform.Array{cborform.Text(pair.Name), cborform.Text(pair.Value)}, nil
}

// UnmarshalCBOR reads the pair.
func (pair *NamedText) UnmarshalCBOR(item cborform.Value) error {
	var fields [2]string
	if err := fromValueInto(item, &fields); err != nil {
		return err
	}
	*pair = NamedText{Name: fields[0], Value: fields[1]}
	return nil
}

// ErrorBodyOf classifies a domain failure as the body the managed surface
// returns.
//
// A caller branches on the code, so the mapping from a failure to its code
// belongs with the failure rather than at each route. Anything that does not
// classify itself is a backend failure, which is the safe default: a client
// retries it rather than treating it as a permanent refusal.
func ErrorBodyOf(failure error) ErrorBody {
	var classifier interface{ ResultCode() ResultCode }
	if !errors.As(failure, &classifier) {
		return NewErrorBody(ResultBackend, failure.Error())
	}
	body := NewErrorBody(classifier.ResultCode(), failure.Error())
	// A filter failure carries the reason it refused, which a caller uses to
	// tell a filter it can fix from one the server cannot evaluate.
	var filterFailure FilterError
	if errors.As(failure, &filterFailure) && filterFailure.Reason != "" {
		body = body.WithDetail(NewDynValue(cborform.Map{{
			Key:   cborform.Text("reason"),
			Value: cborform.Text(filterFailure.Reason),
		}}))
	}
	return body
}
