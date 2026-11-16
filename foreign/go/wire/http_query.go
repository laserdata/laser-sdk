package wire

import (
	"fmt"
	"strings"
)

// The query-parameter names the managed routes read.
//
// They are named here rather than spelled at each call site so the server
// handler and a typed client cannot drift apart on a query string.
const (
	ParamTopic           = "topic"
	ParamNameContains    = "name_contains"
	ParamIDPrefix        = "id_prefix"
	ParamStream          = "stream"
	ParamSearch          = "search"
	ParamPrefix          = "prefix"
	ParamStart           = "start"
	ParamEnd             = "end"
	ParamKeyContains     = "key_contains"
	ParamLimit           = "limit"
	ParamStatus          = "status"
	ParamAgent           = "agent"
	ParamText            = "text"
	ParamRoot            = "root"
	ParamLabelPrefix     = "label_prefix"
	ParamSurface         = "surface"
	ParamAsOf            = "as_of"
	ParamSeries          = "series"
	ParamPage            = "page"
	ParamResults         = "results"
	ParamHistoryLimit    = "history_limit"
	ParamExpiresAtMicros = "expires_at_micros"
	ParamExpectVersion   = "expect_version"
	ParamExpectAbsent    = "expect_absent"
)

// KvExpiresAtMicrosHeader carries an entry's expiry on a write whose body is
// the raw value, where there is no JSON object to put it in.
const KvExpiresAtMicrosHeader = "agdx-expires-at-micros"

// KvVersionHeader carries an entry's version as a decimal string on a raw-body
// read, the value a compare-and-swap expects.
const KvVersionHeader = "agdx-version"

// OnBehalfOfHeader names the end user an intermediary writes for. The
// streaming server forwards it to the backend, which records it beside the
// authenticated user and never trusts it.
const OnBehalfOfHeader = "agdx-on-behalf-of"

// ProjectionListQuery narrows a projection listing.
type ProjectionListQuery struct {
	Topic        *string `cbor:"topic,omitempty"`
	NameContains *string `cbor:"name_contains,omitempty"`
	IDPrefix     *string `cbor:"id_prefix,omitempty"`
	Search       *string `cbor:"search,omitempty"`
	Stream       *string `cbor:"stream,omitempty"`
	Page         *uint32 `cbor:"page,omitempty"`
	Results      *uint32 `cbor:"results,omitempty"`
}

// SchemaListQuery narrows a schema listing.
type SchemaListQuery struct {
	NameContains *string `cbor:"name_contains,omitempty"`
	Stream       *string `cbor:"stream,omitempty"`
	Page         *uint32 `cbor:"page,omitempty"`
	Results      *uint32 `cbor:"results,omitempty"`
}

// SchemaQuery names the registry one schema read resolves against.
type SchemaQuery struct {
	Stream *string `cbor:"stream,omitempty"`
}

// OperationQuery carries the operation identity a managed mutation is
// deduplicated by.
//
// It rides as text because the identity is 128 bits, which JSON cannot hold
// as a number without losing digits.
type OperationQuery struct {
	OperationID *string `cbor:"operation_id,omitempty"`
}

// NewOperationQuery names a mutation's operation identity.
func NewOperationQuery(operationID U128) OperationQuery {
	text := operationID.String()
	return OperationQuery{OperationID: &text}
}

// Identity is the operation identity the query names.
//
// The second result is false when the query names none. A malformed or zero
// identity is an error rather than an absent one: a client that meant to make
// a mutation idempotent must not silently lose that.
func (query OperationQuery) Identity() (U128, bool, error) {
	if query.OperationID == nil || *query.OperationID == "" {
		return U128{}, false, nil
	}
	parsed, err := ParseU128(*query.OperationID)
	if err != nil {
		return U128{}, false, &InvalidError{Reason: fmt.Sprintf(
			"operation id %q is not a 128-bit identity", *query.OperationID)}
	}
	if parsed.IsZero() {
		return U128{}, false, &InvalidError{
			Reason: "operation id must be nonzero"}
	}
	return parsed, true, nil
}

// FilterListQuery narrows and pages a saved-filter listing.
type FilterListQuery struct {
	NameContains *string      `cbor:"name_contains,omitempty"`
	State        *FilterState `cbor:"state,omitempty"`
	// The owner filter is the five fields of a group identity, all or none.
	OwnerStreamID              *uint32 `cbor:"owner_stream_id,omitempty"`
	OwnerStreamCreatedAtMicros *uint64 `cbor:"owner_stream_created_at_micros,omitempty"`
	OwnerTopicID               *uint32 `cbor:"owner_topic_id,omitempty"`
	OwnerTopicCreatedAtMicros  *uint64 `cbor:"owner_topic_created_at_micros,omitempty"`
	OwnerGroupID               *uint64 `cbor:"owner_group_id,omitempty"`
	Page                       *uint32 `cbor:"page,omitempty"`
	Results                    *uint32 `cbor:"results,omitempty"`
}

// FilterRevisionListQuery pages one filter's revisions.
type FilterRevisionListQuery struct {
	Page    *uint32 `cbor:"page,omitempty"`
	Results *uint32 `cbor:"results,omitempty"`
}

// FilterBindingListQuery narrows and pages a binding listing.
type FilterBindingListQuery struct {
	FilterID *uint32 `cbor:"filter_id,omitempty"`
	Stream   *string `cbor:"stream,omitempty"`
	Topic    *string `cbor:"topic,omitempty"`
	Page     *uint32 `cbor:"page,omitempty"`
	Results  *uint32 `cbor:"results,omitempty"`
}

// KvScanQuery narrows and pages a key-value scan.
type KvScanQuery struct {
	Prefix       *string `cbor:"prefix,omitempty"`
	Start        *string `cbor:"start,omitempty"`
	End          *string `cbor:"end,omitempty"`
	KeyContains  *string `cbor:"key_contains,omitempty"`
	Conversation *string `cbor:"conversation,omitempty"`
	Page         *uint32 `cbor:"page,omitempty"`
	Results      *uint32 `cbor:"results,omitempty"`
}

// KvPutQuery carries the expiry of a plain write.
type KvPutQuery struct {
	ExpiresAtMicros *uint64 `cbor:"expires_at_micros,omitempty"`
}

// KvCasQuery carries a conditional write's precondition.
//
// Exactly one of the two preconditions applies: a version the entry must
// currently hold, or the assertion that it does not exist.
type KvCasQuery struct {
	ExpectVersion   *uint64 `cbor:"expect_version,omitempty"`
	ExpectAbsent    *bool   `cbor:"expect_absent,omitempty"`
	ExpiresAtMicros *uint64 `cbor:"expires_at_micros,omitempty"`
}

// Validate checks that the write names exactly one precondition.
func (query KvCasQuery) Validate() error {
	named := 0
	if query.ExpectVersion != nil {
		named++
	}
	if query.ExpectAbsent != nil && *query.ExpectAbsent {
		named++
	}
	if named != 1 {
		return &InvalidError{Reason: "a conditional write names exactly one " +
			"precondition: a version, or that the entry is absent"}
	}
	return nil
}

// GraphNeighborsQuery narrows a one-hop neighbor read.
type GraphNeighborsQuery struct {
	Dir      *string `cbor:"dir,omitempty"`
	EdgeType *string `cbor:"edge_type,omitempty"`
	Depth    *uint32 `cbor:"depth,omitempty"`
	Limit    *uint64 `cbor:"limit,omitempty"`
	// AsOf reads the graph as it stood at an instant, for a bitemporal read.
	AsOf         *uint64 `cbor:"as_of,omitempty"`
	Conversation *string `cbor:"conversation,omitempty"`
}

// SessionsQuery narrows and pages a session listing.
type SessionsQuery struct {
	Status      *string `cbor:"status,omitempty"`
	Root        *string `cbor:"root,omitempty"`
	LabelPrefix *string `cbor:"label_prefix,omitempty"`
	Agent       *string `cbor:"agent,omitempty"`
	Text        *string `cbor:"text,omitempty"`
	Page        *uint32 `cbor:"page,omitempty"`
	Results     *uint32 `cbor:"results,omitempty"`
	// AsOf and Series bound a walk: the values the walk's first page
	// returned. Later pages pass them back so a session folded after the
	// first page stays out.
	AsOf   *uint64 `cbor:"as_of,omitempty"`
	Series *uint64 `cbor:"series,omitempty"`
}

// SessionEventsQuery pages one session's events.
type SessionEventsQuery struct {
	Page    *uint32 `cbor:"page,omitempty"`
	Results *uint32 `cbor:"results,omitempty"`
	// AsOf and Series bound a walk, like a session listing.
	AsOf   *uint64 `cbor:"as_of,omitempty"`
	Series *uint64 `cbor:"series,omitempty"`
}

// SessionStateQuery bounds how much state history one read returns.
type SessionStateQuery struct {
	HistoryLimit *uint32 `cbor:"history_limit,omitempty"`
}

// SessionLinksQuery narrows a session's links to one surface.
type SessionLinksQuery struct {
	Surface *string `cbor:"surface,omitempty"`
}

// SessionChangesQuery reads one page of the session change feed. Page p of
// size r holds the changes with sequence (p-1)*r+1 through p*r, so appends and
// pruning never shift a page.
type SessionChangesQuery struct {
	Page    *uint32 `cbor:"page,omitempty"`
	Results *uint32 `cbor:"results,omitempty"`
}

// QueryString renders named parameters as a query string, omitting the ones
// with no value.
//
// Values are percent-encoded, so a filter term holding a reserved character
// reaches the server as written.
func QueryString(parameters []QueryParameter) string {
	var out strings.Builder
	for _, parameter := range parameters {
		if parameter.Value == "" {
			continue
		}
		if out.Len() == 0 {
			out.WriteByte('?')
		} else {
			out.WriteByte('&')
		}
		out.WriteString(EncodePathSegment(parameter.Name))
		out.WriteByte('=')
		out.WriteString(EncodePathSegment(parameter.Value))
	}
	return out.String()
}

// QueryParameter is one name and value of a query string.
type QueryParameter struct {
	Name  string
	Value string
}

// ClientsQuery narrows and pages the connection discovery read.
type ClientsQuery struct {
	// WithMetadataOnly returns only connections that advertised metadata.
	WithMetadataOnly bool `cbor:"with_metadata_only,omitempty"`
	// UserID returns only connections authenticated as this principal.
	UserID *uint32 `cbor:"user_id,omitempty"`
	// After continues from a connection id, which is the page cursor.
	After *uint32 `cbor:"after,omitempty"`
	// Limit is the page size, clamped server-side to the page cap.
	Limit *uint32 `cbor:"limit,omitempty"`
}

// StreamQuery names the stream a route's resources belong to.
type StreamQuery struct {
	Stream *string `cbor:"stream,omitempty"`
}
