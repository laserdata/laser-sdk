package wire

import (
	"fmt"
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// QueryEngine names the engine that served a query.
type QueryEngine struct {
	Name    string      `cbor:"name"`
	Version string      `cbor:"version"`
	Dialect *SqlDialect `cbor:"dialect,omitempty"`
}

// QueryFrontier is one source partition's applied position in the store that
// served a query.
type QueryFrontier struct {
	Stream      string `cbor:"stream"`
	Topic       string `cbor:"topic"`
	PartitionID uint32 `cbor:"partition_id"`
	// TopicCreatedAtMicros is the creation time of the topic incarnation the
	// rows come from, when the serving node knows it. Offsets of different
	// incarnations do not compare.
	TopicCreatedAtMicros *uint64 `cbor:"topic_created_at_micros,omitempty"`
	// AppliedOffset is the last offset the store applied.
	AppliedOffset uint64 `cbor:"applied_offset"`
}

// ResolvedQueryTargetKind discriminates what a query actually read, after the
// server resolved the request's target.
type ResolvedQueryTargetKind string

const (
	ResolvedOperational ResolvedQueryTargetKind = "operational"
)

type resolvedOperationalFields struct {
	Index                        string            `cbor:"index"`
	BackendResourceID            BackendResourceID `cbor:"backend_resource_id"`
	BackendGeneration            uint64            `cbor:"backend_generation"`
	RuntimeConfigurationRevision uint64            `cbor:"runtime_configuration_revision"`
}

// ResolvedQueryTarget is the exact target a query read, returned as evidence
// with every page. It is internally tagged, like the requested target.
type ResolvedQueryTarget struct {
	Kind        ResolvedQueryTargetKind
	Operational *resolvedOperationalFields
}

// MarshalCBOR writes the internally tagged resolved target.
// NewResolvedOperationalTarget names the index a query actually resolved to,
// with the backend that answered and the generation and revision it ran at.
//
// A client reads this rather than building it, but the contract's own variant
// is constructible, so this one is too: a test or a fake server that could not
// build a reply could not exercise a client that reads one.
func NewResolvedOperationalTarget(
	index string, backendResourceID BackendResourceID,
	backendGeneration, runtimeConfigurationRevision uint64,
) ResolvedQueryTarget {
	return ResolvedQueryTarget{
		Kind: ResolvedOperational,
		Operational: &resolvedOperationalFields{
			Index:                        index,
			BackendResourceID:            backendResourceID,
			BackendGeneration:            backendGeneration,
			RuntimeConfigurationRevision: runtimeConfigurationRevision,
		},
	}
}

func (target ResolvedQueryTarget) MarshalCBOR() (cborform.Value, error) {
	switch target.Kind {
	case ResolvedOperational:
		if target.Operational == nil {
			return nil, fmt.Errorf("wire: a resolved operational target carries no index")
		}
		return marshalInternallyTagged(string(ResolvedOperational), *target.Operational)
	default:
		return nil, fmt.Errorf("wire: unknown resolved query target %q", string(target.Kind))
	}
}

// UnmarshalCBOR reads the internally tagged resolved target.
func (target *ResolvedQueryTarget) UnmarshalCBOR(item cborform.Value) error {
	kind, entries, err := readInternalTag(item)
	if err != nil {
		return err
	}
	switch ResolvedQueryTargetKind(kind) {
	case ResolvedOperational:
		var fields resolvedOperationalFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*target = ResolvedQueryTarget{Kind: ResolvedOperational, Operational: &fields}
		return nil
	default:
		return fmt.Errorf("wire: unknown resolved query target %q", kind)
	}
}

// Index is the operational index a resolved target names.
func (target ResolvedQueryTarget) Index() (string, bool) {
	if target.Kind != ResolvedOperational || target.Operational == nil {
		return "", false
	}
	return target.Operational.Index, true
}

// QueryContext is the target and execution evidence returned with every page.
type QueryContext struct {
	ExecutionID          QueryExecutionID    `cbor:"execution_id"`
	Engine               QueryEngine         `cbor:"engine"`
	ResolvedTarget       ResolvedQueryTarget `cbor:"resolved_target"`
	RequestedConsistency Consistency         `cbor:"requested_consistency"`
	DeliveredConsistency Consistency         `cbor:"delivered_consistency"`
	Truncated            bool                `cbor:"truncated"`
	ElapsedMicros        uint64              `cbor:"elapsed_micros"`
	ScannedBytes         uint64              `cbor:"scanned_bytes"`
	ProducedBytes        uint64              `cbor:"produced_bytes"`
	RowCount             uint64              `cbor:"row_count"`
	// Frontier is how far the store that served the query had applied each
	// source partition. A reply from another node can sit at another point of
	// the same log, so a caller compares positions rather than wall time.
	Frontier []QueryFrontier `cbor:"frontier,omitempty"`
}

// Row is one typed query row. Values are ordered exactly like the result's
// fields.
type Row struct {
	Values []TypedValue `cbor:"values"`
	// Score is the backend-native relevance for a vector or lexical search:
	// vector backends return distance, lexical backends their declared rank.
	Score *float32 `cbor:"score,omitempty"`
}

// QueryResult is a typed page. Each row is positionally aligned with the
// fields.
type QueryResult struct {
	Fields  []LogicalField `cbor:"fields"`
	Rows    []Row          `cbor:"rows"`
	Page    Page           `cbor:"page"`
	Context QueryContext   `cbor:"context"`
}

// Validate checks that every row matches the result schema. A row of the wrong
// width would silently misalign values against field names.
func (result QueryResult) Validate() error {
	if len(result.Fields) == 0 {
		return &InvalidError{Reason: "query result schema must not be empty"}
	}
	for _, row := range result.Rows {
		if len(row.Values) != len(result.Fields) {
			return &InvalidError{
				Reason: "query row value count does not match the result schema",
			}
		}
		for index, value := range row.Values {
			field := result.Fields[index]
			if err := value.ValidateAgainst(field.FieldType, field.Required); err != nil {
				return err
			}
		}
		if row.Score != nil && (math.IsNaN(float64(*row.Score)) || math.IsInf(float64(*row.Score), 0)) {
			return &InvalidError{Reason: "query row score must be finite"}
		}
	}
	if result.Context.ExecutionID.IsZero() {
		return &InvalidError{Reason: "query result execution id must be nonzero"}
	}
	if result.Context.RowCount != uint64(len(result.Rows)) {
		return &InvalidError{Reason: "query result row count does not match the page rows"}
	}
	if err := result.Context.Validate(); err != nil {
		return err
	}
	return result.Page.Validate()
}

// QueryReply is a query reply: a typed result, or a failure.
type QueryReply struct {
	Err    *QueryError
	Result *QueryResult
}

// OkQueryReply wraps a successful result.
func OkQueryReply(result QueryResult) QueryReply { return QueryReply{Result: &result} }

// ErrQueryReply wraps a failure.
func ErrQueryReply(queryError QueryError) QueryReply { return QueryReply{Err: &queryError} }

// IsOk reports a successful reply.
func (reply QueryReply) IsOk() bool { return reply.Err == nil && reply.Result != nil }

// MarshalCBOR writes the externally tagged reply.
func (reply QueryReply) MarshalCBOR() (cborform.Value, error) {
	switch {
	case reply.Err != nil && reply.Result != nil:
		return nil, fmt.Errorf("wire: a query reply is either a result or an error")
	case reply.Err != nil:
		payload, err := reply.Err.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvErrVariant, payload), nil
	case reply.Result != nil:
		payload, err := toValueOf(*reply.Result)
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvOkVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: an empty query reply")
	}
}

// UnmarshalCBOR reads the externally tagged reply.
func (reply *QueryReply) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("QueryReply", name, payload); err != nil {
		return err
	}
	switch name {
	case kvOkVariant:
		var result QueryResult
		if err := fromValueInto(payload, &result); err != nil {
			return err
		}
		*reply = QueryReply{Result: &result}
		return nil
	case kvErrVariant:
		var queryError QueryError
		if err := queryError.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = QueryReply{Err: &queryError}
		return nil
	default:
		return unknownVariant("QueryReply", name)
	}
}
