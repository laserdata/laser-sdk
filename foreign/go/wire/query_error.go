package wire

import (
	"fmt"
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// QueryErrorKind discriminates a query failure.
type QueryErrorKind uint8

const (
	QueryUnsupported QueryErrorKind = iota
	// QueryInvalid means the request itself is malformed: it failed
	// validation, did not decode, or is a raw SQL statement the server
	// refuses to run. Fix the request rather than retrying it.
	QueryInvalid
	QueryUnauthorized
	QueryIndexNotFound
	QueryForkNotFound
	// QuerySchemaNotFound names a writer schema id that is not registered.
	QuerySchemaNotFound
	QueryBackend
	// QueryUnavailable means the identical request may succeed later. Retry
	// with backoff rather than treating it as a fault.
	QueryUnavailable
	// QueryTooLarge means the query asked for more than one reply may carry.
	// Page with a smaller limit, or drop the payload request, rather than
	// retrying unchanged.
	QueryTooLarge
	QueryVersionMismatch
	// QueryStale means a consistency level could not be met within the managed
	// deadline. Fail-not-downgrade, so the caller retries while the projector
	// catches up rather than unknowingly reading stale data.
	QueryStale
	QueryDeadlineExceeded
	QueryStaleGeneration
)

type queryTooLargePayload struct {
	What string `cbor:"what"`
	Size uint64 `cbor:"size"`
	Cap  uint64 `cbor:"cap"`
}

type queryStalePayload struct {
	What     string `cbor:"what"`
	Applied  uint64 `cbor:"applied"`
	Required uint64 `cbor:"required"`
}

type queryExecutionPayload struct {
	ExecutionID QueryExecutionID `cbor:"execution_id"`
}

type queryStaleGenerationPayload struct {
	What      string `cbor:"what"`
	Requested uint64 `cbor:"requested"`
	Observed  uint64 `cbor:"observed"`
}

// QueryError is why a query failed.
type QueryError struct {
	Kind    QueryErrorKind
	Message string
	// TooLarge and Stale detail.
	What     string
	Size     uint64
	Cap      uint64
	Applied  uint64
	Required uint64
	// Version detail.
	Expected uint32
	Got      uint32
	// Execution-scoped detail.
	ExecutionID QueryExecutionID
	// SchemaID names the unregistered writer schema.
	SchemaID uint32
	// Generation detail.
	Requested uint64
	Observed  uint64
}

// The externally tagged query error names.
const (
	queryUnsupportedVariant      = "Unsupported"
	queryInvalidVariant          = "Invalid"
	queryUnauthorizedVariant     = "Unauthorized"
	queryIndexNotFoundVariant    = "IndexNotFound"
	queryForkNotFoundVariant     = "ForkNotFound"
	querySchemaNotFoundVariant   = "SchemaNotFound"
	queryBackendVariant          = "Backend"
	queryUnavailableVariant      = "Unavailable"
	queryTooLargeVariant         = "TooLarge"
	queryVersionVariant          = "Version"
	queryStaleVariant            = "Stale"
	queryDeadlineExceededVariant = "DeadlineExceeded"
	queryStaleGenerationVariant  = "StaleGeneration"
)

// MarshalCBOR writes the externally tagged error.
func (queryError QueryError) MarshalCBOR() (cborform.Value, error) {
	text := func(name string) (cborform.Value, error) {
		return taggedVariant(name, cborform.Text(queryError.Message)), nil
	}
	switch queryError.Kind {
	case QueryUnsupported:
		return text(queryUnsupportedVariant)
	case QueryInvalid:
		return text(queryInvalidVariant)
	case QueryUnauthorized:
		return text(queryUnauthorizedVariant)
	case QueryIndexNotFound:
		return text(queryIndexNotFoundVariant)
	case QueryForkNotFound:
		return text(queryForkNotFoundVariant)
	case QuerySchemaNotFound:
		return taggedVariant(querySchemaNotFoundVariant,
			cborform.Uint(uint64(queryError.SchemaID))), nil
	case QueryBackend:
		return text(queryBackendVariant)
	case QueryUnavailable:
		return text(queryUnavailableVariant)
	case QueryTooLarge:
		return marshalVariantStruct(queryTooLargeVariant, queryTooLargePayload{
			What: queryError.What, Size: queryError.Size, Cap: queryError.Cap,
		})
	case QueryVersionMismatch:
		return marshalVariantStruct(queryVersionVariant, kvVersionPayloadPair{
			Expected: queryError.Expected, Got: queryError.Got,
		})
	case QueryStale:
		return marshalVariantStruct(queryStaleVariant, queryStalePayload{
			What: queryError.What, Applied: queryError.Applied, Required: queryError.Required,
		})
	case QueryDeadlineExceeded:
		return marshalVariantStruct(queryDeadlineExceededVariant, queryExecutionPayload{
			ExecutionID: queryError.ExecutionID,
		})
	case QueryStaleGeneration:
		return marshalVariantStruct(queryStaleGenerationVariant, queryStaleGenerationPayload{
			What: queryError.What, Requested: queryError.Requested, Observed: queryError.Observed,
		})
	default:
		return nil, fmt.Errorf("wire: unknown query error %d", queryError.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged error.
func (queryError *QueryError) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("QueryError", name, payload); err != nil {
		return err
	}

	text := func(kind QueryErrorKind) error {
		message, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: %q carries a message, got %T", name, payload)
		}
		*queryError = QueryError{Kind: kind, Message: string(message)}
		return nil
	}

	switch name {
	case queryUnsupportedVariant:
		return text(QueryUnsupported)
	case queryInvalidVariant:
		return text(QueryInvalid)
	case queryUnauthorizedVariant:
		return text(QueryUnauthorized)
	case queryIndexNotFoundVariant:
		return text(QueryIndexNotFound)
	case queryForkNotFoundVariant:
		return text(QueryForkNotFound)
	case querySchemaNotFoundVariant:
		id, ok := payload.(cborform.Uint)
		if !ok {
			return fmt.Errorf("wire: %q carries a schema id, got %T", name, payload)
		}
		if id > math.MaxUint32 {
			return fmt.Errorf("wire: schema id %d exceeds a u32", id)
		}
		*queryError = QueryError{Kind: QuerySchemaNotFound, SchemaID: uint32(id)}
		return nil
	case queryBackendVariant:
		return text(QueryBackend)
	case queryUnavailableVariant:
		return text(QueryUnavailable)

	case queryTooLargeVariant:
		var detail queryTooLargePayload
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*queryError = QueryError{
			Kind: QueryTooLarge, What: detail.What, Size: detail.Size, Cap: detail.Cap,
		}
		return nil

	case queryVersionVariant:
		var detail kvVersionPayloadPair
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*queryError = QueryError{
			Kind: QueryVersionMismatch, Expected: detail.Expected, Got: detail.Got,
		}
		return nil

	case queryStaleVariant:
		var detail queryStalePayload
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*queryError = QueryError{
			Kind: QueryStale, What: detail.What,
			Applied: detail.Applied, Required: detail.Required,
		}
		return nil

	case queryDeadlineExceededVariant:
		var detail queryExecutionPayload
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*queryError = QueryError{Kind: QueryDeadlineExceeded, ExecutionID: detail.ExecutionID}
		return nil

	case queryStaleGenerationVariant:
		var detail queryStaleGenerationPayload
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*queryError = QueryError{
			Kind: QueryStaleGeneration, What: detail.What,
			Requested: detail.Requested, Observed: detail.Observed,
		}
		return nil

	default:
		return unknownVariant("QueryError", name)
	}
}

// Code is the stable machine-readable classification.
func (queryError QueryError) Code() QueryErrorCode {
	switch queryError.Kind {
	case QueryUnsupported:
		return QueryCodeUnsupported
	case QueryInvalid:
		return QueryCodeInvalid
	case QueryUnauthorized:
		return QueryCodeUnauthorized
	case QueryIndexNotFound:
		return QueryCodeIndexNotFound
	case QueryForkNotFound:
		return QueryCodeForkNotFound
	case QuerySchemaNotFound:
		return QueryCodeSchemaNotFound
	case QueryBackend:
		return QueryCodeBackend
	case QueryUnavailable:
		return QueryCodeUnavailable
	case QueryTooLarge:
		return QueryCodeTooLarge
	case QueryVersionMismatch:
		return QueryCodeVersion
	case QueryStale:
		return QueryCodeStale
	case QueryDeadlineExceeded:
		return QueryCodeDeadlineExceeded
	default:
		return QueryCodeStaleGeneration
	}
}

// ResultCode projects the failure onto the shared outcome dictionary.
//
// An unauthorized query is forbidden rather than unauthenticated: the per-source
// check refused it because the caller authenticated but lacks the grant.
func (queryError QueryError) ResultCode() ResultCode {
	switch queryError.Kind {
	case QueryUnsupported:
		return ResultUnsupported
	case QueryInvalid:
		return ResultInvalidArgument
	case QueryUnauthorized:
		return ResultForbidden
	case QueryIndexNotFound, QueryForkNotFound, QuerySchemaNotFound:
		return ResultNotFound
	case QueryBackend:
		return ResultBackend
	case QueryUnavailable:
		return ResultUnavailable
	case QueryTooLarge:
		return ResultTooLarge
	case QueryVersionMismatch:
		return ResultVersionSkew
	case QueryStale:
		return ResultStale
	case QueryDeadlineExceeded:
		return ResultDeadlineExceeded
	case QueryStaleGeneration:
		return ResultStaleGeneration
	default:
		return ResultBackend
	}
}

// Error renders the failure, matching the Rust messages.
func (queryError QueryError) Error() string {
	switch queryError.Kind {
	case QueryUnsupported:
		return "query not supported: " + queryError.Message
	case QueryInvalid:
		return "invalid query: " + queryError.Message
	case QueryUnauthorized:
		return "unauthorized: " + queryError.Message
	case QueryIndexNotFound:
		return "index not found: " + queryError.Message
	case QueryForkNotFound:
		return "fork not found: " + queryError.Message
	case QuerySchemaNotFound:
		return fmt.Sprintf("writer schema %d is not registered", queryError.SchemaID)
	case QueryBackend:
		return "backend error: " + queryError.Message
	case QueryUnavailable:
		return "temporarily unavailable: " + queryError.Message
	case QueryTooLarge:
		return fmt.Sprintf("result too large: %s %d exceeds cap %d",
			queryError.What, queryError.Size, queryError.Cap)
	case QueryVersionMismatch:
		return fmt.Sprintf("unsupported envelope version (expected %d, got %d)",
			queryError.Expected, queryError.Got)
	case QueryStale:
		return fmt.Sprintf("stale read: %s applied %d, required %d",
			queryError.What, queryError.Applied, queryError.Required)
	case QueryDeadlineExceeded:
		return fmt.Sprintf("query %s exceeded its deadline", queryError.ExecutionID)
	case QueryStaleGeneration:
		return fmt.Sprintf("stale %s generation: requested %d, observed %d",
			queryError.What, queryError.Requested, queryError.Observed)
	default:
		return "unknown query error"
	}
}
