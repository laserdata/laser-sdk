package laser

import (
	"bytes"
	"context"
	"strconv"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// localGuard evaluates every returned record again with the shared evaluator, so a record the server selected and the local evaluation does not fails the page before the caller sees it.
type localGuard struct {
	compiled *wire.CompiledFilter
	digest   wire.Digest32
}

func newLocalGuard(filter wire.ConsumerFilter, schemas []wire.SchemaDef) (*localGuard, error) {
	compiled, err := wire.CompileFilterWithSchemas(filter, schemas)
	if err != nil {
		return nil, &InvalidError{Reason: "filter: " + err.Error()}
	}
	return &localGuard{compiled: compiled, digest: compiled.Digest()}, nil
}

// check holds a served page to the local evaluation of each record it carries.
func (guard *localGuard) check(page wire.FilteredPage, records []MatchedRecord) error {
	if page.Policy.Digest == nil || !bytes.Equal(page.Policy.Digest.Bytes, guard.digest.Bytes) {
		return &FilterError{Failure: wire.NewFilterError(wire.ReasonConflict,
			"the server ran another filter than the one the local guard checks")}
	}
	unevaluated := make(map[uint64]wire.FaultReason, len(page.Unevaluated))
	for _, fault := range page.Unevaluated {
		unevaluated[fault.Offset] = fault.Reason
	}
	filter := guard.compiled.Filter()
	if len(unevaluated) > 0 && filter.FaultPolicy != wire.FaultPass &&
		filter.ForeignPolicy != wire.PolicyPass && filter.MismatchPolicy != wire.PolicyPass {
		return &FilterError{Failure: wire.NewFilterError(wire.ReasonBackend,
			"the server returned unevaluated records without a pass policy")}
	}
	// Selected records fit the page cap. Pass-through faults are checked under the bounds the server reported.
	limits := wire.DecodeLimits{MaxPayloadBytes: wire.MaxFilteredPageBytes, MaxDepth: wire.MaxFilterParseDepth}
	switch reported := page.EvaluationLimits; {
	case reported != nil && reported.MaxPayloadBytes > 0 && reported.MaxPayloadBytes <= wire.MaxFilteredPageBytes &&
		reported.MaxDepth > 0 && reported.MaxDepth <= wire.MaxFilterParseDepth:
		limits = wire.DecodeLimits{MaxPayloadBytes: int(reported.MaxPayloadBytes), MaxDepth: int(reported.MaxDepth)}
	case reported != nil:
		return &FilterError{Failure: wire.NewFilterError(wire.ReasonBackend, "the server returned invalid decoder limits")}
	case len(unevaluated) > 0 && filter.FaultPolicy == wire.FaultPass:
		return &UnsupportedError{Surface: "local verification of pass-through decode faults without the server's evaluation limits"}
	}
	for _, record := range records {
		offset := record.Offset()
		verdict, fault := wire.VerdictFault, wire.FaultMalformed
		if headers, sound := wire.DecodeHeadersFor(guard.compiled.HeaderNeed(), record.Message.UserHeaders); sound {
			verdict, fault = guard.compiled.EvaluateWithFault(wire.FilterRecord{Payload: record.Message.Payload, Headers: headers}, limits)
		}
		agrees := verdict == wire.VerdictSelected
		if reason, passed := unevaluated[offset]; passed {
			agrees = verdict == wire.VerdictFault && fault == reason && guard.compiled.PolicyFor(reason) == wire.FaultPass
		}
		if !agrees {
			return &FilterError{Failure: wire.NewFilterError(wire.ReasonBackend,
				"the server's evaluation of offset "+strconv.FormatUint(offset, 10)+" on partition "+
					strconv.FormatUint(uint64(page.PartitionID), 10)+" disagrees with the local guard")}
		}
	}
	return nil
}

// loadLocalGuard compiles the filter the group runs, with the writer schemas it names.
func loadLocalGuard(ctx context.Context, laser *Laser, page wire.FilteredPage, source wire.FilterSource) (*localGuard, error) {
	filter, err := boundDefinition(ctx, laser, page, source)
	if err != nil {
		return nil, err
	}
	schemas := make([]wire.SchemaDef, 0, len(filter.SchemaRefs))
	for _, id := range filter.SchemaRefs {
		request := wire.GetSchema{V: wire.QueryOpVersion, ID: id, Stream: filter.SchemaStream}
		if stream := laser.resourceStream(); request.Stream == nil && stream != "" {
			request.Stream = &stream
		}
		outcome, err := laser.executeBrowse(ctx, wire.AgdxGetSchemaCode, request)
		if err != nil {
			return nil, err
		}
		if outcome.Kind != wire.BrowseSchema || outcome.Schema == nil || outcome.Schema.Schema.ID != id {
			return nil, &FilterError{Failure: wire.NewFilterError(wire.ReasonNotFound,
				"writer schema "+strconv.FormatUint(uint64(id), 10)+" was not returned")}
		}
		schemas = append(schemas, outcome.Schema.Schema)
	}
	return newLocalGuard(filter, schemas)
}

// boundDefinition reads the filter the catalog binds to the group that served the page.
func boundDefinition(ctx context.Context, laser *Laser, page wire.FilteredPage, source wire.FilterSource) (wire.ConsumerFilter, error) {
	catalog := laser.FilterCatalog()
	query := BindingQuery{Stream: &source.Stream, Topic: &source.Topic}
	generation := page.Generation
	for number := uint32(1); ; number++ {
		bindings, err := catalog.Bindings(ctx, query, number, wire.MaxFilterCatalogPage)
		if err != nil {
			return wire.ConsumerFilter{}, err
		}
		for _, binding := range bindings.Items {
			id := binding.Identity
			if page.Policy.GroupID == nil || id.GroupID != *page.Policy.GroupID ||
				id.StreamID != generation.StreamID || id.StreamCreatedAtMicros != generation.StreamCreatedAtMicros ||
				id.TopicID != generation.TopicID || id.TopicCreatedAtMicros != generation.TopicCreatedAtMicros {
				continue
			}
			if page.Policy.Digest == nil || !bytes.Equal(binding.Digest.Bytes, page.Policy.Digest.Bytes) {
				return wire.ConsumerFilter{}, &ProtocolError{Reason: "served policy differs from the group's catalog binding"}
			}
			return revisionDefinition(ctx, catalog, binding.FilterID, binding.Revision)
		}
		if number >= bindings.TotalPages {
			return wire.ConsumerFilter{}, &FilterError{Failure: wire.NewFilterError(wire.ReasonNotFound,
				"no readable catalog binding matches this group identity")}
		}
	}
}

func revisionDefinition(ctx context.Context, catalog *FilterCatalog, filterID, revision uint32) (wire.ConsumerFilter, error) {
	for number := uint32(1); ; number++ {
		revisions, err := catalog.Revisions(ctx, filterID, number, wire.MaxFilterCatalogPage)
		if err != nil {
			return wire.ConsumerFilter{}, err
		}
		for _, info := range revisions.Items {
			if info.Revision == revision {
				return info.Filter, nil
			}
		}
		if number >= revisions.TotalPages {
			return wire.ConsumerFilter{}, &FilterError{Failure: wire.NewFilterError(wire.ReasonNotFound,
				"filter "+strconv.FormatUint(uint64(filterID), 10)+" has no revision "+strconv.FormatUint(uint64(revision), 10))}
		}
	}
}
