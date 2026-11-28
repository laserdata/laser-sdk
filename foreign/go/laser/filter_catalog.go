package laser

import (
	"context"
	"errors"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Preview runs a filter over a partition without joining a group or storing
// progress, so a filter can be checked against real records before anything
// reads through it.
//
// It is the answer to "what would this select": a filter bound to a group
// before it was previewed is a filter nobody has seen select anything.
type PreviewRequest struct {
	filters     *Filters
	partitionID uint32
	filter      wire.FilterRef
	fromOffset  uint64
	maxExamined uint32
	maxRecords  uint32
	explain     bool
}

// Preview begins a preview of one partition.
func (filters *Filters) Preview(
	partitionID uint32, filter wire.FilterRef,
) *PreviewRequest {
	return &PreviewRequest{
		filters:     filters,
		partitionID: partitionID,
		filter:      filter,
		maxExamined: DefaultPreviewExamined,
		maxRecords:  DefaultPreviewRecords,
	}
}

const (
	// DefaultPreviewExamined bounds how many records a preview looks at.
	DefaultPreviewExamined uint32 = 1000
	// DefaultPreviewRecords bounds how many a preview returns.
	DefaultPreviewRecords uint32 = 20
)

// FromOffset starts the preview at an offset.
func (request *PreviewRequest) FromOffset(offset uint64) *PreviewRequest {
	request.fromOffset = offset
	return request
}

// MaxExamined bounds how many records the preview looks at.
func (request *PreviewRequest) MaxExamined(count uint32) *PreviewRequest {
	request.maxExamined = count
	return request
}

// MaxRecords bounds how many the preview returns.
func (request *PreviewRequest) MaxRecords(count uint32) *PreviewRequest {
	request.maxRecords = count
	return request
}

// Explain includes a per-predicate explanation for every returned record, and
// returns the rejected ones too.
//
// Without it a preview says what matched. With it, it says why each record
// did or did not, which is what makes a filter debuggable rather than only
// testable.
func (request *PreviewRequest) Explain() *PreviewRequest {
	request.explain = true
	return request
}

// Send runs the preview.
func (request *PreviewRequest) Send(ctx context.Context) (*wire.FilterPreview, error) {
	outcome, err := request.filters.execute(ctx, wire.AgdxFilterPreviewCode,
		wire.FilterPreviewRequest{
			V:           wire.FilterOpVersion,
			Source:      request.filters.source,
			PartitionID: request.partitionID,
			Filter:      request.filter,
			FromOffset:  request.fromOffset,
			MaxExamined: request.maxExamined,
			MaxRecords:  request.maxRecords,
			Explain:     request.explain,
		})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.FilterPreviewOutcome || outcome.Preview == nil {
		return nil, unexpectedFilterOutcome("preview", outcome.Kind)
	}
	return outcome.Preview, nil
}

// Test judges one sample record, so a filter can be checked against a record a
// caller holds rather than against whatever happens to be on the topic.
//
// The explanation comes back whether it matched or not, which is what tells a
// caller why a record they expected to select did not.
func (filters *Filters) Test(
	ctx context.Context, filter wire.FilterRef, payload []byte,
	headers []wire.FilterHeader,
) (*wire.FilterTestResult, error) {
	outcome, err := filters.execute(ctx, wire.AgdxFilterTestCode, wire.FilterTestRequest{
		V: wire.FilterOpVersion, Filter: filter, Payload: payload, Headers: headers,
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.FilterTested || outcome.TestResult == nil {
		return nil, unexpectedFilterOutcome("test", outcome.Kind)
	}
	return outcome.TestResult, nil
}

// ValidateFilter compiles a filter locally and reports what it would do.
//
// It runs the contract's own evaluator, the same one the server runs, so a
// filter that compiles here is one the server will accept and that selects the
// same records. Checking locally costs no round trip and works against a
// deployment that serves no filters at all.
//
// The digest is what a binding is conditional on, so a caller that validates
// before binding knows which revision it is pinning. Schemas are needed only
// by a filter that reads Avro or Protobuf payloads.
func ValidateFilter(
	filter wire.ConsumerFilter, schemas []wire.SchemaDef,
) (*wire.FilterValidation, error) {
	compiled, err := wire.CompileFilterWithSchemas(filter, schemas)
	if err != nil {
		return nil, &InvalidError{Reason: "filter: " + err.Error()}
	}
	return &wire.FilterValidation{
		V:            wire.FilterOpVersion,
		Digest:       compiled.Digest(),
		ReadsPayload: compiled.ReadsPayload(),
		ReadsHeaders: compiled.ReadsHeaders(),
	}, nil
}

// AmbiguousMutationError reports a managed mutation whose outcome is unknown:
// the attempt timed out or the transport failed after dispatch, so the server
// may or may not have applied it. A catalog change is one such mutation, and
// so is any command the managed band repeated to its deadline without an
// answer.
//
// It is deliberately not a retryable failure. The identity is what makes
// recovery possible: repeating the change under the same OperationID returns
// the recorded outcome rather than applying it a second time, and reading the
// operation says which happened.
type AmbiguousMutationError struct {
	OperationID wire.U128
	Reason      string
}

func (e *AmbiguousMutationError) Error() string {
	return "the managed mutation " + e.OperationID.String() +
		" has an unknown outcome: " + e.Reason
}

// FilterCatalog administers saved filters: the revisions a deployment keeps
// and the groups bound to them.
//
// The catalog needs a managed backend, where filtered reading does not: a
// deployment can filter without saving anything.
type FilterCatalog struct {
	laser *Laser
	// outcomeWait bounds how long a pending mutation's authoritative outcome
	// is awaited.
	outcomeWait time.Duration
	// pollInterval is how often it is looked for.
	pollInterval time.Duration
}

// FilterCatalog opens the saved-filter catalog.
func (laser *Laser) FilterCatalog() *FilterCatalog {
	return &FilterCatalog{
		laser:        laser,
		outcomeWait:  FilterOutcomeWait,
		pollInterval: outcomePollInterval,
	}
}

// outcomePollInterval is how often a pending mutation is looked for.
const outcomePollInterval = 100 * time.Millisecond

// WaitingFor bounds how long a change waits for its authoritative outcome
// before it is reported ambiguous.
func (catalog *FilterCatalog) WaitingFor(wait time.Duration) *FilterCatalog {
	copied := *catalog
	copied.outcomeWait = wait
	return &copied
}

// requireServed refuses a deployment that does not serve the catalog at this op version.
func (catalog *FilterCatalog) requireServed() error {
	if !catalog.laser.Capabilities().Filters.Catalog {
		return &UnsupportedError{Surface: "the saved-filter catalog"}
	}
	// A catalog serving another op version would answer a shape this client
	// cannot read, and a mutation half-understood is worse than one refused.
	return catalog.laser.requireOpVersion("the saved-filter catalog",
		wire.FilterOpVersion,
		func(versions wire.OpVersions) uint32 { return versions.Filter })
}

// execute sends one catalog command, walking the roster until it is served.
func (catalog *FilterCatalog) execute(
	ctx context.Context, code uint32, request any,
) (*wire.FilterCatalogOutcome, error) {
	return catalog.executeWalking(ctx, code, request, walkingUntilServed())
}

// executeWalking sends one catalog command under the given walk, once for a caller that paces its own repeats.
func (catalog *FilterCatalog) executeWalking(
	ctx context.Context, code uint32, request any, w walk,
) (*wire.FilterCatalogOutcome, error) {
	if err := catalog.requireServed(); err != nil {
		return nil, err
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode catalog request", Cause: err}
	}
	raw, err := catalog.laser.sendManagedWalking(ctx, code, payload, w)
	if err != nil {
		return nil, err
	}
	var reply wire.FilterCatalogReply
	if err := decodeManagedReply(raw, &reply, "decode catalog reply"); err != nil {
		return nil, err
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{
				Reason: "catalog reply carried neither an outcome nor an error"}
		}
		return nil, &FilterError{Failure: *reply.Err}
	}
	if reply.Outcome == nil {
		return nil, &CodecError{Reason: "catalog reply carried no outcome"}
	}
	return reply.Outcome, nil
}

// AppliedMutation is a change the catalog applied, with where it applied it.
type AppliedMutation struct {
	// Result is what the change produced.
	Result wire.FilterMutationResult
	// Position is the control-log record whose fold applied it, absent when
	// the catalog reported none.
	//
	// A group read can require the backend to have folded through it, which
	// is what gives an application that just configured a group
	// read-your-writes through any node.
	Position *wire.CatalogPosition
}

// apply sends one change under a fresh identity and settles it.
func (catalog *FilterCatalog) apply(
	ctx context.Context, mutation wire.FilterMutation,
) (*AppliedMutation, error) {
	return catalog.ApplyAs(ctx, NewOperationID(), mutation)
}

// ApplyAs sends one change under a caller-chosen identity and settles it.
//
// The identity is what makes a retry safe: a change repeated under the same
// one returns the recorded outcome rather than applying twice, so a caller
// that persists the identity can recover across a restart.
func (catalog *FilterCatalog) ApplyAs(
	ctx context.Context, operationID wire.U128, mutation wire.FilterMutation,
) (*AppliedMutation, error) {
	outcome, err := catalog.submit(ctx, operationID, mutation)
	if err != nil {
		return nil, err
	}
	// A change that entered the control log but has not been folded yet is
	// read back rather than sent again, because sending again under a new
	// identity would apply it twice.
	if outcome.Status.Kind == wire.StatusPending {
		outcome, err = catalog.waitForOutcome(ctx, operationID)
		if err != nil {
			return nil, err
		}
	}
	result, err := settled(operationID, outcome.Status)
	if err != nil {
		return nil, err
	}
	return &AppliedMutation{Result: *result, Position: outcome.CatalogPosition}, nil
}

// submit sends one change, repeating a retryable failure under the same
// identity rather than giving up on a transport hiccup.
func (catalog *FilterCatalog) submit(
	ctx context.Context, operationID wire.U128, mutation wire.FilterMutation,
) (*wire.FilterMutationOutcome, error) {
	for attempt := uint32(1); ; attempt++ {
		outcome, err := catalog.MutateAs(ctx, operationID, mutation)
		if err == nil {
			return outcome, nil
		}
		if !retryableFilterFailure(err) {
			return nil, err
		}
		if attempt >= FilterMutationAttempts {
			// Every attempt carried the same identity, so the change may have
			// been applied by one whose reply was lost.
			return nil, &AmbiguousMutationError{
				OperationID: operationID, Reason: err.Error()}
		}
		if err := sleepWithin(ctx, FilterMutationBackoff); err != nil {
			return nil, err
		}
	}
}

// waitForOutcome reads the operation back until the catalog has folded it.
func (catalog *FilterCatalog) waitForOutcome(
	ctx context.Context, operationID wire.U128,
) (*wire.FilterMutationOutcome, error) {
	deadline := time.Now().Add(catalog.outcomeWait)
	for {
		outcome, err := catalog.Operation(ctx, operationID)
		switch {
		case err == nil && outcome.Status.Kind != wire.StatusPending:
			return outcome, nil
		case err == nil:
			// Still pending: the fold has not reached it.
		case notYetRecorded(err), retryableFilterFailure(err):
			// The fold has not recorded it yet, or the catalog is out of reach for now.
		default:
			return nil, err
		}
		if !time.Now().Before(deadline) {
			return nil, &AmbiguousMutationError{OperationID: operationID,
				Reason: "the catalog has not recorded an outcome"}
		}
		if err := sleepWithin(ctx, catalog.pollInterval); err != nil {
			return nil, err
		}
	}
}

// notYetRecorded reports a read that failed because the fold has not reached
// the operation, which is a reason to look again rather than to give up.
func notYetRecorded(err error) bool {
	var failure *FilterError
	if errors.As(err, &failure) {
		return failure.NotFound() || failure.Retryable()
	}
	return retryableFilterFailure(err)
}

// retryableFilterFailure reports a failure the identical request may survive.
func retryableFilterFailure(err error) bool {
	var failure *FilterError
	if errors.As(err, &failure) {
		return failure.Retryable()
	}
	var transport *TransportError
	return errors.As(err, &transport)
}

// sleepWithin waits, giving up as soon as the caller's context is done.
func sleepWithin(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// settled turns a status into its result, or into why there is none.
func settled(
	operationID wire.U128, status wire.FilterMutationStatus,
) (*wire.FilterMutationResult, error) {
	switch status.Kind {
	case wire.StatusApplied:
		if status.Applied == nil {
			return nil, &CodecError{Reason: "an applied mutation carries no result"}
		}
		return status.Applied, nil
	case wire.StatusRejected:
		if status.Rejected == nil {
			return nil, &CodecError{Reason: "a rejected mutation carries no reason"}
		}
		return nil, &FilterError{Failure: *status.Rejected}
	default:
		return nil, &AmbiguousMutationError{OperationID: operationID,
			Reason: "the catalog has not recorded an outcome"}
	}
}

// checkedOutcome holds an outcome to the operation that asked for it.
//
// An outcome for another operation would let a caller act on someone else's
// change, so every identity it carries must be the one asked about.
func checkedOutcome(
	operationID wire.U128, outcome *wire.FilterMutationOutcome,
) (*wire.FilterMutationOutcome, error) {
	mismatched := outcome.V != wire.FilterOpVersion || outcome.OperationID != operationID
	if outcome.CatalogPosition != nil && outcome.CatalogPosition.OperationID != nil &&
		*outcome.CatalogPosition.OperationID != operationID {
		mismatched = true
	}
	if mismatched {
		return nil, &CodecError{
			Reason: "the mutation outcome does not match the requested operation"}
	}
	return outcome, nil
}

// MutateAs sends one change under a caller-chosen identity without waiting
// for it to be folded, so the outcome it returns may still be pending.
//
// Most callers want the typed verbs below, which wait. This is for a caller
// that drives its own recovery: retrying with the same identity never applies
// the change twice.
func (catalog *FilterCatalog) MutateAs(
	ctx context.Context, operationID wire.U128, mutation wire.FilterMutation,
) (*wire.FilterMutationOutcome, error) {
	if operationID.IsZero() {
		return nil, &InvalidError{Reason: "an operation id must be nonzero"}
	}
	outcome, err := catalog.executeWalking(ctx, wire.AgdxFilterMutateCode,
		wire.FilterMutationRequest{
			V: wire.FilterOpVersion, OperationID: operationID, Mutation: mutation,
		}, walkingOnce())
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.CatalogMutation || outcome.Mutation == nil {
		return nil, unexpectedCatalogOutcome("mutate", outcome.Kind)
	}
	return checkedOutcome(operationID, outcome.Mutation)
}

// Register saves a filter as revision 1 under a name that must be unused.
func (catalog *FilterCatalog) Register(
	ctx context.Context, name, description string, filter wire.ConsumerFilter,
) (*wire.FilterRevisionRef, *wire.CatalogPosition, error) {
	applied, err := catalog.apply(ctx, wire.FilterMutation{
		Kind: wire.MutateRegister, Name: name,
		Description: description, Filter: &filter,
	})
	if err != nil {
		return nil, nil, err
	}
	return revisionOf("register", applied, wire.ResultRegistered)
}

// Revise adds a revision, conditional on which revision is currently latest.
//
// The condition is what stops two administrators revising one filter from both
// believing they built on the other's work.
func (catalog *FilterCatalog) Revise(
	ctx context.Context, filterID, expectedRevision uint32, filter wire.ConsumerFilter,
) (*wire.FilterRevisionRef, *wire.CatalogPosition, error) {
	applied, err := catalog.apply(ctx, wire.FilterMutation{
		Kind: wire.MutateRevise, FilterID: filterID,
		ExpectedRevision: expectedRevision, Filter: &filter,
	})
	if err != nil {
		return nil, nil, err
	}
	return revisionOf("revise", applied, wire.ResultRevised)
}

// revisionOf reads the revision an applied change produced.
func revisionOf(
	verb string, applied *AppliedMutation, want wire.FilterMutationResultKind,
) (*wire.FilterRevisionRef, *wire.CatalogPosition, error) {
	if applied.Result.Kind != want || applied.Result.RevisionRef == nil {
		return nil, nil, unexpectedMutationResult(verb, applied.Result.Kind)
	}
	return applied.Result.RevisionRef, applied.Position, nil
}

// Describe replaces a filter's description, leaving what it selects unchanged.
func (catalog *FilterCatalog) Describe(
	ctx context.Context, filterID uint32, description string,
) (*wire.CatalogPosition, error) {
	return catalog.applied(ctx, "describe", wire.ResultDescribed, wire.FilterMutation{
		Kind: wire.MutateDescribe, FilterID: filterID, Description: description,
	})
}

// SetRevisionEnabled pauses or resumes new reads of a revision without
// changing what it selects, so a filter can be stopped without being deleted.
//
// Executable content and its digest stay unchanged, so records already
// delivered under it can still be acknowledged.
func (catalog *FilterCatalog) SetRevisionEnabled(
	ctx context.Context, filterID, revision uint32, enabled bool,
) (*wire.CatalogPosition, error) {
	return catalog.applied(ctx, "set revision enabled", wire.ResultRevisionState,
		wire.FilterMutation{
			Kind:     wire.MutateSetRevisionEnabled,
			FilterID: filterID, Revision: revision, Enabled: enabled,
		})
}

// Archive hides a filter from new bindings, leaving the groups already bound
// to it reading.
func (catalog *FilterCatalog) Archive(
	ctx context.Context, filterID uint32,
) (*wire.CatalogPosition, error) {
	return catalog.applied(ctx, "archive", wire.ResultArchived, wire.FilterMutation{
		Kind: wire.MutateArchive, FilterID: filterID,
	})
}

// Drop tombstones a filter, releasing every bound group first.
//
// Each release advances that group's policy generation, so a reader still
// holding a page from the dropped revision cannot acknowledge it.
func (catalog *FilterCatalog) Drop(
	ctx context.Context, filterID uint32,
) (*wire.CatalogPosition, error) {
	return catalog.applied(ctx, "drop", wire.ResultDropped, wire.FilterMutation{
		Kind: wire.MutateDrop, FilterID: filterID,
	})
}

// applied sends a change whose only result is that it happened.
func (catalog *FilterCatalog) applied(
	ctx context.Context, verb string, want wire.FilterMutationResultKind,
	mutation wire.FilterMutation,
) (*wire.CatalogPosition, error) {
	applied, err := catalog.apply(ctx, mutation)
	if err != nil {
		return nil, err
	}
	if applied.Result.Kind != want {
		return nil, unexpectedMutationResult(verb, applied.Result.Kind)
	}
	return applied.Position, nil
}

// Bind pins a consumer group to a revision, so every member reads through it.
func (catalog *FilterCatalog) Bind(
	ctx context.Context, group wire.FilterGroupRef, filterID, revision uint32,
) (*wire.FilterBinding, *wire.CatalogPosition, error) {
	return catalog.bound(ctx, "bind", wire.ResultBound, wire.FilterMutation{
		Kind: wire.MutateBind, Group: &group,
		FilterID: filterID, Revision: revision,
	})
}

// Unbind releases a group, conditional on the digest it is bound to.
//
// The condition is what stops an unbind racing a rebind: releasing a group
// that was moved to another revision in between would silently undo the move.
func (catalog *FilterCatalog) Unbind(
	ctx context.Context, group wire.FilterGroupRef, expectedDigest wire.Digest32,
) (*wire.FilterBinding, *wire.CatalogPosition, error) {
	return catalog.bound(ctx, "unbind", wire.ResultUnbound, wire.FilterMutation{
		Kind: wire.MutateUnbind, Group: &group, ExpectedDigest: &expectedDigest,
	})
}

// ReleaseBinding releases this exact saved binding, a group incarnation that
// was deleted and recreated under the same name included.
//
// Naming the incarnation is what makes it exact: a release that named only the
// group would release whichever group now holds the name.
func (catalog *FilterCatalog) ReleaseBinding(
	ctx context.Context, binding wire.FilterBinding,
) (*wire.FilterBinding, *wire.CatalogPosition, error) {
	identity := binding.Identity
	return catalog.bound(ctx, "release", wire.ResultUnbound, wire.FilterMutation{
		Kind: wire.MutateUnbind, Group: &binding.Group,
		ExpectedDigest: &binding.Digest, ExpectedIdentity: &identity,
	})
}

// ConfigureGroup sets a group's whole policy in one catalog transaction,
// saving the definition as the group's own filter and binding the group to it.
//
// A repeat with the same content keeps the binding rather than adding a second
// revision, so a deployment that configures its groups on every start
// converges rather than accumulating. Another digest conflicts.
func (catalog *FilterCatalog) ConfigureGroup(
	ctx context.Context, group wire.FilterGroupRef, policy wire.GroupFilterSpec,
) (*wire.FilterBinding, *wire.CatalogPosition, error) {
	policy.Filter = catalog.scopedFilter(policy.Filter)
	return catalog.bound(ctx, "configure group", wire.ResultBound, wire.FilterMutation{
		Kind: wire.MutateConfigureGroup, Group: &group, Policy: &policy,
	})
}

// ConfigureGroupAs configures a group, holding the change to one exact group
// incarnation and one operation identity.
func (catalog *FilterCatalog) ConfigureGroupAs(
	ctx context.Context, operationID wire.U128, group wire.FilterGroupRef,
	policy wire.GroupFilterSpec, expected wire.FilterGroupIdentity,
) (*wire.FilterBinding, *wire.CatalogPosition, error) {
	policy.Filter = catalog.scopedFilter(policy.Filter)
	applied, err := catalog.ApplyAs(ctx, operationID, wire.FilterMutation{
		Kind: wire.MutateConfigureGroup, Group: &group,
		Policy: &policy, ExpectedIdentity: &expected,
	})
	if err != nil {
		return nil, nil, err
	}
	return bindingOf("configure group", applied, wire.ResultBound)
}

// scopedFilter resolves a filter's writer schemas in the registry this handle registers schemas in, unless the caller named one.
func (catalog *FilterCatalog) scopedFilter(filter wire.ConsumerFilter) wire.ConsumerFilter {
	if stream := catalog.laser.resourceStream(); len(filter.SchemaRefs) > 0 && filter.SchemaStream == nil && stream != "" {
		filter.SchemaStream = &stream
	}
	return filter
}

// bound sends a change whose result is a binding.
func (catalog *FilterCatalog) bound(
	ctx context.Context, verb string, want wire.FilterMutationResultKind,
	mutation wire.FilterMutation,
) (*wire.FilterBinding, *wire.CatalogPosition, error) {
	applied, err := catalog.apply(ctx, mutation)
	if err != nil {
		return nil, nil, err
	}
	return bindingOf(verb, applied, want)
}

func bindingOf(
	verb string, applied *AppliedMutation, want wire.FilterMutationResultKind,
) (*wire.FilterBinding, *wire.CatalogPosition, error) {
	if applied.Result.Kind != want || applied.Result.Binding == nil {
		return nil, nil, unexpectedMutationResult(verb, applied.Result.Kind)
	}
	return applied.Result.Binding, applied.Position, nil
}

// Filter reads one saved filter.
func (catalog *FilterCatalog) Filter(
	ctx context.Context, filterID uint32,
) (*wire.FilterDetail, error) {
	outcome, err := catalog.execute(ctx, wire.AgdxGetFilterCode,
		wire.GetFilter{V: wire.FilterOpVersion, FilterID: filterID})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.CatalogFilter || outcome.Filter == nil {
		return nil, unexpectedCatalogOutcome("filter", outcome.Kind)
	}
	return outcome.Filter, nil
}

// FilterQuery narrows a listing of saved filters.
type FilterQuery struct {
	// NameContains keeps only the filters whose name contains it.
	NameContains *string
	// State keeps only the filters in one state.
	State *wire.FilterState
	// Owner keeps only the own filter of one consumer group incarnation,
	// bound or not.
	Owner *wire.FilterGroupIdentity
}

// Filters lists the saved filters, newest first by id. Page counts from 1 and
// results is the page length.
func (catalog *FilterCatalog) Filters(
	ctx context.Context, query FilterQuery, page, results uint32,
) (*wire.FilterPage, error) {
	outcome, err := catalog.execute(ctx, wire.AgdxListFiltersCode, wire.ListFilters{
		V: wire.FilterOpVersion, NameContains: query.NameContains,
		State: query.State, Owner: query.Owner,
		Page: page, Results: results,
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.CatalogFilters || outcome.Filters == nil {
		return nil, unexpectedCatalogOutcome("filters", outcome.Kind)
	}
	return outcome.Filters, nil
}

// OwnFilter is the filter one consumer group incarnation owns, bound or
// released, nil when it has none.
//
// A group's own filter is the one ConfigureGroup saved for it, which is what
// distinguishes a group's policy from a shared filter it happens to be bound
// to.
func (catalog *FilterCatalog) OwnFilter(
	ctx context.Context, owner wire.FilterGroupIdentity,
) (*wire.FilterSummary, error) {
	page, err := catalog.Filters(ctx, FilterQuery{Owner: &owner}, 1, 1)
	if err != nil {
		return nil, err
	}
	for index := range page.Items {
		summary := page.Items[index]
		if summary.Owner != nil && summary.Owner.Identity == owner {
			return &summary, nil
		}
	}
	return nil, nil
}

// Revisions lists one filter's revisions, newest first. Page counts from 1 and
// results is the page length.
func (catalog *FilterCatalog) Revisions(
	ctx context.Context, filterID, page, results uint32,
) (*wire.FilterRevisionPage, error) {
	outcome, err := catalog.execute(ctx, wire.AgdxListFilterRevisionsCode,
		wire.ListFilterRevisions{
			V: wire.FilterOpVersion, FilterID: filterID,
			Page: page, Results: results,
		})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.CatalogRevisions || outcome.Revisions == nil {
		return nil, unexpectedCatalogOutcome("revisions", outcome.Kind)
	}
	return outcome.Revisions, nil
}

// Binding reads one group's binding. The failure is not-found when the group
// is unbound.
func (catalog *FilterCatalog) Binding(
	ctx context.Context, group wire.FilterGroupRef,
) (*wire.FilterBinding, error) {
	outcome, err := catalog.execute(ctx, wire.AgdxGetFilterBindingCode,
		wire.GetFilterBinding{V: wire.FilterOpVersion, Group: group})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.CatalogBinding || outcome.Binding == nil {
		return nil, unexpectedCatalogOutcome("binding", outcome.Kind)
	}
	return outcome.Binding, nil
}

// BindingQuery narrows a listing of group bindings.
type BindingQuery struct {
	// FilterID keeps only the groups bound to one filter.
	FilterID *uint32
	// Stream keeps only the groups on one stream.
	Stream *string
	// Topic keeps only the groups on one topic, which needs a stream too.
	Topic *string
}

// Bindings lists the group bindings. Page counts from 1 and results is the
// page length.
func (catalog *FilterCatalog) Bindings(
	ctx context.Context, query BindingQuery, page, results uint32,
) (*wire.FilterBindingPage, error) {
	outcome, err := catalog.execute(ctx, wire.AgdxListFilterBindingsCode,
		wire.ListFilterBindings{
			V: wire.FilterOpVersion, FilterID: query.FilterID,
			Stream: query.Stream, Topic: query.Topic,
			Page: page, Results: results,
		})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.CatalogBindings || outcome.Bindings == nil {
		return nil, unexpectedCatalogOutcome("bindings", outcome.Kind)
	}
	return outcome.Bindings, nil
}

// Operation reads the outcome a change was recorded under.
//
// A caller whose reply was lost reads it rather than reapplying the change,
// which is what makes a catalog mutation safe to retry at all. The failure is
// not-found until the catalog has folded it.
func (catalog *FilterCatalog) Operation(
	ctx context.Context, operationID wire.U128,
) (*wire.FilterMutationOutcome, error) {
	outcome, err := catalog.executeWalking(ctx, wire.AgdxFilterOperationCode,
		wire.GetFilterOperation{V: wire.FilterOpVersion, OperationID: operationID}, walkingOnce())
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.CatalogMutation || outcome.Mutation == nil {
		return nil, unexpectedCatalogOutcome("operation", outcome.Kind)
	}
	return checkedOutcome(operationID, outcome.Mutation)
}

func unexpectedCatalogOutcome(
	verb string, kind wire.FilterCatalogOutcomeKind,
) error {
	return &CodecError{Reason: "the catalog " + verb +
		" answered with an unexpected outcome: " + itoa(int(kind))}
}

func unexpectedMutationResult(
	verb string, kind wire.FilterMutationResultKind,
) error {
	return &CodecError{Reason: "the catalog " + verb +
		" produced an unexpected result: " + itoa(int(kind))}
}
