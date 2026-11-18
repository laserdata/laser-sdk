package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A topic goes from a log to a queryable table in one call, rather than
// through a projection, a binding and an index a caller keeps consistent by
// hand.
func TestGivenATopicWhenMaterializedThenShouldRegisterAndBindInOneCall(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})

	materialization, err := handle.Topic("readings").Materialize().
		Apply(context.Background())
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if len(server.codes) != 2 {
		t.Fatalf("want a register and a bind, got %d commands", len(server.codes))
	}
	registered := server.control(t, 0)
	if registered.Command.Kind != wire.CommandRegisterProjection {
		t.Errorf("want a register first, got %d", registered.Command.Kind)
	}
	// Nothing else set, so it materializes every record whole into an index
	// named after the topic.
	projection := registered.Command.Projection
	if projection == nil || projection.Kind != wire.ProjectionRecord {
		t.Fatalf("want a whole-record projection, got %+v", projection)
	}
	if projection.ID != "stream:agents/readings" {
		t.Errorf("want the topic's own name, got %q", projection.ID)
	}
	bound := server.control(t, 1)
	if bound.Command.Kind != wire.CommandApplyBinding {
		t.Errorf("want the bind second, got %d", bound.Command.Kind)
	}
	binding := bound.Command.Binding
	if binding == nil || binding.Index != "stream:agents/readings" {
		t.Fatalf("want the index named after the topic, got %+v", binding)
	}
	if binding.Source.Topic != "readings" || binding.Source.Stream != "agents" {
		t.Errorf("want the source named, got %+v", binding.Source)
	}
	// Holding is the default, because skipping silently is how a table
	// quietly stops matching its log. The default is omitted on the wire, so
	// it reads back as the policy rather than as its name.
	if !binding.InvalidRecords.IsBlock() || !binding.Oversize.IsBlock() {
		t.Errorf("want both policies holding, got %+v", binding)
	}
	if materialization.ProjectionID() != "readings" {
		t.Errorf("want the handle on the projection, got %q",
			materialization.ProjectionID())
	}
}

// The two steps of one setup carry derived identities, so a retry of either
// applies once and the pair cannot half-apply under unrelated identities.
func TestGivenASetupWhenAppliedThenShouldDeriveBothStepIdentities(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})
	request := handle.Topic("readings").Materialize()
	setup := request.Operation()

	if _, err := request.Apply(context.Background()); err != nil {
		t.Fatalf("apply: %v", err)
	}

	first := managedIdentity(t, server, 0)
	second := managedIdentity(t, server, 1)
	if first != wire.MaterializationOperationID(setup, 1) {
		t.Error("the register carries the first derived identity")
	}
	if second != wire.MaterializationOperationID(setup, 2) {
		t.Error("the bind carries the second")
	}
	if first == second {
		t.Error("two steps must not share one identity")
	}
	// Re-applying the same request repeats both identities, which is what
	// makes an uncertain answer safe to retry.
	again := &registryServer{command: wire.ResultOk}
	if _, err := request.Apply(context.Background()); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	_ = again
	if managedIdentity(t, server, 2) != first {
		t.Error("a repeated setup repeats its identities")
	}
}

// managedIdentity reads the operation identity one managed request carried.
func managedIdentity(t *testing.T, server *registryServer, index int) wire.U128 {
	t.Helper()
	server.guard.Lock()
	defer server.guard.Unlock()
	var envelope wire.ManagedRequestEnvelope
	if err := wire.Unmarshal(server.requests[index], &envelope); err != nil {
		t.Fatalf("decode the managed envelope: %v", err)
	}
	return envelope.OperationID
}

// A corrected setup must never be refused as a replay of the one it corrects,
// so changing the content mints a fresh identity.
func TestGivenAChangedSetupWhenReappliedThenShouldMintAFreshIdentity(t *testing.T) {
	handle := registryHandle(&registryServer{command: wire.ResultOk}, laser.Options{})

	request := handle.Topic("readings").Materialize()
	before := request.Operation()
	after := request.Index("elsewhere").Operation()

	if before == after {
		t.Error("a changed setup is not a replay of the one it changed")
	}

	// A caller that pins the identity is driving its own recovery, so a
	// change keeps the identity it pinned.
	pinned := laser.NewOperationID()
	held := handle.Topic("readings").Materialize().OperationID(pinned)
	if held.Index("elsewhere").Operation() != pinned {
		t.Error("a pinned identity survives a change")
	}
	if held.Typed(rowProjection("reading.v1")).Operation() != pinned {
		t.Error("a pinned identity survives every change")
	}
}

// A setup names what it writes, so a caller that wants columns rather than
// bodies says so once.
func TestGivenATypedSetupWhenAppliedThenShouldMaterializeItsFields(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})

	if _, err := handle.Topic("readings").Materialize().
		Typed(rowProjection("reading.v1")).
		Index("reading_rows").
		Retention(wire.KeepRetention()).
		InvalidRecords(wire.InvalidRecordDeadLetter).
		Apply(context.Background()); err != nil {
		t.Fatalf("apply: %v", err)
	}

	projection := server.control(t, 0).Command.Projection
	if projection == nil || projection.Kind != wire.ProjectionRow {
		t.Fatalf("want a row projection, got %+v", projection)
	}
	// Without an explicit id, a typed setup takes the projection's own.
	if projection.ID != "stream:agents/reading.v1" {
		t.Errorf("want the projection's own id, got %q", projection.ID)
	}
	binding := server.control(t, 1).Command.Binding
	if binding == nil || binding.Index != "stream:agents/reading_rows" {
		t.Fatalf("want the named index, got %+v", binding)
	}
	if binding.InvalidRecords != wire.InvalidRecordDeadLetter {
		t.Errorf("want the policy carried, got %q", binding.InvalidRecords)
	}
	if binding.Retention == nil || binding.Retention.Kind != wire.RetentionKeep {
		t.Errorf("want the retention carried, got %+v", binding.Retention)
	}
}

// An explicit projection id wins over both the typed projection's own and the
// index name, because a caller that named one meant it.
func TestGivenAnExplicitProjectionIDWhenAppliedThenShouldUseIt(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})

	if _, err := handle.Topic("readings").Materialize().
		Typed(rowProjection("reading.v1")).ProjectionID("chosen").
		Apply(context.Background()); err != nil {
		t.Fatalf("apply: %v", err)
	}

	projection := server.control(t, 0).Command.Projection
	if projection == nil || projection.ID != "stream:agents/chosen" {
		t.Fatalf("want the chosen id, got %+v", projection)
	}
	binding := server.control(t, 1).Command.Binding
	if len(binding.AllowedProjections) != 1 ||
		binding.AllowedProjections[0] != "stream:agents/chosen" {
		t.Errorf("want the binding to allow it, got %v", binding.AllowedProjections)
	}
	if binding.DefaultProjection == nil ||
		*binding.DefaultProjection != "stream:agents/chosen" {
		t.Errorf("want it as the default, got %v", binding.DefaultProjection)
	}
}

// A keyed view keeps its rows unless the caller says otherwise, because the
// row is the current value rather than one event among many.
func TestGivenAKeyedProjectionWhenAppliedThenShouldKeepItsRows(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})
	keyed := rowProjection("reading.v1")
	keyed.History = wire.KeyedHistory(wire.PointerProjectionKey("/host"), nil)

	if _, err := handle.Topic("readings").Materialize().Typed(keyed).
		Apply(context.Background()); err != nil {
		t.Fatalf("apply: %v", err)
	}

	binding := server.control(t, 1).Command.Binding
	if binding.Retention == nil || binding.Retention.Kind != wire.RetentionKeep {
		t.Fatalf("want keep retention, got %+v", binding.Retention)
	}
}

// A setup the backend would refuse is refused here, so a refused binding
// never leaves a registered projection behind with nothing routing into it.
func TestGivenASetupTheBackendWouldRefuseWhenAppliedThenShouldSendNothing(t *testing.T) {
	keyed := rowProjection("reading.v1")
	keyed.History = wire.KeyedHistory(wire.PointerProjectionKey("/host"), nil)
	graph := rowProjection("entities.v1")
	graph.Kind = wire.ProjectionGraph

	for name, refused := range map[string]func(*laser.MaterializeRequest) *laser.MaterializeRequest{
		"a graph projection": func(request *laser.MaterializeRequest) *laser.MaterializeRequest {
			return request.Typed(graph)
		},
		"a keyed view that expires its rows": func(
			request *laser.MaterializeRequest,
		) *laser.MaterializeRequest {
			// Expiring a keyed row would answer as though the key had been
			// deleted.
			return request.Typed(keyed).Retention(wire.MirrorLogRetention())
		},
		"a body reference on a row projection": func(
			request *laser.MaterializeRequest,
		) *laser.MaterializeRequest {
			// A reference stands in for a body, and a row projection has none.
			return request.Typed(rowProjection("reading.v1")).
				Oversize(wire.OversizeReference)
		},
		"a setup with no identity": func(
			request *laser.MaterializeRequest,
		) *laser.MaterializeRequest {
			return request.OperationID(wire.U128{})
		},
		"a projection the contract itself refuses": func(
			request *laser.MaterializeRequest,
		) *laser.MaterializeRequest {
			// A whole-record projection materializes the record as it stands,
			// so an extraction plan beside it describes a second, different
			// row.
			contradictory := rowProjection("reading.v1")
			contradictory.Kind = wire.ProjectionRecord
			return request.Typed(contradictory)
		},
		"a binding the contract itself refuses": func(
			request *laser.MaterializeRequest,
		) *laser.MaterializeRequest {
			// A referenced body lives only in the source log, so a row that
			// outlived its record would point at nothing. Only the binding's
			// own rules catch that pairing.
			return request.Oversize(wire.OversizeReference).
				Retention(wire.KeepRetention())
		},
		"a retention the contract itself refuses": func(
			request *laser.MaterializeRequest,
		) *laser.MaterializeRequest {
			return request.Retention(wire.TimeToLiveRetention(0))
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := &registryServer{command: wire.ResultOk}
			handle := registryHandle(server, laser.Options{})

			_, err := refused(handle.Topic("readings").Materialize()).
				Apply(context.Background())

			var invalid *laser.InvalidError
			if !errors.As(err, &invalid) {
				t.Fatalf("want an InvalidError, got %T: %v", err, err)
			}
			if len(server.codes) != 0 {
				t.Error("a refused setup must register nothing")
			}
		})
	}
}

// Materializations are served where a managed backend serves them, so a
// deployment without one is refused rather than left with a half setup.
func TestGivenNoMaterializationsWhenAppliedThenShouldRefuseIt(t *testing.T) {
	capabilities := registryCapabilities()
	capabilities.Materializations = false
	server := &registryServer{command: wire.ResultOk}

	_, err := registryHandle(server, laser.Options{Capabilities: capabilities}).
		Topic("readings").Materialize().Apply(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported setup must not reach the socket")
	}
}

// A handle tied to one source reports that source alone, because a caller
// waiting on its own topic does not want another's progress.
func TestGivenAHandleOnOneSourceWhenStatusIsReadThenShouldReportThatSource(t *testing.T) {
	ours := wire.SourceSelector{Stream: "agents", Topic: "readings"}
	theirs := wire.SourceSelector{Stream: "agents", Topic: "elsewhere"}
	server := &registryServer{outcome: &wire.BrowseOutcome{
		Kind: wire.BrowseProjection,
		Projection: &wire.ProjectionInfo{
			Projection: rowProjection("stream:agents/readings"),
			Bindings: []wire.ProjectionBinding{
				{Source: ours, Index: "stream:agents/readings"},
				{Source: theirs, Index: "stream:agents/elsewhere"},
			},
			Status: []wire.BindingStatus{
				{Source: ours, Index: "stream:agents/readings"},
				{Source: theirs, Index: "stream:agents/elsewhere"},
			},
		},
	}}
	handle := registryHandle(server, laser.Options{})
	materialization, err := handle.Topic("readings").Materialization("readings")
	if err != nil {
		t.Fatalf("handle: %v", err)
	}

	status, followed, err := materialization.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if !followed {
		t.Fatal("the source is bound, so its status is followed")
	}
	if len(status) != 1 || status[0].Source.Topic != "readings" {
		t.Errorf("want this source alone, got %+v", status)
	}

	// A handle on the projection rather than one source reports every
	// binding.
	every, followed, err := handle.Materialization("readings").
		Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !followed || len(every) != 2 {
		t.Errorf("want every binding, got %d", len(every))
	}
}

// A binding that was removed is as gone as a projection that never existed,
// which is what a caller waiting on a setup needs to be told.
func TestGivenAnUnboundSourceWhenStatusIsReadThenShouldReportItGone(t *testing.T) {
	theirs := wire.SourceSelector{Stream: "agents", Topic: "elsewhere"}
	server := &registryServer{outcome: &wire.BrowseOutcome{
		Kind: wire.BrowseProjection,
		Projection: &wire.ProjectionInfo{
			Projection: rowProjection("stream:agents/readings"),
			Bindings: []wire.ProjectionBinding{
				{Source: theirs, Index: "stream:agents/elsewhere"},
			},
		},
	}}
	handle := registryHandle(server, laser.Options{})
	materialization, _ := handle.Topic("readings").Materialization("readings")

	_, followed, err := materialization.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if followed {
		t.Error("a source that is no longer bound is not followed")
	}
}

// Waiting ends as soon as the level is reached, so a caller is not left
// polling a setup that already landed.
func TestGivenARealizedBindingWhenWaitedOnThenShouldReportItReady(t *testing.T) {
	server := &registryServer{outcome: realizedProjection(nil, nil)}
	handle := registryHandle(server, laser.Options{})

	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessRealized, time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	if !wait.Ready() {
		t.Fatalf("want ready, got %q", wait.Kind)
	}
	if len(wait.Status) != 1 {
		t.Errorf("want the status that proved it, got %+v", wait.Status)
	}
}

// A record or a gap holding a partition means the level will not be reached
// without an operator, so the wait says so rather than timing out.
func TestGivenABlockedBindingWhenWaitedOnThenShouldStopAtOnce(t *testing.T) {
	outcome := realizedProjection(nil, nil)
	outcome.Projection.Status[0].Holds = []wire.Hold{{PartitionID: 0}}
	server := &registryServer{outcome: outcome}
	handle := registryHandle(server, laser.Options{})

	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessRealized, 5*time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	if wait.Kind != laser.WaitBlocked {
		t.Fatalf("want blocked, got %q", wait.Kind)
	}
}

// A destination table that failed for good will not realize on its own, so
// the wait reports it rather than polling until the bound.
func TestGivenAPermanentFailureWhenWaitedOnThenShouldReportItFailed(t *testing.T) {
	outcome := realizedProjection(nil, nil)
	outcome.Projection.Status[0].Realization = wire.RealizationPending
	permanent := wire.FailureSchema
	outcome.Projection.Status[0].Failure = &permanent
	server := &registryServer{outcome: outcome}
	handle := registryHandle(server, laser.Options{})

	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessRealized, 5*time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	if wait.Kind != laser.WaitFailed {
		t.Fatalf("want failed, got %q", wait.Kind)
	}
}

// Waiting on a projection nobody registered ends rather than polling for
// something that will never appear.
func TestGivenAnAbsentProjectionWhenWaitedOnThenShouldReportItNotFound(t *testing.T) {
	server := &registryServer{outcome: &wire.BrowseOutcome{Kind: wire.BrowseProjection}}
	handle := registryHandle(server, laser.Options{})

	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessRealized, 5*time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	if wait.Kind != laser.WaitNotFound {
		t.Fatalf("want not found, got %q", wait.Kind)
	}
}

// A setup that has not realized yet is still waiting, so the bound is what
// ends it, with the last status it read.
func TestGivenASetupThatNeverRealizesWhenWaitedOnThenShouldTimeOut(t *testing.T) {
	outcome := realizedProjection(nil, nil)
	outcome.Projection.Status[0].Realization = wire.RealizationPending
	server := &registryServer{outcome: outcome}
	handle := registryHandle(server, laser.Options{})

	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessRealized, time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	if wait.Kind != laser.WaitTimedOut {
		t.Fatalf("want a timeout, got %q", wait.Kind)
	}
	if len(wait.Status) != 1 {
		t.Error("a timeout carries the last status it read")
	}
	// Each pause doubles the one before it, so a second of waiting costs a
	// handful of reads rather than twenty.
	server.guard.Lock()
	reads := len(server.codes)
	server.guard.Unlock()
	if reads > 8 {
		t.Errorf("want a backed-off wait, got %d reads in a second", reads)
	}
}

// A status read needs a deployment that materializes, so one that does not is
// refused rather than reporting a setup it never had.
func TestGivenNoMaterializationsWhenAHandleReadsStatusThenShouldRefuseIt(t *testing.T) {
	capabilities := registryCapabilities()
	capabilities.Materializations = false
	server := &registryServer{}
	handle := registryHandle(server, laser.Options{Capabilities: capabilities})

	_, _, err := handle.Materialization("readings").Status(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported read must not reach the socket")
	}

	// And a wait over it reports the same rather than polling.
	if _, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessRealized, time.Second); err == nil {
		t.Error("a wait on a deployment that materializes nothing must be refused")
	}
}

// A caller that gives up stops the wait rather than holding its bound.
func TestGivenACancelledCallerWhenWaitingThenShouldStop(t *testing.T) {
	outcome := realizedProjection(nil, nil)
	outcome.Projection.Status[0].Realization = wire.RealizationPending
	server := &registryServer{outcome: outcome}
	handle := registryHandle(server, laser.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := handle.Materialization("readings").Wait(
		ctx, wire.ReadinessRealized, time.Minute)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want the cancellation, got %v", err)
	}
}

// realizedProjection answers one realized binding, at an optional source head
// and applied offset.
func realizedProjection(head, applied *uint64) *wire.BrowseOutcome {
	source := wire.SourceSelector{Stream: "agents", Topic: "readings"}
	incarnation := uint64(7)
	status := wire.BindingStatus{
		Source: source, Index: "stream:agents/readings",
		Realization: wire.RealizationRealized, SourceIncarnation: &incarnation,
	}
	if head != nil {
		status.Partitions = []wire.PartitionProgress{{
			PartitionID: 0, HeadOffset: head, AppliedOffset: applied,
		}}
	}
	return &wire.BrowseOutcome{
		Kind: wire.BrowseProjection,
		Projection: &wire.ProjectionInfo{
			Projection: rowProjection("stream:agents/readings"),
			Bindings: []wire.ProjectionBinding{
				{Source: source, Index: "stream:agents/readings"},
			},
			Status: []wire.BindingStatus{status},
		},
	}
}

// A catch-up is measured against the heads the first complete read captured,
// not against whatever the source holds now: a source that keeps growing
// would otherwise never be caught up with.
func TestGivenAGrowingSourceWhenCaughtUpThenShouldMeasureTheCapturedHeads(t *testing.T) {
	behind := realizedProjection(ptr(uint64(10)), ptr(uint64(5)))
	// By the second read the destination reached the captured head, and the
	// source has grown past it.
	arrived := realizedProjection(ptr(uint64(40)), ptr(uint64(10)))
	server := &registryServer{script: []*wire.BrowseOutcome{behind, arrived}}
	handle := registryHandle(server, laser.Options{})

	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessCaughtUp, 5*time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	if !wait.Ready() {
		t.Fatalf("want ready once the captured head was reached, got %q", wait.Kind)
	}
}

// A source recreated during the wait no longer holds the heads the target
// captured, so the wait measures the new incarnation rather than waiting
// forever for offsets that no longer exist.
func TestGivenARecreatedSourceWhenCaughtUpThenShouldRetargetTheNewIncarnation(t *testing.T) {
	first := realizedProjection(ptr(uint64(100)), ptr(uint64(10)))
	recreated := realizedProjection(ptr(uint64(3)), ptr(uint64(3)))
	fresh := uint64(9)
	recreated.Projection.Status[0].SourceIncarnation = &fresh
	server := &registryServer{script: []*wire.BrowseOutcome{first, recreated}}
	handle := registryHandle(server, laser.Options{})

	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessCaughtUp, 5*time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	if !wait.Ready() {
		t.Fatalf("want the new incarnation measured, got %q", wait.Kind)
	}
}

// A status that does not say where its source stands cannot be a target, so
// capturing one would measure a head nobody reported.
func TestGivenNoSourceIncarnationWhenCaughtUpThenShouldNotCaptureATarget(t *testing.T) {
	unknown := realizedProjection(ptr(uint64(10)), ptr(uint64(10)))
	unknown.Projection.Status[0].SourceIncarnation = nil
	server := &registryServer{outcome: unknown}
	handle := registryHandle(server, laser.Options{})

	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessCaughtUp, 120*time.Millisecond)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	if wait.Kind != laser.WaitTimedOut {
		t.Errorf("want a timeout rather than a false catch-up, got %q", wait.Kind)
	}
}

// Waiting on a projection with no bindings at all has reached nothing, so it
// is not ready.
func TestGivenNoBindingsWhenWaitedOnThenShouldNotReportItReady(t *testing.T) {
	empty := &wire.BrowseOutcome{
		Kind: wire.BrowseProjection,
		Projection: &wire.ProjectionInfo{
			Projection: rowProjection("stream:agents/readings"),
		},
	}
	server := &registryServer{outcome: empty}
	handle := registryHandle(server, laser.Options{})

	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessRealized, 120*time.Millisecond)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	if wait.Kind != laser.WaitTimedOut {
		t.Errorf("a projection nothing is bound to has reached nothing, got %q",
			wait.Kind)
	}
}

// A binding that was in the target and is gone from a later read has not
// finished what the wait set out to measure.
func TestGivenABindingThatDisappearsWhenCaughtUpThenShouldNotReportItReady(t *testing.T) {
	ours := wire.SourceSelector{Stream: "agents", Topic: "readings"}
	theirs := wire.SourceSelector{Stream: "agents", Topic: "elsewhere"}
	incarnation := uint64(7)
	both := &wire.BrowseOutcome{
		Kind: wire.BrowseProjection,
		Projection: &wire.ProjectionInfo{
			Projection: rowProjection("stream:agents/readings"),
			Bindings: []wire.ProjectionBinding{
				{Source: ours, Index: "a"}, {Source: theirs, Index: "b"},
			},
			Status: []wire.BindingStatus{
				caughtUpStatus(ours, "a", incarnation),
				behindStatus(theirs, "b", incarnation),
			},
		},
	}
	one := &wire.BrowseOutcome{
		Kind: wire.BrowseProjection,
		Projection: &wire.ProjectionInfo{
			Projection: rowProjection("stream:agents/readings"),
			Bindings:   []wire.ProjectionBinding{{Source: ours, Index: "a"}},
			Status: []wire.BindingStatus{
				caughtUpStatus(ours, "a", incarnation),
			},
		},
	}
	// The first read captures both, and by the second one is gone.
	server := &registryServer{script: []*wire.BrowseOutcome{both, one}}
	handle := registryHandle(server, laser.Options{})

	// Reading one binding at a time would call this ready. Having captured
	// both, it is not.
	wait, err := handle.Materialization("readings").Wait(
		context.Background(), wire.ReadinessCaughtUp, 150*time.Millisecond)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if wait.Kind != laser.WaitTimedOut {
		t.Errorf("want the missing binding to hold the wait, got %q", wait.Kind)
	}
}

// behindStatus is a binding whose destination is behind its source.
func behindStatus(
	source wire.SourceSelector, index string, incarnation uint64,
) wire.BindingStatus {
	status := caughtUpStatus(source, index, incarnation)
	status.Partitions[0].AppliedOffset = ptr(uint64(5))
	return status
}

// caughtUpStatus is a binding that has applied everything its source holds.
func caughtUpStatus(
	source wire.SourceSelector, index string, incarnation uint64,
) wire.BindingStatus {
	head := uint64(10)
	return wire.BindingStatus{
		Source: source, Index: index,
		Realization: wire.RealizationRealized, SourceIncarnation: &incarnation,
		Partitions: []wire.PartitionProgress{{
			PartitionID: 0, HeadOffset: &head, AppliedOffset: &head,
		}},
	}
}
