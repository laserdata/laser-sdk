package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The pauses between two status reads of a materialization wait. Each pause
// doubles the one before it up to the cap, so a setup that lands quickly is
// noticed quickly and a slow one is not polled to death.
const (
	MaterializationPollStart = 50 * time.Millisecond
	MaterializationPollMax   = time.Second
)

// MaterializeRequest is one topic's materialization setup.
//
// It is the short path through the registries: a topic goes from a log to a
// queryable table in one call, rather than through a projection, a binding and
// an index the caller has to keep consistent by hand.
type MaterializeRequest struct {
	topic        *Topic
	index        string
	projectionID string
	projection   *wire.Projection
	backend      *wire.BackendBinding
	retention    *wire.RetentionPolicy
	invalid      wire.InvalidRecordPolicy
	oversize     wire.OversizePolicy
	operationID  wire.U128
	pinned       bool
}

// Materialize begins a materialization setup for this topic.
//
// With nothing else set it materializes every record whole into an index named
// after the topic, which is the setup that needs no decisions.
func (topic *Topic) Materialize() *MaterializeRequest {
	return &MaterializeRequest{
		topic:       topic,
		invalid:     wire.InvalidRecordBlock,
		oversize:    wire.OversizeBlock,
		operationID: NewOperationID(),
	}
}

// Index is the index the binding writes, the topic name by default.
func (request *MaterializeRequest) Index(index string) *MaterializeRequest {
	request.index = index
	return request.changed()
}

// ProjectionID is the projection's id, the typed projection's own id or else
// the index name by default.
func (request *MaterializeRequest) ProjectionID(id string) *MaterializeRequest {
	request.projectionID = id
	return request.changed()
}

// Typed materializes the fields a projection extracts instead of whole
// records, for a caller that wants columns rather than bodies.
func (request *MaterializeRequest) Typed(
	projection wire.Projection,
) *MaterializeRequest {
	request.projection = &projection
	return request.changed()
}

// Backend is where the index lives, the deployment's default when unset.
func (request *MaterializeRequest) Backend(
	backend wire.BackendBinding,
) *MaterializeRequest {
	request.backend = &backend
	return request.changed()
}

// Retention is how long rows stay. Keyed history keeps them by default,
// because a keyed view that expired rows would answer as though keys had been
// deleted.
func (request *MaterializeRequest) Retention(
	retention wire.RetentionPolicy,
) *MaterializeRequest {
	request.retention = &retention
	return request.changed()
}

// InvalidRecords is what a record that does not materialize does: hold its
// partition, the default, or skip to the dead-letter topic.
//
// Holding is the default because skipping silently is how a table quietly
// stops matching its log.
func (request *MaterializeRequest) InvalidRecords(
	policy wire.InvalidRecordPolicy,
) *MaterializeRequest {
	request.invalid = policy
	return request.changed()
}

// Oversize is what a body over the backend's limit does: hold its partition,
// the default, skip it, or keep the row without the body the source log still
// holds.
func (request *MaterializeRequest) Oversize(
	policy wire.OversizePolicy,
) *MaterializeRequest {
	request.oversize = policy
	return request.changed()
}

// OperationID pins the setup identity.
//
// Without it every content change mints a fresh one, so a corrected setup is
// never refused as a replay of the one it corrects. Pinning is for a caller
// that drives its own recovery across a restart.
func (request *MaterializeRequest) OperationID(id wire.U128) *MaterializeRequest {
	request.operationID, request.pinned = id, true
	return request
}

// Operation is the setup identity Apply sends. Re-applying the same request
// after an uncertain answer applies the setup once.
func (request *MaterializeRequest) Operation() wire.U128 { return request.operationID }

// changed mints a new identity for a changed setup, unless the caller pinned
// one.
func (request *MaterializeRequest) changed() *MaterializeRequest {
	if !request.pinned {
		request.operationID = NewOperationID()
	}
	return request
}

// Apply registers the projection, applies its binding, and returns the handle
// to wait on.
//
// The two steps carry derived identities of one setup, so a retry of either
// applies once and the pair cannot half-apply under two unrelated identities.
func (request *MaterializeRequest) Apply(
	ctx context.Context,
) (*Materialization, error) {
	stream := request.topic.stream
	if stream == "" {
		return nil, errNoStream()
	}
	laser := request.topic.laser
	if !laser.capabilities.Managed || !laser.capabilities.Materializations {
		return nil, &UnsupportedError{Surface: "materializations"}
	}
	scoped := laser.WithDefaultStream(stream)
	projection, binding, err := request.plan(stream)
	if err != nil {
		return nil, err
	}
	id, index := string(projection.ID), binding.Index
	registered := scoped.scopedProjection(projection)
	bound := scoped.scopedBinding(binding)
	if err := scoped.PublishControlAs(ctx, wire.ControlCommand{
		Kind: wire.CommandRegisterProjection, Projection: &registered,
	}, stream, wire.MaterializationOperationID(request.operationID, 1)); err != nil {
		return nil, err
	}
	if err := scoped.PublishControlAs(ctx, wire.ControlCommand{
		Kind: wire.CommandApplyBinding, Binding: &bound,
	}, stream, wire.MaterializationOperationID(request.operationID, 2)); err != nil {
		return nil, err
	}
	return &Materialization{
		laser: scoped, projectionID: id,
		source: &wire.SourceSelector{Stream: stream, Topic: request.topic.name},
		index:  index, hasIndex: true,
	}, nil
}

// plan is the projection and binding Apply publishes.
//
// Every rule the managed backend checks is checked here first, so a binding
// the backend would refuse never leaves a registered projection behind with
// nothing routing into it.
func (request *MaterializeRequest) plan(
	stream string,
) (wire.Projection, wire.ProjectionBinding, error) {
	refuse := func(reason string) (wire.Projection, wire.ProjectionBinding, error) {
		return wire.Projection{}, wire.ProjectionBinding{},
			&InvalidError{Reason: reason}
	}
	if request.operationID.IsZero() {
		return refuse("an operation id must be nonzero")
	}
	index := request.index
	if index == "" {
		index = request.topic.name
	}
	id := request.projectionID
	switch {
	case id != "":
	case request.projection != nil:
		id = string(request.projection.ID)
	default:
		id = index
	}
	projection := wire.Projection{
		ID: wire.ProjectionID(id), Kind: wire.ProjectionRecord,
		ContentType: wire.ContentAny,
	}
	if request.projection != nil {
		projection = *request.projection
	}
	if projection.Kind == wire.ProjectionGraph {
		return refuse("a materialization needs a row or record projection")
	}
	projection.ID = wire.ProjectionID(id)
	// A keyed view that expired rows would answer as though keys had been
	// deleted, so only the retentions that keep a row are allowed with one.
	if !projection.History.IsAppend() && request.retention != nil &&
		!keepsRows(*request.retention) {
		return refuse("keyed history needs Keep, KeepUntilSourceDeleted or " +
			"TimeToLive retention")
	}
	// A reference stands in for a body, and a row projection has no body to
	// stand in for.
	if request.oversize == wire.OversizeReference &&
		projection.Kind != wire.ProjectionRecord {
		return refuse("body references apply only to a whole-record projection")
	}
	binding := wire.ProjectionBinding{
		Source:             wire.SourceSelector{Stream: stream, Topic: request.topic.name},
		AllowedProjections: []wire.ProjectionID{wire.ProjectionID(id)},
		DefaultProjection:  ptrOf(wire.ProjectionID(id)),
		Index:              index,
		InvalidRecords:     request.invalid,
		Oversize:           request.oversize,
		Backend:            request.backend,
	}
	switch {
	case request.retention != nil:
		binding.Retention = request.retention
	case !projection.History.IsAppend():
		// A keyed view keeps its rows unless the caller says otherwise, since
		// the row is the current value rather than one event among many.
		binding.Retention = ptrOf(wire.KeepRetention())
	}
	// Only the binding is checked here. The control publish validates the
	// projection on its way out, and the register goes first, so what this
	// has to catch early is a binding that would be refused after the
	// projection was already registered with nothing routing into it.
	if err := binding.Validate(); err != nil {
		return refuse(err.Error())
	}
	return projection, binding, nil
}

// keepsRows reports a retention under which a row survives.
func keepsRows(retention wire.RetentionPolicy) bool {
	switch retention.Kind {
	case wire.RetentionKeep, wire.RetentionKeepUntilSourceDeleted,
		wire.RetentionTimeToLive:
		return true
	default:
		return false
	}
}

// Materialization is a handle to one materialization's progress.
type Materialization struct {
	laser        *Laser
	projectionID string
	// source and index are what this handle follows. A handle that follows
	// none reports every binding of the projection.
	source   *wire.SourceSelector
	index    string
	hasIndex bool
}

// Materialization opens a handle to every binding of one projection, named
// under this handle's default stream.
func (laser *Laser) Materialization(projectionID string) *Materialization {
	return &Materialization{laser: laser, projectionID: projectionID}
}

// Materialization opens a handle to this topic's materialization, read under
// the topic's own stream.
func (topic *Topic) Materialization(projectionID string) (*Materialization, error) {
	if topic.stream == "" {
		return nil, errNoStream()
	}
	return &Materialization{
		laser:        topic.laser.WithDefaultStream(topic.stream),
		projectionID: projectionID,
		source:       &wire.SourceSelector{Stream: topic.stream, Topic: topic.name},
	}, nil
}

// ProjectionID is the projection this handle follows.
func (materialization *Materialization) ProjectionID() string {
	return materialization.projectionID
}

// Status is each followed binding's progress, nil when no projection holds
// the id or, for a handle tied to one source, when that source is no longer
// bound to its index.
//
// Those two are deliberately one answer: a caller waiting on a setup wants to
// know it is gone, and a binding that was removed is as gone as a projection
// that never existed.
func (materialization *Materialization) Status(
	ctx context.Context,
) ([]wire.BindingStatus, bool, error) {
	if !materialization.laser.capabilities.Materializations {
		return nil, false, &UnsupportedError{Surface: "materialization status"}
	}
	info, err := materialization.laser.Projections().read(
		ctx, materialization.projectionID, true)
	if err != nil || info == nil {
		return nil, false, err
	}
	status, followed := materialization.followed(*info)
	return status, followed, nil
}

// followed narrows a projection's bindings to the ones this handle follows.
func (materialization *Materialization) followed(
	info wire.ProjectionInfo,
) ([]wire.BindingStatus, bool) {
	if materialization.source == nil {
		return info.Status, true
	}
	follows := func(source wire.SourceSelector, index string) bool {
		if source != *materialization.source {
			return false
		}
		return !materialization.hasIndex || index == materialization.index
	}
	bound := false
	for _, binding := range info.Bindings {
		if follows(binding.Source, binding.Index) {
			bound = true
			break
		}
	}
	if !bound {
		return nil, false
	}
	kept := make([]wire.BindingStatus, 0, len(info.Status))
	for _, status := range info.Status {
		if follows(status.Source, status.Index) {
			kept = append(kept, status)
		}
	}
	return kept, true
}

// MaterializationWaitKind is how a wait ended.
type MaterializationWaitKind string

const (
	// WaitReady means every followed binding reached the level.
	WaitReady MaterializationWaitKind = "ready"
	// WaitBlocked means a record or a history gap holds a partition, so the
	// level will not be reached without an operator.
	WaitBlocked MaterializationWaitKind = "blocked"
	// WaitFailed means creating a destination table failed in a way no retry
	// recovers.
	WaitFailed MaterializationWaitKind = "failed"
	// WaitNotFound means no projection holds the id, or the followed source
	// is no longer bound to it.
	WaitNotFound MaterializationWaitKind = "not_found"
	// WaitTimedOut means the bound passed first.
	WaitTimedOut MaterializationWaitKind = "timed_out"
)

// MaterializationWait is how a wait ended, with the last status it read.
type MaterializationWait struct {
	Kind   MaterializationWaitKind
	Status []wire.BindingStatus
}

// Ready reports a wait that reached its level.
func (wait MaterializationWait) Ready() bool { return wait.Kind == WaitReady }

// Wait reads the status until every followed binding reaches a level, a
// record or a gap holds one, realization fails for good, the binding is gone,
// or the bound passes.
//
// A level past realized is measured against the heads captured on the first
// complete read rather than against whatever the source holds now, because a
// source that keeps growing would otherwise never be caught up with. A source
// recreated during the wait drops that target: the heads it captured no longer
// exist.
func (materialization *Materialization) Wait(
	ctx context.Context, level wire.MaterializationReadiness, bound time.Duration,
) (*MaterializationWait, error) {
	deadline := time.Now().Add(bound)
	var target, last []wire.BindingStatus
	pause := MaterializationPollStart
	for {
		waiting, cancel := context.WithDeadline(ctx, deadline)
		status, followed, err := materialization.Status(waiting)
		cancel()
		switch {
		case err != nil && ctx.Err() != nil:
			return nil, err
		case err != nil && !time.Now().Before(deadline):
			return &MaterializationWait{Kind: WaitTimedOut, Status: last}, nil
		case err != nil:
			return nil, err
		case !followed:
			return &MaterializationWait{Kind: WaitNotFound}, nil
		}
		if holds, kind := settledWait(status); holds {
			return &MaterializationWait{Kind: kind, Status: status}, nil
		}
		target = retargeted(target, status)
		if ready(level, status, target) {
			return &MaterializationWait{Kind: WaitReady, Status: status}, nil
		}
		last = status
		if !sleepUntil(ctx, pause, deadline) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return &MaterializationWait{Kind: WaitTimedOut, Status: last}, nil
		}
		pause = min(pause*2, MaterializationPollMax)
	}
}

// settledWait reports a status no further reading will improve.
func settledWait(
	status []wire.BindingStatus,
) (bool, MaterializationWaitKind) {
	for _, binding := range status {
		if binding.IsBlocked() {
			return true, WaitBlocked
		}
	}
	for _, binding := range status {
		if binding.IsFailed() {
			return true, WaitFailed
		}
	}
	return false, ""
}

// retargeted captures the heads a catch-up is measured against, dropping a
// target whose source was recreated under it.
func retargeted(
	target, status []wire.BindingStatus,
) []wire.BindingStatus {
	if recreated(target, status) {
		target = nil
	}
	if target != nil || len(status) == 0 {
		return target
	}
	// A status without an incarnation has not reported where its source
	// stands, so capturing it would target a head nobody measured.
	for _, binding := range status {
		if binding.SourceIncarnation == nil {
			return nil
		}
	}
	captured := make([]wire.BindingStatus, len(status))
	copy(captured, status)
	return captured
}

// recreated reports a source that was replaced since the target was captured,
// so the heads it holds can never be reached.
func recreated(target, status []wire.BindingStatus) bool {
	for _, wanted := range target {
		for _, binding := range status {
			if binding.Source != wanted.Source || binding.Index != wanted.Index {
				continue
			}
			if binding.SourceIncarnation == nil {
				continue
			}
			if wanted.SourceIncarnation == nil ||
				*binding.SourceIncarnation != *wanted.SourceIncarnation {
				return true
			}
		}
	}
	return false
}

// ready reports every followed binding having reached the level.
func ready(
	level wire.MaterializationReadiness, status, target []wire.BindingStatus,
) bool {
	if len(status) == 0 {
		return false
	}
	for _, binding := range status {
		if level <= wire.ReadinessRealized {
			if binding.Readiness() < level {
				return false
			}
			continue
		}
		if !caughtUp(binding, target) {
			return false
		}
	}
	// A binding that was in the target and is not in this read has gone
	// missing, so the wait has not finished what it set out to measure.
	for _, wanted := range target {
		present := false
		for _, binding := range status {
			if binding.Source == wanted.Source && binding.Index == wanted.Index {
				present = true
				break
			}
		}
		if !present {
			return false
		}
	}
	return true
}

// caughtUp reports one binding having reached the head its target captured.
func caughtUp(binding wire.BindingStatus, target []wire.BindingStatus) bool {
	for _, wanted := range target {
		if wanted.Source == binding.Source && wanted.Index == binding.Index &&
			binding.CaughtUpTo(wanted) {
			return true
		}
	}
	return false
}

// sleepUntil waits out a pause, reporting false when the deadline or the
// caller's context came first.
func sleepUntil(ctx context.Context, pause time.Duration, deadline time.Time) bool {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	if pause > remaining {
		pause = remaining
	}
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return time.Now().Before(deadline)
	}
}
