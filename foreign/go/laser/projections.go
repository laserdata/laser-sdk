package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Projections administers the projection registry: what a deployment
// materializes from a topic, and into what shape.
//
// Writes append control commands and are applied asynchronously, so a caller
// observes the apply by reading the projection back rather than by the append
// succeeding. Reads are registry browses.
type Projections struct{ laser *Laser }

// Projections opens the projection registry.
func (laser *Laser) Projections() *Projections {
	return &Projections{laser: laser}
}

// Register declares a projection, so the deployment creates its backing table
// and starts applying it.
//
// A graph projection materializes nodes and edges rather than a queryable row
// table, so it is refused here and registered through RegisterGraph instead:
// the two create different things and a deployment gates them separately.
func (projections *Projections) Register(
	ctx context.Context, projection wire.Projection,
) error {
	if projection.Kind == wire.ProjectionGraph {
		return &InvalidError{Reason: "the projection " + string(projection.ID) +
			" is a graph projection, so register it with RegisterGraph"}
	}
	scoped := projections.laser.scopedProjection(projection)
	return projections.laser.PublishControl(ctx, wire.ControlCommand{
		Kind: wire.CommandRegisterProjection, Projection: &scoped,
	})
}

// RegisterGraph declares a graph projection: the named knowledge graph, so it
// is discoverable, and the plan for extracting nodes and edges from records.
//
// It creates no row table. A non-graph projection is refused, for the same
// reason a graph one is refused by Register.
func (projections *Projections) RegisterGraph(
	ctx context.Context, projection wire.Projection,
) error {
	if projection.Kind != wire.ProjectionGraph || projection.EntitySchema == nil {
		return &InvalidError{Reason: "the projection " + string(projection.ID) +
			" is not a graph projection, so register it with Register"}
	}
	scoped := projections.laser.scopedProjection(projection)
	return projections.laser.PublishControl(ctx, wire.ControlCommand{
		Kind: wire.CommandRegisterGraph, Projection: &scoped,
	})
}

// Drop stops a projection being applied. The rows it already materialized are
// left alone, because dropping a plan is not the same as deleting data.
func (projections *Projections) Drop(ctx context.Context, id string) error {
	return projections.laser.PublishControl(ctx, wire.ControlCommand{
		Kind: wire.CommandDropProjection,
		Name: projections.laser.ResourceName(id),
	})
}

// DropGraph stops a graph projection being applied, leaving its materialized
// nodes and edges alone.
func (projections *Projections) DropGraph(ctx context.Context, id string) error {
	return projections.laser.PublishControl(ctx, wire.ControlCommand{
		Kind: wire.CommandDropGraph,
		Name: projections.laser.ResourceName(id),
	})
}

// Get reads one projection with its bindings, nil when no projection holds
// that id.
func (projections *Projections) Get(
	ctx context.Context, id string,
) (*wire.ProjectionInfo, error) {
	return projections.read(ctx, id, false)
}

// Status reads each binding's materialization status for one projection, nil
// when no projection holds that id.
//
// A binding whose index the caller may not read is left out rather than
// reported as absent, so a partial answer is not mistaken for a missing one.
func (projections *Projections) Status(
	ctx context.Context, id string,
) ([]wire.BindingStatus, error) {
	if !projections.laser.capabilities.Materializations {
		return nil, &UnsupportedError{Surface: "materialization status"}
	}
	info, err := projections.read(ctx, id, true)
	if err != nil || info == nil {
		return nil, err
	}
	return info.Status, nil
}

func (projections *Projections) read(
	ctx context.Context, id string, status bool,
) (*wire.ProjectionInfo, error) {
	outcome, err := projections.laser.executeBrowse(
		ctx, wire.AgdxGetProjectionCode, wire.GetProjection{
			V:      wire.QueryOpVersion,
			ID:     projections.laser.ResourceName(id),
			Status: status,
		})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.BrowseProjection {
		return nil, unexpectedBrowseOutcome("projection read", outcome.Kind)
	}
	if outcome.Projection == nil {
		return nil, nil
	}
	local := projections.laser.localProjectionInfo(*outcome.Projection)
	return &local, nil
}

// ProjectionQuery narrows a listing of projections.
type ProjectionQuery struct {
	// Topics keeps the projections bound to any of these source topics.
	Topics []string
	// NameContains and IDPrefix match the name and the id.
	NameContains *string
	IDPrefix     *string
	// Search is the one-box convenience matching either.
	Search *string
	// Kinds keeps only the projections of these kinds.
	Kinds []wire.ProjectionKind
}

// List reads one page of the matching projections, each with its extraction
// schema, content type, indexed fields and bindings. No filter lists them all.
func (projections *Projections) List(
	ctx context.Context, query ProjectionQuery, page, results uint32,
) (*wire.ProjectionPage, error) {
	request := wire.ListProjections{
		V: wire.QueryOpVersion, Topics: query.Topics,
		NameContains: query.NameContains, IDPrefix: query.IDPrefix,
		Search: query.Search, Kinds: query.Kinds,
		Page: page, Results: results,
	}
	if stream := projections.laser.resourceStream(); stream != "" {
		request.Stream = &stream
	}
	outcome, err := projections.laser.executeBrowse(
		ctx, wire.AgdxListProjectionsCode, request)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.BrowseProjections {
		return nil, unexpectedBrowseOutcome("projection listing", outcome.Kind)
	}
	page_ := outcome.Projections
	for index := range page_.Items {
		page_.Items[index] = projections.laser.localProjectionInfo(page_.Items[index])
	}
	return &page_, nil
}

// Bindings administers which source topics route into which projections.
type Bindings struct{ laser *Laser }

// Bindings opens the binding surface.
func (laser *Laser) Bindings() *Bindings { return &Bindings{laser: laser} }

// Apply routes a source into its projections, so the deployment starts a
// projector for it. The referenced projections must be registered first.
func (bindings *Bindings) Apply(
	ctx context.Context, binding wire.ProjectionBinding,
) error {
	scoped := bindings.laser.scopedBinding(binding)
	return bindings.laser.PublishControl(ctx, wire.ControlCommand{
		Kind: wire.CommandApplyBinding, Binding: &scoped,
	})
}

// Remove stops a projector for one source, so new records no longer
// materialize. The rows already written stay.
//
// Naming one projection scopes the removal to it, leaving the rest of the
// binding routing.
func (bindings *Bindings) Remove(
	ctx context.Context, source wire.SourceSelector, projection string,
) error {
	command := wire.ControlCommand{
		Kind: wire.CommandRemoveBinding, Source: &source,
	}
	if projection != "" {
		scoped := bindings.laser.ResourceName(projection)
		command.ProjectionRef = &scoped
	}
	return bindings.laser.PublishControl(ctx, command)
}

// Schemas administers the writer-schema registry: the Avro, Protobuf and JSON
// Schema definitions a schema-first payload is stamped with.
//
// Every call names the default stream's registry, or the deployment-wide one
// when there is no default stream, and ids are unique within one registry.
type Schemas struct{ laser *Laser }

// Schemas opens the writer-schema registry.
func (laser *Laser) Schemas() *Schemas { return &Schemas{laser: laser} }

// RegisterSchemaRequest is an unsent schema registration.
type RegisterSchemaRequest struct {
	schemas *Schemas
	source  wire.SchemaSource
	name    *string
	version *uint32
}

// Register begins registering a writer schema.
func (schemas *Schemas) Register(source wire.SchemaSource) *RegisterSchemaRequest {
	return &RegisterSchemaRequest{schemas: schemas, source: source}
}

// Name is a human label. It is stored and returned, never dispatched on, and
// it is not unique.
func (request *RegisterSchemaRequest) Name(name string) *RegisterSchemaRequest {
	request.name = &name
	return request
}

// Version is a caller-tracked version. It is stored and returned, never
// dispatched on.
func (request *RegisterSchemaRequest) Version(version uint32) *RegisterSchemaRequest {
	request.version = &version
	return request
}

// Send registers the schema and returns the id the registry allocated.
//
// It is synchronous, unlike the other registry writes: the id is what a
// producer stamps on every record, so a caller cannot proceed without it. A
// definition the registry refuses allocates nothing.
func (request *RegisterSchemaRequest) Send(ctx context.Context) (uint32, error) {
	call := wire.RegisterSchema{
		V: wire.QueryOpVersion, Source: request.source,
		Name: request.name, Version: request.version,
	}
	if stream := request.schemas.laser.resourceStream(); stream != "" {
		call.Stream = &stream
	}
	if err := call.Validate(); err != nil {
		return 0, &InvalidError{Reason: err.Error()}
	}
	outcome, err := request.schemas.laser.executeBrowse(
		ctx, wire.AgdxRegisterSchemaCode, call)
	if err != nil {
		return 0, err
	}
	if outcome.Kind != wire.BrowseSchemaRegistered {
		return 0, unexpectedBrowseOutcome("schema register", outcome.Kind)
	}
	return outcome.SchemaID, nil
}

// Drop tombstones the schema at an id.
//
// It does not delete: records already stamped with the id keep decoding, and
// the id stays reserved against re-registration under a different definition.
// Dropping an id nobody registered does nothing.
func (schemas *Schemas) Drop(ctx context.Context, id uint32) error {
	return schemas.laser.PublishControl(ctx, wire.ControlCommand{
		Kind: wire.CommandDropSchema, SchemaID: id,
	})
}

// Get reads the schema occupying an id, active or tombstoned, nil when the id
// is free.
func (schemas *Schemas) Get(
	ctx context.Context, id uint32,
) (*wire.SchemaInfo, error) {
	request := wire.GetSchema{V: wire.QueryOpVersion, ID: id}
	if stream := schemas.laser.resourceStream(); stream != "" {
		request.Stream = &stream
	}
	outcome, err := schemas.laser.executeBrowse(
		ctx, wire.AgdxGetSchemaCode, request)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.BrowseSchema {
		return nil, unexpectedBrowseOutcome("schema read", outcome.Kind)
	}
	return outcome.Schema, nil
}

// List reads one page of the registered schemas, active and tombstoned, in id
// order.
func (schemas *Schemas) List(
	ctx context.Context, nameContains *string, page, results uint32,
) (*wire.SchemaPage, error) {
	request := wire.ListSchemas{
		V: wire.QueryOpVersion, NameContains: nameContains,
		Page: page, Results: results,
	}
	if stream := schemas.laser.resourceStream(); stream != "" {
		request.Stream = &stream
	}
	outcome, err := schemas.laser.executeBrowse(
		ctx, wire.AgdxListSchemasCode, request)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.BrowseSchemas {
		return nil, unexpectedBrowseOutcome("schema listing", outcome.Kind)
	}
	return &outcome.Schemas, nil
}

// Decode asks the deployment for a record body's decoded form under the
// schema at an id, the same decode its projector runs.
//
// It is a convenience for a tool holding a schema-first payload. A client
// that holds the definition decodes locally with CompileSchema instead, which
// costs no round trip.
func (schemas *Schemas) Decode(
	ctx context.Context, id uint32, payload []byte,
) (*wire.DynValue, error) {
	request := wire.DecodeRecord{V: wire.QueryOpVersion, ID: id, Payload: payload}
	if stream := schemas.laser.resourceStream(); stream != "" {
		request.Stream = &stream
	}
	outcome, err := schemas.laser.executeBrowse(
		ctx, wire.AgdxDecodeRecordCode, request)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.BrowseDecoded {
		return nil, unexpectedBrowseOutcome("record decode", outcome.Kind)
	}
	return outcome.Decoded, nil
}

// scopedProjection names a projection under this handle's stream.
func (laser *Laser) scopedProjection(projection wire.Projection) wire.Projection {
	projection.ID = wire.ProjectionID(laser.ResourceName(string(projection.ID)))
	return projection
}

// scopedBinding names every resource a binding references under this handle's
// stream, which is how two streams in one deployment keep separate registries.
func (laser *Laser) scopedBinding(binding wire.ProjectionBinding) wire.ProjectionBinding {
	allowed := make([]wire.ProjectionID, len(binding.AllowedProjections))
	for index, id := range binding.AllowedProjections {
		allowed[index] = wire.ProjectionID(laser.ResourceName(string(id)))
	}
	binding.AllowedProjections = allowed
	if binding.DefaultProjection != nil {
		scoped := wire.ProjectionID(
			laser.ResourceName(string(*binding.DefaultProjection)))
		binding.DefaultProjection = &scoped
	}
	binding.Index = laser.ResourceName(binding.Index)
	return binding
}

// localProjectionInfo maps a listing back to the names the caller wrote, so a
// caller never sees the scoping it did not ask for.
func (laser *Laser) localProjectionInfo(info wire.ProjectionInfo) wire.ProjectionInfo {
	info.Projection.ID = wire.ProjectionID(
		laser.localProjectionID(string(info.Projection.ID)))
	bindings := make([]wire.ProjectionBinding, len(info.Bindings))
	for index, binding := range info.Bindings {
		bindings[index] = laser.localBinding(binding)
	}
	info.Bindings = bindings
	status := make([]wire.BindingStatus, len(info.Status))
	copy(status, info.Status)
	for index := range status {
		if local, scoped := laser.LocalResourceName(status[index].Index); scoped {
			status[index].Index = local
		}
	}
	info.Status = status
	return info
}

func (laser *Laser) localBinding(binding wire.ProjectionBinding) wire.ProjectionBinding {
	allowed := make([]wire.ProjectionID, len(binding.AllowedProjections))
	for index, id := range binding.AllowedProjections {
		allowed[index] = wire.ProjectionID(laser.localProjectionID(string(id)))
	}
	binding.AllowedProjections = allowed
	if binding.DefaultProjection != nil {
		local := wire.ProjectionID(
			laser.localProjectionID(string(*binding.DefaultProjection)))
		binding.DefaultProjection = &local
	}
	if local, scoped := laser.LocalResourceName(binding.Index); scoped {
		binding.Index = local
	}
	return binding
}

// localProjectionID keeps a name this handle's stream does not own unchanged,
// because a listing may carry a reference the caller cannot rename.
func (laser *Laser) localProjectionID(id string) string {
	local, scoped := laser.LocalResourceName(id)
	if scoped && local != "" {
		return local
	}
	return id
}
