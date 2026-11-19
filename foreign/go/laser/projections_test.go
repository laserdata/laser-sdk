package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// registryServer answers control commands and registry browses from a script.
type registryServer struct {
	clusterOfOne
	guard sync.Mutex
	// outcome answers every browse, and script answers them in turn when it
	// is set, so a test can watch a status change between reads.
	outcome  *wire.BrowseOutcome
	script   []*wire.BrowseOutcome
	failure  *wire.QueryError
	command  wire.ResultCode
	message  string
	codes    []uint32
	requests [][]byte
}

func (server *registryServer) Close() error { return nil }

func (server *registryServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.guard.Lock()
	server.codes = append(server.codes, code)
	server.requests = append(server.requests, payload)
	server.guard.Unlock()
	if code == wire.AgdxControlPublishCode {
		return wire.Marshal(wire.CommandError{
			Code: server.command, Message: server.message,
		})
	}
	if server.failure != nil {
		return wire.Marshal(wire.BrowseReply{Err: server.failure})
	}
	server.guard.Lock()
	outcome := server.outcome
	if len(server.script) > 0 {
		outcome = server.script[0]
		if len(server.script) > 1 {
			server.script = server.script[1:]
		}
	}
	server.guard.Unlock()
	return wire.Marshal(wire.BrowseReply{Outcome: outcome})
}

// control reads back the control envelope the client appended.
func (server *registryServer) control(t *testing.T, index int) wire.ControlEnvelope {
	t.Helper()
	server.guard.Lock()
	defer server.guard.Unlock()
	body := server.requests[index]
	if wire.IsIdempotentManagedRequest(server.codes[index]) {
		var envelope wire.ManagedRequestEnvelope
		if err := wire.Unmarshal(body, &envelope); err != nil {
			t.Fatalf("decode the managed envelope: %v", err)
		}
		body = envelope.Payload
	}
	var control wire.ControlEnvelope
	if err := wire.Unmarshal(body, &control); err != nil {
		t.Fatalf("decode the control envelope: %v", err)
	}
	return control
}

// browse reads back a browse request the client sent.
func browseRequest[T any](
	t *testing.T, server *registryServer, index int, target *T,
) {
	t.Helper()
	server.guard.Lock()
	defer server.guard.Unlock()
	body := server.requests[index]
	if wire.IsIdempotentManagedRequest(server.codes[index]) {
		var envelope wire.ManagedRequestEnvelope
		if err := wire.Unmarshal(body, &envelope); err != nil {
			t.Fatalf("decode the managed envelope: %v", err)
		}
		body = envelope.Payload
	}
	if err := wire.Unmarshal(body, target); err != nil {
		t.Fatalf("decode request %d: %v", index, err)
	}
}

func registryCapabilities() *laser.Capabilities {
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion)
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Hello = laser.HelloAnswered
	capabilities.Materializations = true
	capabilities.Versions = &versions
	return &capabilities
}

func registryHandle(server iggcon.Client, options laser.Options) *laser.Laser {
	if options.Capabilities == nil {
		options.Capabilities = registryCapabilities()
	}
	if options.DefaultStream == "" {
		options.DefaultStream = "agents"
	}
	return laser.FromClient(server, options)
}

func rowProjection(id string) wire.Projection {
	return wire.Projection{
		ID:          wire.ProjectionID(id),
		Name:        "readings",
		Version:     1,
		ContentType: wire.ContentJSON,
		Extraction: wire.IndexSchema{
			Fields: []wire.IndexField{
				wire.TypedIndexField("host", "/host", wire.FieldText),
			},
		},
	}
}

// A projection declares what a deployment materializes, so registering one
// appends the command that creates its backing table.
func TestGivenAProjectionWhenRegisteredThenShouldAppendItsControlCommand(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})

	if err := handle.Projections().Register(
		context.Background(), rowProjection("reading.v1")); err != nil {
		t.Fatalf("register: %v", err)
	}

	if server.codes[0] != wire.AgdxControlPublishCode {
		t.Errorf("want the control code, got %d", server.codes[0])
	}
	control := server.control(t, 0)
	if control.Command.Kind != wire.CommandRegisterProjection {
		t.Errorf("want a register, got %d", control.Command.Kind)
	}
	// Resource names are scoped to the stream, which is how two streams in
	// one deployment keep separate registries.
	if control.Command.Projection == nil ||
		control.Command.Projection.ID != "stream:agents/reading.v1" {
		t.Errorf("want the id scoped, got %v", control.Command.Projection)
	}
	if control.Stream == nil || *control.Stream != "agents" {
		t.Errorf("want the stream named, got %v", control.Stream)
	}
	if control.V != wire.ControlOpVersion {
		t.Errorf("want the control version stamped, got %d", control.V)
	}
}

// A graph materializes nodes and edges rather than a row table, so the two
// registrations are not interchangeable.
func TestGivenTheWrongProjectionKindWhenRegisteredThenShouldRefuseIt(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})
	graph := rowProjection("entities.v1")
	graph.Kind = wire.ProjectionGraph

	if err := handle.Projections().Register(context.Background(), graph); err == nil {
		t.Error("a graph projection must not register as a row projection")
	}
	if err := handle.Projections().RegisterGraph(
		context.Background(), rowProjection("reading.v1")); err == nil {
		t.Error("a row projection must not register as a graph")
	}
	// A graph without an extraction plan declares nothing to extract.
	if err := handle.Projections().RegisterGraph(
		context.Background(), graph); err == nil {
		t.Error("a graph projection without an entity schema must be refused")
	}
	if len(server.codes) != 0 {
		t.Error("a refused registration must not reach the socket")
	}
}

// Dropping a plan is not deleting data, and every drop names its resource
// under the caller's stream.
func TestGivenADropWhenAppendedThenShouldNameTheScopedResource(t *testing.T) {
	for name, drop := range map[string]struct {
		send func(*laser.Laser) error
		kind wire.ControlCommandKind
	}{
		"a projection": {
			kind: wire.CommandDropProjection,
			send: func(handle *laser.Laser) error {
				return handle.Projections().Drop(context.Background(), "reading.v1")
			},
		},
		"a graph": {
			kind: wire.CommandDropGraph,
			send: func(handle *laser.Laser) error {
				return handle.Projections().DropGraph(context.Background(), "reading.v1")
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := &registryServer{command: wire.ResultOk}
			handle := registryHandle(server, laser.Options{})

			if err := drop.send(handle); err != nil {
				t.Fatalf("drop: %v", err)
			}

			control := server.control(t, 0)
			if control.Command.Kind != drop.kind {
				t.Errorf("want kind %d, got %d", drop.kind, control.Command.Kind)
			}
			if control.Command.Name != "stream:agents/reading.v1" {
				t.Errorf("want the name scoped, got %q", control.Command.Name)
			}
		})
	}
}

// A binding routes a source into projections, so every reference it carries
// is scoped the same way the projections themselves were.
func TestGivenABindingWhenAppliedThenShouldScopeEveryReference(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})
	fallback := wire.ProjectionID("reading.v1")

	if err := handle.Bindings().Apply(context.Background(), wire.ProjectionBinding{
		Source:             wire.SourceSelector{Stream: "agents", Topic: "readings"},
		AllowedProjections: []wire.ProjectionID{"reading.v1", "reading.v2"},
		DefaultProjection:  &fallback,
		Index:              "readings",
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	control := server.control(t, 0)
	binding := control.Command.Binding
	if binding == nil {
		t.Fatal("want the binding carried")
	}
	for _, id := range binding.AllowedProjections {
		if id != "stream:agents/reading.v1" && id != "stream:agents/reading.v2" {
			t.Errorf("want every allowed projection scoped, got %q", id)
		}
	}
	if binding.DefaultProjection == nil ||
		*binding.DefaultProjection != "stream:agents/reading.v1" {
		t.Errorf("want the default scoped, got %v", binding.DefaultProjection)
	}
	if binding.Index != "stream:agents/readings" {
		t.Errorf("want the index scoped, got %q", binding.Index)
	}
}

// Removing a binding stops new records materializing, and naming one
// projection scopes the removal to it rather than to the whole binding.
func TestGivenABindingRemovalWhenAppendedThenShouldScopeToWhatItNames(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})
	source := wire.SourceSelector{Stream: "agents", Topic: "readings"}

	if err := handle.Bindings().Remove(
		context.Background(), source, "reading.v1"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	control := server.control(t, 0)
	if control.Command.ProjectionRef == nil ||
		*control.Command.ProjectionRef != "stream:agents/reading.v1" {
		t.Errorf("want the projection scoped, got %v", control.Command.ProjectionRef)
	}

	if err := handle.Bindings().Remove(context.Background(), source, ""); err != nil {
		t.Fatalf("remove: %v", err)
	}
	whole := server.control(t, 1)
	if whole.Command.ProjectionRef != nil {
		t.Error("naming no projection removes the whole binding")
	}
	if whole.Command.Source == nil || whole.Command.Source.Topic != "readings" {
		t.Errorf("want the source named, got %v", whole.Command.Source)
	}
}

// A listing comes back under the names the caller wrote, so a caller never
// sees the scoping it did not ask for.
func TestGivenAScopedListingWhenReadThenShouldReturnTheCallersNames(t *testing.T) {
	fallback := wire.ProjectionID("stream:agents/reading.v1")
	server := &registryServer{outcome: &wire.BrowseOutcome{
		Kind: wire.BrowseProjections,
		Projections: wire.ProjectionPage{
			Items: []wire.ProjectionInfo{{
				Projection: rowProjection("stream:agents/reading.v1"),
				Bindings: []wire.ProjectionBinding{{
					Source:             wire.SourceSelector{Stream: "agents", Topic: "readings"},
					AllowedProjections: []wire.ProjectionID{"stream:agents/reading.v1"},
					DefaultProjection:  &fallback,
					Index:              "stream:agents/readings",
				}},
			}},
			Page: 1, TotalPages: 1, TotalResults: 1,
		},
	}}
	handle := registryHandle(server, laser.Options{})

	page, err := handle.Projections().List(
		context.Background(), laser.ProjectionQuery{}, 1, 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(page.Items) != 1 {
		t.Fatalf("want one projection, got %d", len(page.Items))
	}
	if page.Items[0].Projection.ID != "reading.v1" {
		t.Errorf("want the local id, got %q", page.Items[0].Projection.ID)
	}
	binding := page.Items[0].Bindings[0]
	if binding.AllowedProjections[0] != "reading.v1" {
		t.Errorf("want the local allowed id, got %q", binding.AllowedProjections[0])
	}
	if binding.DefaultProjection == nil || *binding.DefaultProjection != "reading.v1" {
		t.Errorf("want the local default, got %v", binding.DefaultProjection)
	}
	if binding.Index != "readings" {
		t.Errorf("want the local index, got %q", binding.Index)
	}
}

// A listing is narrowed server side, because a deployment with many
// projections would otherwise send them all to be filtered here.
func TestGivenAProjectionListingWhenNarrowedThenShouldPushTheFilterDown(t *testing.T) {
	server := &registryServer{outcome: &wire.BrowseOutcome{
		Kind:        wire.BrowseProjections,
		Projections: wire.ProjectionPage{Page: 2, TotalPages: 3},
	}}
	handle := registryHandle(server, laser.Options{})
	name, prefix := "reading", "read"

	if _, err := handle.Projections().List(context.Background(), laser.ProjectionQuery{
		Topics: []string{"readings"}, NameContains: &name, IDPrefix: &prefix,
		Kinds: []wire.ProjectionKind{wire.ProjectionRow},
	}, 2, 20); err != nil {
		t.Fatalf("list: %v", err)
	}

	var request wire.ListProjections
	browseRequest(t, server, 0, &request)
	if len(request.Topics) != 1 || request.Topics[0] != "readings" {
		t.Errorf("want the topic narrowing sent, got %v", request.Topics)
	}
	if request.NameContains == nil || *request.NameContains != "reading" {
		t.Errorf("want the name narrowing sent, got %v", request.NameContains)
	}
	if request.IDPrefix == nil || *request.IDPrefix != "read" {
		t.Errorf("want the id prefix sent, got %v", request.IDPrefix)
	}
	if request.Page != 2 || request.Results != 20 {
		t.Errorf("want page 2 of 20, got %d of %d", request.Page, request.Results)
	}
	// The listing covers this handle's own stream, which is what the server
	// authorizes under stream tenancy.
	if request.Stream == nil || *request.Stream != "agents" {
		t.Errorf("want the stream named, got %v", request.Stream)
	}
}

// Reading an id nobody registered is a routine miss, not a failure.
func TestGivenAnUnknownProjectionWhenReadThenShouldReportItAbsent(t *testing.T) {
	server := &registryServer{outcome: &wire.BrowseOutcome{Kind: wire.BrowseProjection}}
	handle := registryHandle(server, laser.Options{})

	info, err := handle.Projections().Get(context.Background(), "reading.v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if info != nil {
		t.Errorf("want no projection, got %+v", info)
	}
}

// Materialization status is served only where materializations are, so a
// deployment without them is refused rather than reporting nothing.
func TestGivenNoMaterializationsWhenStatusIsReadThenShouldRefuseIt(t *testing.T) {
	capabilities := registryCapabilities()
	capabilities.Materializations = false
	server := &registryServer{}

	_, err := registryHandle(server, laser.Options{Capabilities: capabilities}).
		Projections().Status(context.Background(), "reading.v1")

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported read must not reach the socket")
	}
}

// Status answers per binding, and asks the server for it explicitly, because
// a plain read does not pay for it.
func TestGivenAStatusReadWhenSentThenShouldAskForItAndAnswerPerBinding(t *testing.T) {
	server := &registryServer{outcome: &wire.BrowseOutcome{
		Kind: wire.BrowseProjection,
		Projection: &wire.ProjectionInfo{
			Projection: rowProjection("stream:agents/reading.v1"),
			Status: []wire.BindingStatus{{
				Index: "stream:agents/readings",
			}},
		},
	}}
	handle := registryHandle(server, laser.Options{})

	status, err := handle.Projections().Status(context.Background(), "reading.v1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}

	if len(status) != 1 {
		t.Fatalf("want one binding's status, got %d", len(status))
	}
	if status[0].Index != "readings" {
		t.Errorf("want the local index, got %q", status[0].Index)
	}
	var request wire.GetProjection
	browseRequest(t, server, 0, &request)
	if !request.Status {
		t.Error("a status read asks for the status")
	}
	if request.ID != "stream:agents/reading.v1" {
		t.Errorf("want the scoped id, got %q", request.ID)
	}
}

// A schema id is what a producer stamps on every record, so registering is
// synchronous: a caller cannot proceed without the id.
func TestGivenASchemaWhenRegisteredThenShouldReturnTheAllocatedID(t *testing.T) {
	server := &registryServer{outcome: &wire.BrowseOutcome{
		Kind: wire.BrowseSchemaRegistered, SchemaID: 7,
	}}
	handle := registryHandle(server, laser.Options{})

	id, err := handle.Schemas().
		Register(wire.AvroSchema(`{"type":"record","name":"R","fields":[]}`)).
		Name("reading").Version(3).Send(context.Background())
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	if id != 7 {
		t.Errorf("want the allocated id, got %d", id)
	}
	if server.codes[0] != wire.AgdxRegisterSchemaCode {
		t.Errorf("want the register code, got %d", server.codes[0])
	}
	var request wire.RegisterSchema
	browseRequest(t, server, 0, &request)
	if request.Name == nil || *request.Name != "reading" {
		t.Errorf("want the label carried, got %v", request.Name)
	}
	if request.Version == nil || *request.Version != 3 {
		t.Errorf("want the version carried, got %v", request.Version)
	}
	if request.Stream == nil || *request.Stream != "agents" {
		t.Errorf("want the registry named, got %v", request.Stream)
	}
}

// A definition the registry would refuse is refused here, where the caller
// can still fix it, and nothing is allocated.
func TestGivenAnInvalidSchemaWhenRegisteredThenShouldRefuseItBeforeSending(t *testing.T) {
	server := &registryServer{}
	handle := registryHandle(server, laser.Options{})

	// A JSON Schema of another draft is not one this registry accepts.
	_, err := handle.Schemas().Register(wire.JSONSchema(
		`{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}`)).
		Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("a refused definition must not reach the socket")
	}
}

// Dropping a schema tombstones it: records stamped with the id keep decoding,
// so the id is reserved rather than freed.
func TestGivenASchemaDropWhenAppendedThenShouldTombstoneItsID(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})

	if err := handle.Schemas().Drop(context.Background(), 7); err != nil {
		t.Fatalf("drop: %v", err)
	}

	control := server.control(t, 0)
	if control.Command.Kind != wire.CommandDropSchema || control.Command.SchemaID != 7 {
		t.Errorf("want a drop of 7, got %+v", control.Command)
	}
}

// A schema read answers whether the id is free, active or tombstoned, because
// a tombstoned id still decodes the records already stamped with it.
func TestGivenASchemaWhenReadThenShouldReportWhetherItIsDropped(t *testing.T) {
	definition := wire.SchemaDef{
		ID: 7, Source: wire.AvroSchema(`{"type":"record","name":"R","fields":[]}`),
	}
	server := &registryServer{outcome: &wire.BrowseOutcome{
		Kind:   wire.BrowseSchema,
		Schema: &wire.SchemaInfo{Schema: definition, Dropped: true},
	}}
	handle := registryHandle(server, laser.Options{})

	info, err := handle.Schemas().Get(context.Background(), 7)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if info == nil || !info.Dropped {
		t.Fatalf("want a tombstoned schema, got %+v", info)
	}
	if info.Schema.ID != 7 {
		t.Errorf("want the id back, got %d", info.Schema.ID)
	}
	var request wire.GetSchema
	browseRequest(t, server, 0, &request)
	if request.ID != 7 {
		t.Errorf("want the id asked for, got %d", request.ID)
	}
}

// A schema listing is paged and narrowed server side.
func TestGivenASchemaListingWhenReadThenShouldCarryItsPageAndFilter(t *testing.T) {
	server := &registryServer{outcome: &wire.BrowseOutcome{
		Kind:    wire.BrowseSchemas,
		Schemas: wire.SchemaPage{Page: 2, TotalPages: 4, TotalResults: 35},
	}}
	handle := registryHandle(server, laser.Options{})
	name := "reading"

	page, err := handle.Schemas().List(context.Background(), &name, 2, 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if page.TotalResults != 35 {
		t.Errorf("want the total reported, got %d", page.TotalResults)
	}

	var request wire.ListSchemas
	browseRequest(t, server, 0, &request)
	if request.NameContains == nil || *request.NameContains != "reading" {
		t.Errorf("want the filter sent, got %v", request.NameContains)
	}
	if request.Page != 2 || request.Results != 10 {
		t.Errorf("want page 2 of 10, got %d of %d", request.Page, request.Results)
	}
}

// A registry browse has no topic transport, so a deployment that serves no
// managed backend is refused rather than left waiting for a reply.
func TestGivenNoManagedBackendWhenBrowsingThenShouldRefuseIt(t *testing.T) {
	capabilities := laser.OpenCapabilities()
	server := &registryServer{}
	handle := registryHandle(server, laser.Options{Capabilities: &capabilities})

	if _, err := handle.Projections().Get(
		context.Background(), "reading.v1"); err == nil {
		t.Error("a browse without a backend must be refused")
	}
	if err := handle.Projections().Drop(
		context.Background(), "reading.v1"); err == nil {
		t.Error("a control command without a backend must be refused")
	}
	if len(server.codes) != 0 {
		t.Error("neither must reach the socket")
	}
}

// A deployment serving another control version would fold a command under a
// contract neither side agreed to.
func TestGivenAnotherControlVersionWhenAppendingThenShouldRefuseIt(t *testing.T) {
	capabilities := registryCapabilities()
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion+1, wire.KvOpVersion, wire.ForkOpVersion)
	capabilities.Versions = &versions
	server := &registryServer{}

	err := registryHandle(server, laser.Options{Capabilities: capabilities}).
		Projections().Drop(context.Background(), "reading.v1")

	var skew *laser.VersionError
	if !errors.As(err, &skew) {
		t.Fatalf("want a VersionError, got %T: %v", err, err)
	}
}

// A browse rides the query envelope version, so a skew there is refused too.
func TestGivenAnotherQueryVersionWhenBrowsingThenShouldRefuseIt(t *testing.T) {
	capabilities := registryCapabilities()
	versions := wire.NewOpVersions(
		wire.QueryOpVersion+1, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion)
	capabilities.Versions = &versions
	server := &registryServer{}

	_, err := registryHandle(server, laser.Options{Capabilities: capabilities}).
		Schemas().Get(context.Background(), 7)

	var skew *laser.VersionError
	if !errors.As(err, &skew) {
		t.Fatalf("want a VersionError, got %T: %v", err, err)
	}
}

// A command the deployment refused is reported with the code it refused
// under, so a caller can tell a permanent refusal from a retryable one.
func TestGivenARefusedCommandWhenAppendedThenShouldCarryItsCode(t *testing.T) {
	server := &registryServer{
		command: wire.ResultUnavailable, message: "the control topic is rebuilding",
	}
	handle := registryHandle(server, laser.Options{})

	err := handle.Projections().Drop(context.Background(), "reading.v1")

	var failure *laser.CommandFailure
	if !errors.As(err, &failure) {
		t.Fatalf("want a CommandFailure, got %T: %v", err, err)
	}
	if failure.ResultCode() != wire.ResultUnavailable {
		t.Errorf("want the code carried, got %q", failure.ResultCode())
	}
	if !failure.Retryable() {
		t.Error("an unavailable control topic may take the command later")
	}
}

// A control command is applied once however many times a transport retry
// reaches the deployment.
func TestGivenAControlCommandWhenAppendedThenShouldCarryAnOperationIdentity(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})

	if err := handle.Projections().Drop(
		context.Background(), "reading.v1"); err != nil {
		t.Fatalf("drop: %v", err)
	}

	var envelope wire.ManagedRequestEnvelope
	if err := wire.Unmarshal(server.requests[0], &envelope); err != nil {
		t.Fatalf("decode the managed envelope: %v", err)
	}
	if envelope.OperationID.IsZero() {
		t.Error("want an operation identity")
	}

	// A caller that persists the identity applies once across a restart.
	chosen := laser.NewOperationID()
	if err := handle.PublishControlAs(context.Background(), wire.ControlCommand{
		Kind: wire.CommandDropSchema, SchemaID: 7,
	}, "agents", chosen); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := wire.Unmarshal(server.requests[1], &envelope); err != nil {
		t.Fatalf("decode the managed envelope: %v", err)
	}
	if envelope.OperationID != chosen {
		t.Error("want the caller's identity carried")
	}
}

// A command that would be refused by the contract is refused here, where the
// caller can still fix it.
func TestGivenAnInvalidCommandWhenAppendedThenShouldRefuseItBeforeSending(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{})

	err := handle.PublishControl(context.Background(), wire.ControlCommand{
		Kind: wire.CommandDropProjection, Name: "",
	})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an invalid command must not reach the socket")
	}
}

// A reply of another shape is refused rather than read as the one asked for,
// because a projection read as a schema would report nonsense.
func TestGivenABrowseOfAnotherShapeWhenReadThenShouldRefuseIt(t *testing.T) {
	server := &registryServer{outcome: &wire.BrowseOutcome{
		Kind: wire.BrowseSchemaRegistered, SchemaID: 7,
	}}
	handle := registryHandle(server, laser.Options{})

	if _, err := handle.Projections().Get(
		context.Background(), "reading.v1"); err == nil {
		t.Error("a projection read answered with a schema id must be refused")
	}
	if _, err := handle.Schemas().Get(context.Background(), 7); err == nil {
		t.Error("a schema read answered with a registration must be refused")
	}
	if _, err := handle.Schemas().List(
		context.Background(), nil, 1, 10); err == nil {
		t.Error("a listing answered with a registration must be refused")
	}

	// And the other way round: a register answered with anything but an
	// allocated id would hand back an id the registry never allocated, and a
	// listing answered with one item would report a page nobody asked for.
	elsewhere := &registryServer{outcome: &wire.BrowseOutcome{
		Kind: wire.BrowseSchema, Schema: &wire.SchemaInfo{Schema: wire.SchemaDef{
			ID: 7, Source: wire.AvroSchema(`{"type":"record","name":"R","fields":[]}`),
		}},
	}}
	if _, err := registryHandle(elsewhere, laser.Options{}).Schemas().
		Register(wire.AvroSchema(`{"type":"record","name":"R","fields":[]}`)).
		Send(context.Background()); err == nil {
		t.Error("a register answered with one schema must be refused")
	}
	if _, err := registryHandle(elsewhere, laser.Options{}).Projections().
		List(context.Background(), laser.ProjectionQuery{}, 1, 10); err == nil {
		t.Error("a projection listing answered with one schema must be refused")
	}
}

// A browse the deployment refused carries the query failure it refused with.
func TestGivenARefusedBrowseWhenReadThenShouldCarryItsReason(t *testing.T) {
	server := &registryServer{failure: &wire.QueryError{
		Kind: wire.QueryIndexNotFound, Message: "no such registry",
	}}
	handle := registryHandle(server, laser.Options{})

	_, err := handle.Schemas().Get(context.Background(), 7)

	var failure *laser.QueryError
	if !errors.As(err, &failure) {
		t.Fatalf("want a QueryError, got %T: %v", err, err)
	}
}

// Bare naming leaves resource names alone, for a deployment that does not
// scope its registries per stream.
func TestGivenBareNamingWhenRegisteringThenShouldLeaveNamesUnchanged(t *testing.T) {
	server := &registryServer{command: wire.ResultOk}
	handle := registryHandle(server, laser.Options{
		ResourceNaming: laser.ResourceNamingBare,
	})

	if err := handle.Projections().Register(
		context.Background(), rowProjection("reading.v1")); err != nil {
		t.Fatalf("register: %v", err)
	}

	control := server.control(t, 0)
	if control.Command.Projection.ID != "reading.v1" {
		t.Errorf("want the bare id, got %q", control.Command.Projection.ID)
	}
	if control.Stream != nil {
		t.Error("bare naming addresses the deployment-wide registry")
	}
}
