package laser

import (
	"context"
	"encoding/json"
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Fork is a handle on one copy-on-write branch of the read model.
//
// A fork overlays speculative rows on the trunk without writing to it, so a
// what-if can be explored and then promoted or discarded. A severed fork is
// frozen at the trunk offsets captured when it was created, and a continuous
// one keeps seeing the trunk grow.
type Fork struct {
	laser  *Laser
	forkID string
}

// Fork opens a handle on a fork. Nothing is sent until a call is made on it.
func (laser *Laser) Fork(forkID string) *Fork {
	return &Fork{laser: laser, forkID: forkID}
}

// ID is the caller's own name for this fork, unscoped.
func (fork *Fork) ID() string { return fork.forkID }

// ResourceID is the name sent to the deployment, scoped when the handle
// scopes.
func (fork *Fork) ResourceID() string { return fork.laser.ResourceName(fork.forkID) }

// Forks lists the forks this caller can see, with the handle's own names
// rather than the scoped ones the deployment stores.
func (laser *Laser) Forks(ctx context.Context) ([]wire.ForkInfo, error) {
	request := wire.ForkList{V: wire.ForkOpVersion}
	// In stream tenancy the listing names the stream it covers, so the server
	// authorizes that stream and lists only its resources.
	if stream := laser.resourceStream(); stream != "" {
		request.Stream = &stream
	}
	outcome, err := laser.executeFork(ctx, wire.AgdxForkListCode, request)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.ForkListed {
		return nil, unexpectedForkOutcome("list", outcome.Kind)
	}
	local := make([]wire.ForkInfo, 0, len(outcome.List))
	for _, info := range outcome.List {
		name, ok := laser.LocalResourceName(info.ForkID)
		if !ok {
			continue
		}
		info.ForkID = name
		if info.Parent != nil {
			parent, ok := laser.LocalResourceName(*info.Parent)
			if !ok {
				continue
			}
			info.Parent = &parent
		}
		local = append(local, info)
	}
	return local, nil
}

// ForkCreateRequest is an unsent create.
type ForkCreateRequest struct {
	fork   *Fork
	parent *string
	kind   wire.ForkKind
	tables []string
}

// Create begins creating this fork. It is continuous unless Severed is called.
func (fork *Fork) Create() *ForkCreateRequest {
	return &ForkCreateRequest{fork: fork, kind: wire.ForkContinuous}
}

// Severed freezes the fork at the trunk's current offsets, so later trunk
// appends stay invisible to it.
func (request *ForkCreateRequest) Severed() *ForkCreateRequest {
	request.kind = wire.ForkSevered
	return request
}

// Continuous lets the fork keep seeing the trunk grow, with its own rows
// overlaid. The default.
func (request *ForkCreateRequest) Continuous() *ForkCreateRequest {
	request.kind = wire.ForkContinuous
	return request
}

// Parent branches this fork from another rather than from the trunk.
func (request *ForkCreateRequest) Parent(parent string) *ForkCreateRequest {
	scoped := request.fork.laser.ResourceName(parent)
	request.parent = &scoped
	return request
}

// Tables narrows the fork to these tables. Naming none covers every table.
func (request *ForkCreateRequest) Tables(tables ...string) *ForkCreateRequest {
	scoped := make([]string, 0, len(tables))
	for _, table := range tables {
		scoped = append(scoped, request.fork.laser.ResourceName(table))
	}
	request.tables = scoped
	return request
}

// Send creates the fork and returns its metadata.
func (request *ForkCreateRequest) Send(ctx context.Context) (*wire.ForkInfo, error) {
	if err := wire.ValidateForkID(request.fork.forkID); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	// Points are the trunk positions a severed fork freezes at. The server
	// that logs the request stamps them, so every replica folds the same fork,
	// and a client leaves them unset.
	outcome, err := request.fork.laser.executeFork(ctx, wire.AgdxForkCreateCode, wire.ForkCreate{
		V:      wire.ForkOpVersion,
		ForkID: request.fork.ResourceID(),
		Parent: request.parent,
		Kind:   request.kind,
		Tables: request.tables,
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.ForkCreated {
		return nil, unexpectedForkOutcome("create", outcome.Kind)
	}
	if outcome.Info == nil {
		return nil, &CodecError{Reason: "fork create answered without the fork it made"}
	}
	info := *outcome.Info
	if name, ok := request.fork.laser.LocalResourceName(info.ForkID); ok {
		info.ForkID = name
	}
	return &info, nil
}

// Promote applies this fork's speculative rows to the trunk and reports how
// many were written.
func (fork *Fork) Promote(ctx context.Context) (uint64, error) {
	outcome, err := fork.laser.executeFork(ctx, wire.AgdxForkPromoteCode, wire.ForkPromote{
		V: wire.ForkOpVersion, ForkID: fork.ResourceID(),
	})
	if err != nil {
		return 0, err
	}
	if outcome.Kind != wire.ForkPromotedRows {
		return 0, unexpectedForkOutcome("promote", outcome.Kind)
	}
	return outcome.Rows, nil
}

// Squash discards this fork's speculative rows, reporting whether an open fork
// was there to remove.
func (fork *Fork) Squash(ctx context.Context) (bool, error) {
	outcome, err := fork.laser.executeFork(ctx, wire.AgdxForkDeleteCode, wire.ForkDelete{
		V: wire.ForkOpVersion, ForkID: fork.ResourceID(),
	})
	if err != nil {
		return false, err
	}
	if outcome.Kind != wire.ForkDeleted {
		return false, unexpectedForkOutcome("squash", outcome.Kind)
	}
	return outcome.Deleted, nil
}

// ForkPutRequest is an unsent speculative row.
type ForkPutRequest struct {
	fork      *Fork
	put       wire.ForkPut
	embedding []float32
	err       error
}

// PutRow begins a speculative row at one trunk coordinate. A row written here
// overlays the trunk row at that coordinate inside this fork alone.
func (fork *Fork) PutRow(table string, partitionID uint32, offset uint64) *ForkPutRequest {
	return &ForkPutRequest{fork: fork, put: wire.ForkPut{
		V:           wire.ForkOpVersion,
		ForkID:      fork.ResourceID(),
		Table:       fork.laser.ResourceName(table),
		PartitionID: partitionID,
		Offset:      offset,
	}}
}

// Projection names the projection and version this row belongs to.
func (request *ForkPutRequest) Projection(id string, version uint32) *ForkPutRequest {
	request.put.ProjectionID = request.fork.laser.ResourceName(id)
	request.put.ProjectionVersion = version
	return request
}

// Field adds one indexed field.
func (request *ForkPutRequest) Field(name, value string) *ForkPutRequest {
	if request.put.Fields == nil {
		request.put.Fields = map[string]string{}
	}
	request.put.Fields[name] = value
	return request
}

// Metadata adds one metadata entry, which is carried but not indexed.
func (request *ForkPutRequest) Metadata(name, value string) *ForkPutRequest {
	if request.put.Metadata == nil {
		request.put.Metadata = map[string]string{}
	}
	request.put.Metadata[name] = value
	return request
}

// Payload attaches the opaque body.
func (request *ForkPutRequest) Payload(payload []byte) *ForkPutRequest {
	request.put.Payload = payload
	return request
}

// Embedding attaches a vector. It travels as a JSON array literal, and a
// component that is not a finite number is refused rather than written as a
// literal no reader can compare.
func (request *ForkPutRequest) Embedding(embedding []float32) *ForkPutRequest {
	request.embedding = embedding
	return request
}

// Tombstone hides the trunk row at this coordinate from the fork's view
// instead of writing a value.
func (request *ForkPutRequest) Tombstone() *ForkPutRequest {
	request.put.Tombstone = true
	return request
}

// Send writes the speculative row.
func (request *ForkPutRequest) Send(ctx context.Context) error {
	if request.err != nil {
		return request.err
	}
	if request.embedding != nil {
		literal, err := embeddingLiteral(request.embedding)
		if err != nil {
			return err
		}
		request.put.Embedding = &literal
	}
	outcome, err := request.fork.laser.executeFork(ctx, wire.AgdxForkPutCode, request.put)
	if err != nil {
		return err
	}
	if outcome.Kind != wire.ForkWritten {
		return unexpectedForkOutcome("put row", outcome.Kind)
	}
	return nil
}

// embeddingLiteral renders a vector the way the store reads it.
//
// A non-finite component is refused: JSON has no spelling for one, so it would
// travel as something a reader cannot compare, silently changing which rows a
// nearest-neighbour search returns.
func embeddingLiteral(embedding []float32) (string, error) {
	for _, component := range embedding {
		if math.IsNaN(float64(component)) || math.IsInf(float64(component), 0) {
			return "", &InvalidError{
				Reason: "an embedding component must be a finite number"}
		}
	}
	encoded, err := json.Marshal(embedding)
	if err != nil {
		return "", &CodecError{Reason: "encode the embedding", Cause: err}
	}
	return string(encoded), nil
}

// executeFork sends one fork command and opens its reply.
func (laser *Laser) executeFork(
	ctx context.Context, code uint32, request any,
) (*wire.ForkOutcome, error) {
	if !laser.capabilities.Managed || !laser.capabilities.Forks {
		return nil, &UnsupportedError{Surface: "the fork surface"}
	}
	if err := laser.requireOpVersion("fork", wire.ForkOpVersion,
		func(versions wire.OpVersions) uint32 { return versions.Fork }); err != nil {
		return nil, err
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode fork request", Cause: err}
	}
	raw, err := laser.SendManaged(ctx, code, payload)
	if err != nil {
		return nil, err
	}
	var reply wire.ForkReply
	if err := wire.Unmarshal(raw, &reply); err != nil {
		return nil, &CodecError{Reason: "decode fork reply", Cause: err}
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{Reason: "fork reply carried neither an outcome nor an error"}
		}
		return nil, &ForkError{Failure: *reply.Err}
	}
	return reply.Outcome, nil
}

// ForkError reports a fork command the server refused.
type ForkError struct{ Failure wire.ForkError }

func (e *ForkError) Error() string { return "fork: " + e.Failure.Error() }

// Kind is the contract's classification of the failure.
func (e *ForkError) Kind() wire.ForkErrorKind { return e.Failure.Kind }

func unexpectedForkOutcome(operation string, kind wire.ForkOutcomeKind) error {
	return &CodecError{Reason: "fork " + operation +
		" answered with an unexpected outcome: " + forkOutcomeName(kind)}
}

func forkOutcomeName(kind wire.ForkOutcomeKind) string {
	switch kind {
	case wire.ForkCreated:
		return "created"
	case wire.ForkDeleted:
		return "deleted"
	case wire.ForkPromotedRows:
		return "promoted"
	case wire.ForkListed:
		return "list"
	case wire.ForkWritten:
		return "written"
	default:
		return "kind " + itoa(int(kind))
	}
}
