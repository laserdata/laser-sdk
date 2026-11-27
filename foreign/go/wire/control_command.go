package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// ControlCommandKind discriminates one control command.
type ControlCommandKind uint8

const (
	CommandRegisterProjection ControlCommandKind = iota
	CommandDropProjection
	CommandApplyBinding
	CommandRemoveBinding
	// CommandRegisterSchema registers, or replaces by id, a writer schema for
	// a schema-first codec.
	CommandRegisterSchema
	CommandDropSchema
	// CommandAllocateSchema registers a writer schema under the id the
	// control fold chooses. The record's operation identity keeps the chosen
	// id, so a retry returns it rather than allocating a second.
	CommandAllocateSchema
	// CommandRegisterGraph registers a graph projection. It is distinct from
	// registering a row projection so a deployment can gate graph
	// registration separately.
	CommandRegisterGraph
	CommandDropGraph
	// CommandRegisterSessionSource registers the named session topics in one
	// stream, or all of them.
	CommandRegisterSessionSource
	CommandRemoveSessionSource
	// CommandFilterCatalog is one consumer-filter catalog mutation, stamped
	// by the streaming server.
	CommandFilterCatalog
	// CommandAcceptGap records an operator accepting that one source
	// partition lost history to retention. A projection that resumes past the
	// gap continues, and keeps the gap as a record that its history is
	// incomplete. Only the backend's operator path appends it.
	CommandAcceptGap
	// CommandSkipRecord records the backend skipping one record a binding's
	// destination or extraction deterministically rejected. The streaming
	// server never forwards it from a client.
	CommandSkipRecord
	// CommandSourceRetained records the backend observing that one source
	// partition incarnation retains no record below a floor. Skips and
	// accepted gaps below it can never be reached again and leave the
	// registry. The streaming server never forwards it from a client.
	CommandSourceRetained
)

type removeBindingFields struct {
	Source        SourceSelector `cbor:"source"`
	ProjectionRef *string        `cbor:"projection_ref"`
	// Generation removes the binding only when it belongs to this incarnation
	// of its source stream or an earlier one. Absent removes it whatever its
	// incarnation.
	Generation *ControlStreamGeneration `cbor:"generation,omitempty"`
}

type registerSessionSourceFields struct {
	Stream string        `cbor:"stream"`
	Topics SessionTopics `cbor:"topics"`
}

type removeSessionSourceFields struct {
	Stream string `cbor:"stream"`
}

// ControlCommand is one command on the control topic. A client SDK, or any
// tool driving the deployment, publishes these to register projections,
// bindings and schemas.
type ControlCommand struct {
	Kind ControlCommandKind
	// Projection carries a register-projection or register-graph command.
	Projection *Projection
	// Name carries a drop command's projection or graph id.
	Name string
	// Binding carries an apply-binding command.
	Binding *ProjectionBinding
	// Source, ProjectionRef and Generation carry a remove-binding command.
	Source        *SourceSelector
	ProjectionRef *string
	Generation    *ControlStreamGeneration
	// Schema carries a register-schema command, SchemaID a drop, and
	// Allocation an allocate.
	Schema     *SchemaDef
	SchemaID   uint32
	Allocation *SchemaAllocation
	// Stream and Topics carry the session-source commands.
	Stream string
	Topics *SessionTopics
	// Catalog carries a filter-catalog mutation.
	Catalog *FilterCatalogCommand
	// Gap, Skip and Retention carry the backend-appended commands.
	Gap       *GapAcceptance
	Skip      *RecordSkip
	Retention *SourceRetention
}

// The externally tagged control command names. They keep their Rust
// PascalCase spelling, since the enum declares no rename.
const (
	commandRegisterProjectionVariant    = "RegisterProjection"
	commandDropProjectionVariant        = "DropProjection"
	commandApplyBindingVariant          = "ApplyBinding"
	commandRemoveBindingVariant         = "RemoveBinding"
	commandRegisterSchemaVariant        = "RegisterSchema"
	commandDropSchemaVariant            = "DropSchema"
	commandAllocateSchemaVariant        = "AllocateSchema"
	commandRegisterGraphVariant         = "RegisterGraph"
	commandDropGraphVariant             = "DropGraph"
	commandRegisterSessionSourceVariant = "RegisterSessionSource"
	commandRemoveSessionSourceVariant   = "RemoveSessionSource"
	commandFilterCatalogVariant         = "FilterCatalog"
	commandAcceptGapVariant             = "AcceptGap"
	commandSkipRecordVariant            = "SkipRecord"
	commandSourceRetainedVariant        = "SourceRetained"
)

// MarshalCBOR writes the externally tagged command.
func (command ControlCommand) MarshalCBOR() (cborform.Value, error) {
	switch command.Kind {
	case CommandRegisterProjection, CommandRegisterGraph:
		if command.Projection == nil {
			return nil, fmt.Errorf("wire: a registration carries no projection")
		}
		name := commandRegisterProjectionVariant
		if command.Kind == CommandRegisterGraph {
			name = commandRegisterGraphVariant
		}
		return marshalVariantStruct(name, *command.Projection)

	case CommandDropProjection, CommandDropGraph:
		name := commandDropProjectionVariant
		if command.Kind == CommandDropGraph {
			name = commandDropGraphVariant
		}
		return taggedVariant(name, cborform.Text(command.Name)), nil

	case CommandApplyBinding:
		if command.Binding == nil {
			return nil, fmt.Errorf("wire: an apply carries no binding")
		}
		return marshalVariantStruct(commandApplyBindingVariant, *command.Binding)

	case CommandRemoveBinding:
		if command.Source == nil {
			return nil, fmt.Errorf("wire: a remove names no source")
		}
		return marshalVariantStruct(commandRemoveBindingVariant, removeBindingFields{
			Source:        *command.Source,
			ProjectionRef: command.ProjectionRef,
			Generation:    command.Generation,
		})

	case CommandRegisterSchema:
		if command.Schema == nil {
			return nil, fmt.Errorf("wire: a schema registration carries no schema")
		}
		return marshalVariantStruct(commandRegisterSchemaVariant, *command.Schema)

	case CommandDropSchema:
		return taggedVariant(commandDropSchemaVariant,
			cborform.Uint(uint64(command.SchemaID))), nil

	case CommandAllocateSchema:
		if command.Allocation == nil {
			return nil, fmt.Errorf("wire: an allocation carries no schema source")
		}
		return marshalVariantStruct(commandAllocateSchemaVariant, *command.Allocation)

	case CommandRegisterSessionSource:
		if command.Topics == nil {
			return nil, fmt.Errorf("wire: a session registration names no topics")
		}
		return marshalVariantStruct(commandRegisterSessionSourceVariant,
			registerSessionSourceFields{Stream: command.Stream, Topics: *command.Topics})

	case CommandRemoveSessionSource:
		return marshalVariantStruct(commandRemoveSessionSourceVariant,
			removeSessionSourceFields{Stream: command.Stream})

	case CommandFilterCatalog:
		if command.Catalog == nil {
			return nil, fmt.Errorf("wire: a catalog command carries no mutation")
		}
		return marshalVariantStruct(commandFilterCatalogVariant, *command.Catalog)

	case CommandAcceptGap:
		if command.Gap == nil {
			return nil, fmt.Errorf("wire: a gap acceptance carries no range")
		}
		return marshalVariantStruct(commandAcceptGapVariant, *command.Gap)

	case CommandSkipRecord:
		if command.Skip == nil {
			return nil, fmt.Errorf("wire: a skip carries no record")
		}
		return marshalVariantStruct(commandSkipRecordVariant, *command.Skip)

	case CommandSourceRetained:
		if command.Retention == nil {
			return nil, fmt.Errorf("wire: a retention floor carries no source")
		}
		return marshalVariantStruct(commandSourceRetainedVariant, *command.Retention)

	default:
		return nil, fmt.Errorf("wire: unknown control command %d", command.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged command.
func (command *ControlCommand) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("ControlCommand", name, payload); err != nil {
		return err
	}

	switch name {
	case commandRegisterProjectionVariant, commandRegisterGraphVariant:
		var projection Projection
		if err := fromValueInto(payload, &projection); err != nil {
			return err
		}
		kind := CommandRegisterProjection
		if name == commandRegisterGraphVariant {
			kind = CommandRegisterGraph
		}
		*command = ControlCommand{Kind: kind, Projection: &projection}

	case commandDropProjectionVariant, commandDropGraphVariant:
		text, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: %q carries an id, got %T", name, payload)
		}
		kind := CommandDropProjection
		if name == commandDropGraphVariant {
			kind = CommandDropGraph
		}
		*command = ControlCommand{Kind: kind, Name: string(text)}

	case commandApplyBindingVariant:
		var binding ProjectionBinding
		if err := fromValueInto(payload, &binding); err != nil {
			return err
		}
		*command = ControlCommand{Kind: CommandApplyBinding, Binding: &binding}

	case commandRemoveBindingVariant:
		var fields removeBindingFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*command = ControlCommand{
			Kind: CommandRemoveBinding, Source: &fields.Source,
			ProjectionRef: fields.ProjectionRef, Generation: fields.Generation,
		}

	case commandRegisterSchemaVariant:
		var definition SchemaDef
		if err := fromValueInto(payload, &definition); err != nil {
			return err
		}
		*command = ControlCommand{Kind: CommandRegisterSchema, Schema: &definition}

	case commandDropSchemaVariant:
		number, ok := payload.(cborform.Uint)
		if !ok {
			return fmt.Errorf("wire: a schema drop carries an id, got %T", payload)
		}
		*command = ControlCommand{Kind: CommandDropSchema, SchemaID: uint32(number)}

	case commandAllocateSchemaVariant:
		var allocation SchemaAllocation
		if err := fromValueInto(payload, &allocation); err != nil {
			return err
		}
		*command = ControlCommand{Kind: CommandAllocateSchema, Allocation: &allocation}

	case commandRegisterSessionSourceVariant:
		var fields registerSessionSourceFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*command = ControlCommand{
			Kind: CommandRegisterSessionSource, Stream: fields.Stream, Topics: &fields.Topics,
		}

	case commandRemoveSessionSourceVariant:
		var fields removeSessionSourceFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*command = ControlCommand{Kind: CommandRemoveSessionSource, Stream: fields.Stream}

	case commandFilterCatalogVariant:
		var catalog FilterCatalogCommand
		if err := fromValueInto(payload, &catalog); err != nil {
			return err
		}
		*command = ControlCommand{Kind: CommandFilterCatalog, Catalog: &catalog}

	case commandAcceptGapVariant:
		var gap GapAcceptance
		if err := fromValueInto(payload, &gap); err != nil {
			return err
		}
		*command = ControlCommand{Kind: CommandAcceptGap, Gap: &gap}

	case commandSkipRecordVariant:
		var skip RecordSkip
		if err := fromValueInto(payload, &skip); err != nil {
			return err
		}
		*command = ControlCommand{Kind: CommandSkipRecord, Skip: &skip}

	case commandSourceRetainedVariant:
		var retention SourceRetention
		if err := fromValueInto(payload, &retention); err != nil {
			return err
		}
		*command = ControlCommand{Kind: CommandSourceRetained, Retention: &retention}

	default:
		return unknownVariant("ControlCommand", name)
	}
	return nil
}

// ControlEnvelope is the versioned wrapper around a control command.
type ControlEnvelope struct {
	V               uint32         `cbor:"v"`
	TimestampMicros uint64         `cbor:"timestamp_micros"`
	Command         ControlCommand `cbor:"command"`
	// Stream is the stream the command's resource names belong to, set by a
	// client that scopes its resources to a stream. The writer-schema
	// registry keys schema commands by stream and id when it is set. Absent
	// addresses the deployment-wide registry.
	Stream *string `cbor:"stream,omitempty"`
	// Stamp is the identity the backend binds to the record before appending
	// it. A client never sets it.
	Stamp *ControlStamp `cbor:"stamp,omitempty"`
}

// NewControlEnvelope wraps a command at the current op version.
func NewControlEnvelope(command ControlCommand, timestampMicros uint64) ControlEnvelope {
	return ControlEnvelope{
		V: ControlOpVersion, TimestampMicros: timestampMicros, Command: command,
	}
}
