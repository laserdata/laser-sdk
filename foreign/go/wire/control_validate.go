package wire

import (
	"fmt"
	"strings"
)

// The control surface's validation rules.
//
// A control record is ordered and replayed, so a malformed one reconfigures
// every replica the same wrong way. Refusing it here keeps it off the log
// instead of leaving it to be refused once per replica.

// Validate checks a projection's identity, history, extraction plan and
// graph schema.
func (projection Projection) Validate() error {
	if err := projection.ID.Validate(); err != nil {
		return err
	}
	if err := projection.validateHistory(); err != nil {
		return err
	}

	// A whole-record projection materializes the record as it stands, so an
	// extraction plan beside it would describe a second, different row.
	if projection.Kind == ProjectionRecord {
		if len(projection.Extraction.Fields) != 0 ||
			projection.Extraction.VectorField != nil ||
			projection.EntitySchema != nil {
			return &InvalidError{Reason: "a whole-record projection extracts nothing, " +
				"so it declares no fields, vector field or entity schema"}
		}
	}
	if projection.Name != "" {
		if err := ValidateBoundedText(
			"projection name", projection.Name, MaxProjectionNameBytes); err != nil {
			return err
		}
	}
	for _, field := range projection.Extraction.Fields {
		if err := validateIndexFieldName(field.Name); err != nil {
			return err
		}
		if err := ValidateBoundedText(
			"index field pointer", field.Pointer, MaxIndexFieldBytes); err != nil {
			return err
		}
	}
	if projection.Extraction.VectorField != nil {
		if err := ValidateBoundedText("index vector field",
			*projection.Extraction.VectorField, MaxIndexFieldBytes); err != nil {
			return err
		}
	}
	if projection.EntitySchema != nil {
		for _, node := range projection.EntitySchema.Nodes {
			if err := ValidateBoundedText(
				"graph node label", node.Label, MaxGraphLabelBytes); err != nil {
				return err
			}
		}
		for _, edge := range projection.EntitySchema.Edges {
			if err := ValidateBoundedText(
				"graph edge type", edge.EdgeType, MaxGraphLabelBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateHistory checks a keyed projection's key and deletion header.
func (projection Projection) validateHistory() error {
	if projection.History.Kind != HistoryKeyed {
		return nil
	}
	// A graph has nodes and edges rather than rows, so there is no row for a
	// key to replace.
	if projection.Kind == ProjectionGraph {
		return &InvalidError{Reason: "graph projections do not support keyed history"}
	}
	if projection.History.DeleteHeader != nil {
		if err := ValidateBoundedText("deletion header",
			*projection.History.DeleteHeader, MaxIndexFieldBytes); err != nil {
			return err
		}
	}

	key := projection.History.Key
	switch key.Kind {
	case ProjectionKeyHeader:
		return ValidateBoundedText("key header", key.Name, MaxIndexFieldBytes)
	case ProjectionKeyPointer:
		// Reading a key out of the payload means decoding it, which only a
		// row projection does.
		if projection.Kind != ProjectionRow {
			return &InvalidError{Reason: "payload key pointers require a row projection"}
		}
		if key.Pointer != "" {
			if err := ValidateBoundedText(
				"key pointer", key.Pointer, MaxIndexFieldBytes); err != nil {
				return err
			}
			if !strings.HasPrefix(key.Pointer, "/") {
				return &InvalidError{Reason: "key pointer must be a JSON pointer"}
			}
		}
		return validatePointerEscapes(key.Pointer)
	default:
		return &InvalidError{Reason: fmt.Sprintf(
			"unknown projection key kind %q", string(key.Kind))}
	}
}

// validatePointerEscapes checks the two escapes RFC 6901 defines, since any
// other one would address a field that does not exist.
func validatePointerEscapes(pointer string) error {
	for i := 0; i < len(pointer); i++ {
		if pointer[i] != '~' {
			continue
		}
		if i+1 >= len(pointer) || (pointer[i+1] != '0' && pointer[i+1] != '1') {
			return &InvalidError{Reason: "key pointer has an invalid escape"}
		}
		i++
	}
	return nil
}

// validateIndexFieldName checks an index field name, which becomes a column
// of the materialized row and so follows the identifier rule rather than the
// looser bounded-text one.
func validateIndexFieldName(name string) error {
	if err := ValidateBoundedText("index field name", name, MaxIndexFieldBytes); err != nil {
		return err
	}
	leads := func(b byte) bool {
		return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_'
	}
	valid := leads(name[0])
	for i := 1; valid && i < len(name); i++ {
		b := name[i]
		valid = leads(b) || (b >= '0' && b <= '9')
	}
	if !valid {
		return &InvalidError{Reason: fmt.Sprintf(
			"index field name `%s` must start with an ASCII letter or `_` and hold "+
				"only ASCII letters, digits and `_`", name)}
	}
	return nil
}

// Validate checks a binding's projections and the policies it applies.
func (binding ProjectionBinding) Validate() error {
	for _, projection := range binding.AllowedProjections {
		if err := projection.Validate(); err != nil {
			return err
		}
	}
	if binding.DefaultProjection != nil {
		if err := binding.DefaultProjection.Validate(); err != nil {
			return err
		}
	}

	// An unknown policy decoded as a passthrough, which means this build
	// cannot tell what it asked for. Re-applying it would write back a policy
	// whose meaning was lost on the way in.
	unknownRetention := binding.Retention != nil &&
		binding.Retention.Kind == RetentionUnknown
	if binding.InvalidRecords == InvalidRecordUnknown ||
		binding.Oversize == OversizeUnknown || unknownRetention {
		return &InvalidError{
			Reason: "a binding cannot apply a policy this build does not know",
		}
	}

	if binding.Retention != nil {
		switch {
		case binding.Retention.Kind == RetentionTimeToLive && binding.Retention.TTLMicros == 0,
			binding.Retention.Kind == RetentionMaxRows && binding.Retention.Rows == 0:
			return &InvalidError{Reason: "a time to live retention needs a lifetime " +
				"above zero, and a max rows retention at least one row"}
		}
	}

	// A referenced body lives only in the source log, so a row that outlived
	// its record would point at nothing.
	if binding.Oversize == OversizeReference && binding.Retention != nil &&
		binding.Retention.Kind != RetentionMirrorLog {
		return &InvalidError{Reason: "a binding that keeps body references needs " +
			"mirror_log retention, so no row outlives its record"}
	}
	return nil
}

// Validate checks a registered writer schema's id and metadata.
func (definition SchemaDef) Validate() error {
	if definition.ID == 0 {
		return &InvalidError{Reason: "writer schema ids start at 1"}
	}
	return validateSchemaMetadata(definition.Source, definition.Name)
}

// Validate checks an allocation's metadata. It names no id, because the
// control fold chooses one.
func (allocation SchemaAllocation) Validate() error {
	return validateSchemaMetadata(allocation.Source, allocation.Name)
}

func validateSchemaMetadata(source SchemaSource, name *string) error {
	if name != nil {
		if err := ValidateBoundedText("schema name", *name, MaxSchemaNameBytes); err != nil {
			return err
		}
	}
	return source.Validate()
}

// Validate checks whichever command the record carries.
func (command ControlCommand) Validate() error {
	switch command.Kind {
	case CommandRegisterProjection, CommandRegisterGraph:
		if command.Projection == nil {
			return &InvalidError{Reason: "a registration carries no projection"}
		}
		return command.Projection.Validate()
	case CommandDropProjection, CommandDropGraph:
		return ProjectionID(command.Name).Validate()
	case CommandApplyBinding:
		if command.Binding == nil {
			return &InvalidError{Reason: "an apply carries no binding"}
		}
		return command.Binding.Validate()
	case CommandRemoveBinding:
		if command.ProjectionRef == nil {
			return nil
		}
		return ProjectionID(*command.ProjectionRef).Validate()
	case CommandRegisterSchema:
		if command.Schema == nil {
			return &InvalidError{Reason: "a schema registration carries no schema"}
		}
		return command.Schema.Validate()
	case CommandAllocateSchema:
		if command.Allocation == nil {
			return &InvalidError{Reason: "an allocation carries no schema source"}
		}
		return command.Allocation.Validate()
	case CommandFilterCatalog:
		if command.Catalog == nil {
			return &InvalidError{Reason: "a catalog command carries no mutation"}
		}
		return command.Catalog.Mutation.Validate()
	default:
		// The remaining commands carry values the fold checks against state
		// this record cannot see, so there is nothing to check here.
		return nil
	}
}
