package wire

import (
	"fmt"
	"strings"
)

// avroKind is the physical shape an Avro schema node decodes as.
//
// Logical annotations are stripped before parsing, so a decimal reads as the
// bytes it is written with and a timestamp as the integer. Comparisons use
// physical values and explicit coercions, identically in every SDK.
type avroKind uint8

const (
	avroNull avroKind = iota
	avroBoolean
	avroInt
	avroLong
	avroFloat
	avroDouble
	avroBytes
	avroString
	avroEnum
	avroFixed
	avroUnion
	avroRecord
	avroArray
	avroMap
	// avroRef names a type declared earlier in the schema.
	avroRef
)

// avroSchema is one node of a parsed writer schema.
type avroSchema struct {
	kind avroKind
	// name is the full name of a named type, and the referenced name for a
	// reference.
	name string
	// size is the width of a fixed type.
	size int
	// fields are a record's fields, in the order they are written.
	fields []avroField
	// variants are a union's branches, in their index order.
	variants []*avroSchema
	// items is the element schema of an array, or the value schema of a map.
	items *avroSchema
	// symbols are an enum's values, in their index order.
	symbols []string
}

type avroField struct {
	fallback *payloadValue
	name     string
	schema   *avroSchema
}

// avroNames resolves named types while a schema is parsed, so a reference to a
// record declared earlier reaches the same node.
type avroNames struct {
	byName map[string]*avroSchema
}

// parseAvroSchema reads a writer schema from its JSON text.
//
// Logical types are removed first: they describe how a producer interprets a
// value, not how it is written, and the contract compares physical values.
func parseAvroSchema(text string) (*avroSchema, error) {
	if len(text) > MaxFilterSchemaBytes {
		return nil, fmt.Errorf("schema exceeds %d bytes", MaxFilterSchemaBytes)
	}
	document, reason := decodeJSONPayload([]byte(text), DecodeLimits{
		MaxPayloadBytes: MaxFilterSchemaBytes, MaxDepth: MaxFilterSchemaDepth,
	})
	if reason != "" {
		return nil, fmt.Errorf("schema JSON is %s", reason)
	}
	names := &avroNames{byName: map[string]*avroSchema{}}
	return parseAvroNode(document, "", names, 0)
}

func parseAvroNode(
	node payloadValue, enclosing string, names *avroNames, depth int,
) (*avroSchema, error) {
	if depth > MaxFilterSchemaDepth {
		return nil, fmt.Errorf("schema nests deeper than %d", MaxFilterSchemaDepth)
	}
	switch node.kind {
	case payloadText:
		return parseAvroNamedType(node.text, enclosing, names)
	case payloadArray:
		// A bare array is a union of its branches.
		union := &avroSchema{kind: avroUnion}
		for _, item := range node.array {
			variant, err := parseAvroNode(item, enclosing, names, depth+1)
			if err != nil {
				return nil, err
			}
			union.variants = append(union.variants, variant)
		}
		return union, nil
	case payloadObject:
		return parseAvroObject(node, enclosing, names, depth)
	default:
		return nil, fmt.Errorf("a schema node is not a type")
	}
}

// parseAvroNamedType reads a type named by a bare string: a primitive, or a
// reference to a type declared earlier.
func parseAvroNamedType(
	name, enclosing string, names *avroNames,
) (*avroSchema, error) {
	switch name {
	case "null":
		return &avroSchema{kind: avroNull}, nil
	case "boolean":
		return &avroSchema{kind: avroBoolean}, nil
	case "int":
		return &avroSchema{kind: avroInt}, nil
	case "long":
		return &avroSchema{kind: avroLong}, nil
	case "float":
		return &avroSchema{kind: avroFloat}, nil
	case "double":
		return &avroSchema{kind: avroDouble}, nil
	case "bytes":
		return &avroSchema{kind: avroBytes}, nil
	case "string":
		return &avroSchema{kind: avroString}, nil
	}
	full := avroFullName(name, enclosing)
	if _, declared := names.byName[full]; !declared {
		if _, declared := names.byName[name]; declared {
			full = name
		} else {
			return nil, fmt.Errorf("schema names an undeclared type %q", name)
		}
	}
	return &avroSchema{kind: avroRef, name: full}, nil
}

func parseAvroObject(
	node payloadValue, enclosing string, names *avroNames, depth int,
) (*avroSchema, error) {
	typeNode, named := node.object["type"]
	if !named {
		return nil, fmt.Errorf("a schema object names no type")
	}
	// A nested type definition may itself be an object or a union.
	if typeNode.kind != payloadText {
		return parseAvroNode(typeNode, enclosing, names, depth+1)
	}

	switch typeNode.text {
	case "record", "error":
		return parseAvroRecord(node, enclosing, names, depth)
	case "enum":
		return parseAvroEnum(node, enclosing, names)
	case "fixed":
		return parseAvroFixed(node, enclosing, names)
	case "array":
		items, err := parseAvroChild(node, "items", enclosing, names, depth)
		if err != nil {
			return nil, err
		}
		return &avroSchema{kind: avroArray, items: items}, nil
	case "map":
		values, err := parseAvroChild(node, "values", enclosing, names, depth)
		if err != nil {
			return nil, err
		}
		return &avroSchema{kind: avroMap, items: values}, nil
	default:
		return parseAvroNamedType(typeNode.text, enclosing, names)
	}
}

func parseAvroChild(
	node payloadValue, key, enclosing string, names *avroNames, depth int,
) (*avroSchema, error) {
	child, present := node.object[key]
	if !present {
		return nil, fmt.Errorf("a schema node names no %s", key)
	}
	return parseAvroNode(child, enclosing, names, depth+1)
}

func parseAvroRecord(
	node payloadValue, enclosing string, names *avroNames, depth int,
) (*avroSchema, error) {
	full, namespace, err := avroDeclaredName(node, enclosing)
	if err != nil {
		return nil, err
	}
	record := &avroSchema{kind: avroRecord, name: full}
	// The record is registered before its fields are read, so a field may
	// refer back to the record it is declared in.
	names.byName[full] = record

	fields, present := node.object["fields"]
	if !present || fields.kind != payloadArray {
		return nil, fmt.Errorf("record %q lists no fields", full)
	}
	for _, field := range fields.array {
		if field.kind != payloadObject {
			return nil, fmt.Errorf("record %q has a field that is not an object", full)
		}
		name, named := field.object["name"]
		if !named || name.kind != payloadText {
			return nil, fmt.Errorf("record %q has an unnamed field", full)
		}
		schema, err := parseAvroChild(field, "type", namespace, names, depth)
		if err != nil {
			return nil, err
		}
		entry := avroField{name: name.text, schema: schema}
		if fallback, has := field.object["default"]; has {
			entry.fallback = &fallback
		}
		record.fields = append(record.fields, entry)
	}
	return record, nil
}

func parseAvroEnum(
	node payloadValue, enclosing string, names *avroNames,
) (*avroSchema, error) {
	full, _, err := avroDeclaredName(node, enclosing)
	if err != nil {
		return nil, err
	}
	symbols, present := node.object["symbols"]
	if !present || symbols.kind != payloadArray {
		return nil, fmt.Errorf("enum %q lists no symbols", full)
	}
	enum := &avroSchema{kind: avroEnum, name: full}
	for _, symbol := range symbols.array {
		if symbol.kind != payloadText {
			return nil, fmt.Errorf("enum %q has a symbol that is not a name", full)
		}
		enum.symbols = append(enum.symbols, symbol.text)
	}
	names.byName[full] = enum
	return enum, nil
}

func parseAvroFixed(
	node payloadValue, enclosing string, names *avroNames,
) (*avroSchema, error) {
	full, _, err := avroDeclaredName(node, enclosing)
	if err != nil {
		return nil, err
	}
	size, present := node.object["size"]
	if !present || size.kind != payloadNumber {
		return nil, fmt.Errorf("fixed %q names no size", full)
	}
	width, ok := integerOf(size.number)
	if !ok {
		return nil, fmt.Errorf("fixed %q has a size that is not an integer", full)
	}
	count, fits := width.signed()
	if !fits || count < 0 || count > MaxFilterSchemaBytes {
		return nil, fmt.Errorf("fixed %q has an unusable size", full)
	}
	fixed := &avroSchema{kind: avroFixed, name: full, size: int(count)}
	names.byName[full] = fixed
	return fixed, nil
}

// avroDeclaredName is a named type's full name and the namespace its nested
// declarations resolve in.
func avroDeclaredName(node payloadValue, enclosing string) (string, string, error) {
	name, named := node.object["name"]
	if !named || name.kind != payloadText {
		return "", "", fmt.Errorf("a named schema type has no name")
	}
	namespace := enclosing
	if declared, present := node.object["namespace"]; present &&
		declared.kind == payloadText {
		namespace = declared.text
	}
	full := avroFullName(name.text, namespace)
	// A name written in full carries its own namespace for what it encloses.
	if dot := strings.LastIndex(full, "."); dot >= 0 {
		namespace = full[:dot]
	} else {
		namespace = ""
	}
	return full, namespace, nil
}

func avroFullName(name, namespace string) string {
	if namespace == "" || strings.Contains(name, ".") {
		return name
	}
	return namespace + "." + name
}

// resolve follows a reference to the type it names.
func (names *avroNames) resolve(schema *avroSchema) (*avroSchema, bool) {
	for schema.kind == avroRef {
		target, declared := names.byName[schema.name]
		if !declared {
			return nil, false
		}
		schema = target
	}
	return schema, true
}
