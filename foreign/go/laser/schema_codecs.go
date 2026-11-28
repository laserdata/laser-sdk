package laser

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/dlclark/regexp2/v2"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// CompiledSchema validates and decodes records under one registered schema.
type CompiledSchema struct {
	id        uint32
	kind      wire.ContentType
	writer    *wire.WriterSchema
	validator *jsonschema.Schema
}

type schemaResources struct{}

func (schemaResources) Load(url string) (any, error) {
	return nil, fmt.Errorf("JSON Schema reference outside the registered document: %s", url)
}

// The validator's regexp callback returns only bool. A match that exhausts
// its budget must abort the entire validation, including a negated pattern.
type schemaPattern struct{ *regexp2.Regexp }
type patternEvaluationError struct{ cause error }

func compilePattern(pattern string) (jsonschema.Regexp, error) {
	re, err := regexp2.Compile(pattern, regexp2.ECMAScript|regexp2.Unicode, regexp2.OptionMaxBacktrackingStackSize(128*1024))
	if err != nil {
		return nil, err
	}
	re.MatchTimeout = 100 * time.Millisecond
	return schemaPattern{re}, nil
}

func (pattern schemaPattern) MatchString(text string) bool {
	matched, err := pattern.Regexp.MatchString(text)
	if err != nil {
		panic(patternEvaluationError{err})
	}
	return matched
}

func catchPatternFailure(err *error) {
	if recovered := recover(); recovered != nil {
		if failure, ok := recovered.(patternEvaluationError); ok {
			*err = &CodecError{Reason: "JSON Schema regular expression could not be evaluated", Cause: failure.cause}
			return
		}
		panic(recovered)
	}
}

// CompileSchema resolves a registered definition once for repeated reads and writes.
func CompileSchema(def wire.SchemaDef) (_ *CompiledSchema, err error) {
	defer catchPatternFailure(&err)
	if err := def.Source.Validate(); err != nil {
		return nil, &InvalidError{Reason: fmt.Sprintf("schema %d: %v", def.ID, err)}
	}
	compiled := &CompiledSchema{id: def.ID, kind: def.ContentType()}
	if def.Source.Kind != wire.SchemaJSON {
		writer, err := wire.CompileWriterSchema(def.Source)
		if err != nil {
			return nil, &InvalidError{Reason: fmt.Sprintf("schema %d: %v", def.ID, err)}
		}
		compiled.writer = writer
		return compiled, nil
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseRegexpEngine(compilePattern)
	compiler.UseLoader(schemaResources{})
	const location = "urn:laser:writer-schema"
	doc, err := jsonschema.UnmarshalJSON(bytes.NewBufferString(def.Source.Schema))
	if err == nil {
		err = compiler.AddResource(location, doc)
	}
	if err == nil {
		compiled.validator, err = compiler.Compile(location)
	}
	if err != nil {
		return nil, &InvalidError{Reason: fmt.Sprintf("schema %d: %v", def.ID, err)}
	}
	return compiled, nil
}

// ContentType is the content type records written under this schema carry.
func (schema *CompiledSchema) ContentType() wire.ContentType { return schema.kind }

func (schema *CompiledSchema) validatedJSON(payload []byte) (_ any, err error) {
	defer catchPatternFailure(&err)
	if !utf8.Valid(payload) {
		return nil, &CodecError{Reason: "invalid UTF-8 in JSON payload"}
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(payload))
	if err == nil {
		err = schema.validator.Validate(value)
	}
	if err != nil {
		return nil, &CodecError{Reason: fmt.Sprintf("payload fails JSON Schema %d", schema.id), Cause: err}
	}
	return value, nil
}

// Validate checks one complete encoded payload against its writer schema.
func (schema *CompiledSchema) Validate(payload []byte) bool {
	if schema == nil {
		return false
	}
	if schema.validator != nil {
		_, err := schema.validatedJSON(payload)
		return err == nil
	}
	if schema.writer == nil {
		return false
	}
	_, reason := schema.writer.Decode(payload, wire.RecordDecodeLimits)
	return reason == ""
}

// ValidateValue checks a Go value against a JSON Schema.
func (schema *CompiledSchema) ValidateValue(value any) bool {
	if schema == nil || schema.validator == nil {
		return false
	}
	raw, err := json.Marshal(value)
	return err == nil && schema.Validate(raw)
}

// Decode lowers a complete record to the same JSON shape as the managed projector.
func (schema *CompiledSchema) Decode(payload []byte, into any) error {
	if schema == nil || (schema.validator == nil && schema.writer == nil) {
		return &InvalidError{Reason: "missing compiled schema"}
	}
	if schema.validator != nil {
		if _, err := schema.validatedJSON(payload); err != nil {
			return err
		}
	} else {
		decoded, reason := schema.writer.Decode(payload, wire.RecordDecodeLimits)
		if reason != "" {
			return &CodecError{Reason: fmt.Sprintf("payload fails schema %d: %s", schema.id, reason)}
		}
		payload = []byte(decoded.JSON())
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(into); err != nil {
		return &CodecError{Reason: "decoded record does not fit the target type", Cause: err}
	}
	return nil
}

// EncodeAvro encodes one schema-guided value as an Avro datum.
func (schema *CompiledSchema) EncodeAvro(value any) ([]byte, error) {
	if schema == nil || schema.writer == nil || schema.kind != wire.ContentAvro {
		return nil, &InvalidError{Reason: "encoding Avro requires an Avro schema"}
	}
	raw, err := json.Marshal(value)
	if err == nil {
		avro, _ := schema.writer.Avro()
		raw, err = avro.EncodeJSON(raw)
	}
	if err != nil {
		return nil, &CodecError{Reason: "body does not match the Avro schema", Cause: err}
	}
	return raw, nil
}

// EncodeProtobuf encodes one schema-guided value as a Protobuf message.
func (schema *CompiledSchema) EncodeProtobuf(value any) ([]byte, error) {
	if schema == nil || schema.writer == nil || schema.kind != wire.ContentProtobuf {
		return nil, &InvalidError{Reason: "encoding Protobuf requires a Protobuf schema"}
	}
	raw, err := json.Marshal(value)
	if err == nil {
		descriptor, _ := schema.writer.Protobuf()
		raw, err = wire.EncodeProtobufJSON(descriptor, raw)
	}
	if err != nil {
		return nil, &CodecError{Reason: "body does not match the Protobuf schema", Cause: err}
	}
	return raw, nil
}

// Encode writes a body as JSON, Avro or Protobuf, by the schema's kind.
func (schema *CompiledSchema) Encode(value any) ([]byte, error) {
	if schema == nil {
		return nil, &InvalidError{Reason: "missing compiled schema"}
	}
	switch schema.kind {
	case wire.ContentAvro:
		return schema.EncodeAvro(value)
	case wire.ContentJSON:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, &CodecError{Reason: "encode JSON", Cause: err}
		}
		if _, err := schema.validatedJSON(raw); err != nil {
			return nil, err
		}
		return raw, nil
	case wire.ContentProtobuf:
		return schema.EncodeProtobuf(value)
	default:
		return nil, &InvalidError{Reason: fmt.Sprintf("schema %d has no encoder for %s", schema.id, schema.kind)}
	}
}

// TypedSchema resolves one registered schema and pins it to the topic handle.
func TypedSchema[T any](ctx context.Context, topic *Topic, id uint32) (*TypedTopic[T], error) {
	info, err := topic.laser.WithDefaultStream(topic.stream).Schemas().Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, &InvalidError{Reason: fmt.Sprintf("schema %d is not registered", id)}
	}
	compiled, err := CompileSchema(info.Schema)
	if err != nil {
		return nil, err
	}
	return TypedWith[T](topic, compiled), nil
}
