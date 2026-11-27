package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// The contract's payload-carrying enums ride externally tagged, the serde
// default: a unit variant is its bare name, and a variant with a payload is a
// one-entry map from the name to that payload. Variant names keep their Rust
// PascalCase spelling unless the type renames them.
//
// These helpers keep that shape in one place so no surface spells it
// differently.

// unitVariant encodes a payload-free variant as its bare name.
func unitVariant(name string) cborform.Value {
	return cborform.Text(name)
}

// taggedVariant encodes a variant carrying a payload.
func taggedVariant(name string, payload cborform.Value) cborform.Value {
	return cborform.Map{{Key: cborform.Text(name), Value: payload}}
}

// marshalVariantStruct encodes a struct-shaped variant, whose payload is a map
// of its named fields.
func marshalVariantStruct(name string, fields any) (cborform.Value, error) {
	payload, err := toValueOf(fields)
	if err != nil {
		return nil, fmt.Errorf("variant %q: %w", name, err)
	}
	return taggedVariant(name, payload), nil
}

// readVariant splits an externally-tagged value into its variant name and
// payload. The payload is nil for a unit variant.
func readVariant(item cborform.Value) (string, cborform.Value, error) {
	switch typed := item.(type) {
	case cborform.Text:
		return string(typed), nil, nil
	case cborform.Map:
		if len(typed) != 1 {
			return "", nil, fmt.Errorf(
				"wire: an externally tagged variant carries one entry, got %d", len(typed))
		}
		name, ok := typed[0].Key.(cborform.Text)
		if !ok {
			return "", nil, fmt.Errorf("wire: a variant name must be text, got %T", typed[0].Key)
		}
		return string(name), typed[0].Value, nil
	default:
		return "", nil, fmt.Errorf("wire: cannot read %T as a variant", item)
	}
}

// unknownVariant is the error a surface returns for a variant it does not name.
//
// The reply enums decode fail-closed on purpose: treating an unexpected variant
// as success would, for instance, have a caller trust an unextended lease
// deadline.
func unknownVariant(surface, name string) error {
	return fmt.Errorf("wire: %s has no variant %q", surface, name)
}

// requirePayload rejects a unit encoding where a payload is required.
func requirePayload(surface, name string, payload cborform.Value) error {
	if payload == nil {
		return fmt.Errorf("wire: %s variant %q requires a payload", surface, name)
	}
	return nil
}
