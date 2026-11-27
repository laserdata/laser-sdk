package wire

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// PatchOp is one RFC 6902 operation name.
type PatchOp string

const (
	PatchAdd     PatchOp = "add"
	PatchRemove  PatchOp = "remove"
	PatchReplace PatchOp = "replace"
	PatchMove    PatchOp = "move"
	PatchCopy    PatchOp = "copy"
	PatchTest    PatchOp = "test"
)

// PatchOperation is one RFC 6902 operation of a state patch.
//
// It is internally tagged by its op, and which of the other fields are present
// depends on which op it is.
type PatchOperation struct {
	Op   PatchOp `cbor:"op"`
	Path string  `cbor:"path"`
	// From is the source pointer of a move or a copy.
	From string `cbor:"from,omitempty"`
	// Value is what an add, a replace, or a test carries.
	Value *DynValue `cbor:"value,omitempty"`
}

// The patch operation constructors.
func AddOperation(path string, value DynValue) PatchOperation {
	return PatchOperation{Op: PatchAdd, Path: path, Value: &value}
}

func RemoveOperation(path string) PatchOperation {
	return PatchOperation{Op: PatchRemove, Path: path}
}

func ReplaceOperation(path string, value DynValue) PatchOperation {
	return PatchOperation{Op: PatchReplace, Path: path, Value: &value}
}

func MoveOperation(from, path string) PatchOperation {
	return PatchOperation{Op: PatchMove, Path: path, From: from}
}

func CopyOperation(from, path string) PatchOperation {
	return PatchOperation{Op: PatchCopy, Path: path, From: from}
}

func TestOperation(path string, value DynValue) PatchOperation {
	return PatchOperation{Op: PatchTest, Path: path, Value: &value}
}

// Validate checks that the operation carries what its op needs.
func (operation PatchOperation) Validate() error {
	switch operation.Op {
	case PatchAdd, PatchReplace, PatchTest:
		if operation.Value == nil {
			return &InvalidError{Reason: fmt.Sprintf(
				"a JSON Patch %s requires a value", operation.Op)}
		}
	case PatchMove, PatchCopy:
		if operation.From == "" {
			return &InvalidError{Reason: fmt.Sprintf(
				"a JSON Patch %s requires a source pointer", operation.Op)}
		}
	case PatchRemove:
	default:
		return &InvalidError{Reason: fmt.Sprintf(
			"unknown JSON Patch operation %q", operation.Op)}
	}
	if _, err := decodeJSONPointer(operation.Path); err != nil {
		return err
	}
	return nil
}

// forbiddenPointerTokens address an object's internals rather than its data.
//
// A patch is peer-supplied, so a pointer naming one of these must never reach
// a property write.
var forbiddenPointerTokens = map[string]bool{
	"__proto__": true, "constructor": true, "prototype": true,
}

// decodeJSONPointer splits an RFC 6901 pointer into its unescaped tokens.
func decodeJSONPointer(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	if !strings.HasPrefix(path, "/") {
		return nil, &InvalidError{Reason: fmt.Sprintf("invalid JSON Pointer %q", path)}
	}
	parts := strings.Split(path[1:], "/")
	tokens := make([]string, len(parts))
	for index, part := range parts {
		// A tilde escapes only ~0 and ~1, and anything else is malformed.
		for at := strings.IndexByte(part, '~'); at >= 0; {
			if at+1 >= len(part) || (part[at+1] != '0' && part[at+1] != '1') {
				return nil, &InvalidError{
					Reason: fmt.Sprintf("invalid JSON Pointer %q", path)}
			}
			next := strings.IndexByte(part[at+2:], '~')
			if next < 0 {
				break
			}
			at += next + 2
		}
		token := strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		if forbiddenPointerTokens[token] {
			return nil, &InvalidError{Reason: fmt.Sprintf(
				"JSON Patch path %q addresses a reserved property", path)}
		}
		tokens[index] = token
	}
	return tokens, nil
}

// patchArrayIndex reads one array token, which may be the end marker when the
// operation appends.
func patchArrayIndex(token string, length int, allowEnd bool) (int, error) {
	if token == "-" && allowEnd {
		return length, nil
	}
	if token == "" || (token != "0" && token[0] == '0') {
		return 0, &InvalidError{Reason: fmt.Sprintf(
			"invalid JSON Patch array index %q", token)}
	}
	index, err := strconv.Atoi(token)
	maximum := length - 1
	if allowEnd {
		maximum = length
	}
	if err != nil || index < 0 || index > maximum {
		return 0, &InvalidError{Reason: fmt.Sprintf(
			"JSON Patch array index %q is out of bounds", token)}
	}
	return index, nil
}

// ApplyJSONPatch applies a state patch to a document, returning the result.
//
// The document is never mutated: a patch that fails part way leaves the caller
// holding the document it started with, so a rejected delta cannot half-apply.
func ApplyJSONPatch(document DynValue, patch []PatchOperation) (DynValue, error) {
	if len(patch) > MaxStatePatchOps {
		return DynValue{}, &InvalidError{Reason: "state patch has too many operations"}
	}
	result := document.Clone()
	for _, operation := range patch {
		applied, err := applyPatchOperation(result, operation)
		if err != nil {
			return DynValue{}, err
		}
		result = applied
	}
	if size, err := jsonByteSize(result); err != nil {
		return DynValue{}, err
	} else if size > MaxStateDocumentBytes {
		return DynValue{}, &InvalidError{
			Reason: "state document exceeds its byte cap"}
	}
	return result, nil
}

func applyPatchOperation(document DynValue, operation PatchOperation) (DynValue, error) {
	if err := operation.Validate(); err != nil {
		return DynValue{}, err
	}
	switch operation.Op {
	case PatchAdd:
		return addAtPointer(document, operation.Path, operation.Value.Clone())
	case PatchRemove:
		removed, _, err := removeAtPointer(document, operation.Path)
		return removed, err
	case PatchReplace:
		// The path must already exist, so a replace never creates a field.
		if _, err := valueAtPointer(document, operation.Path); err != nil {
			return DynValue{}, err
		}
		removed, _, err := removeAtPointer(document, operation.Path)
		if err != nil {
			return DynValue{}, err
		}
		return addAtPointer(removed, operation.Path, operation.Value.Clone())
	case PatchMove:
		// Moving a value into its own child would lose it.
		if strings.HasPrefix(operation.Path, operation.From+"/") {
			return DynValue{}, &InvalidError{
				Reason: "JSON Patch cannot move a value into its child"}
		}
		removed, taken, err := removeAtPointer(document, operation.From)
		if err != nil {
			return DynValue{}, err
		}
		return addAtPointer(removed, operation.Path, taken)
	case PatchCopy:
		taken, err := valueAtPointer(document, operation.From)
		if err != nil {
			return DynValue{}, err
		}
		return addAtPointer(document, operation.Path, taken.Clone())
	default:
		found, err := valueAtPointer(document, operation.Path)
		if err != nil {
			return DynValue{}, err
		}
		if !found.Equal(*operation.Value) {
			return DynValue{}, &InvalidError{Reason: fmt.Sprintf(
				"JSON Patch test failed at %q", operation.Path)}
		}
		return document, nil
	}
}

// jsonByteSize is how many bytes the value occupies as JSON, which is what the
// document and patch caps are measured in.
func jsonByteSize(value DynValue) (int, error) {
	encoded, err := MarshalCanonicalJSON(value)
	if err != nil {
		return 0, err
	}
	return len(encoded), nil
}

// valueAtPointer reads the value one pointer addresses.
func valueAtPointer(document DynValue, path string) (DynValue, error) {
	tokens, err := decodeJSONPointer(path)
	if err != nil {
		return DynValue{}, err
	}
	current := document.Item()
	for _, token := range tokens {
		switch typed := current.(type) {
		case cborform.Array:
			index, err := patchArrayIndex(token, len(typed), false)
			if err != nil {
				return DynValue{}, err
			}
			current = typed[index]
		case cborform.Map:
			found, present := DynValue{item: typed}.Field(token)
			if !present {
				return DynValue{}, &InvalidError{Reason: fmt.Sprintf(
					"JSON Patch path %q does not exist", path)}
			}
			current = found.Item()
		default:
			return DynValue{}, &InvalidError{Reason: fmt.Sprintf(
				"JSON Patch path %q does not address a container", path)}
		}
	}
	return DynValue{item: current}, nil
}

// addAtPointer inserts a value at one pointer, returning the new document.
func addAtPointer(document DynValue, path string, value DynValue) (DynValue, error) {
	tokens, err := decodeJSONPointer(path)
	if err != nil {
		return DynValue{}, err
	}
	// An empty pointer addresses the root, which the value replaces whole.
	if len(tokens) == 0 {
		return value, nil
	}
	rebuilt, err := rewriteAt(document.Item(), tokens, path, func(
		container cborform.Value, token string,
	) (cborform.Value, error) {
		switch typed := container.(type) {
		case cborform.Array:
			index, err := patchArrayIndex(token, len(typed), true)
			if err != nil {
				return nil, err
			}
			items := make(cborform.Array, 0, len(typed)+1)
			items = append(items, typed[:index]...)
			items = append(items, value.Item())
			return append(items, typed[index:]...), nil
		case cborform.Map:
			entries := make(cborform.Map, 0, len(typed)+1)
			replaced := false
			for _, entry := range typed {
				if key, ok := entry.Key.(cborform.Text); ok && string(key) == token {
					entries = append(entries, cborform.Entry{
						Key: entry.Key, Value: value.Item(),
					})
					replaced = true
					continue
				}
				entries = append(entries, entry)
			}
			if !replaced {
				entries = append(entries, cborform.Entry{
					Key: cborform.Text(token), Value: value.Item(),
				})
			}
			return entries, nil
		default:
			return nil, &InvalidError{Reason: fmt.Sprintf(
				"JSON Patch path %q does not address a container", path)}
		}
	})
	if err != nil {
		return DynValue{}, err
	}
	return DynValue{item: rebuilt}, nil
}

// removeAtPointer takes the value one pointer addresses out of the document,
// returning the new document and what was removed.
func removeAtPointer(document DynValue, path string) (DynValue, DynValue, error) {
	tokens, err := decodeJSONPointer(path)
	if err != nil {
		return DynValue{}, DynValue{}, err
	}
	if len(tokens) == 0 {
		return DynValue{item: cborform.Null{}}, document, nil
	}
	var taken cborform.Value
	rebuilt, err := rewriteAt(document.Item(), tokens, path, func(
		container cborform.Value, token string,
	) (cborform.Value, error) {
		switch typed := container.(type) {
		case cborform.Array:
			index, err := patchArrayIndex(token, len(typed), false)
			if err != nil {
				return nil, err
			}
			taken = typed[index]
			items := make(cborform.Array, 0, len(typed)-1)
			items = append(items, typed[:index]...)
			return append(items, typed[index+1:]...), nil
		case cborform.Map:
			entries := make(cborform.Map, 0, len(typed))
			for _, entry := range typed {
				if key, ok := entry.Key.(cborform.Text); ok && string(key) == token {
					taken = entry.Value
					continue
				}
				entries = append(entries, entry)
			}
			if taken == nil {
				return nil, &InvalidError{Reason: fmt.Sprintf(
					"JSON Patch path %q does not exist", path)}
			}
			return entries, nil
		default:
			return nil, &InvalidError{Reason: fmt.Sprintf(
				"JSON Patch path %q does not address a container", path)}
		}
	})
	if err != nil {
		return DynValue{}, DynValue{}, err
	}
	return DynValue{item: rebuilt}, DynValue{item: taken}, nil
}

// rewriteAt walks to the container the last token names and rebuilds the path
// back to the root around whatever edit returns.
func rewriteAt(
	current cborform.Value, tokens []string, path string,
	edit func(cborform.Value, string) (cborform.Value, error),
) (cborform.Value, error) {
	if len(tokens) == 1 {
		return edit(current, tokens[0])
	}
	token := tokens[0]
	switch typed := current.(type) {
	case cborform.Array:
		index, err := patchArrayIndex(token, len(typed), false)
		if err != nil {
			return nil, err
		}
		child, err := rewriteAt(typed[index], tokens[1:], path, edit)
		if err != nil {
			return nil, err
		}
		items := make(cborform.Array, len(typed))
		copy(items, typed)
		items[index] = child
		return items, nil
	case cborform.Map:
		entries := make(cborform.Map, len(typed))
		copy(entries, typed)
		for index, entry := range entries {
			key, ok := entry.Key.(cborform.Text)
			if !ok || string(key) != token {
				continue
			}
			child, err := rewriteAt(entry.Value, tokens[1:], path, edit)
			if err != nil {
				return nil, err
			}
			entries[index] = cborform.Entry{Key: entry.Key, Value: child}
			return entries, nil
		}
		return nil, &InvalidError{Reason: fmt.Sprintf(
			"JSON Patch path %q does not exist", path)}
	default:
		return nil, &InvalidError{Reason: fmt.Sprintf(
			"JSON Patch path %q does not address a container", path)}
	}
}
