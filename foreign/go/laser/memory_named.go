package laser

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Set writes named point state under the caller's key, as one record on the memory topic.
func (memory *Memory) Set(ctx context.Context, key string, body []byte) error {
	if err := memory.requireNamed("Set"); err != nil {
		return err
	}
	id := memory.namedKey(key)
	if err := memory.writeNamed(ctx, id, wire.RememberItem(id, string(MemoryFact), body)); err != nil {
		return err
	}
	memory.reader.mutex.Lock()
	defer memory.reader.mutex.Unlock()
	memory.reader.fold.named[id] = bytes.Clone(body)
	return nil
}

// Fetch reads the named item from the managed view, or folds the topic where the deployment serves none.
func (memory *Memory) Fetch(ctx context.Context, key string) ([]byte, bool, error) {
	if err := memory.requireNamed("Fetch"); err != nil {
		return nil, false, err
	}
	if memory.laser.Negotiated().Kv.Available {
		return memory.laser.Kv(memory.namespace).Get(ctx, []byte(memory.namedKey(key)))
	}
	return memory.FetchFolded(ctx, key)
}

// FetchFolded reads the named item by folding the memory topic in process.
func (memory *Memory) FetchFolded(ctx context.Context, key string) ([]byte, bool, error) {
	if err := memory.requireNamed("FetchFolded"); err != nil {
		return nil, false, err
	}
	if memory.stream == "" {
		return nil, false, errNoStream()
	}
	memory.reader.mutex.Lock()
	defer memory.reader.mutex.Unlock()
	if err := memory.catchUp(ctx); err != nil {
		return nil, false, err
	}
	body, found := memory.reader.fold.named[memory.namedKey(key)]
	if !found {
		return nil, false, nil
	}
	return bytes.Clone(body), true, nil
}

// Update merges a JSON merge patch (RFC 7386) into the named item and writes the result as one record.
func (memory *Memory) Update(ctx context.Context, key string, patch []byte) error {
	if err := memory.requireNamed("Update"); err != nil {
		return err
	}
	// The merge reads through the fold, which shows this handle's own writes at once.
	current, found, err := memory.FetchFolded(ctx, key)
	if err != nil {
		return err
	}
	if !found {
		current = nil
	}
	merged, err := mergeNamed(current, patch)
	if err != nil {
		return err
	}
	return memory.Set(ctx, key, merged)
}

// Remove deletes the named item. Removing an absent key succeeds.
func (memory *Memory) Remove(ctx context.Context, key string) error {
	if err := memory.requireNamed("Remove"); err != nil {
		return err
	}
	id := memory.namedKey(key)
	if err := memory.writeNamed(ctx, id, wire.ForgetItem(id)); err != nil {
		return err
	}
	memory.reader.mutex.Lock()
	defer memory.reader.mutex.Unlock()
	delete(memory.reader.fold.named, id)
	return nil
}

// requireNamed refuses a named verb on a handle with no key space.
func (memory *Memory) requireNamed(verb string) error {
	if memory.vector == nil && memory.custom == nil {
		return nil
	}
	return &UnsupportedError{Surface: "memory", Message: verb +
		"(key) addresses a named item and needs the durable memory handle, this handle resolved to " +
		memory.Backend()}
}

// namedKey prefixes the key with the namespace, so two namespaces on one topic never collide.
func (memory *Memory) namedKey(key string) string {
	return memory.namespace + "/" + key
}

// writeNamed appends a keyed record. The conversation is derived from the key,
// so every write of one key shares a partition and folds in offset order.
func (memory *Memory) writeNamed(ctx context.Context, id string, record wire.MemoryRecord) error {
	if memory.stream == "" {
		return errNoStream()
	}
	if err := record.Validate(); err != nil {
		return &InvalidError{Reason: err.Error()}
	}
	payload, err := wire.Marshal(record)
	if err != nil {
		return &CodecError{Reason: "encode named memory record", Cause: err}
	}
	conversation := DeriveConversationID(id)
	governed, err := memory.laser.govern(ctx, &GovernedAction{
		Kind: ActionMemoryWrite, Stream: memory.stream, Topic: memory.topic,
		Conversation: &conversation, Payload: payload,
	})
	if err != nil {
		return err
	}
	if governed != nil {
		payload = governed
	}
	headers, err := memoryHeaders(MemoryScope{Conversation: &conversation}, memory.laser.ResourceName(memory.namespace), id)
	if err != nil {
		return err
	}
	return memory.send(ctx, headers, conversation.String(), payload)
}

// mergeNamed applies patch to the current value, an absent value merging onto null.
func mergeNamed(current, patch []byte) ([]byte, error) {
	change, err := decodeJSONValue(patch)
	if err != nil {
		return nil, &CodecError{Reason: "update patch is not JSON", Cause: err}
	}
	var base any
	if current != nil {
		if base, err = decodeJSONValue(current); err != nil {
			return nil, &CodecError{Reason: "stored value is not JSON", Cause: err}
		}
	}
	var merged bytes.Buffer
	encoder := json.NewEncoder(&merged)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(mergePatch(base, change)); err != nil {
		return nil, &CodecError{Reason: "encode merged value", Cause: err}
	}
	return bytes.TrimSuffix(merged.Bytes(), []byte("\n")), nil
}

// decodeJSONValue reads one JSON value, keeping numbers as written.
func decodeJSONValue(payload []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, &InvalidError{Reason: "more than one JSON value"}
	}
	return value, nil
}

// mergePatch is RFC 7386: an object patch overwrites matching fields and a null
// field removes one, any other patch replaces the base.
func mergePatch(base, patch any) any {
	fields, object := patch.(map[string]any)
	if !object {
		return patch
	}
	merged, held := base.(map[string]any)
	if !held {
		merged = map[string]any{}
	}
	for key, value := range fields {
		if value == nil {
			delete(merged, key)
			continue
		}
		merged[key] = mergePatch(merged[key], value)
	}
	return merged
}
