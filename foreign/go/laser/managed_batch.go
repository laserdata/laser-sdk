package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ExecuteBatch sends independent managed commands in one round trip. Each
// result contains the corresponding command's encoded success or error reply.
func (laser *Laser) ExecuteBatch(ctx context.Context, ops []wire.BatchItem) ([][]byte, error) {
	if !laser.Capabilities().Managed {
		return nil, &UnsupportedError{Surface: "the managed batch surface"}
	}
	request := wire.BatchRequest{V: wire.BatchOpVersion, Ops: ops}
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	if !laser.Capabilities().Kv.FencedLeases {
		for _, item := range ops {
			if requiresFencedLeases(item) {
				return nil, &UnsupportedError{Surface: "the fenced-lease contract"}
			}
		}
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode batch request", Cause: err}
	}
	raw, err := laser.SendManaged(ctx, wire.AgdxBatchCode, payload)
	if err != nil {
		return nil, err
	}
	var reply wire.BatchReply
	if err := decodeManagedReply(raw, &reply, "decode batch reply"); err != nil {
		return nil, err
	}
	if len(reply.Results) != len(ops) {
		return nil, &CodecError{Reason: "batch reply must contain one result per command"}
	}
	return reply.Results, nil
}

func requiresFencedLeases(item wire.BatchItem) bool {
	switch item.Code {
	case wire.AgdxKvLeaseCode, wire.AgdxKvLeaseRenewCode, wire.AgdxKvReleaseCode, wire.AgdxKvCasFencedCode:
		return true
	case wire.AgdxKvGetCode:
		var request wire.KvGet
		return wire.Unmarshal(item.Payload, &request) == nil && request.MinPosition != nil
	default:
		return false
	}
}

// GetMany reads keys in one round trip. A missing key returns a nil slot,
// and a stored empty value returns a non-nil, zero-length slot.
func (kv *Kv) GetMany(ctx context.Context, keys [][]byte) ([][]byte, error) {
	if err := kv.laser.requireKv(); err != nil {
		return nil, err
	}
	if len(keys) > wire.MaxBatchOps {
		return nil, &InvalidError{Reason: "too many keys for one managed batch"}
	}
	if err := wire.ValidateKvNamespace(kv.namespace); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	request := wire.KvGet{V: wire.KvOpVersion, Namespace: kv.ResourceNamespace()}
	if kv.laser.Capabilities().Kv.FencedLeases {
		shared := kv.laser.state()
		shared.mutex.RLock()
		request.MinPosition = copyPosition(shared.frontier.Kv)
		shared.mutex.RUnlock()
	}
	ops := make([]wire.BatchItem, len(keys))
	for i, key := range keys {
		if err := validatedKey(key); err != nil {
			return nil, err
		}
		request.Key = key
		payload, err := wire.Marshal(request)
		if err != nil {
			return nil, &CodecError{Reason: "encode batch get", Cause: err}
		}
		ops[i] = wire.BatchItem{Code: wire.AgdxKvGetCode, Payload: payload}
	}
	results, err := kv.laser.ExecuteBatch(ctx, ops)
	if err != nil {
		return nil, err
	}
	for i, raw := range results {
		outcome, err := decodeKvOutcome(raw)
		if err != nil {
			return nil, err
		}
		if outcome.Kind != wire.KvValue {
			return nil, unexpectedKvOutcome("get", outcome.Kind)
		}
		results[i] = nil
		if outcome.Entry != nil {
			results[i] = outcome.Entry.Value
			if results[i] == nil {
				results[i] = []byte{}
			}
		}
	}
	return results, nil
}
