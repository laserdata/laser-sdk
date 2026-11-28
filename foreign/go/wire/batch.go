package wire

import "fmt"

// BatchItem is one managed request inside a batch: the command code it would
// have been sent under on its own, and its encoded request bytes verbatim.
type BatchItem struct {
	Code uint32 `cbor:"code"`
	// Payload is the op's own encoded request frame, exactly what a standalone
	// send would carry.
	Payload []byte `cbor:"payload"`
}

// BatchRequest is the mixed-operation batch request: several managed requests
// in one round trip, each carrying its own command code and payload.
//
// The batch amortizes the round trip and nothing else. Items execute
// independently in order, each yields its own result, and a failed item fails
// alone: it is explicitly not a transaction.
type BatchRequest struct {
	V uint32 `cbor:"v"`
	// Ops are the managed requests, executed in order.
	Ops []BatchItem `cbor:"ops"`
}

// Validate checks the batch bounds and refuses a nested batch.
func (request BatchRequest) Validate() error {
	if request.V != BatchOpVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"batch version must be %d, got %d", BatchOpVersion, request.V)}
	}
	if len(request.Ops) > MaxBatchOps {
		return &InvalidError{Reason: fmt.Sprintf(
			"batch holds %d ops, exceeds cap %d", len(request.Ops), MaxBatchOps)}
	}
	total := 0
	for _, op := range request.Ops {
		if op.Code == AgdxBatchCode {
			return &InvalidError{Reason: "a batch must not nest another batch"}
		}
		if len(op.Payload) > MaxValueBytes {
			return &InvalidError{Reason: fmt.Sprintf(
				"batch op payload is %dB, exceeds cap %dB", len(op.Payload), MaxValueBytes)}
		}
		total += len(op.Payload)
	}
	if total > MaxFrameBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"batch total payload is %dB, exceeds cap %dB", total, MaxFrameBytes)}
	}
	return nil
}

// BatchReply is each op's own reply bytes, in request order, exactly what a
// standalone round trip would have returned.
//
// A failed item carries its surface's typed error reply, so the caller decodes
// each slot with that item's own reply type.
type BatchReply struct {
	// Results are per-op reply frames, index-aligned with the request's ops.
	Results [][]byte `cbor:"results"`
}

// RequiredFieldNames refuses a reply without its results, so a frame-level
// error never reads as an empty batch.
func (*BatchReply) RequiredFieldNames() []string { return []string{"results"} }
