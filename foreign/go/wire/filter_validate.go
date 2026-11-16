package wire

import "fmt"

// The filter read surface's validation rules.
//
// A filtered read is answered from a consumer's stored progress, so a request
// that named the wrong policy, partition or group would acknowledge records it
// never evaluated. Each rule below keeps one such mismatch off the wire.

// Validate checks an inline filter, since a reference to a saved one is
// resolved by the catalog rather than here.
func (ref FilterRef) Validate() error {
	if ref.Kind != RefInline {
		return nil
	}
	if ref.Inline == nil {
		return &InvalidError{Reason: "an inline filter reference carries no filter"}
	}
	return ref.Inline.Validate()
}

// needsGroup reports a reference that resolves against a group's binding, so
// it cannot be read by an independent consumer.
func (ref FilterRef) needsGroup() bool {
	return ref.Kind == RefBound || ref.Kind == RefGroup
}

// Validate checks an acknowledgment against the page it claims to settle.
func (ack FilteredAck) Validate() error {
	if err := validateFilterVersion(ack.V); err != nil {
		return err
	}
	if err := ack.Source.Validate(); err != nil {
		return err
	}
	if err := ack.Consumer.Validate(); err != nil {
		return err
	}
	if err := validatePolicyIdentity(ack.Mode, ack.Digest); err != nil {
		return err
	}
	if !ack.Mode.IsFiltered() && !ack.Consumer.IsGroup() {
		return &InvalidError{Reason: "only a consumer group runs unfiltered"}
	}
	// The group identity is what the stored progress is keyed by, so it must
	// travel with the acknowledgment or settle against nothing.
	if ack.Consumer.IsGroup() != (ack.GroupID != nil) {
		return &InvalidError{
			Reason: "a group acknowledgment must retain its group identity",
		}
	}
	if ack.Generation.PartitionID != ack.PartitionID {
		return &InvalidError{
			Reason: "an acknowledgment names another partition's generation",
		}
	}
	return nil
}

// Validate checks a filtered poll's bounds and its continuation.
func (request FilteredPollRequest) Validate() error {
	if err := validateFilterVersion(request.V); err != nil {
		return err
	}
	if err := request.Source.Validate(); err != nil {
		return err
	}
	if err := request.Consumer.Validate(); err != nil {
		return err
	}
	if err := request.Filter.Validate(); err != nil {
		return err
	}
	if request.Filter.needsGroup() && !request.Consumer.IsGroup() {
		return &InvalidError{
			Reason: "a `bound` or `group` filter needs a consumer group",
		}
	}
	if request.Count == 0 || request.Count > MaxFilteredPageRecords {
		return &InvalidError{Reason: fmt.Sprintf(
			"count must be 1..=%d", MaxFilteredPageRecords)}
	}
	if request.MaxExamined != nil {
		examined := *request.MaxExamined
		if examined == 0 || examined > MaxFilteredPageExamined {
			return &InvalidError{Reason: fmt.Sprintf(
				"max_examined must be 1..=%d", MaxFilteredPageExamined)}
		}
	}
	if request.MaxReplyBytes == 0 || request.MaxReplyBytes > MaxFilteredPageBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"max_reply_bytes must be 1..=%d", MaxFilteredPageBytes)}
	}
	return request.validateContinuation()
}

// validateContinuation checks that a resumed read belongs to the page it
// continues: the same policy, the same group and the same partition.
func (request FilteredPollRequest) validateContinuation() error {
	if request.Start.Kind != StartContinue {
		return nil
	}
	continuation := request.Start.Continuation
	if continuation == nil {
		return &InvalidError{Reason: "a continuation start carries no continuation"}
	}
	if err := validatePolicyIdentity(
		continuation.Mode, continuation.Digest); err != nil {
		return err
	}
	if !continuation.Mode.IsFiltered() && !request.Consumer.IsGroup() {
		return &InvalidError{Reason: "only a consumer group runs unfiltered"}
	}
	if request.Consumer.IsGroup() != (continuation.GroupID != nil) {
		return &InvalidError{
			Reason: "a group continuation must retain its group identity",
		}
	}
	if continuation.Generation.PartitionID != request.PartitionID {
		return &InvalidError{Reason: "a continuation belongs to another partition"}
	}
	return nil
}

// Validate checks a preview, which joins no group and so resolves no binding.
func (request FilterPreviewRequest) Validate() error {
	if err := validateFilterVersion(request.V); err != nil {
		return err
	}
	if err := request.Source.Validate(); err != nil {
		return err
	}
	if err := request.Filter.Validate(); err != nil {
		return err
	}
	if request.Filter.needsGroup() {
		return &InvalidError{
			Reason: "a preview executes an inline filter or a saved revision",
		}
	}
	if request.MaxExamined == 0 || request.MaxExamined > MaxFilterPreviewExamined {
		return &InvalidError{Reason: fmt.Sprintf(
			"max_examined must be 1..=%d", MaxFilterPreviewExamined)}
	}
	if request.MaxRecords == 0 || request.MaxRecords > MaxFilterPreviewRecords {
		return &InvalidError{Reason: fmt.Sprintf(
			"max_records must be 1..=%d", MaxFilterPreviewRecords)}
	}
	return nil
}

// Validate checks a sample test, which reads nothing from a stream.
func (request FilterTestRequest) Validate() error {
	if err := validateFilterVersion(request.V); err != nil {
		return err
	}
	if err := request.Filter.Validate(); err != nil {
		return err
	}
	if request.Filter.needsGroup() {
		return &InvalidError{
			Reason: "a sample test executes an inline filter or a saved revision",
		}
	}
	if len(request.Payload) > MaxFilterSampleBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"a sample payload exceeds %dB", MaxFilterSampleBytes)}
	}
	if len(request.Headers) > MaxFilterSampleHeaders {
		return &InvalidError{Reason: fmt.Sprintf(
			"a sample carries more than %d headers", MaxFilterSampleHeaders)}
	}
	for _, header := range request.Headers {
		if err := validateFilterName("sample header key", header.Key); err != nil {
			return err
		}
		bytes := 0
		switch header.Value.Kind {
		case ScalarString:
			bytes = len(header.Value.Str)
		case ScalarRaw:
			bytes = len(header.Value.Raw)
		}
		if bytes > MaxFilterStringBytes {
			return &InvalidError{Reason: "a sample header value exceeds its byte limit"}
		}
	}
	return nil
}

// Validate checks a catalog mutation request and its operation identity.
func (request FilterMutationRequest) Validate() error {
	if err := validateFilterVersion(request.V); err != nil {
		return err
	}
	// The operation id is what a retry is deduplicated by, so a zero would
	// let the same mutation apply twice.
	if request.OperationID.IsZero() {
		return &InvalidError{Reason: "operation_id must not be zero"}
	}
	return request.Mutation.Validate()
}
