package laser

import (
	"bytes"
	"context"
	"encoding/binary"

	iggyserde "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Filters is the server-side consumer-filter surface for one topic.
//
// The server evaluates the filter and sends back only what matched, so a
// reader pays for the records it wanted rather than the whole topic. That is
// the point of the feature: the saving is in what never leaves the server.
type Filters struct {
	laser    *Laser
	source   wire.FilterSource
	consumer wire.FilterConsumer
}

// Filters opens the filter surface for a topic, following one consumer's
// progress.
func (topic *Topic) Filters(consumer wire.FilterConsumer) *Filters {
	return &Filters{
		laser:    topic.laser,
		source:   wire.FilterSource{Stream: topic.stream, Topic: topic.name},
		consumer: consumer,
	}
}

// Source is the stream and topic these reads cover.
func (filters *Filters) Source() wire.FilterSource { return filters.source }

// execute sends one filter command and opens its reply.
//
// Filtered delivery is served by the streaming server itself rather than by a
// managed backend, so it is gated on the native capability. The saved catalog
// needs a backend and is gated separately, where it is used.
func (filters *Filters) execute(
	ctx context.Context, code uint32, request any,
) (*wire.FilterOutcome, error) {
	if !filters.laser.capabilities.Filters.Native {
		return nil, &UnsupportedError{Surface: "server-side consumer filters"}
	}
	// Native filtering is gated on the native capability alone, as Rust's
	// own native path is (sdk/src/filters/client.rs). It used to also
	// require the *catalog's* op version here, which is a different
	// surface: OpVersions.Filter describes the saved-filter catalog, and
	// the contract says zero there means saved filters and group bindings
	// are not served -- it says nothing about native filtering. So a
	// deployment that serves native filters without a catalog had this
	// client advertise Filters.Native and then refuse every filtered poll
	// with "served at op version 0, but this client speaks 1". The catalog
	// keeps that requirement where it belongs, in FilterCatalog.execute.
	if filters.source.Stream == "" {
		return nil, errNoStream()
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode filter request", Cause: err}
	}
	raw, err := filters.laser.SendManaged(ctx, code, payload)
	if err != nil {
		return nil, err
	}
	var reply wire.FilterReply
	if err := wire.Unmarshal(raw, &reply); err != nil {
		return nil, &CodecError{Reason: "decode filter reply", Cause: err}
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{
				Reason: "filter reply carried neither an outcome nor an error"}
		}
		return nil, &FilterError{Failure: *reply.Err}
	}
	return reply.Outcome, nil
}

// MatchedPage is one page of records the filter selected, with everything a
// reader needs to acknowledge it and ask for the next.
type MatchedPage struct {
	// Page is the server's own reply, carrying the scan boundaries and the
	// policy that ran.
	Page wire.FilteredPage
	// Records are the matching records, decoded and classified.
	Records []MatchedRecord
	// PartitionID is the partition the page was read from.
	PartitionID uint32
}

// RecordEvaluation is why a record is in a page.
//
// A reader that treats every delivered record as a match would act on records
// the filter never judged, which is the difference between a filter that
// selects and one that merely narrows.
type RecordEvaluation struct {
	// Kind says whether the filter selected this record, could not judge it,
	// or never ran.
	Kind RecordEvaluationKind
	// Reason is why an unevaluated record could not be judged.
	Reason wire.FaultReason
}

// RecordEvaluationKind discriminates a record's standing in a page.
type RecordEvaluationKind uint8

const (
	// EvaluationSelected means the filter judged this record and it matched.
	EvaluationSelected RecordEvaluationKind = iota
	// EvaluationUnevaluated means the filter could not judge this record and
	// the fault policy passed it through rather than dropping it.
	EvaluationUnevaluated
	// EvaluationUnfiltered means no filter ran: the page is a group's whole
	// examined range.
	EvaluationUnfiltered
)

// Selected reports a record the filter judged and matched.
func (evaluation RecordEvaluation) Selected() bool {
	return evaluation.Kind == EvaluationSelected
}

// MatchedRecord is one record a filtered page carried.
type MatchedRecord struct {
	// Message is the record as the log holds it: its offset, id, timestamps,
	// user headers and payload bytes are the producer's own.
	Message iggcon.IggyMessage
	// PartitionID is the partition it was read from.
	PartitionID uint32
	// Evaluation is why it is here.
	Evaluation RecordEvaluation
}

// Offset is the record's position in its partition.
func (record MatchedRecord) Offset() uint64 { return record.Message.Header.Offset }

// Payload is the record's body.
func (record MatchedRecord) Payload() []byte { return record.Message.Payload }

// Examined is how many source records the server looked at to find this page.
//
// It is what the saving is measured against: a page of 4 records examined from
// 240 is the feature working.
func (page MatchedPage) Examined() uint32 { return page.Page.Examined }

// Matched is how many records the filter selected.
func (page MatchedPage) Matched() uint32 { return page.Page.Matched }

// Exhausted reports a page that reached the end of what the serving replica
// can see, so a reader waits rather than polling again immediately.
//
// A page stopped by a budget is a different thing: more records may follow at
// once, so a reader asks again rather than waiting.
func (page MatchedPage) Exhausted() bool {
	return page.Page.Stop == wire.StopEndOfVisible
}

// FilteredReadRequest is an unsent filtered poll.
type FilteredReadRequest struct {
	filters     *Filters
	partitionID uint32
	filter      wire.FilterRef
	start       wire.FilteredStart
	count       uint32
	maxExamined *uint32
	maxBytes    uint32
	err         error
}

// Read begins a filtered read of one partition.
//
// A filtered read is per partition, because the scan boundary and the
// acknowledgment are both partition-scoped: one offset cannot describe
// progress across several.
func (filters *Filters) Read(partitionID uint32, filter wire.FilterRef) *FilteredReadRequest {
	return &FilteredReadRequest{
		filters:     filters,
		partitionID: partitionID,
		filter:      filter,
		start:       wire.StartAtNext(),
		count:       DefaultFilteredCount,
		maxBytes:    DefaultFilteredReplyBytes,
	}
}

const (
	// DefaultFilteredCount is how many matches one page asks for.
	DefaultFilteredCount uint32 = 100
	// DefaultFilteredReplyBytes bounds one page's record bytes. The server may
	// lower it.
	DefaultFilteredReplyBytes uint32 = 1 << 20
)

// From sets where the scan begins.
func (request *FilteredReadRequest) From(start wire.FilteredStart) *FilteredReadRequest {
	request.start = start
	return request
}

// Count is the most matching records to return.
func (request *FilteredReadRequest) Count(count uint32) *FilteredReadRequest {
	request.count = count
	return request
}

// MaxExamined bounds how many source records the server looks at.
//
// A normal consumer sets it to the count, so a batch of 100 examines at most
// 100 and a sparse filter returns quickly rather than scanning a whole
// partition. Leaving it unset uses the server's own budget, which is what a
// sweep over a cold topic wants.
func (request *FilteredReadRequest) MaxExamined(records uint32) *FilteredReadRequest {
	request.maxExamined = &records
	return request
}

// MaxReplyBytes bounds the record bytes one page carries.
func (request *FilteredReadRequest) MaxReplyBytes(bytes uint32) *FilteredReadRequest {
	request.maxBytes = bytes
	return request
}

// Fetch reads one page.
func (request *FilteredReadRequest) Fetch(ctx context.Context) (*MatchedPage, error) {
	if request.err != nil {
		return nil, request.err
	}
	poll := wire.FilteredPollRequest{
		V:             wire.FilterOpVersion,
		Source:        request.filters.source,
		PartitionID:   request.partitionID,
		Consumer:      request.filters.consumer,
		Filter:        request.filter,
		Start:         request.start,
		Count:         request.count,
		MaxReplyBytes: request.maxBytes,
		MaxExamined:   request.maxExamined,
	}
	outcome, err := request.filters.execute(ctx, wire.AgdxFilteredPollCode, poll)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.FilterPageOutcome || outcome.Page == nil {
		return nil, unexpectedFilterOutcome("read", outcome.Kind)
	}
	page := *outcome.Page
	records, err := matchedRecords(poll, page)
	if err != nil {
		return nil, err
	}
	// The scan continues from where this page stopped, so a caller that keeps
	// fetching walks the partition rather than re-reading its head.
	request.start = page.NextStart(request.start)
	return &MatchedPage{
		Page: page, Records: records, PartitionID: request.partitionID,
	}, nil
}

// Each reads pages until the partition is exhausted or visit returns false.
//
// It does not acknowledge: a reader that has not finished with a record must
// not have its progress stored, so acknowledging stays the caller's call.
func (request *FilteredReadRequest) Each(
	ctx context.Context, visit func(*MatchedPage) bool,
) error {
	for {
		page, err := request.Fetch(ctx)
		if err != nil {
			return err
		}
		if !visit(page) {
			return nil
		}
		if page.Exhausted() {
			return nil
		}
	}
}

// The poll-response framing a page's record body carries: the partition id,
// the head offset, the record count, then the partition context.
const pollPrefixSize = 16 + iggcon.PartitionContextSize

// The canonical batch record framing inside that body.
const (
	batchHeaderSize        = 256
	batchMessageHeaderSize = 48
)

// matchedRecords decodes and checks the records a page carries.
//
// They arrive in the server's poll-response framing rather than one by one, so
// they are decoded with the transport's own decoder: a second implementation
// of someone else's wire format would drift from it silently. What this does
// add is the checking the transport cannot do, because only the request knows
// what was asked for.
func matchedRecords(
	request wire.FilteredPollRequest, page wire.FilteredPage,
) ([]MatchedRecord, error) {
	if err := validateRecordBody(request, page); err != nil {
		return nil, err
	}
	polled, err := iggyserde.DeserializeFetchMessagesResponse(
		page.Records, iggcon.MESSAGE_COMPRESSION_NONE)
	if err != nil {
		return nil, &CodecError{Reason: "decode the matched records", Cause: err}
	}
	if err := validatePage(request, page, polled); err != nil {
		return nil, err
	}
	unevaluated := make(map[uint64]wire.FaultReason, len(page.Unevaluated))
	for _, fault := range page.Unevaluated {
		unevaluated[fault.Offset] = fault.Reason
	}
	records := make([]MatchedRecord, 0, len(polled.Messages))
	for _, message := range polled.Messages {
		records = append(records, MatchedRecord{
			Message:     message,
			PartitionID: page.PartitionID,
			Evaluation: recordEvaluation(
				page.Policy.Mode, unevaluated, message.Header.Offset),
		})
	}
	return records, nil
}

// recordEvaluation says why one record is in a page.
func recordEvaluation(
	mode wire.ExecutionMode, unevaluated map[uint64]wire.FaultReason, offset uint64,
) RecordEvaluation {
	if !mode.IsFiltered() {
		return RecordEvaluation{Kind: EvaluationUnfiltered}
	}
	if reason, passed := unevaluated[offset]; passed {
		return RecordEvaluation{Kind: EvaluationUnevaluated, Reason: reason}
	}
	return RecordEvaluation{Kind: EvaluationSelected}
}

func badFraming() error {
	return &CodecError{
		Reason: "the filtered record body has invalid framing or counts"}
}

// validateRecordBody walks the record body's framing before anything is
// decoded from it.
//
// The count in the body's head is what a decoder allocates against, so it is
// checked against both the page's own count and what the request asked for
// first: a forged count would otherwise size an allocation off a number no one
// checked.
func validateRecordBody(request wire.FilteredPollRequest, page wire.FilteredPage) error {
	if len(page.Records) < pollPrefixSize {
		return badFraming()
	}
	count := binary.LittleEndian.Uint32(page.Records[12:16])
	if count != page.Matched || count > request.Count {
		return badFraming()
	}
	rest := page.Records[pollPrefixSize:]
	decoded := uint32(0)
	for len(rest) > 0 {
		if len(rest) < batchHeaderSize {
			return badFraming()
		}
		baseOffset := binary.LittleEndian.Uint64(rest[8:16])
		originTimestamp := binary.LittleEndian.Uint64(rest[24:32])
		batchLength := binary.LittleEndian.Uint64(rest[32:40])
		messages := binary.LittleEndian.Uint32(rest[48:52])
		if batchLength < batchHeaderSize || batchLength > uint64(len(rest)) {
			return badFraming()
		}
		if decoded > ^uint32(0)-messages {
			return badFraming()
		}
		decoded += messages
		// Each frame's deltas resolve against the batch bases, and a sum that
		// wraps would place a record at an offset the page never examined.
		cursor := uint64(batchHeaderSize)
		for cursor < batchLength {
			if batchLength-cursor < batchMessageHeaderSize {
				return badFraming()
			}
			frame := rest[cursor : cursor+batchMessageHeaderSize]
			offsetDelta := uint64(binary.LittleEndian.Uint32(frame[24:28]))
			timestampDelta := uint64(binary.LittleEndian.Uint32(frame[28:32]))
			headerBytes := uint64(binary.LittleEndian.Uint32(frame[32:36]))
			payloadBytes := uint64(binary.LittleEndian.Uint32(frame[36:40]))
			if baseOffset > ^uint64(0)-offsetDelta ||
				originTimestamp > ^uint64(0)-timestampDelta {
				return badFraming()
			}
			// A frame that claims more than its batch holds would read past
			// the record it belongs to and into the next one.
			body := batchMessageHeaderSize + payloadBytes + headerBytes
			if body < payloadBytes || batchLength-cursor < body {
				return badFraming()
			}
			cursor += body
		}
		rest = rest[batchLength:]
	}
	if decoded != count {
		return badFraming()
	}
	return nil
}

func badPage() error {
	return &CodecError{
		Reason: "the filtered page does not match its request or scan boundary"}
}

// validatePage holds the server's reply to what was asked for.
//
// How many records the body carries is already pinned to page.Matched by
// validateRecordBody and by the transport's decoder, so what is left here is
// everything only the request knows: the partition, the budget, the policy,
// and the scan boundary.
//
// A filtered reader stores progress through records it never saw, so a page
// that misstates its scan boundary is worse than a lost one: the records it
// skipped past are never read again. Every invariant here is one a correct
// server already keeps, and checking them is what makes a wrong one visible
// rather than silently lossy.
func validatePage(
	request wire.FilteredPollRequest, page wire.FilteredPage, polled *iggcon.PolledMessage,
) error {
	if page.V != wire.FilterOpVersion ||
		page.PartitionID != request.PartitionID ||
		page.Generation.PartitionID != request.PartitionID ||
		polled.PartitionId != request.PartitionID ||
		polled.CurrentOffset != page.Frontier ||
		polled.Context.Incarnation != page.Generation.PartitionIncarnation ||
		polled.Context.OwnerGeneration != page.AckContext.OwnerGeneration ||
		polled.Context.MetadataOp != page.AckContext.MetadataOp ||
		page.Matched > page.Examined ||
		(request.MaxExamined != nil && page.Examined > *request.MaxExamined) ||
		page.Policy.Validate() != nil {
		return badPage()
	}
	// A strict group read runs a binding. An unfiltered page is the whole
	// examined range, so nothing was selected out of it.
	// The contract's own Validate already refuses an unfiltered policy that
	// names a revision or no group, so what is left is what only the request
	// knows.
	if !page.Policy.Mode.IsFiltered() {
		if request.Filter.Kind == wire.RefBound || page.Matched != page.Examined {
			return badPage()
		}
	}
	if request.Consumer.Kind == wire.ConsumerGroupID &&
		(page.Policy.GroupID == nil || *page.Policy.GroupID != request.Consumer.ID) {
		return &CodecError{
			Reason: "the filtered page identifies another consumer group"}
	}
	if request.Consumer.IsGroup() != (page.Policy.GroupID != nil) {
		return badPage()
	}
	start, known := startOffset(request.Start, page)
	if !known && request.Start.Kind == wire.StartContinue {
		return badPage()
	}
	// The last offset the page examined, which it offers as the safe
	// acknowledgment offset.
	var scannedTo uint64
	scanned := false
	if page.NextScanOffset != nil && *page.NextScanOffset > 0 {
		scannedTo, scanned = *page.NextScanOffset-1, true
	}
	if page.Examined == 0 {
		if page.SafeAckOffset != nil || !sameOffset(page.NextScanOffset, start, known) {
			return badPage()
		}
	} else if !scanned || page.SafeAckOffset == nil || *page.SafeAckOffset != scannedTo {
		return badPage()
	}
	if scanned && scannedTo > page.Frontier {
		return badPage()
	}
	if known && page.NextScanOffset != nil && *page.NextScanOffset < start {
		return badPage()
	}
	previous, seen := uint64(0), false
	for _, message := range polled.Messages {
		offset := message.Header.Offset
		if (seen && offset <= previous) ||
			(known && offset < start) ||
			!scanned || offset > scannedTo || offset > page.Frontier {
			return badPage()
		}
		previous, seen = offset, true
	}
	if err := validateUnevaluated(page, polled); err != nil {
		return err
	}
	faulted := page.Stop == wire.StopFault || page.Stop == wire.StopOversizedRecord
	if faulted != (page.Fault != nil) {
		return badPage()
	}
	// The faulting record is past the scan boundary, because a page that
	// claims to have scanned through its own fault would skip it.
	if page.Fault != nil && scanned && scannedTo >= page.Fault.Offset {
		return badPage()
	}
	return nil
}

// validateUnevaluated holds every reported fault to one record the page
// actually carried, so a reader cannot be told a record it never saw was
// passed through unjudged.
func validateUnevaluated(page wire.FilteredPage, polled *iggcon.PolledMessage) error {
	if len(page.Unevaluated) == 0 {
		return nil
	}
	delivered := make(map[uint64]struct{}, len(polled.Messages))
	for _, message := range polled.Messages {
		delivered[message.Header.Offset] = struct{}{}
	}
	seen := make(map[uint64]struct{}, len(page.Unevaluated))
	for _, fault := range page.Unevaluated {
		if _, carried := delivered[fault.Offset]; !carried {
			return badPage()
		}
		if _, twice := seen[fault.Offset]; twice {
			return badPage()
		}
		seen[fault.Offset] = struct{}{}
	}
	return nil
}

// startOffset is the first offset the request asked the server to examine,
// and whether the request names one at all. A next, last or timestamp start
// leaves the boundary for the server to resolve.
func startOffset(start wire.FilteredStart, page wire.FilteredPage) (uint64, bool) {
	switch start.Kind {
	case wire.StartContinue:
		if start.Continuation == nil {
			return 0, false
		}
		cursor := *start.Continuation
		if cursor.Generation != page.Generation ||
			!sameDigest(cursor.Digest, page.Policy.Digest) ||
			!sameGroup(cursor.GroupID, page.Policy.GroupID) ||
			cursor.Mode != page.Policy.Mode ||
			cursor.PolicyGeneration != page.Policy.PolicyGeneration {
			return 0, false
		}
		return cursor.NextScanOffset, true
	case wire.StartAtOffset:
		return start.Offset, true
	case wire.StartFirst:
		return 0, true
	default:
		return 0, false
	}
}

// sameOffset compares the page's next scan offset against a start the request
// named, treating an unnamed start as matching either way.
func sameOffset(next *uint64, start uint64, known bool) bool {
	if !known {
		return next == nil
	}
	return next != nil && *next == start
}

func sameDigest(left, right *wire.Digest32) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return bytes.Equal(left.Bytes, right.Bytes)
}

func sameGroup(left, right *uint64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// Ack stores progress through a page's safe offset.
//
// The safe offset is the last examined record, not the last matched one: a
// filtered reader that stored only its matches would re-examine everything the
// filter rejected on the next read.
//
// It is refused by the server when the source generation moved on or the
// consumer no longer owns the partition, which is what stops a reader that
// lost its assignment from storing progress another member has passed.
func (filters *Filters) Ack(ctx context.Context, page *MatchedPage) error {
	if page.Page.SafeAckOffset == nil {
		// Nothing was examined, so there is no progress to store. A reader
		// acknowledging anyway would move its offset past records the server
		// never looked at.
		return nil
	}
	acknowledgment := wire.FilteredAck{
		V:                wire.FilterOpVersion,
		Source:           filters.source,
		PartitionID:      page.PartitionID,
		Consumer:         filters.consumer,
		Generation:       page.Page.Generation,
		AckContext:       page.Page.AckContext,
		Digest:           page.Page.Policy.Digest,
		Offset:           *page.Page.SafeAckOffset,
		Mode:             page.Page.Policy.Mode,
		PolicyGeneration: page.Page.Policy.PolicyGeneration,
	}
	if page.Page.Policy.GroupID != nil {
		acknowledgment.GroupID = page.Page.Policy.GroupID
	}
	outcome, err := filters.execute(ctx, wire.AgdxFilteredAckCode, acknowledgment)
	if err != nil {
		return err
	}
	if outcome.Kind != wire.FilterAcknowledged {
		return unexpectedFilterOutcome("acknowledge", outcome.Kind)
	}
	return nil
}

// FilterError reports a filter command the server refused.
type FilterError struct{ Failure wire.FilterError }

func (e *FilterError) Error() string {
	return "consumer filter: " + string(e.Failure.Reason) + ": " + e.Failure.Message
}

// Reason is the contract's classification of the failure.
func (e *FilterError) Reason() wire.FilterErrorReason { return e.Failure.Reason }

// ResultCode is the shared code this failure projects onto, so a filter
// failure branches the same way as every other managed failure.
func (e *FilterError) ResultCode() wire.ResultCode { return e.Failure.ResultCode() }

// Retryable reports a condition where the identical request may succeed
// later, so a caller can repeat it rather than give up.
func (e *FilterError) Retryable() bool { return e.ResultCode().IsRetryable() }

// NotFound reports a filter, revision, binding or operation the catalog does
// not hold.
func (e *FilterError) NotFound() bool {
	return e.ResultCode() == wire.ResultNotFound
}

func unexpectedFilterOutcome(operation string, kind wire.FilterOutcomeKind) error {
	return &CodecError{Reason: "filter " + operation +
		" answered with an unexpected outcome: " + itoa(int(kind))}
}
