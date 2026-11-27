package laser_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// filterServer answers filter and catalog commands from a script.
type filterServer struct {
	clusterOfOne
	outcome  wire.FilterOutcome
	catalog  *wire.FilterCatalogOutcome
	failure  *wire.FilterError
	refusals int
	// verbatim answers the scripted outcome unchanged, rather than stamping
	// it with the identity the request carried.
	verbatim bool
	codes    []uint32
	requests [][]byte
}

func (server *filterServer) Close() error { return nil }

func (server *filterServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.codes = append(server.codes, code)
	server.requests = append(server.requests, payload)
	if server.refusals > 0 {
		server.refusals--
		failure := wire.FilterError{
			Code:    wire.ReasonCatalogUnavailable.Code(),
			Reason:  wire.ReasonCatalogUnavailable,
			Message: "the backend is restarting",
		}
		if server.catalog != nil {
			return wire.Marshal(wire.FilterCatalogReply{Err: &failure})
		}
		return wire.Marshal(wire.FilterReply{Err: &failure})
	}
	if server.failure != nil {
		if server.catalog != nil {
			return wire.Marshal(wire.FilterCatalogReply{Err: server.failure})
		}
		return wire.Marshal(wire.FilterReply{Err: server.failure})
	}
	if server.catalog != nil {
		outcome := *server.catalog
		if outcome.Mutation != nil && !server.verbatim {
			answered := *outcome.Mutation
			answered.OperationID = requestedOperation(code, payload)
			if answered.CatalogPosition != nil {
				position := *answered.CatalogPosition
				position.OperationID = &answered.OperationID
				answered.CatalogPosition = &position
			}
			outcome.Mutation = &answered
		}
		return wire.Marshal(wire.FilterCatalogReply{Outcome: &outcome})
	}
	return wire.Marshal(wire.FilterReply{Outcome: &server.outcome})
}

// requestedOperation reads back the identity a catalog command carried, so a
// fake answers the operation it was asked about.
func requestedOperation(code uint32, payload []byte) wire.U128 {
	body := payload
	if wire.IsIdempotentManagedRequest(code) {
		var envelope wire.ManagedRequestEnvelope
		if wire.Unmarshal(payload, &envelope) != nil {
			return wire.U128{}
		}
		body = envelope.Payload
	}
	if code == wire.AgdxFilterMutateCode {
		var request wire.FilterMutationRequest
		if wire.Unmarshal(body, &request) != nil {
			return wire.U128{}
		}
		return request.OperationID
	}
	var request wire.GetFilterOperation
	if wire.Unmarshal(body, &request) != nil {
		return wire.U128{}
	}
	return request.OperationID
}

func filterCapabilities() *laser.Capabilities {
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion).
		WithFilter(wire.FilterOpVersion)
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Hello = laser.HelloAnswered
	capabilities.Filters = laser.FilterCaps{Native: true, Catalog: true}
	capabilities.Versions = &versions
	return &capabilities
}

func filterHandle(server iggcon.Client, options laser.Options) *laser.Laser {
	if options.Capabilities == nil {
		options.Capabilities = filterCapabilities()
	}
	if options.DefaultStream == "" {
		options.DefaultStream = "agents"
	}
	return laser.FromClient(server, options)
}

func decodeFilterRequest[T any](t *testing.T, server *filterServer, index int, target *T) {
	t.Helper()
	body := server.requests[index]
	if wire.IsIdempotentManagedRequest(server.codes[index]) {
		var envelope wire.ManagedRequestEnvelope
		if err := wire.Unmarshal(body, &envelope); err != nil {
			t.Fatalf("decode the envelope: %v", err)
		}
		body = envelope.Payload
	}
	if err := wire.Unmarshal(body, target); err != nil {
		t.Fatalf("decode request %d: %v", index, err)
	}
}

// pollReply builds the poll-response framing a filtered page carries, so the
// decode and every check that walks it run against the layout the server
// really sends rather than against a shape invented here.
func pollReply(
	partitionID uint32, frontier uint64, context iggcon.PartitionContext,
	offsets []uint64, payload []byte,
) []byte {
	const prefix = 16 + iggcon.PartitionContextSize
	body := make([]byte, prefix)
	binary.LittleEndian.PutUint32(body[0:4], partitionID)
	binary.LittleEndian.PutUint64(body[4:12], frontier)
	binary.LittleEndian.PutUint32(body[12:16], uint32(len(offsets)))
	binary.LittleEndian.PutUint64(body[16:24], context.Incarnation)
	binary.LittleEndian.PutUint64(body[24:32], context.OwnerGeneration)
	binary.LittleEndian.PutUint64(body[32:40], context.MetadataOp)
	for _, offset := range offsets {
		body = append(body, batchRecord(offset, payload)...)
	}
	return body
}

// batchRecord encodes one record in the canonical batch framing: a 256-byte
// batch header, then a 48-byte frame header, then the payload.
func batchRecord(offset uint64, payload []byte) []byte {
	const (
		headerSize = 256
		frameSize  = 48
	)
	record := make([]byte, headerSize+frameSize+len(payload))
	binary.LittleEndian.PutUint64(record[8:16], offset)
	binary.LittleEndian.PutUint64(record[32:40], uint64(len(record)))
	binary.LittleEndian.PutUint32(record[48:52], 1)
	frame := record[headerSize:]
	binary.LittleEndian.PutUint32(frame[36:40], uint32(len(payload)))
	copy(frame[frameSize:], payload)
	return record
}

// matchedPage builds a page consistent with what a correct server answers for
// a read of one partition from offset zero.
func matchedPage(offsets []uint64, examined uint32, payload []byte) wire.FilteredPage {
	next := offsets[len(offsets)-1] + 1
	if examined > 0 {
		next = uint64(examined)
	}
	safe := next - 1
	context := iggcon.PartitionContext{Incarnation: 3, OwnerGeneration: 0, MetadataOp: 9}
	return wire.FilteredPage{
		V:           wire.FilterOpVersion,
		PartitionID: 0,
		Policy:      wire.AppliedPolicy{Mode: wire.ModeFiltered, Digest: inlineDigest()},
		Generation: wire.SourceGeneration{
			StreamID: 1, TopicID: 2, PartitionID: 0, PartitionIncarnation: 3,
		},
		AckContext:     wire.AckContext{OwnerGeneration: 0, MetadataOp: 9},
		NextScanOffset: &next,
		SafeAckOffset:  &safe,
		Frontier:       safe,
		Examined:       examined,
		Matched:        uint32(len(offsets)),
		Stop:           wire.StopEndOfVisible,
		Records:        pollReply(0, safe, context, offsets, payload),
	}
}

func inlineDigest() *wire.Digest32 {
	compiled, err := wire.CompileFilter(inlineDefinition())
	if err != nil {
		panic(err)
	}
	digest := compiled.Digest()
	return &digest
}

// A filter that compiles here is one the server accepts and that selects the
// same records, which is what sharing one evaluator buys.
func TestGivenAFilterWhenValidatedLocallyThenShouldReportWhatItReads(t *testing.T) {
	validation, err := laser.ValidateFilter(inlineDefinition(), nil)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !validation.ReadsHeaders {
		t.Error("a header predicate reads headers")
	}
	if validation.ReadsPayload {
		t.Error("a headers-only filter must not need the payload")
	}
	// The digest is what a binding is conditional on, so it must be stable.
	again, err := laser.ValidateFilter(inlineDefinition(), nil)
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !bytes.Equal(validation.Digest.Bytes, again.Digest.Bytes) {
		t.Error("one filter must compile to one digest")
	}
}

// A filter the contract refuses must be refused here, where the caller can
// still fix it, rather than at the server.
func TestGivenABrokenFilterWhenValidatedThenShouldRefuseIt(t *testing.T) {
	broken := wire.ConsumerFilter{V: wire.FilterOpVersion}

	_, err := laser.ValidateFilter(broken, nil)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// The saving is the point: a few records returned out of many examined is the
// feature working, so both counts come back with the records.
func TestGivenAFilteredReadWhenFetchedThenShouldReportWhatItExamined(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome,
		Page: ptr(matchedPage([]uint64{17, 103}, 240, []byte("matched"))),
	}}
	topic := filterHandle(server, laser.Options{}).Topic("events")
	filters := topic.Filters(wire.IndependentConsumer("reader"))

	page, err := filters.Read(0, inlineFilter(t)).From(wire.StartAtFirst()).
		Count(10).MaxExamined(240).Fetch(context.Background())
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if page.Examined() != 240 || page.Matched() != 2 {
		t.Errorf("want 2 of 240, got %d of %d", page.Matched(), page.Examined())
	}
	if len(page.Records) != 2 {
		t.Fatalf("want the two records decoded, got %d", len(page.Records))
	}
	if page.Records[0].Offset() != 17 || page.Records[1].Offset() != 103 {
		t.Errorf("want the source offsets kept, got %d and %d",
			page.Records[0].Offset(), page.Records[1].Offset())
	}
	if string(page.Records[0].Payload()) != "matched" {
		t.Errorf("want the payload bytes kept, got %q", page.Records[0].Payload())
	}
	if !page.Records[0].Evaluation.Selected() {
		t.Error("a filtered page delivers records the filter selected")
	}
	if !page.Exhausted() {
		t.Error("a page at the end of what is visible is exhausted")
	}
	var request wire.FilteredPollRequest
	decodeFilterRequest(t, server, 0, &request)
	if request.Source.Topic != "events" || request.Source.Stream != "agents" {
		t.Errorf("want the source named, got %+v", request.Source)
	}
	if request.MaxExamined == nil || *request.MaxExamined != 240 {
		t.Errorf("want the examine budget sent, got %v", request.MaxExamined)
	}
	if server.codes[0] != wire.AgdxFilteredPollCode {
		t.Errorf("want the filtered poll code, got %d", server.codes[0])
	}
}

// A record the filter could not judge is passed through under the pass fault
// policy, and a reader that mistook it for a match would act on a record
// nothing selected.
func TestGivenAnUnevaluatedRecordWhenReadThenShouldSayItWasNotJudged(t *testing.T) {
	page := matchedPage([]uint64{17, 103}, 240, []byte("matched"))
	page.Unevaluated = []wire.RecordFault{{Offset: 103, Reason: wire.FaultMalformed}}
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome, Page: &page,
	}}

	read, err := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader")).
		Read(0, inlineFilter(t)).From(wire.StartAtFirst()).MaxExamined(240).
		Fetch(context.Background())
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if !read.Records[0].Evaluation.Selected() {
		t.Error("the first record was judged and matched")
	}
	if read.Records[1].Evaluation.Selected() {
		t.Error("the second record was never judged")
	}
	if read.Records[1].Evaluation.Reason != wire.FaultMalformed {
		t.Errorf("want the fault named, got %q", read.Records[1].Evaluation.Reason)
	}
}

// A fault reported against a record the page never carried would tell a reader
// something it cannot check, so it is refused.
func TestGivenAFaultForAnAbsentRecordWhenReadThenShouldRefuseThePage(t *testing.T) {
	page := matchedPage([]uint64{17}, 240, []byte("matched"))
	page.Unevaluated = []wire.RecordFault{{Offset: 999, Reason: wire.FaultMalformed}}
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome, Page: &page,
	}}

	_, err := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader")).
		Read(0, inlineFilter(t)).From(wire.StartAtFirst()).MaxExamined(240).
		Fetch(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}

// A page whose body does not hold what its header counted is one a reader
// cannot trust: acknowledging it would store progress past records never seen.
func TestGivenAPageThatMiscountsItsRecordsWhenReadThenShouldRefuseIt(t *testing.T) {
	page := matchedPage([]uint64{17}, 240, []byte("matched"))
	page.Matched = 5 // claims five, carries one
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome, Page: &page,
	}}

	_, err := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader")).
		Read(0, inlineFilter(t)).From(wire.StartAtFirst()).MaxExamined(240).
		Fetch(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}

// A page is refused when it does not answer what was asked for. Each of these
// would otherwise move a reader's stored offset past records it never saw.
func TestGivenAPageThatContradictsItsRequestWhenReadThenShouldRefuseIt(t *testing.T) {
	type contradiction struct {
		start   wire.FilteredStart
		corrupt func(*wire.FilteredPage)
	}
	fromFirst := func(corrupt func(*wire.FilteredPage)) contradiction {
		return contradiction{start: wire.StartAtFirst(), corrupt: corrupt}
	}

	for name, broken := range map[string]contradiction{
		"another partition": fromFirst(func(page *wire.FilteredPage) {
			page.PartitionID = 4
		}),
		"a record past the scan boundary": fromFirst(func(page *wire.FilteredPage) {
			beyond := uint64(5)
			page.NextScanOffset, page.SafeAckOffset = &beyond, ptr(beyond-1)
		}),
		"more matched than examined": fromFirst(func(page *wire.FilteredPage) {
			page.Examined = 1
		}),
		"more examined than the budget allowed": fromFirst(func(page *wire.FilteredPage) {
			page.Examined = 9000
		}),
		"a frontier behind its own records": fromFirst(func(page *wire.FilteredPage) {
			page.Frontier = 1
		}),
		"a frontier its own record body contradicts": fromFirst(func(page *wire.FilteredPage) {
			// The body says the partition head is somewhere else, so one of
			// the two is lying about how far the read reached.
			binary.LittleEndian.PutUint64(page.Records[4:12], 1_000_000)
		}),
		"another partition incarnation": fromFirst(func(page *wire.FilteredPage) {
			page.Generation.PartitionIncarnation = 99
		}),
		"another metadata operation": fromFirst(func(page *wire.FilteredPage) {
			page.AckContext.MetadataOp = 99
		}),
		"a start it was never asked to read from": fromFirst(func(page *wire.FilteredPage) {
			// Nothing was examined, so the only honest answer is the start
			// the request named. This one resumes somewhere else.
			page.Examined, page.Matched = 0, 0
			page.SafeAckOffset = nil
			page.NextScanOffset = ptr(uint64(77))
			page.Frontier = 0
			page.Records = pollReply(0, 0,
				iggcon.PartitionContext{Incarnation: 3, MetadataOp: 9}, nil, nil)
		}),
		"progress offered by a page that examined nothing": fromFirst(
			func(page *wire.FilteredPage) {
				// Storing this offset would pass records the server never
				// looked at.
				page.Examined = 0
				page.Matched = 0
				page.NextScanOffset = ptr(uint64(0))
				page.SafeAckOffset = ptr(uint64(239))
				page.Frontier = 0
				page.Records = pollReply(0, 0,
					iggcon.PartitionContext{Incarnation: 3, MetadataOp: 9}, nil, nil)
			}),
		"progress past what it says it scanned": fromFirst(func(page *wire.FilteredPage) {
			page.SafeAckOffset = ptr(*page.NextScanOffset + 50)
		}),
		"a group policy an independent consumer never joined": fromFirst(
			func(page *wire.FilteredPage) {
				page.Policy.GroupID = ptr(uint64(7))
			}),
		"a fault it claims to have scanned past": fromFirst(func(page *wire.FilteredPage) {
			page.Stop = wire.StopFault
			page.Fault = &wire.RecordFault{Offset: 1, Reason: wire.FaultMalformed}
		}),
		"a stop reason without the fault it names": fromFirst(func(page *wire.FilteredPage) {
			page.Stop = wire.StopFault
		}),
		"one record reported unevaluated twice": fromFirst(func(page *wire.FilteredPage) {
			page.Unevaluated = []wire.RecordFault{
				{Offset: 17, Reason: wire.FaultMalformed},
				{Offset: 17, Reason: wire.FaultTooLarge},
			}
		}),
		"a scan that examined records yet rewound before its start": {
			// The offset it offers to resume from is behind where the read
			// began, so the records in between would be examined twice.
			start: wire.StartFromOffset(500),
			corrupt: func(page *wire.FilteredPage) {
				page.Matched = 0
				page.Records = pollReply(0, *page.SafeAckOffset,
					iggcon.PartitionContext{Incarnation: 3, MetadataOp: 9}, nil, nil)
			},
		},
		"a scan boundary past the partition head it reported": {
			start: wire.StartAtFirst(),
			corrupt: func(page *wire.FilteredPage) {
				// Nothing matched, so only the boundary and the head are left
				// to disagree, and they do.
				page.Matched = 0
				page.Frontier = 1
				page.Records = pollReply(0, 1,
					iggcon.PartitionContext{Incarnation: 3, MetadataOp: 9}, nil, nil)
			},
		},
		"another op version": fromFirst(func(page *wire.FilteredPage) {
			page.V = wire.FilterOpVersion + 1
		}),
		"a generation for another partition": fromFirst(func(page *wire.FilteredPage) {
			page.Generation.PartitionID = 4
		}),
		"a record body from another partition": fromFirst(func(page *wire.FilteredPage) {
			binary.LittleEndian.PutUint32(page.Records[0:4], 4)
		}),
		"a filtered policy naming no filter": fromFirst(func(page *wire.FilteredPage) {
			// A page that ran a filter but will not say which cannot be
			// acknowledged: the store names the digest that ran.
			page.Policy.Digest = nil
		}),
		"a continuation of another filter digest": {
			start: wire.StartFromContinuation(wire.Continuation{
				NextScanOffset: 0, Mode: wire.ModeFiltered,
				Digest: ptr(wire.NewDigest32([32]byte{7})),
				Generation: wire.SourceGeneration{
					StreamID: 1, TopicID: 2, PartitionID: 0, PartitionIncarnation: 3,
				},
			}),
			corrupt: func(page *wire.FilteredPage) {},
		},
		"a continuation of a replaced source": {
			start: wire.StartFromContinuation(wire.Continuation{
				NextScanOffset: 0, Mode: wire.ModeFiltered, Digest: inlineDigest(),
				Generation: wire.SourceGeneration{
					StreamID: 1, TopicID: 2, PartitionID: 0, PartitionIncarnation: 99,
				},
			}),
			corrupt: func(page *wire.FilteredPage) {},
		},
		"a scan that went backwards from where it was asked to start": {
			// Reading on from here would leave the records between 50 and 500
			// examined twice, and a page that rewound further would skip them.
			start:   wire.StartFromOffset(500),
			corrupt: func(page *wire.FilteredPage) {},
		},
		"a continuation of another group": {
			start: wire.StartFromContinuation(wire.Continuation{
				NextScanOffset: 0, Mode: wire.ModeFiltered,
				Digest: inlineDigest(), GroupID: ptr(uint64(7)),
				Generation: wire.SourceGeneration{
					StreamID: 1, TopicID: 2, PartitionID: 0, PartitionIncarnation: 3,
				},
			}),
			corrupt: func(page *wire.FilteredPage) {},
		},
		"a continuation of an unfiltered read": {
			start: wire.StartFromContinuation(wire.Continuation{
				NextScanOffset: 0, Mode: wire.ModeUnfiltered, Digest: inlineDigest(),
				Generation: wire.SourceGeneration{
					StreamID: 1, TopicID: 2, PartitionID: 0, PartitionIncarnation: 3,
				},
			}),
			corrupt: func(page *wire.FilteredPage) {},
		},
		"a continuation of another policy generation": {
			start: wire.StartFromContinuation(wire.Continuation{
				NextScanOffset: 0, Mode: wire.ModeFiltered,
				Digest: inlineDigest(), PolicyGeneration: 4,
				Generation: wire.SourceGeneration{
					StreamID: 1, TopicID: 2, PartitionID: 0, PartitionIncarnation: 3,
				},
			}),
			corrupt: func(page *wire.FilteredPage) {},
		},
	} {
		t.Run(name, func(t *testing.T) {
			page := matchedPage([]uint64{17, 103}, 240, []byte("matched"))
			broken.corrupt(&page)
			server := &filterServer{outcome: wire.FilterOutcome{
				Kind: wire.FilterPageOutcome, Page: &page,
			}}

			_, err := filterHandle(server, laser.Options{}).Topic("events").
				Filters(wire.IndependentConsumer("reader")).
				Read(0, inlineFilter(t)).From(broken.start).MaxExamined(240).
				Fetch(context.Background())

			var codec *laser.CodecError
			if !errors.As(err, &codec) {
				t.Fatalf("want a CodecError, got %T: %v", err, err)
			}
		})
	}
}

// A group read is refused when the page answers for another group or another
// policy, because a member storing it would pass records its group never saw.
func TestGivenAGroupPageThatAnswersAnotherPolicyWhenReadThenShouldRefuseIt(t *testing.T) {
	type contradiction struct {
		consumer wire.FilterConsumer
		filter   wire.FilterRef
		corrupt  func(*wire.FilteredPage)
	}

	for name, broken := range map[string]contradiction{
		"another group id": {
			consumer: wire.GroupByID(5),
			filter:   wire.BoundRevision(),
			corrupt: func(page *wire.FilteredPage) {
				page.Policy.GroupID = ptr(uint64(7))
			},
		},
		"no group at all": {
			consumer: wire.NamedGroup("readers"),
			filter:   wire.BoundRevision(),
			corrupt: func(page *wire.FilteredPage) {
				page.Policy.GroupID = nil
			},
		},
		"an unfiltered page for a read of the bound revision": {
			consumer: wire.NamedGroup("readers"),
			filter:   wire.BoundRevision(),
			corrupt: func(page *wire.FilteredPage) {
				// A read of the binding cannot be answered with every record:
				// the binding is what was asked to run.
				page.Policy = wire.AppliedPolicy{
					Mode: wire.ModeUnfiltered, GroupID: ptr(uint64(7)),
				}
				page.Examined = page.Matched
			},
		},
		"an unfiltered page that selected records out": {
			consumer: wire.NamedGroup("readers"),
			filter:   wire.GroupPolicy(),
			corrupt: func(page *wire.FilteredPage) {
				page.Policy = wire.AppliedPolicy{
					Mode: wire.ModeUnfiltered, GroupID: ptr(uint64(7)),
				}
			},
		},
		"an unfiltered page still naming a revision": {
			consumer: wire.NamedGroup("readers"),
			filter:   wire.GroupPolicy(),
			corrupt: func(page *wire.FilteredPage) {
				page.Policy = wire.AppliedPolicy{
					Mode: wire.ModeUnfiltered, GroupID: ptr(uint64(7)),
					FilterID: ptr(uint32(4)), Revision: ptr(uint32(1)),
				}
				page.Examined = page.Matched
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			page := matchedPage([]uint64{17, 103}, 240, []byte("matched"))
			page.Policy.GroupID = ptr(uint64(5))
			broken.corrupt(&page)
			server := &filterServer{outcome: wire.FilterOutcome{
				Kind: wire.FilterPageOutcome, Page: &page,
			}}

			_, err := filterHandle(server, laser.Options{}).Topic("events").
				Filters(broken.consumer).
				Read(0, broken.filter).From(wire.StartAtFirst()).MaxExamined(240).
				Fetch(context.Background())

			var codec *laser.CodecError
			if !errors.As(err, &codec) {
				t.Fatalf("want a CodecError, got %T: %v", err, err)
			}
		})
	}
}

// A record body whose framing does not hold together is refused by the walk
// that precedes the decode, rather than by the decoder.
//
// Which layer refuses it is the point: the walk runs before anything is
// decoded or allocated from a body the server controls, so these cases assert
// the framing refusal by name rather than only that something failed.
func TestGivenABrokenRecordBodyWhenReadThenShouldRefuseItBeforeDecoding(t *testing.T) {
	const prefix = 16 + iggcon.PartitionContextSize
	const frame = prefix + 256

	for name, corrupt := range map[string]func(*wire.FilteredPage){
		"a body shorter than its own head": func(page *wire.FilteredPage) {
			page.Records = page.Records[:8]
		},
		"a body with no room for the batch it promises": func(page *wire.FilteredPage) {
			page.Records = page.Records[:prefix+8]
		},
		"a batch longer than the body holding it": func(page *wire.FilteredPage) {
			binary.LittleEndian.PutUint64(page.Records[prefix+32:prefix+40], 1<<40)
		},
		"a batch shorter than its own header": func(page *wire.FilteredPage) {
			binary.LittleEndian.PutUint64(page.Records[prefix+32:prefix+40], 8)
		},
		"a batch counting more frames than it carries": func(page *wire.FilteredPage) {
			binary.LittleEndian.PutUint32(page.Records[prefix+48:prefix+52], 2)
		},
		"a batch counting fewer frames than it carries": func(page *wire.FilteredPage) {
			binary.LittleEndian.PutUint32(page.Records[prefix+48:prefix+52], 0)
		},
		"a frame claiming far more payload than its batch holds": func(page *wire.FilteredPage) {
			binary.LittleEndian.PutUint32(page.Records[frame+36:frame+40], 1<<30)
		},
		"a frame one byte longer than its batch holds": func(page *wire.FilteredPage) {
			binary.LittleEndian.PutUint32(page.Records[frame+36:frame+40],
				uint32(len("matched")+1))
		},
		"a batch with no room left for the frame it promises": func(page *wire.FilteredPage) {
			// The payload shrinks to nothing, so the tail of the batch is too
			// short to hold another frame header.
			binary.LittleEndian.PutUint32(page.Records[frame+36:frame+40], 0)
		},
		"two batches whose frame counts wrap around": func(page *wire.FilteredPage) {
			// Summed without a bound the two counts wrap to the one count the
			// page declares, so the sum is guarded rather than trusted.
			binary.LittleEndian.PutUint32(page.Records[prefix+48:prefix+52], ^uint32(0))
			second := make([]byte, 256)
			binary.LittleEndian.PutUint64(second[32:40], 256)
			binary.LittleEndian.PutUint32(second[48:52], 2)
			page.Records = append(page.Records, second...)
		},
		"a frame whose offset delta runs off the end of the log": func(page *wire.FilteredPage) {
			binary.LittleEndian.PutUint64(page.Records[prefix+8:prefix+16], ^uint64(0))
			binary.LittleEndian.PutUint32(page.Records[frame+24:frame+28], 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			page := matchedPage([]uint64{17}, 240, []byte("matched"))
			corrupt(&page)
			server := &filterServer{outcome: wire.FilterOutcome{
				Kind: wire.FilterPageOutcome, Page: &page,
			}}

			_, err := filterHandle(server, laser.Options{}).Topic("events").
				Filters(wire.IndependentConsumer("reader")).
				Read(0, inlineFilter(t)).From(wire.StartAtFirst()).MaxExamined(240).
				Fetch(context.Background())

			var codec *laser.CodecError
			if !errors.As(err, &codec) {
				t.Fatalf("want a CodecError, got %T: %v", err, err)
			}
			if codec.Reason != "the filtered record body has invalid framing or counts" {
				t.Errorf("want the framing walk to refuse it, got %q", codec.Reason)
			}
		})
	}
}

// The count in the record body's head is what a decoder sizes its allocation
// against, so a forged one is caught before anything is decoded from it.
func TestGivenAForgedRecordCountWhenReadThenShouldRefuseItBeforeDecoding(t *testing.T) {
	page := matchedPage([]uint64{17}, 240, []byte("matched"))
	binary.LittleEndian.PutUint32(page.Records[12:16], 1<<30)
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome, Page: &page,
	}}

	_, err := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader")).
		Read(0, inlineFilter(t)).From(wire.StartAtFirst()).MaxExamined(240).
		Fetch(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}

// A page carrying more records than were asked for is refused before anything
// is decoded from it, because the count in its head is what a decoder sizes
// its allocation against.
func TestGivenMoreRecordsThanAskedForWhenReadThenShouldRefuseItBeforeDecoding(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome,
		Page: ptr(matchedPage([]uint64{17, 103}, 240, []byte("matched"))),
	}}

	_, err := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader")).
		Read(0, inlineFilter(t)).From(wire.StartAtFirst()).Count(1).MaxExamined(240).
		Fetch(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}

// A continuation resumes exactly where the last page stopped, so a reader that
// keeps fetching walks the partition rather than re-reading its head.
func TestGivenAFetchedPageWhenFetchedAgainThenShouldContinueWhereItStopped(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome,
		Page: ptr(matchedPage([]uint64{17}, 240, []byte("matched"))),
	}}
	read := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader")).
		Read(0, inlineFilter(t)).From(wire.StartAtFirst()).MaxExamined(240)

	if _, err := read.Fetch(context.Background()); err != nil {
		t.Fatalf("read: %v", err)
	}
	// The second read is refused by the page checks, because the fake answers
	// the same page: what matters is the request it sent.
	_, _ = read.Fetch(context.Background())

	var again wire.FilteredPollRequest
	decodeFilterRequest(t, server, 1, &again)
	if again.Start.Kind != wire.StartContinue || again.Start.Continuation == nil {
		t.Fatalf("want a continuation, got %+v", again.Start)
	}
	if again.Start.Continuation.NextScanOffset != 240 {
		t.Errorf("want the scan to continue at 240, got %d",
			again.Start.Continuation.NextScanOffset)
	}
}

// A filtered reader that stored only its matches would re-examine everything
// the filter rejected, so progress is stored through the safe offset.
func TestGivenAPageWhenAcknowledgedThenShouldStoreThroughTheSafeOffset(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterAcknowledged, Receipt: &wire.AckReceipt{},
	}}
	filters := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader"))
	safe := uint64(239)

	page := &laser.MatchedPage{PartitionID: 0, Page: wire.FilteredPage{
		SafeAckOffset: &safe,
		Policy:        wire.AppliedPolicy{Mode: wire.ModeFiltered, PolicyGeneration: 7},
	}}
	if err := filters.Ack(context.Background(), page); err != nil {
		t.Fatalf("ack: %v", err)
	}

	var acknowledgment wire.FilteredAck
	decodeFilterRequest(t, server, 0, &acknowledgment)
	if acknowledgment.Offset != 239 {
		t.Errorf("want the safe offset stored, got %d", acknowledgment.Offset)
	}
	// The server refuses the acknowledgment when the group's generation moved
	// on, so it has to carry the one the page ran under.
	if acknowledgment.PolicyGeneration != 7 {
		t.Errorf("want the policy generation carried, got %d",
			acknowledgment.PolicyGeneration)
	}
}

// A reader acknowledging a page that examined nothing would move its offset
// past records the server never looked at.
func TestGivenAPageThatExaminedNothingWhenAcknowledgedThenShouldStoreNothing(t *testing.T) {
	server := &filterServer{}
	filters := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader"))

	if err := filters.Ack(context.Background(),
		&laser.MatchedPage{Page: wire.FilteredPage{}}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if len(server.codes) != 0 {
		t.Error("there was no progress to store, so nothing must be sent")
	}
}

// Filtered delivery is served by the streaming server, so it is gated on the
// native capability rather than on a managed backend.
func TestGivenNoNativeFiltersWhenReadingThenShouldRefuseIt(t *testing.T) {
	capabilities := filterCapabilities()
	capabilities.Filters.Native = false
	server := &filterServer{}

	_, err := filterHandle(server, laser.Options{Capabilities: capabilities}).
		Topic("events").Filters(wire.IndependentConsumer("reader")).
		Read(0, inlineFilter(t)).Fetch(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported read must not reach the socket")
	}
}

// A deployment can filter without saving anything, so the catalog is gated
// separately from filtered reading.
func TestGivenNoCatalogWhenRegisteringThenShouldRefuseIt(t *testing.T) {
	capabilities := filterCapabilities()
	capabilities.Filters.Catalog = false
	server := &filterServer{}

	_, _, err := filterHandle(server, laser.Options{Capabilities: capabilities}).
		FilterCatalog().Register(context.Background(), "tenant-acme", "", inlineDefinition())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
}

// A catalog speaking another op version would answer a shape this client
// cannot read, and a change half-understood is worse than one refused.
func TestGivenACatalogOfAnotherOpVersionWhenUsedThenShouldRefuseIt(t *testing.T) {
	capabilities := filterCapabilities()
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion).
		WithFilter(wire.FilterOpVersion + 1)
	capabilities.Versions = &versions
	server := &filterServer{}

	_, _, err := filterHandle(server, laser.Options{Capabilities: capabilities}).
		FilterCatalog().Register(context.Background(), "tenant-acme", "", inlineDefinition())

	var skew *laser.VersionError
	if !errors.As(err, &skew) {
		t.Fatalf("want a VersionError, got %T: %v", err, err)
	}
}

// A preview says what a filter would select without joining a group or storing
// progress, which is what makes a filter checkable before anything reads it.
func TestGivenAPreviewWhenSentThenShouldCarryItsBoundsAndExplanation(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPreviewOutcome,
		Preview: &wire.FilterPreview{
			V: wire.FilterOpVersion, Examined: 50, Matched: 2,
			Stop:   wire.StopBudget,
			Policy: wire.AppliedPolicy{Mode: wire.ModeFiltered, Digest: inlineDigest()},
		},
	}}
	filters := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader"))

	preview, err := filters.Preview(0, inlineFilter(t)).
		FromOffset(100).MaxExamined(50).MaxRecords(5).Explain().
		Send(context.Background())
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if preview.Matched != 2 || preview.Examined != 50 {
		t.Errorf("want 2 of 50, got %d of %d", preview.Matched, preview.Examined)
	}

	var request wire.FilterPreviewRequest
	decodeFilterRequest(t, server, 0, &request)
	if request.FromOffset != 100 || request.MaxExamined != 50 || request.MaxRecords != 5 {
		t.Errorf("want the bounds sent, got %+v", request)
	}
	if !request.Explain {
		t.Error("want the explanation asked for")
	}
}

// A revision is conditional on which is currently latest, so two
// administrators cannot both believe they built on the other's work.
func TestGivenARevisionWhenSentThenShouldCarryItsExpectedRevision(t *testing.T) {
	server := &filterServer{catalog: appliedResult(wire.FilterMutationResult{
		Kind:        wire.ResultRevised,
		RevisionRef: &wire.FilterRevisionRef{FilterID: 4, Revision: 3},
	})}
	catalog := filterHandle(server, laser.Options{}).FilterCatalog()

	revision, position, err := catalog.Revise(
		context.Background(), 4, 2, inlineDefinition())
	if err != nil {
		t.Fatalf("revise: %v", err)
	}
	if revision.FilterID != 4 || revision.Revision != 3 {
		t.Errorf("want revision 4/3 back, got %+v", revision)
	}
	// The position is what a group read waits for, so a caller that just
	// revised can read its own write through any node.
	if position == nil || position.Offset != 11 {
		t.Errorf("want the applied position, got %+v", position)
	}

	var request wire.FilterMutationRequest
	decodeFilterRequest(t, server, 0, &request)
	if request.Mutation.Kind != wire.MutateRevise {
		t.Errorf("want a revise, got %d", request.Mutation.Kind)
	}
	if request.Mutation.FilterID != 4 || request.Mutation.ExpectedRevision != 2 {
		t.Errorf("want the condition carried, got %+v", request.Mutation)
	}
	// A catalog change is applied once however many times it is retried.
	if request.OperationID.IsZero() {
		t.Error("want an operation identity")
	}
}

// Releasing a group that was moved to another revision in between would
// silently undo the move, so an unbind is conditional on the digest.
func TestGivenAnUnbindWhenSentThenShouldCarryTheExpectedDigest(t *testing.T) {
	digest := wire.NewDigest32([32]byte{1, 2, 3})
	server := &filterServer{catalog: appliedResult(wire.FilterMutationResult{
		Kind: wire.ResultUnbound, Binding: &wire.FilterBinding{Digest: digest},
	})}
	catalog := filterHandle(server, laser.Options{}).FilterCatalog()

	binding, _, err := catalog.Unbind(context.Background(), wire.FilterGroupRef{}, digest)
	if err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if !bytes.Equal(binding.Digest.Bytes, digest.Bytes) {
		t.Error("want the released binding back")
	}

	var request wire.FilterMutationRequest
	decodeFilterRequest(t, server, 0, &request)
	if request.Mutation.ExpectedDigest == nil ||
		!bytes.Equal(request.Mutation.ExpectedDigest.Bytes, digest.Bytes) {
		t.Errorf("want the digest carried, got %v", request.Mutation.ExpectedDigest)
	}
}

// A release that named only the group would release whichever group now holds
// the name, so an exact release names the incarnation too.
func TestGivenAReleaseOfOneBindingWhenSentThenShouldNameItsIncarnation(t *testing.T) {
	identity := wire.FilterGroupIdentity{StreamID: 1, TopicID: 2, GroupID: 7}
	held := wire.FilterBinding{
		Group:    wire.FilterGroupRef{Stream: "agents", Topic: "events", Group: "readers"},
		Identity: identity,
		Digest:   wire.NewDigest32([32]byte{9}),
	}
	server := &filterServer{catalog: appliedResult(wire.FilterMutationResult{
		Kind: wire.ResultUnbound, Binding: &held,
	})}

	if _, _, err := filterHandle(server, laser.Options{}).FilterCatalog().
		ReleaseBinding(context.Background(), held); err != nil {
		t.Fatalf("release: %v", err)
	}

	var request wire.FilterMutationRequest
	decodeFilterRequest(t, server, 0, &request)
	if request.Mutation.ExpectedIdentity == nil ||
		*request.Mutation.ExpectedIdentity != identity {
		t.Errorf("want the incarnation carried, got %v", request.Mutation.ExpectedIdentity)
	}
}

// A change whose reply was lost is repeated under the same identity, so the
// catalog applies it once however many attempts reach it.
func TestGivenARetryableRefusalWhenMutatingThenShouldRepeatOneIdentity(t *testing.T) {
	server := &filterServer{
		refusals: 2,
		catalog: appliedResult(wire.FilterMutationResult{
			Kind: wire.ResultArchived, FilterID: 4,
		}),
	}

	if _, err := filterHandle(server, laser.Options{}).FilterCatalog().
		Archive(context.Background(), 4); err != nil {
		t.Fatalf("archive: %v", err)
	}

	if len(server.codes) != 3 {
		t.Fatalf("want three attempts, got %d", len(server.codes))
	}
	var first, last wire.FilterMutationRequest
	decodeFilterRequest(t, server, 0, &first)
	decodeFilterRequest(t, server, 2, &last)
	if first.OperationID != last.OperationID {
		t.Error("every attempt of one change must carry one identity")
	}
}

// A change that keeps failing has an unknown outcome rather than a known one,
// because an attempt whose reply was lost may still have applied.
func TestGivenEveryAttemptRefusedWhenMutatingThenShouldReportItAmbiguous(t *testing.T) {
	server := &filterServer{refusals: 99, catalog: &wire.FilterCatalogOutcome{}}

	_, err := filterHandle(server, laser.Options{}).FilterCatalog().
		Archive(context.Background(), 4)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an AmbiguousMutationError, got %T: %v", err, err)
	}
	if ambiguous.OperationID.IsZero() {
		t.Error("the identity is what makes recovery possible, so it must be named")
	}
}

// A change the catalog refused outright is a decision, not an unknown: the
// caller must not repeat it.
func TestGivenARejectedMutationWhenSettledThenShouldReportTheRefusal(t *testing.T) {
	server := &filterServer{catalog: &wire.FilterCatalogOutcome{
		Kind: wire.CatalogMutation,
		Mutation: &wire.FilterMutationOutcome{
			V: wire.FilterOpVersion, Status: wire.FilterMutationStatus{
				Kind: wire.StatusRejected,
				Rejected: &wire.FilterError{
					Code:    wire.ReasonConflict.Code(),
					Reason:  wire.ReasonConflict,
					Message: "another revision is latest",
				},
			},
		},
	}}

	_, _, err := filterHandle(server, laser.Options{}).FilterCatalog().
		Revise(context.Background(), 4, 2, inlineDefinition())

	var failure *laser.FilterError
	if !errors.As(err, &failure) {
		t.Fatalf("want a FilterError, got %T: %v", err, err)
	}
	if failure.Reason() != wire.ReasonConflict {
		t.Errorf("want the conflict reason, got %q", failure.Reason())
	}
}

// A change that entered the control log but has not been folded is read back
// rather than sent again, because sending again would apply it twice.
func TestGivenAPendingMutationWhenAppliedThenShouldReadItsOutcomeBack(t *testing.T) {
	server := &pendingServer{applied: wire.FilterMutationResult{
		Kind: wire.ResultArchived, FilterID: 4,
	}}

	position, err := filterHandle(server, laser.Options{}).FilterCatalog().
		Archive(context.Background(), 4)
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if position == nil {
		t.Error("the folded outcome names where it applied")
	}

	if server.calls() != 2 {
		t.Fatalf("want a mutate and one read back, got %d calls", server.calls())
	}
	if server.code(1) != wire.AgdxFilterOperationCode {
		t.Errorf("want the outcome read, got code %d", server.code(1))
	}
}

// A mutation nobody ever folds is ambiguous rather than lost: it may still
// apply, so the caller is told to recover rather than to repeat.
func TestGivenAMutationThatIsNeverFoldedWhenAppliedThenShouldGiveUpAmbiguously(t *testing.T) {
	server := &pendingServer{never: true}
	catalog := filterHandle(server, laser.Options{}).FilterCatalog().
		WaitingFor(50 * time.Millisecond)

	_, err := catalog.Archive(context.Background(), 4)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an AmbiguousMutationError, got %T: %v", err, err)
	}
}

// A caller whose reply was lost reads the outcome rather than reapplying the
// change, which is what makes a catalog mutation safe to retry.
func TestGivenALostReplyWhenTheOperationIsReadThenShouldNotReapply(t *testing.T) {
	operation := laser.NewOperationID()
	server := &filterServer{catalog: &wire.FilterCatalogOutcome{
		Kind: wire.CatalogMutation,
		Mutation: &wire.FilterMutationOutcome{
			V: wire.FilterOpVersion, OperationID: operation,
			Status: wire.FilterMutationStatus{Kind: wire.StatusPending},
		},
	}}
	catalog := filterHandle(server, laser.Options{}).FilterCatalog()

	outcome, err := catalog.Operation(context.Background(), operation)
	if err != nil {
		t.Fatalf("operation: %v", err)
	}
	if outcome.Status.Kind != wire.StatusPending {
		t.Error("a mutation the catalog has not folded is still pending")
	}

	if server.codes[0] != wire.AgdxFilterOperationCode {
		t.Errorf("want the operation read, got code %d", server.codes[0])
	}
	var request wire.GetFilterOperation
	decodeFilterRequest(t, server, 0, &request)
	if request.OperationID != operation {
		t.Error("want the operation named")
	}
}

// An outcome for another operation would let a caller act on someone else's
// change, so every identity it carries must be the one asked about.
func TestGivenAFilterOutcomeForAnotherOperationWhenReadThenShouldRefuseIt(t *testing.T) {
	asked := laser.NewOperationID()
	for name, outcome := range map[string]*wire.FilterMutationOutcome{
		"another operation id": {
			V: wire.FilterOpVersion, OperationID: laser.NewOperationID(),
			Status: wire.FilterMutationStatus{Kind: wire.StatusPending},
		},
		"another op version": {
			V: wire.FilterOpVersion + 1, OperationID: asked,
			Status: wire.FilterMutationStatus{Kind: wire.StatusPending},
		},
		"a position for another operation": {
			V: wire.FilterOpVersion, OperationID: asked,
			Status: wire.FilterMutationStatus{Kind: wire.StatusPending},
			CatalogPosition: &wire.CatalogPosition{
				Offset: 4, OperationID: ptr(laser.NewOperationID()),
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := &filterServer{verbatim: true, catalog: &wire.FilterCatalogOutcome{
				Kind: wire.CatalogMutation, Mutation: outcome,
			}}

			_, err := filterHandle(server, laser.Options{}).FilterCatalog().
				Operation(context.Background(), asked)

			var codec *laser.CodecError
			if !errors.As(err, &codec) {
				t.Fatalf("want a CodecError, got %T: %v", err, err)
			}
		})
	}
}

// A listing is paged, because a deployment with thousands of saved filters
// would otherwise answer with all of them.
func TestGivenAFilterListingWhenReadThenShouldCarryItsPageAndNarrowing(t *testing.T) {
	server := &filterServer{catalog: &wire.FilterCatalogOutcome{
		Kind:    wire.CatalogFilters,
		Filters: &wire.FilterPage{Page: 2, TotalPages: 5, TotalResults: 97},
	}}
	name := "tenant"
	state := wire.FilterActive

	page, err := filterHandle(server, laser.Options{}).FilterCatalog().Filters(
		context.Background(),
		laser.FilterQuery{NameContains: &name, State: &state}, 2, 20)
	if err != nil {
		t.Fatalf("filters: %v", err)
	}
	if page.TotalResults != 97 {
		t.Errorf("want the total reported, got %d", page.TotalResults)
	}

	var request wire.ListFilters
	decodeFilterRequest(t, server, 0, &request)
	if request.Page != 2 || request.Results != 20 {
		t.Errorf("want page 2 of 20, got %d of %d", request.Page, request.Results)
	}
	if request.NameContains == nil || *request.NameContains != "tenant" {
		t.Errorf("want the name narrowing sent, got %v", request.NameContains)
	}
	if request.State == nil || *request.State != wire.FilterActive {
		t.Errorf("want the state narrowing sent, got %v", request.State)
	}
}

// A group's own filter is the one configuring it saved, which is what
// distinguishes a group's policy from a shared filter it is merely bound to.
func TestGivenAGroupsOwnFilterWhenLookedUpThenShouldMatchItsIncarnation(t *testing.T) {
	owner := wire.FilterGroupIdentity{StreamID: 1, TopicID: 2, GroupID: 7}
	other := wire.FilterGroupIdentity{StreamID: 1, TopicID: 2, GroupID: 8}
	server := &filterServer{catalog: &wire.FilterCatalogOutcome{
		Kind: wire.CatalogFilters,
		Filters: &wire.FilterPage{Items: []wire.FilterSummary{
			{ID: 9, State: wire.FilterActive, Owner: &wire.FilterOwner{Identity: other}},
		}},
	}}
	catalog := filterHandle(server, laser.Options{}).FilterCatalog()

	// A listing that answered with another group's filter must not be taken
	// for this group's own.
	summary, err := catalog.OwnFilter(context.Background(), owner)
	if err != nil {
		t.Fatalf("own filter: %v", err)
	}
	if summary != nil {
		t.Errorf("want no own filter, got %+v", summary)
	}

	var request wire.ListFilters
	decodeFilterRequest(t, server, 0, &request)
	if request.Owner == nil || *request.Owner != owner {
		t.Errorf("want the owner narrowing sent, got %v", request.Owner)
	}
}

func TestGivenAFilterFailureWhenReportedThenShouldCarryItsReason(t *testing.T) {
	server := &filterServer{failure: &wire.FilterError{
		Code:    wire.ReasonNotFound.Code(),
		Reason:  wire.ReasonNotFound,
		Message: "no such filter",
	}}
	filters := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader"))

	_, err := filters.Read(0, inlineFilter(t)).Fetch(context.Background())

	var failure *laser.FilterError
	if !errors.As(err, &failure) {
		t.Fatalf("want a FilterError, got %T: %v", err, err)
	}
	if failure.Reason() != wire.ReasonNotFound {
		t.Errorf("want the not-found reason, got %q", failure.Reason())
	}
	if !failure.NotFound() || failure.Retryable() {
		t.Error("a missing filter is not found, and repeating the read will not find it")
	}
	// A filter failure branches the same way as every other managed failure.
	if failure.ResultCode().String() == "" {
		t.Error("want a shared result code")
	}
}

// pendingServer answers a mutation as pending, then folds it on the read back.
type pendingServer struct {
	clusterOfOne
	applied wire.FilterMutationResult
	never   bool
	// missing answers the first read back as not found, the way a catalog
	// that has not folded the change yet does.
	missing bool
	// A caller can watch the calls from another goroutine while the catalog
	// is still polling, so the record is guarded.
	guard sync.Mutex
	codes []uint32
}

// calls is the commands this fake has answered so far.
func (server *pendingServer) calls() int {
	server.guard.Lock()
	defer server.guard.Unlock()
	return len(server.codes)
}

// code is the nth command this fake answered.
func (server *pendingServer) code(index int) uint32 {
	server.guard.Lock()
	defer server.guard.Unlock()
	return server.codes[index]
}

func (server *pendingServer) Close() error { return nil }

func (server *pendingServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.guard.Lock()
	server.codes = append(server.codes, code)
	server.guard.Unlock()
	var envelope wire.ManagedRequestEnvelope
	body := payload
	if wire.IsIdempotentManagedRequest(code) {
		if err := wire.Unmarshal(payload, &envelope); err != nil {
			return nil, err
		}
		body = envelope.Payload
	}
	operation := wire.U128{}
	if code == wire.AgdxFilterMutateCode {
		var request wire.FilterMutationRequest
		if err := wire.Unmarshal(body, &request); err != nil {
			return nil, err
		}
		operation = request.OperationID
	} else {
		var request wire.GetFilterOperation
		if err := wire.Unmarshal(body, &request); err != nil {
			return nil, err
		}
		operation = request.OperationID
	}
	outcome := wire.FilterMutationOutcome{
		V: wire.FilterOpVersion, OperationID: operation,
		Status: wire.FilterMutationStatus{Kind: wire.StatusPending},
	}
	if code != wire.AgdxFilterMutateCode && server.missing && server.calls() < 3 {
		return wire.Marshal(wire.FilterCatalogReply{Err: &wire.FilterError{
			Code:    wire.ReasonNotFound.Code(),
			Reason:  wire.ReasonNotFound,
			Message: "no outcome for that operation yet",
		}})
	}
	if code != wire.AgdxFilterMutateCode && !server.never {
		applied := server.applied
		outcome.Status = wire.FilterMutationStatus{
			Kind: wire.StatusApplied, Applied: &applied,
		}
		outcome.CatalogPosition = &wire.CatalogPosition{Offset: 11, OperationID: &operation}
	}
	return wire.Marshal(wire.FilterCatalogReply{Outcome: &wire.FilterCatalogOutcome{
		Kind: wire.CatalogMutation, Mutation: &outcome,
	}})
}

// appliedResult answers every mutation as applied with one result.
func appliedResult(result wire.FilterMutationResult) *wire.FilterCatalogOutcome {
	return &wire.FilterCatalogOutcome{
		Kind: wire.CatalogMutation,
		Mutation: &wire.FilterMutationOutcome{
			V: wire.FilterOpVersion,
			Status: wire.FilterMutationStatus{
				Kind: wire.StatusApplied, Applied: &result,
			},
			CatalogPosition: &wire.CatalogPosition{Offset: 11},
		},
	}
}

func inlineDefinition() wire.ConsumerFilter {
	return wire.ConsumerFilter{
		V:                wire.FilterOpVersion,
		EvaluatorVersion: wire.FilterEvaluatorVersion,
		Expr:             wire.ExprHeaderPredicate("tenant", wire.CmpEq, wire.StringTyped("acme")),
		Codec:            wire.CodecHeadersOnly,
		FaultPolicy:      wire.FaultStop,
	}
}

func inlineFilter(t *testing.T) wire.FilterRef {
	t.Helper()
	return wire.InlineFilter(inlineDefinition())
}

func ptr[T any](value T) *T { return &value }

// Every catalog verb sends the change it names, under an operation identity,
// and reads back the result that change produces. A verb wired to the wrong
// mutation would silently administer something else.
func TestGivenEachCatalogVerbWhenSentThenShouldCarryItsOwnMutation(t *testing.T) {
	group := wire.FilterGroupRef{Stream: "agents", Topic: "events", Group: "readers"}
	binding := wire.FilterBinding{Group: group, Digest: wire.NewDigest32([32]byte{4})}

	for name, verb := range map[string]struct {
		result wire.FilterMutationResult
		send   func(*laser.FilterCatalog) error
		want   wire.FilterMutationKind
	}{
		"register": {
			result: wire.FilterMutationResult{
				Kind:        wire.ResultRegistered,
				RevisionRef: &wire.FilterRevisionRef{FilterID: 1, Revision: 1},
			},
			want: wire.MutateRegister,
			send: func(catalog *laser.FilterCatalog) error {
				_, _, err := catalog.Register(context.Background(),
					"tenant-acme", "acme only", inlineDefinition())
				return err
			},
		},
		"describe": {
			result: wire.FilterMutationResult{Kind: wire.ResultDescribed, FilterID: 4},
			want:   wire.MutateDescribe,
			send: func(catalog *laser.FilterCatalog) error {
				_, err := catalog.Describe(context.Background(), 4, "now acme and beta")
				return err
			},
		},
		"set revision enabled": {
			result: wire.FilterMutationResult{
				Kind: wire.ResultRevisionState, FilterID: 4, Revision: 2, Enabled: false,
			},
			want: wire.MutateSetRevisionEnabled,
			send: func(catalog *laser.FilterCatalog) error {
				_, err := catalog.SetRevisionEnabled(context.Background(), 4, 2, false)
				return err
			},
		},
		"drop": {
			result: wire.FilterMutationResult{Kind: wire.ResultDropped, FilterID: 4},
			want:   wire.MutateDrop,
			send: func(catalog *laser.FilterCatalog) error {
				_, err := catalog.Drop(context.Background(), 4)
				return err
			},
		},
		"bind": {
			result: wire.FilterMutationResult{Kind: wire.ResultBound, Binding: &binding},
			want:   wire.MutateBind,
			send: func(catalog *laser.FilterCatalog) error {
				_, _, err := catalog.Bind(context.Background(), group, 4, 2)
				return err
			},
		},
		"configure group": {
			result: wire.FilterMutationResult{Kind: wire.ResultBound, Binding: &binding},
			want:   wire.MutateConfigureGroup,
			send: func(catalog *laser.FilterCatalog) error {
				_, _, err := catalog.ConfigureGroup(context.Background(), group,
					wire.DefinitionSpec("tenant-acme", inlineDefinition()))
				return err
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := &filterServer{catalog: appliedResult(verb.result)}

			if err := verb.send(
				filterHandle(server, laser.Options{}).FilterCatalog()); err != nil {
				t.Fatalf("%s: %v", name, err)
			}

			if server.codes[0] != wire.AgdxFilterMutateCode {
				t.Errorf("want the mutate code, got %d", server.codes[0])
			}
			var request wire.FilterMutationRequest
			decodeFilterRequest(t, server, 0, &request)
			if request.Mutation.Kind != verb.want {
				t.Errorf("want mutation %d, got %d", verb.want, request.Mutation.Kind)
			}
			if request.OperationID.IsZero() {
				t.Error("every catalog change carries an identity")
			}
		})
	}
}

// A verb that accepted any result would report a change the catalog never
// made, so each one holds the reply to the result it asked for.
func TestGivenAnotherResultWhenAVerbSettlesThenShouldRefuseIt(t *testing.T) {
	server := &filterServer{catalog: appliedResult(wire.FilterMutationResult{
		Kind: wire.ResultDropped, FilterID: 4,
	})}
	catalog := filterHandle(server, laser.Options{}).FilterCatalog()

	if _, err := catalog.Archive(context.Background(), 4); err == nil {
		t.Error("an archive answered with a drop must be refused")
	}
	if _, _, err := catalog.Bind(
		context.Background(), wire.FilterGroupRef{}, 4, 1); err == nil {
		t.Error("a bind answered with a drop must be refused")
	}
	if _, _, err := catalog.Register(
		context.Background(), "n", "", inlineDefinition()); err == nil {
		t.Error("a register answered with a drop must be refused")
	}
}

// Every catalog read sends its own command, and a reply of another shape is
// refused rather than read as the one asked for.
func TestGivenEachCatalogReadWhenSentThenShouldSendItsOwnCommand(t *testing.T) {
	for name, read := range map[string]struct {
		outcome *wire.FilterCatalogOutcome
		send    func(*laser.FilterCatalog) error
		code    uint32
	}{
		"one filter": {
			outcome: &wire.FilterCatalogOutcome{
				Kind: wire.CatalogFilter,
				Filter: &wire.FilterDetail{
					Summary: wire.FilterSummary{ID: 4, State: wire.FilterActive},
					Latest: wire.FilterRevisionInfo{
						Enabled: true, Revision: 1, Filter: inlineDefinition(),
					},
				},
			},
			code: wire.AgdxGetFilterCode,
			send: func(catalog *laser.FilterCatalog) error {
				_, err := catalog.Filter(context.Background(), 4)
				return err
			},
		},
		"its revisions": {
			outcome: &wire.FilterCatalogOutcome{
				Kind: wire.CatalogRevisions, Revisions: &wire.FilterRevisionPage{Page: 1},
			},
			code: wire.AgdxListFilterRevisionsCode,
			send: func(catalog *laser.FilterCatalog) error {
				_, err := catalog.Revisions(context.Background(), 4, 1, 20)
				return err
			},
		},
		"one binding": {
			outcome: &wire.FilterCatalogOutcome{
				Kind: wire.CatalogBinding, Binding: &wire.FilterBinding{},
			},
			code: wire.AgdxGetFilterBindingCode,
			send: func(catalog *laser.FilterCatalog) error {
				_, err := catalog.Binding(context.Background(), wire.FilterGroupRef{})
				return err
			},
		},
		"every binding": {
			outcome: &wire.FilterCatalogOutcome{
				Kind: wire.CatalogBindings, Bindings: &wire.FilterBindingPage{Page: 1},
			},
			code: wire.AgdxListFilterBindingsCode,
			send: func(catalog *laser.FilterCatalog) error {
				_, err := catalog.Bindings(context.Background(), laser.BindingQuery{}, 1, 20)
				return err
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := &filterServer{catalog: read.outcome}
			catalog := filterHandle(server, laser.Options{}).FilterCatalog()

			if err := read.send(catalog); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if server.codes[0] != read.code {
				t.Errorf("want code %d, got %d", read.code, server.codes[0])
			}

			// The same read answered with another shape is refused.
			wrong := &filterServer{catalog: &wire.FilterCatalogOutcome{
				Kind: wire.CatalogMutation,
				Mutation: &wire.FilterMutationOutcome{
					V:      wire.FilterOpVersion,
					Status: wire.FilterMutationStatus{Kind: wire.StatusPending},
				},
			}}
			if err := read.send(
				filterHandle(wrong, laser.Options{}).FilterCatalog()); err == nil {
				t.Error("a reply of another shape must be refused")
			}
		})
	}
}

// A binding listing is narrowed server side, because a deployment with many
// groups would otherwise send them all to be filtered here.
func TestGivenABindingListingWhenNarrowedThenShouldSendTheNarrowing(t *testing.T) {
	server := &filterServer{catalog: &wire.FilterCatalogOutcome{
		Kind: wire.CatalogBindings, Bindings: &wire.FilterBindingPage{Page: 1},
	}}
	stream, topic, filterID := "agents", "events", uint32(4)

	if _, err := filterHandle(server, laser.Options{}).FilterCatalog().Bindings(
		context.Background(),
		laser.BindingQuery{FilterID: &filterID, Stream: &stream, Topic: &topic},
		1, 20); err != nil {
		t.Fatalf("bindings: %v", err)
	}

	var request wire.ListFilterBindings
	decodeFilterRequest(t, server, 0, &request)
	if request.FilterID == nil || *request.FilterID != 4 {
		t.Errorf("want the filter narrowing sent, got %v", request.FilterID)
	}
	if request.Stream == nil || *request.Stream != "agents" ||
		request.Topic == nil || *request.Topic != "events" {
		t.Errorf("want the source narrowing sent, got %v and %v",
			request.Stream, request.Topic)
	}
}

// A filter can be judged against a record the caller holds, rather than
// against whatever happens to be on the topic.
func TestGivenASampleRecordWhenTestedThenShouldExplainTheVerdict(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterTested,
		TestResult: &wire.FilterTestResult{
			V:      wire.FilterOpVersion,
			Policy: wire.AppliedPolicy{Mode: wire.ModeFiltered, Digest: inlineDigest()},
			Explanation: wire.FilterExplanation{
				Verdict: wire.VerdictSelected,
				Root:    wire.ExplainNode{Label: "tenant == acme"},
			},
		},
	}}
	filters := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader"))

	result, err := filters.Test(context.Background(), inlineFilter(t),
		[]byte(`{"tenant":"acme"}`),
		[]wire.FilterHeader{{Key: "tenant", Value: wire.StringHeaderScalar("acme")}})
	if err != nil {
		t.Fatalf("test: %v", err)
	}
	if result.Explanation.Verdict != wire.VerdictSelected {
		t.Errorf("want a selection, got %q", result.Explanation.Verdict)
	}
	// The explanation is what says why, which is the difference between a
	// filter that is testable and one that is debuggable.
	if result.Explanation.Root.Label == "" {
		t.Error("a test result explains its verdict")
	}

	if server.codes[0] != wire.AgdxFilterTestCode {
		t.Errorf("want the test code, got %d", server.codes[0])
	}
	var request wire.FilterTestRequest
	decodeFilterRequest(t, server, 0, &request)
	if string(request.Payload) != `{"tenant":"acme"}` || len(request.Headers) != 1 {
		t.Errorf("want the sample record sent, got %+v", request)
	}
}

// A reply of the wrong shape is refused rather than read as the one asked
// for, because a preview read as a page would report records nobody selected.
func TestGivenAFilterReplyOfAnotherShapeWhenReadThenShouldRefuseIt(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterAcknowledged, Receipt: &wire.AckReceipt{},
	}}
	filters := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader"))

	if _, err := filters.Read(0, inlineFilter(t)).
		Fetch(context.Background()); err == nil {
		t.Error("a read answered with an acknowledgment must be refused")
	}
	if _, err := filters.Preview(0, inlineFilter(t)).
		Send(context.Background()); err == nil {
		t.Error("a preview answered with an acknowledgment must be refused")
	}
	if _, err := filters.Test(context.Background(),
		inlineFilter(t), nil, nil); err == nil {
		t.Error("a test answered with an acknowledgment must be refused")
	}
}

// Each walks the partition rather than one page of it, which is what a sweep
// over a cold topic wants, and it stops where the caller says.
func TestGivenASweepWhenEachPageIsVisitedThenShouldStopWhereTheCallerSays(t *testing.T) {
	page := matchedPage([]uint64{17}, 240, []byte("matched"))
	page.Stop = wire.StopBudget // more follows at once
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome, Page: &page,
	}}
	filters := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader"))
	seen := 0

	err := filters.Read(0, inlineFilter(t)).From(wire.StartAtFirst()).
		MaxReplyBytes(4096).MaxExamined(240).
		Each(context.Background(), func(*laser.MatchedPage) bool {
			seen++
			return false
		})
	if err != nil {
		t.Fatalf("each: %v", err)
	}
	if seen != 1 {
		t.Errorf("want the sweep to stop after one page, got %d", seen)
	}

	var request wire.FilteredPollRequest
	decodeFilterRequest(t, server, 0, &request)
	if request.MaxReplyBytes != 4096 {
		t.Errorf("want the byte budget sent, got %d", request.MaxReplyBytes)
	}
	if filters.Source().Topic != "events" {
		t.Errorf("want the source named, got %+v", filters.Source())
	}
}

// A sweep stops on its own once the partition is exhausted, so a caller that
// keeps returning true does not poll a cold topic forever.
func TestGivenAnExhaustedPartitionWhenSweptThenShouldStopOnItsOwn(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome,
		Page: ptr(matchedPage([]uint64{17}, 240, []byte("matched"))),
	}}
	seen := 0

	err := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader")).
		Read(0, inlineFilter(t)).From(wire.StartAtFirst()).MaxExamined(240).
		Each(context.Background(), func(*laser.MatchedPage) bool {
			seen++
			return true
		})
	if err != nil {
		t.Fatalf("each: %v", err)
	}
	if seen != 1 {
		t.Errorf("want one page before the partition is exhausted, got %d", seen)
	}
}

// Configuring a group holds the change to one exact incarnation, so a group
// deleted and recreated under the same name is not configured by mistake.
func TestGivenAGroupConfiguredByIdentityWhenSentThenShouldHoldItToThatIncarnation(t *testing.T) {
	group := wire.FilterGroupRef{Stream: "agents", Topic: "events", Group: "readers"}
	identity := wire.FilterGroupIdentity{StreamID: 1, TopicID: 2, GroupID: 7}
	server := &filterServer{catalog: appliedResult(wire.FilterMutationResult{
		Kind:    wire.ResultBound,
		Binding: &wire.FilterBinding{Group: group, Identity: identity},
	})}
	operation := laser.NewOperationID()

	binding, _, err := filterHandle(server, laser.Options{}).FilterCatalog().
		ConfigureGroupAs(context.Background(), operation, group,
			wire.DefinitionSpec("tenant-acme", inlineDefinition()), identity)
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	if binding.Identity != identity {
		t.Errorf("want the configured incarnation back, got %+v", binding.Identity)
	}

	var request wire.FilterMutationRequest
	decodeFilterRequest(t, server, 0, &request)
	if request.OperationID != operation {
		t.Error("want the caller's identity carried, so a retry applies once")
	}
	if request.Mutation.ExpectedIdentity == nil ||
		*request.Mutation.ExpectedIdentity != identity {
		t.Errorf("want the group incarnation carried, got %v",
			request.Mutation.ExpectedIdentity)
	}
}

// An acknowledgment from a group member carries the group, because the server
// stores progress against the group rather than against the member.
func TestGivenAGroupPageWhenAcknowledgedThenShouldCarryItsGroup(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterAcknowledged, Receipt: &wire.AckReceipt{},
	}}
	filters := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.NamedGroup("readers"))

	if err := filters.Ack(context.Background(), &laser.MatchedPage{
		Page: wire.FilteredPage{
			SafeAckOffset: ptr(uint64(239)),
			Policy: wire.AppliedPolicy{
				Mode: wire.ModeFiltered, GroupID: ptr(uint64(7)),
			},
		},
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	var acknowledgment wire.FilteredAck
	decodeFilterRequest(t, server, 0, &acknowledgment)
	if acknowledgment.GroupID == nil || *acknowledgment.GroupID != 7 {
		t.Errorf("want the group carried, got %v", acknowledgment.GroupID)
	}
}

// An acknowledgment answered with anything but a receipt is refused, because
// a reader that treated it as stored would never send it again.
func TestGivenAnAcknowledgmentAnsweredWithAPageWhenSentThenShouldRefuseIt(t *testing.T) {
	server := &filterServer{outcome: wire.FilterOutcome{
		Kind: wire.FilterPageOutcome,
		Page: ptr(matchedPage([]uint64{17}, 240, []byte("matched"))),
	}}

	err := filterHandle(server, laser.Options{}).Topic("events").
		Filters(wire.IndependentConsumer("reader")).
		Ack(context.Background(), &laser.MatchedPage{Page: wire.FilteredPage{
			SafeAckOffset: ptr(uint64(239)),
			Policy:        wire.AppliedPolicy{Mode: wire.ModeFiltered},
		}})

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}

// A change with an unknown outcome has to say which identity to recover
// under, because that identity is the only way to find out what happened.
func TestGivenAnAmbiguousMutationWhenReportedThenShouldNameItsIdentity(t *testing.T) {
	server := &filterServer{refusals: 99, catalog: &wire.FilterCatalogOutcome{}}

	_, err := filterHandle(server, laser.Options{}).FilterCatalog().
		Archive(context.Background(), 4)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an AmbiguousMutationError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), ambiguous.OperationID.String()) {
		t.Errorf("want the identity in the message, got %q", err.Error())
	}
}

// A caller that gives up stops the wait, rather than holding the deadline for
// a change nobody is waiting on any more.
func TestGivenACancelledCallerWhenAMutationIsPendingThenShouldStopWaiting(t *testing.T) {
	server := &pendingServer{never: true}
	catalog := filterHandle(server, laser.Options{}).FilterCatalog().
		WaitingFor(time.Minute)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for server.calls() < 2 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()
	_, err := catalog.Archive(ctx, 4)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want the cancellation, got %T: %v", err, err)
	}
}

// A catalog that has not folded a change yet answers not found, which is a
// reason to look again rather than to give up: the change may still apply.
func TestGivenAnUnrecordedOutcomeWhenWaitingThenShouldKeepLookingForIt(t *testing.T) {
	server := &pendingServer{missing: true, applied: wire.FilterMutationResult{
		Kind: wire.ResultArchived, FilterID: 4,
	}}
	catalog := filterHandle(server, laser.Options{}).FilterCatalog().
		WaitingFor(5 * time.Second)

	if _, err := catalog.Archive(context.Background(), 4); err != nil {
		t.Fatalf("archive: %v", err)
	}

	if server.calls() < 3 {
		t.Errorf("want the outcome looked for again, got %d calls", server.calls())
	}
}

// A retry schedule without jitter would have every client that failed
// together come back together, so the delay is spread and never exceeds its
// ceiling.
func TestGivenARetryDelayWhenTakenThenShouldStayWithinItsCeiling(t *testing.T) {
	backoff := laser.Backoff{Base: 100 * time.Millisecond, Max: time.Second}

	for attempt := uint32(1); attempt <= 8; attempt++ {
		ceiling := backoff.Ceiling(attempt)
		delay := backoff.Delay(attempt)
		if delay < ceiling/2 || delay > ceiling {
			t.Errorf("attempt %d: want a delay in [%v, %v], got %v",
				attempt, ceiling/2, ceiling, delay)
		}
	}
}
