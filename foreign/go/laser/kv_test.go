package laser_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// kvServer answers key-value commands from a script, recording what reached it
// so a test can assert both the request and what the client made of the reply.
type kvServer struct {
	clusterOfOne
	outcomes []wire.KvOutcome
	failure  *wire.KvError
	codes    []uint32
	requests [][]byte
}

func (server *kvServer) Close() error { return nil }

func (server *kvServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.codes = append(server.codes, code)
	server.requests = append(server.requests, payload)
	reply := wire.KvReply{}
	switch {
	case server.failure != nil:
		reply = wire.ErrReply(*server.failure)
	case len(server.outcomes) > 0:
		reply = wire.OkReply(server.outcomes[0])
		if len(server.outcomes) > 1 {
			server.outcomes = server.outcomes[1:]
		}
	}
	encoded, err := wire.Marshal(reply)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// kvCapabilities is a deployment serving the store, which every key-value call
// requires.
func kvCapabilities() *laser.Capabilities {
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion)
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Hello = laser.HelloAnswered
	capabilities.Kv = laser.KvCaps{Available: true, CAS: true}
	capabilities.Versions = &versions
	return &capabilities
}

func kvHandle(server iggcon.Client, options laser.Options) *laser.Laser {
	if options.Capabilities == nil {
		options.Capabilities = kvCapabilities()
	}
	return laser.FromClient(server, options)
}

// decodeRequest reads back what the client sent, unwrapping the managed
// envelope a write is carried in.
//
// The command code decides whether to unwrap. Sniffing the body cannot: a
// plain request carries its own `v` field, so it decodes as an envelope with
// an empty payload rather than failing.
func decodeRequest[T any](t *testing.T, server *kvServer, index int, target *T) {
	t.Helper()
	decodeManagedTestRequest(t, server.codes[index], server.requests[index], target)
}

func decodeManagedTestRequest(t *testing.T, code uint32, frame []byte, target any) {
	t.Helper()
	body := frame
	if wire.IsIdempotentManagedRequest(code) {
		var envelope wire.ManagedRequestEnvelope
		if err := wire.Unmarshal(frame, &envelope); err != nil {
			t.Fatal(err)
		}
		body = envelope.Payload
	} else if wire.IsBarrierRead(code) {
		var envelope wire.ManagedReadEnvelope
		if err := wire.Unmarshal(frame, &envelope); err != nil {
			t.Fatal(err)
		}
		body = envelope.Read
	}
	if err := wire.Unmarshal(body, target); err != nil {
		t.Fatal(err)
	}
}

func TestGivenAStoredKeyWhenReadThenShouldSeparateAnEmptyValueFromAnAbsentOne(t *testing.T) {
	stored := &kvServer{outcomes: []wire.KvOutcome{
		{Kind: wire.KvValue, Entry: &wire.KvEntry{Key: []byte("k"), Value: []byte{}, Version: 4}},
	}}
	value, found, err := kvHandle(stored, laser.Options{}).Kv("sessions").
		Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found || len(value) != 0 {
		t.Errorf("a stored empty value is present, got %v (found %v)", value, found)
	}

	absent := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvValue}}}
	_, found, err = kvHandle(absent, laser.Options{}).Kv("sessions").
		Get(context.Background(), []byte("k"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if found {
		t.Error("an absent key must not read as present")
	}
}

// The namespace a deployment stores is the scoped one, which is how two
// streams keep separate namespaces.
func TestGivenADefaultStreamWhenReadingThenShouldSendTheScopedNamespace(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvValue}}}
	kv := kvHandle(server, laser.Options{DefaultStream: "agents"}).Kv("sessions")

	if _, _, err := kv.Get(context.Background(), []byte("k")); err != nil {
		t.Fatalf("get: %v", err)
	}

	var request wire.KvGet
	decodeRequest(t, server, 0, &request)
	if request.Namespace != "stream:agents/sessions" {
		t.Errorf("want the scoped namespace, got %q", request.Namespace)
	}
	if kv.Namespace() != "sessions" {
		t.Errorf("the caller's own name stays unscoped, got %q", kv.Namespace())
	}
	if request.V != wire.KvOpVersion {
		t.Errorf("want the pinned op version, got %d", request.V)
	}
}

// An up-to-date cache skips the body transfer, so the two answers must stay
// distinguishable.
func TestGivenAConditionalReadWhenTheVersionMatchesThenShouldReportNotModified(t *testing.T) {
	unchanged := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvNotModified}}}
	revalidation, err := kvHandle(unchanged, laser.Options{}).Kv("ns").
		GetEntryIfChanged(context.Background(), []byte("k"), 7)
	if err != nil {
		t.Fatalf("conditional get: %v", err)
	}
	if !revalidation.NotModified || revalidation.Entry != nil {
		t.Errorf("want not-modified with no body, got %+v", revalidation)
	}

	var request wire.KvGet
	decodeRequest(t, unchanged, 0, &request)
	if request.IfNoneMatch == nil || *request.IfNoneMatch != 7 {
		t.Errorf("want the held version sent, got %v", request.IfNoneMatch)
	}

	changed := &kvServer{outcomes: []wire.KvOutcome{
		{Kind: wire.KvValue, Entry: &wire.KvEntry{Key: []byte("k"), Value: []byte("v"), Version: 8}},
	}}
	revalidation, err = kvHandle(changed, laser.Options{}).Kv("ns").
		GetEntryIfChanged(context.Background(), []byte("k"), 7)
	if err != nil {
		t.Fatalf("conditional get: %v", err)
	}
	if revalidation.NotModified || revalidation.Entry == nil || revalidation.Entry.Version != 8 {
		t.Errorf("want the changed entry, got %+v", revalidation)
	}
}

func TestGivenAWriteWithALifetimeWhenSentThenShouldCarryTheRelativeForm(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvWritten}}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	if err := kv.Set([]byte("k")).Bytes([]byte("v")).
		TTL(90 * time.Second).Send(context.Background()); err != nil {
		t.Fatalf("set: %v", err)
	}

	var request wire.KvSet
	decodeRequest(t, server, 0, &request)
	if request.TTLMicros == nil || *request.TTLMicros != 90_000_000 {
		t.Errorf("want 90s as micros, got %v", request.TTLMicros)
	}
	// A client clock cannot skew the relative form, so an absolute expiry must
	// not be derived from it here.
	if request.ExpiresAtMicros != nil {
		t.Error("a time to live must not be resolved on the client")
	}
}

func TestGivenBothExpirySettersWhenSendingThenShouldUseTheLastSetter(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvWritten}, {Kind: wire.KvWritten}}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")
	at := time.Now().Add(time.Hour)
	if err := kv.Set([]byte("a")).TTL(time.Minute).ExpiresAt(at).Send(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := kv.Set([]byte("b")).ExpiresAt(at).TTL(time.Minute).Send(context.Background()); err != nil {
		t.Fatal(err)
	}
	var first, second wire.KvSet
	decodeRequest(t, server, 0, &first)
	decodeRequest(t, server, 1, &second)
	if first.TTLMicros != nil || first.ExpiresAtMicros == nil || *first.ExpiresAtMicros != uint64(at.UnixMicro()) ||
		second.ExpiresAtMicros != nil || second.TTLMicros == nil || *second.TTLMicros != uint64(time.Minute.Microseconds()) {
		t.Fatalf("unexpected expiry selections: %+v %+v", first, second)
	}
}

func TestGivenMissingOrDroppedWritePreconditionsWhenSentThenShouldRefuseBeforeSending(t *testing.T) {
	server := &kvServer{}
	kv := kvHandle(server, laser.Options{Capabilities: fencedCapabilities()}).Kv("ns")
	plain := kv.Set([]byte("a"))
	plain.ExpectAbsent()
	_, fenced := kv.CasFenced([]byte("a"), "locks", []byte("owner"), 1).Commit(context.Background())
	var invalid *laser.InvalidError
	if !errors.As(plain.Send(context.Background()), &invalid) || !errors.As(fenced, &invalid) || len(server.codes) != 0 {
		t.Fatal("lost a write precondition")
	}
}

// A builder reports the first thing that was wrong, not the last, so the error
// names the step the caller has to fix.
func TestGivenSeveralBuilderMistakesWhenSentThenShouldReportTheFirst(t *testing.T) {
	server := &kvServer{}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	err := kv.Set(nil).Bytes([]byte("v")).TTL(-time.Second).Send(context.Background())

	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("want the empty key reported first, got %v", err)
	}
}

// A compare-and-swap hands back the new version so a caller chains the next
// conditional write without a re-read.
func TestGivenACompareAndSwapWhenCommittedThenShouldReturnTheNewVersion(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvCommitted, Version: 9}}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	version, err := kv.Set([]byte("k")).Bytes([]byte("v")).
		ExpectVersion(8).Commit(context.Background())
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if version != 9 {
		t.Errorf("want version 9, got %d", version)
	}

	var request wire.KvCas
	decodeRequest(t, server, 0, &request)
	if request.Expect.Kind != wire.CasMatch || request.Expect.Version != 8 {
		t.Errorf("want a version precondition, got %+v", request.Expect)
	}
	if server.codes[0] != wire.AgdxKvCasCode {
		t.Errorf("want the cas code, got %d", server.codes[0])
	}
}

// Claiming a name exactly once is the absent precondition, so it must reach
// the wire as that rather than as version zero.
func TestGivenAnAbsentPreconditionWhenCommittedThenShouldSendTheAbsentExpectation(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvCommitted, Version: 1}}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	if _, err := kv.Set([]byte("k")).Bytes([]byte("v")).
		ExpectAbsent().Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}

	var request wire.KvCas
	decodeRequest(t, server, 0, &request)
	if request.Expect.Kind != wire.CasAbsent {
		t.Errorf("want the absent expectation, got %+v", request.Expect)
	}
}

// Compare-and-swap has its own capability bit, so a store serving plain reads
// and writes refuses it rather than appearing to apply it.
func TestGivenNoCompareAndSwapSupportWhenCommittedThenShouldRefuseIt(t *testing.T) {
	capabilities := kvCapabilities()
	capabilities.Kv.CAS = false
	server := &kvServer{}
	kv := kvHandle(server, laser.Options{Capabilities: capabilities}).Kv("ns")

	_, err := kv.Set([]byte("k")).Bytes([]byte("v")).ExpectVersion(1).Commit(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported call must not reach the socket")
	}
}

// A failed precondition is resolved by re-reading, and a momentary outage by
// retrying. Confusing the two is how a caller spins on a write that will never
// apply.
func TestGivenAStoreFailureWhenReportedThenShouldClassifyItForTheCaller(t *testing.T) {
	for name, expected := range map[string]struct {
		kind      wire.KvErrorKind
		retryable bool
		conflict  bool
	}{
		"momentary outage":    {wire.KvUnavailable, true, false},
		"failed precondition": {wire.KvVersionConflict, false, true},
		"backend fault":       {wire.KvBackend, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			server := &kvServer{failure: &wire.KvError{Kind: expected.kind, Message: name}}
			_, _, err := kvHandle(server, laser.Options{}).Kv("ns").
				Get(context.Background(), []byte("k"))

			var failure *laser.KvError
			if !errors.As(err, &failure) {
				t.Fatalf("want a KvError, got %T: %v", err, err)
			}
			if failure.Kind() != expected.kind {
				t.Errorf("want kind %d, got %d", expected.kind, failure.Kind())
			}
			if failure.Retryable() != expected.retryable {
				t.Errorf("want retryable %v, got %v", expected.retryable, failure.Retryable())
			}
			if failure.Conflict() != expected.conflict {
				t.Errorf("want conflict %v, got %v", expected.conflict, failure.Conflict())
			}
		})
	}
}

// A payload sent across a version gap would be read under a contract neither
// side agreed to, so the gap is refused before the round trip.
func TestGivenADifferentServedOpVersionWhenCalledThenShouldRefuseBeforeSending(t *testing.T) {
	capabilities := kvCapabilities()
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion+1, wire.ForkOpVersion)
	capabilities.Versions = &versions
	server := &kvServer{}

	_, _, err := kvHandle(server, laser.Options{Capabilities: capabilities}).Kv("ns").
		Get(context.Background(), []byte("k"))

	var version *laser.VersionError
	if !errors.As(err, &version) {
		t.Fatalf("want a VersionError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("a version gap must not reach the socket")
	}
}

// A server that advertised nothing predates the announcement, so the round
// trip decides rather than a refusal here.
func TestGivenNoAdvertisedVersionsWhenCalledThenShouldStillSend(t *testing.T) {
	capabilities := kvCapabilities()
	capabilities.Versions = nil
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvValue}}}

	if _, _, err := kvHandle(server, laser.Options{Capabilities: capabilities}).Kv("ns").
		Get(context.Background(), []byte("k")); err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(server.codes) != 1 {
		t.Error("an unversioned server must still be called")
	}
}

// A managed deployment can still not serve the store, which is the case this
// gate exists for. With no managed backend at all the managed gate fires
// first, so that path proves nothing about this one.
func TestGivenAManagedDeploymentWithoutTheStoreWhenCalledThenShouldRefuseIt(t *testing.T) {
	capabilities := kvCapabilities()
	capabilities.Kv.Available = false
	server := &kvServer{}

	_, _, err := kvHandle(server, laser.Options{Capabilities: capabilities}).Kv("ns").
		Get(context.Background(), []byte("k"))

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "key-value") {
		t.Errorf("want the surface named, got %v", err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported call must not reach the socket")
	}
}

func TestGivenNoManagedBackendWhenCallingTheStoreThenShouldRefuseIt(t *testing.T) {
	server := &kvServer{}
	handle := laser.FromClient(server, laser.Options{})

	_, _, err := handle.Kv("ns").Get(context.Background(), []byte("k"))

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
}

func TestGivenAnInvalidKeyWhenUsedThenShouldRefuseBeforeSending(t *testing.T) {
	server := &kvServer{}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	for name, key := range map[string][]byte{
		"empty":    {},
		"too long": make([]byte, wire.MaxKeyBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := kv.Get(context.Background(), key); err == nil {
				t.Error("an invalid key must be refused")
			}
		})
	}
	if len(server.codes) != 0 {
		t.Error("an invalid key must not reach the socket")
	}
}

// Paging follows the count the store reports, because a page can be short
// while later pages still hold matches.
func TestGivenAMultiPageScanWhenWalkedThenShouldFollowTheReportedPageCount(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{
		{Kind: wire.KvPageOutcome, Page: &wire.KvPage{
			Items:      []wire.KvEntry{{Key: []byte("a")}},
			Page:       1,
			TotalPages: 3,
		}},
		{Kind: wire.KvPageOutcome, Page: &wire.KvPage{
			Items:      []wire.KvEntry{},
			Page:       2,
			TotalPages: 3,
		}},
		{Kind: wire.KvPageOutcome, Page: &wire.KvPage{
			Items:      []wire.KvEntry{{Key: []byte("c")}},
			Page:       3,
			TotalPages: 3,
		}},
	}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	entries, err := kv.Scan().Prefix([]byte("p")).All(context.Background())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("want both entries across the short page, got %d", len(entries))
	}
	if len(server.codes) != 3 {
		t.Errorf("want three pages read, got %d", len(server.codes))
	}
	var request wire.KvScan
	decodeRequest(t, server, 0, &request)
	if string(request.Prefix) != "p" {
		t.Errorf("want the prefix sent, got %q", request.Prefix)
	}
}

func TestGivenAScanWhenStoppedEarlyThenShouldReadNoFurtherPages(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{
		{Kind: wire.KvPageOutcome, Page: &wire.KvPage{
			Items:      []wire.KvEntry{{Key: []byte("a")}, {Key: []byte("b")}},
			Page:       1,
			TotalPages: 9,
		}},
	}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	seen := 0
	err := kv.Scan().Each(context.Background(), func(wire.KvEntry) bool {
		seen++
		return false
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if seen != 1 {
		t.Errorf("want one entry visited, got %d", seen)
	}
	if len(server.codes) != 1 {
		t.Errorf("stopping early must read one page, got %d", len(server.codes))
	}
}

func TestGivenABulkDeleteWhenSentThenShouldReportHowManyWereRemoved(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvDeletedMany, DeletedMany: 12}}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	removed, err := kv.DeleteMany().Prefix([]byte("tmp/")).Send(context.Background())
	if err != nil {
		t.Fatalf("delete many: %v", err)
	}
	if removed != 12 {
		t.Errorf("want 12 removed, got %d", removed)
	}
}

func TestGivenADeleteWhenConditionalThenShouldCarryTheHeldVersion(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvDeleted, Deleted: true}}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")

	removed, err := kv.DeleteIfVersion(context.Background(), []byte("k"), 5)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !removed {
		t.Error("want the entry reported removed")
	}
	var request wire.KvDelete
	decodeRequest(t, server, 0, &request)
	if request.IfMatch == nil || *request.IfMatch != 5 {
		t.Errorf("want the held version sent, got %v", request.IfMatch)
	}
}

// A listing answers with the stored names, so each is mapped back, and one
// belonging to another stream is left out rather than handed back unusable.
func TestGivenAListingWhenScopedThenShouldReturnOnlyThisStreamsOwnNames(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{
		Kind: wire.KvNamespacesOutcome,
		Namespaces: []wire.KvNamespaceInfo{
			{Namespace: "stream:agents/sessions", Entries: 3},
			{Namespace: "stream:billing/invoices", Entries: 9},
			{Namespace: "unscoped", Entries: 1},
		},
	}}}
	handle := kvHandle(server, laser.Options{DefaultStream: "agents"})

	namespaces, err := handle.KvNamespaces(context.Background())
	if err != nil {
		t.Fatalf("namespaces: %v", err)
	}
	if len(namespaces) != 1 {
		t.Fatalf("want only this stream's namespaces, got %+v", namespaces)
	}
	if namespaces[0].Namespace != "sessions" || namespaces[0].Entries != 3 {
		t.Errorf("want the caller's own name, got %+v", namespaces[0])
	}
	var request wire.KvNamespaces
	decodeRequest(t, server, 0, &request)
	if request.Stream == nil || *request.Stream != "agents" {
		t.Errorf("a scoped listing names its stream, got %v", request.Stream)
	}
}

// An outcome shaped for another operation means the client and the server
// disagree about the contract, which is reported rather than read as a value.
func TestGivenAnOutcomeForAnotherOperationWhenReadThenShouldRefuseIt(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvDeleted, Deleted: true}}}

	_, _, err := kvHandle(server, laser.Options{}).Kv("ns").Get(context.Background(), []byte("k"))

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}

func TestGivenAnExpiryWhenSetThenShouldSendTheFormTheCallerChose(t *testing.T) {
	relative := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvVersioned, Version: 6}}}
	version, err := kvHandle(relative, laser.Options{}).Kv("ns").
		Expire(context.Background(), []byte("k"), time.Hour)
	if err != nil || version != 6 {
		t.Fatalf("expire: version %d, %v, want the entry's version 6", version, err)
	}
	var request wire.KvExpire
	decodeRequest(t, relative, 0, &request)
	if request.TTLMicros == nil || *request.TTLMicros != 3_600_000_000 {
		t.Errorf("want an hour as micros, got %v", request.TTLMicros)
	}

	// A zero time clears the expiry, which is the only way to make an entry
	// permanent again.
	cleared := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvVersioned, Version: 7}}}
	version, err = kvHandle(cleared, laser.Options{}).Kv("ns").
		ExpireAt(context.Background(), []byte("k"), time.Time{})
	if err != nil || version != 7 {
		t.Fatalf("expire at: version %d, %v, want the entry's version 7", version, err)
	}
	var clearing wire.KvExpire
	decodeRequest(t, cleared, 0, &clearing)
	if clearing.TTLMicros != nil || clearing.ExpiresAtMicros != nil {
		t.Errorf("clearing an expiry names neither form, got %+v", clearing)
	}
}

func TestGivenAnExpiryBeforeTheEpochWhenSetInPlaceThenShouldRefuseBeforeSending(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvVersioned}}}

	_, err := kvHandle(server, laser.Options{}).Kv("ns").
		ExpireAt(context.Background(), []byte("k"), time.Unix(-1, 0))

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) || len(server.codes) != 0 {
		t.Fatalf("want an InvalidError before any send, got %T %v after %d sends", err, err, len(server.codes))
	}
}

func TestGivenAConditionalWriteWhenGivenAnExpiryThenShouldCarryTheLastFormChosen(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvCommitted, Version: 2}}}
	kv := kvHandle(server, laser.Options{}).Kv("ns")
	at := time.UnixMicro(1_900_000_000_000_000)

	if _, err := kv.Set([]byte("a")).Bytes([]byte("v")).ExpectVersion(1).ExpiresAt(at).TTL(time.Minute).
		Commit(context.Background()); err != nil {
		t.Fatalf("commit with a ttl: %v", err)
	}
	if _, err := kv.Set([]byte("b")).Bytes([]byte("v")).ExpectAbsent().TTL(time.Minute).ExpiresAt(at).
		Commit(context.Background()); err != nil {
		t.Fatalf("commit with an absolute expiry: %v", err)
	}

	var relative, absolute wire.KvCas
	decodeRequest(t, server, 0, &relative)
	decodeRequest(t, server, 1, &absolute)
	minute, instant := uint64(time.Minute.Microseconds()), uint64(at.UnixMicro())
	wantRelative := wire.KvCas{V: wire.KvOpVersion, Namespace: "ns", Key: []byte("a"), Value: []byte("v"),
		TTLMicros: &minute, Expect: wire.MatchVersion(1)}
	wantAbsolute := wire.KvCas{V: wire.KvOpVersion, Namespace: "ns", Key: []byte("b"), Value: []byte("v"),
		ExpiresAtMicros: &instant, Expect: wire.AbsentKey()}
	if !reflect.DeepEqual(relative, wantRelative) || !reflect.DeepEqual(absolute, wantAbsolute) {
		t.Fatalf("sent %+v and %+v, want %+v and %+v", relative, absolute, wantRelative, wantAbsolute)
	}
}

func TestGivenLinkedKvWhenPatchingThenShouldSendTheVersionAndSession(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvVersioned, Version: 4}}}
	ref := wire.SessionRef{Stream: "jobs", Session: laser.NewConversationID()}
	kv := kvHandle(server, laser.Options{DefaultStream: "jobs"}).Kv("locks").InSession(ref)
	version, err := kv.PatchIfVersion(t.Context(), []byte("task"), []byte("{}"), 3)
	if err != nil || version != 4 {
		t.Fatalf("patch: %d, %v", version, err)
	}
	var got wire.KvPatch
	decodeRequest(t, server, 0, &got)
	if got.Namespace != "stream:jobs/locks" || got.IfMatch == nil || *got.IfMatch != 3 || got.Session == nil || *got.Session != ref || server.codes[0] != wire.AgdxKvPatchCode {
		t.Fatalf("unexpected patch: %+v", got)
	}
}

func TestGivenACopyOrMoveWhenSentThenShouldScopeTheDestination(t *testing.T) {
	server := &kvServer{outcomes: []wire.KvOutcome{{Kind: wire.KvCommitted, Version: 7}}}
	kv := kvHandle(server, laser.Options{DefaultStream: "jobs"}).Kv("queue")
	for i, build := range []func([]byte, []byte) *laser.KvCopyRequest{kv.CopyTo, kv.MoveTo} {
		version, err := build([]byte("a"), []byte("b")).IntoNamespace("done").Send(t.Context())
		if err != nil || version != 7 {
			t.Fatalf("transfer: %d, %v", version, err)
		}
		var got wire.KvCopy
		decodeRequest(t, server, i, &got)
		if got.ToNamespace == nil || *got.ToNamespace != "stream:jobs/done" || string(got.ToKey) != "b" || string(got.Key) != "a" || server.codes[i] != []uint32{wire.AgdxKvCopyCode, wire.AgdxKvMoveCode}[i] {
			t.Fatalf("unexpected transfer: %+v", got)
		}
	}
}
