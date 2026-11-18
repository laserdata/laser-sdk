package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// managedClient records what reached the socket and answers with a scripted
// reply, so a test can state both halves of a managed call.
type managedClient struct {
	iggcon.Client
	reply    []byte
	err      error
	lastCode uint32
	lastBody []byte
	calls    int
}

func (client *managedClient) Close() error { return nil }

func (client *managedClient) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	client.calls++
	client.lastCode = code
	client.lastBody = payload
	return client.reply, client.err
}

// managedHandle wraps a client that already announced a managed backend, which
// is the state every managed call needs.
func managedHandle(client iggcon.Client, options laser.Options) *laser.Laser {
	if options.Capabilities == nil {
		capabilities := laser.OpenCapabilities()
		capabilities.Managed = true
		capabilities.Hello = laser.HelloAnswered
		options.Capabilities = &capabilities
	}
	return laser.FromClient(client, options)
}

// Managed names are scoped to one stream by default, which is how two streams
// in one deployment keep separate key-value namespaces and graph names.
func TestGivenADefaultStreamWhenNamingAResourceThenShouldScopeItToThatStream(t *testing.T) {
	handle := managedHandle(&managedClient{}, laser.Options{DefaultStream: "agents"})

	if name := handle.ResourceName("sessions"); name != "stream:agents/sessions" {
		t.Errorf("want the scoped name, got %q", name)
	}
	// A name that already carries the prefix is passed through, so scoping is
	// never applied twice.
	already := handle.ResourceName("stream:agents/sessions")
	if already != "stream:agents/sessions" {
		t.Errorf("want the name unchanged, got %q", already)
	}
}

func TestGivenBareNamingOrNoStreamWhenNamingAResourceThenShouldSendItUnchanged(t *testing.T) {
	bare := managedHandle(&managedClient{},
		laser.Options{DefaultStream: "agents", ResourceNaming: laser.ResourceNamingBare})
	if name := bare.ResourceName("sessions"); name != "sessions" {
		t.Errorf("bare naming must not scope, got %q", name)
	}
	unpinned := managedHandle(&managedClient{}, laser.Options{})
	if name := unpinned.ResourceName("sessions"); name != "sessions" {
		t.Errorf("no default stream means nothing to scope to, got %q", name)
	}
}

// A listing hands back the names the deployment stores, so the caller's own
// name is the local part, and a name belonging to another stream is not one
// this handle can address.
func TestGivenAListedNameWhenLocalizedThenShouldStripOnlyThisHandlesPrefix(t *testing.T) {
	handle := managedHandle(&managedClient{}, laser.Options{DefaultStream: "agents"})

	if local, ok := handle.LocalResourceName("stream:agents/sessions"); !ok || local != "sessions" {
		t.Errorf("want sessions, got %q (%v)", local, ok)
	}
	if _, ok := handle.LocalResourceName("stream:billing/sessions"); ok {
		t.Error("another stream's name is not addressable from here")
	}
	if _, ok := handle.LocalResourceName("sessions"); ok {
		t.Error("while scoping is active an unscoped name belongs to no stream")
	}

	bare := managedHandle(&managedClient{},
		laser.Options{DefaultStream: "agents", ResourceNaming: laser.ResourceNamingBare})
	if local, ok := bare.LocalResourceName("sessions"); !ok || local != "sessions" {
		t.Errorf("bare naming keeps every name, got %q (%v)", local, ok)
	}
}

// The first slash is what ends the stream in a scoped name, so a stream whose
// name contains one cannot be scoped to without addressing something else.
func TestGivenAStreamWithASlashWhenSendingManagedThenShouldRefuseBeforeSending(t *testing.T) {
	client := &managedClient{}
	handle := managedHandle(client, laser.Options{DefaultStream: "tenant/agents"})

	_, err := handle.SendManaged(context.Background(), wire.AgdxKvGetCode, []byte{1})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if client.calls != 0 {
		t.Error("a name that cannot be scoped must not reach the socket")
	}
}

// A read is sent as the caller wrote it: there is no state to apply once.
func TestGivenAManagedReadWhenSentThenShouldCarryThePayloadUnwrapped(t *testing.T) {
	client := &managedClient{reply: []byte{9, 9}}
	handle := managedHandle(client, laser.Options{})

	reply, err := handle.SendManaged(context.Background(), wire.AgdxKvGetCode, []byte{1, 2, 3})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if string(reply) != "\x09\x09" {
		t.Errorf("want the reply returned, got %v", reply)
	}
	if string(client.lastBody) != "\x01\x02\x03" {
		t.Errorf("a read must not be wrapped, got %v", client.lastBody)
	}
	if client.lastCode != wire.AgdxKvGetCode {
		t.Errorf("want the kv get code, got %d", client.lastCode)
	}
}

// A write carries an operation identity, so a transport retry of the same call
// is applied once rather than twice.
func TestGivenAManagedWriteWhenSentThenShouldCarryAnOperationIdentity(t *testing.T) {
	client := &managedClient{}
	handle := managedHandle(client, laser.Options{})

	if _, err := handle.SendManaged(
		context.Background(), wire.AgdxKvSetCode, []byte{7}); err != nil {
		t.Fatalf("send: %v", err)
	}

	var envelope wire.ManagedRequestEnvelope
	if err := wire.Unmarshal(client.lastBody, &envelope); err != nil {
		t.Fatalf("a write must be wrapped in a managed envelope: %v", err)
	}
	if envelope.V != wire.ManagedRequestVersion {
		t.Errorf("want version %d, got %d", wire.ManagedRequestVersion, envelope.V)
	}
	if envelope.OperationID.IsZero() {
		t.Error("a write must name a nonzero operation")
	}
	if string(envelope.Payload) != "\x07" {
		t.Errorf("want the caller's payload carried, got %v", envelope.Payload)
	}
	if err := envelope.Validate(); err != nil {
		t.Errorf("the envelope must satisfy the contract: %v", err)
	}
}

// A retry that outlives the process reuses the identity, which is the whole
// point of letting the caller choose it.
func TestGivenACallerChosenOperationWhenSentTwiceThenShouldCarryTheSameIdentity(t *testing.T) {
	client := &managedClient{}
	handle := managedHandle(client, laser.Options{})
	operation := laser.NewOperationID()

	var bodies [2][]byte
	for attempt := range bodies {
		if _, err := handle.SendManagedWithOperation(
			context.Background(), wire.AgdxKvSetCode, []byte{7}, operation); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		bodies[attempt] = append([]byte(nil), client.lastBody...)
	}
	if string(bodies[0]) != string(bodies[1]) {
		t.Error("the same operation must produce the same frame on a retry")
	}
}

// A zero identity would be refused by the backend, so it is refused here,
// where the error can say which call was wrong.
func TestGivenAZeroOperationWhenSentThenShouldRefuseIt(t *testing.T) {
	client := &managedClient{}
	handle := managedHandle(client, laser.Options{})

	_, err := handle.SendManagedWithOperation(
		context.Background(), wire.AgdxKvSetCode, nil, wire.U128{})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T", err)
	}
	if client.calls != 0 {
		t.Error("a refused call must not reach the socket")
	}
}

// The band itself asks no capability question, because its surfaces do not
// share one answer: the key-value store needs a managed backend, while the
// authorization verbs are served by the streaming server itself. Gating here
// would refuse a deployment that serves authz without a backend.
func TestGivenNoManagedBackendWhenSendingAServerNativeCommandThenShouldStillSend(t *testing.T) {
	client := &managedClient{}
	handle := laser.FromClient(client, laser.Options{})

	if _, err := handle.SendManaged(
		context.Background(), wire.AgdxAuthzWhoamiCode, nil); err != nil {
		t.Fatalf("a server-native command must not be gated on a backend: %v", err)
	}
	if client.calls != 1 {
		t.Errorf("want the command sent, got %d calls", client.calls)
	}
	// An authorization verb carries its own mutation id, so it is not wrapped
	// in the managed envelope a backend-served write uses.
	if len(client.lastBody) != 0 {
		t.Errorf("want the payload sent as given, got %v", client.lastBody)
	}
}

// Every managed reply raises the frontier, and a later read of that surface
// sends it, so whichever node answers has applied what this client saw.
func TestGivenAPositionedReplyWhenOpenedThenShouldRaiseTheSeenFrontier(t *testing.T) {
	positioned, err := wire.Marshal(wire.ManagedReplyEnvelope{
		V: wire.ManagedRequestVersion,
		Frontier: wire.AppliedFrontier{
			Kv: &wire.MutationPosition{TopicGeneration: 3, Partition: 1, Offset: 42},
		},
		Reply: []byte{5, 5},
	})
	if err != nil {
		t.Fatalf("encode a positioned reply: %v", err)
	}
	client := &managedClient{reply: positioned}
	handle := managedHandle(client, laser.Options{})

	reply, err := handle.SendManaged(context.Background(), wire.AgdxKvGetCode, nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if string(reply) != "\x05\x05" {
		t.Errorf("want the surface reply unwrapped, got %v", reply)
	}
	seen := handle.SeenPositions()
	if seen.Kv == nil || seen.Kv.Offset != 42 || seen.Kv.TopicGeneration != 3 {
		t.Errorf("want the kv position recorded, got %+v", seen.Kv)
	}
}

// A surface reply that is not positioned arrives as it was sent. The envelope
// refuses unknown fields, so a plain reply cannot be mistaken for one.
func TestGivenAPlainReplyWhenOpenedThenShouldReturnItUnchanged(t *testing.T) {
	plain, err := wire.Marshal(wire.KvGet{Namespace: "ns", Key: []byte("k")})
	if err != nil {
		t.Fatalf("encode a surface reply: %v", err)
	}
	client := &managedClient{reply: plain}
	handle := managedHandle(client, laser.Options{})

	reply, err := handle.SendManaged(context.Background(), wire.AgdxKvGetCode, nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if string(reply) != string(plain) {
		t.Error("a plain reply must not be opened as a positioned one")
	}
	if handle.SeenPositions().Kv != nil {
		t.Error("a plain reply carries no position to record")
	}
}

func TestGivenAClosedHandleWhenSendingManagedThenShouldReportTheTransportGone(t *testing.T) {
	handle := managedHandle(&managedClient{}, laser.Options{})
	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err := handle.SendManaged(context.Background(), wire.AgdxKvGetCode, nil)

	var transport *laser.TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("want a TransportError, got %T: %v", err, err)
	}
}

// Two writes must never share an identity, or the backend would apply the
// second as a repeat of the first and drop it.
func TestGivenMintedOperationIdsWhenComparedThenShouldBeDistinctAndNonzero(t *testing.T) {
	seen := map[wire.U128]bool{}
	for range 1000 {
		id := laser.NewOperationID()
		if id.IsZero() {
			t.Fatal("a minted operation id must not be zero")
		}
		if seen[id] {
			t.Fatal("two minted operation ids collided")
		}
		seen[id] = true
	}
}

// The handle is documented as safe for concurrent use, and the frontier every
// managed reply raises is shared mutable state. Under the race detector this
// fails loudly if the lock stops covering it, or if Close races a call in
// flight.
func TestGivenConcurrentManagedCallsWhenRunThenShouldShareTheFrontierSafely(t *testing.T) {
	positioned, err := wire.Marshal(wire.ManagedReplyEnvelope{
		V: wire.ManagedRequestVersion,
		Frontier: wire.AppliedFrontier{
			Kv: &wire.MutationPosition{TopicGeneration: 1, Partition: 0, Offset: 7},
		},
		Reply: []byte{1},
	})
	if err != nil {
		t.Fatalf("encode a positioned reply: %v", err)
	}
	handle := managedHandle(&concurrentClient{reply: positioned}, laser.Options{})

	var waiting sync.WaitGroup
	for range 32 {
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			// A call racing a Close returns an error rather than corrupting
			// anything, so only a panic or a detected race fails this test.
			_, _ = handle.SendManaged(context.Background(), wire.AgdxKvGetCode, nil)
			_ = handle.SeenPositions()
			_ = handle.Client()
		}()
	}
	waiting.Add(1)
	go func() {
		defer waiting.Done()
		_ = handle.Close()
	}()
	waiting.Wait()
}

// concurrentClient is safe to call from many goroutines, so the race detector
// reports the handle under test rather than the stand-in.
type concurrentClient struct {
	iggcon.Client
	reply []byte
}

func (client *concurrentClient) Close() error { return nil }

func (client *concurrentClient) SendBinaryRequest(
	_ context.Context, _ uint32, _ []byte,
) ([]byte, error) {
	return client.reply, nil
}
