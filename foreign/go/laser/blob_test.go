package laser_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// memoryBlobs is a blob store that keeps what it was given, and can be told
// to hand back something else.
type memoryBlobs struct {
	guard   sync.Mutex
	blobs   map[string][]byte
	tamper  []byte
	failPut error
	failGet error
}

func newBlobs() *memoryBlobs {
	return &memoryBlobs{blobs: map[string][]byte{}}
}

func (store *memoryBlobs) Put(
	_ context.Context, payload []byte,
) (string, error) {
	if store.failPut != nil {
		return "", store.failPut
	}
	digest := sha256.Sum256(payload)
	reference := "blob://" + hex.EncodeToString(digest[:])
	store.guard.Lock()
	defer store.guard.Unlock()
	store.blobs[reference] = append([]byte(nil), payload...)
	return reference, nil
}

func (store *memoryBlobs) Get(
	_ context.Context, reference string,
) ([]byte, error) {
	if store.failGet != nil {
		return nil, store.failGet
	}
	store.guard.Lock()
	defer store.guard.Unlock()
	if store.tamper != nil {
		return store.tamper, nil
	}
	held, present := store.blobs[reference]
	if !present {
		return nil, errors.New("no such blob")
	}
	return held, nil
}

// A body under the threshold comes back untouched, so turning the claim check
// on changes nothing for the records that never needed it.
func TestGivenASmallBodyWhenCheckedInThenShouldPassItThrough(t *testing.T) {
	store := newBlobs()

	body, contentType, err := laser.CheckIn(
		context.Background(), store, 1024, []byte("small"))
	if err != nil {
		t.Fatalf("check in: %v", err)
	}

	if string(body) != "small" {
		t.Errorf("want the body untouched, got %q", body)
	}
	if contentType != nil {
		t.Errorf("want no restamping, got %v", contentType)
	}
	if len(store.blobs) != 0 {
		t.Error("a small body must not reach the store")
	}
}

// A body at or over the threshold is externalized and replaced by the capsule
// that says where it went.
func TestGivenALargeBodyWhenCheckedInThenShouldExternalizeIt(t *testing.T) {
	store := newBlobs()
	original := []byte(strings.Repeat("x", 4096))

	body, contentType, err := laser.CheckIn(
		context.Background(), store, 1024, original)
	if err != nil {
		t.Fatalf("check in: %v", err)
	}

	if contentType == nil || *contentType != wire.ContentBodyRef {
		t.Fatalf("want the body restamped as a reference, got %v", contentType)
	}
	var capsule wire.BodyRef
	if err := wire.Unmarshal(body, &capsule); err != nil {
		t.Fatalf("decode the capsule: %v", err)
	}
	if capsule.SizeBytes != uint64(len(original)) {
		t.Errorf("want the size recorded, got %d", capsule.SizeBytes)
	}
	// The digest is what makes a reader able to check the store later.
	digest := sha256.Sum256(original)
	if string(capsule.SHA256) != string(digest[:]) {
		t.Error("want the body's own digest in the capsule")
	}

	// It round-trips back to the same bytes.
	resolved, err := laser.ResolveBody(context.Background(), store, body)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if string(resolved) != string(original) {
		t.Error("want the body back as it went in")
	}
}

// A store that returns something other than what the producer externalized is
// refused: a body nobody can vouch for is worse than no body at all.
func TestGivenATamperedBlobWhenResolvedThenShouldRefuseIt(t *testing.T) {
	store := newBlobs()
	original := []byte(strings.Repeat("x", 4096))
	body, _, err := laser.CheckIn(context.Background(), store, 1024, original)
	if err != nil {
		t.Fatalf("check in: %v", err)
	}

	for name, tamper := range map[string][]byte{
		"a different body of the same length": []byte(strings.Repeat("y", 4096)),
		"a shorter body":                      []byte("short"),
	} {
		t.Run(name, func(t *testing.T) {
			store.tamper = tamper
			defer func() { store.tamper = nil }()

			_, err := laser.ResolveBody(context.Background(), store, body)

			var integrity *laser.IntegrityError
			if !errors.As(err, &integrity) {
				t.Fatalf("want an IntegrityError, got %T: %v", err, err)
			}
			if integrity.Reference == "" {
				t.Error("want the reference named, so an operator can find it")
			}
		})
	}
}

// A capsule arrives off the log, so its reference is untrusted input to
// whatever store the caller supplied: the contract's bounds are enforced
// before it is handed over.
func TestGivenAForgedCapsuleWhenResolvedThenShouldRefuseItBeforeFetching(t *testing.T) {
	store := newBlobs()
	forged, err := wire.Marshal(wire.BodyRef{
		Reference: strings.Repeat("x", 1<<20), SizeBytes: 1,
		SHA256: make([]byte, 32),
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	_, err = laser.ResolveBody(context.Background(), store, forged)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(store.blobs) != 0 {
		t.Error("a refused capsule must not reach the store")
	}
}

// A body that is not a capsule at all cannot be resolved as one.
func TestGivenANonCapsuleWhenResolvedThenShouldRefuseIt(t *testing.T) {
	_, err := laser.ResolveBody(
		context.Background(), newBlobs(), []byte("just some bytes"))

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}

// A claim check needs somewhere to put the body, and a resolve needs
// somewhere to read it from.
func TestGivenNoStoreWhenCheckedInThenShouldRefuseIt(t *testing.T) {
	if _, _, err := laser.CheckIn(
		context.Background(), nil, 1, []byte("big enough")); err == nil {
		t.Error("a claim check without a store must be refused")
	}
	// A capsule that would otherwise decode and fetch, so the refusal is the
	// missing store rather than a body that was never readable.
	capsule, err := wire.Marshal(wire.NewBodyRef(
		"blob://somewhere", 4, [32]byte{1}))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := laser.ResolveBody(
		context.Background(), nil, capsule); err == nil {
		t.Error("a resolve without a store must be refused")
	}
}

// A store that cannot take the body fails the send rather than publishing a
// capsule pointing at nothing.
func TestGivenAFailingStoreWhenCheckedInThenShouldFailTheSend(t *testing.T) {
	store := newBlobs()
	store.failPut = errors.New("the bucket is full")

	_, _, err := laser.CheckIn(context.Background(), store, 1,
		[]byte("big enough"))

	if err == nil {
		t.Fatal("want the failure surfaced")
	}
	if !strings.Contains(err.Error(), "the bucket is full") {
		t.Errorf("want the store's own reason, got %q", err.Error())
	}
}

// A send externalizes its body when it reaches the threshold, and the record
// on the log carries the capsule rather than the body.
func TestGivenALargeSendWhenClaimCheckedThenShouldPublishTheCapsule(t *testing.T) {
	store := newBlobs()
	server := &publishServer{}
	handle := governedHandle(server)
	source, _ := wire.NewAgentID("planner")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	original := []byte(strings.Repeat("x", 4096))

	if _, err := handle.Agdx(source, conversation).
		Emit(original).Broadcast().ClaimCheck(store, 1024).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(server.published(t)[0].Payload, &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(envelope.Body) >= len(original) {
		t.Fatalf("want the capsule rather than the body, got %d bytes",
			len(envelope.Body))
	}
	resolved, err := laser.ResolveBody(context.Background(), store, envelope.Body)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if string(resolved) != string(original) {
		t.Error("want the original body behind the capsule")
	}
}

// A claim check runs before signing, so the signature covers the capsule the
// log actually carries rather than the body it replaced.
func TestGivenASignedClaimCheckWhenSentThenShouldSignTheCapsule(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.Enroll("user-7", key.VerifyingKey())
	store := newBlobs()
	server := &publishServer{}
	handle := governedHandle(server)
	source, _ := wire.NewAgentID("planner")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}

	if _, err := handle.Agdx(source, conversation).
		Emit([]byte(strings.Repeat("x", 4096))).Broadcast().
		ClaimCheck(store, 1024).Signed(key).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(server.published(t)[0].Payload, &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, err := registry.Verify(envelope); err != nil {
		t.Errorf("the signature must cover what the log carries: %v", err)
	}
}

// A message resolves uniformly: a body stamped as a reference is fetched and
// verified, and any other content type is the body itself.
func TestGivenAMessageWhenResolvedThenShouldFetchOnlyAReference(t *testing.T) {
	store := newBlobs()
	server := &publishServer{}
	handle := governedHandle(server)
	source, _ := wire.NewAgentID("planner")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	original := []byte(strings.Repeat("x", 4096))

	if _, err := handle.Agdx(source, conversation).
		Emit(original).Broadcast().ClaimCheck(store, 1024).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}
	record, err := laser.OpenRecord(server.published(t)[0])
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	message, err := laser.OpenAgdx(record)
	if err != nil {
		t.Fatalf("open the envelope: %v", err)
	}

	resolved, err := message.ResolveBody(context.Background(), store)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if string(resolved) != string(original) {
		t.Error("want the real body back")
	}

	// A plain body is itself, with no fetch at all.
	plain := &laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{Body: []byte("right here")},
	}
	body, err := plain.ResolveBody(context.Background(), nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if string(body) != "right here" {
		t.Errorf("want the body itself, got %q", body)
	}
}
