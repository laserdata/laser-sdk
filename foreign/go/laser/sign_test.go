package laser_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func signedEnvelope(t *testing.T) wire.AgentEnvelope {
	t.Helper()
	source, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	record := wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())}
	return wire.AgentEnvelope{
		Kind:         wire.KindEvent,
		Record:       &record,
		Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})},
		Source:       source,
		Body:         []byte("the claim"),
	}
}

// A record on a shared topic says who sent it, and only a signature makes
// that something a reader can check.
func TestGivenASignedEnvelopeWhenVerifiedThenShouldNameItsEnrolledPrincipal(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("user-7", key.VerifyingKey())
	envelope := signedEnvelope(t)

	signature, err := key.Sign(envelope)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	envelope.Signature = signature

	principal, err := registry.Verify(envelope)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	// The key proves the principal it was enrolled under, never the source
	// the envelope claims for itself: comparing the two is the binding.
	if principal != "user-7" {
		t.Errorf("want the enrolled principal, got %q", principal)
	}
	if !bytes.Equal(signature.KeyID, key.KeyID()) {
		t.Error("a signature names the key that made it")
	}
	if signature.Scheme != wire.SignatureSchemeEd25519 {
		t.Errorf("want the Ed25519 scheme, got %d", signature.Scheme)
	}
}

// A signature covers the envelope, so changing any part of it after signing
// must fail rather than carry the old claim onto new content.
func TestGivenATamperedEnvelopeWhenVerifiedThenShouldRefuseIt(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.Enroll("user-7", key.VerifyingKey())

	for name, tamper := range map[string]func(*wire.AgentEnvelope){
		"an edited body": func(envelope *wire.AgentEnvelope) {
			envelope.Body = []byte("a different claim")
		},
		"another author": func(envelope *wire.AgentEnvelope) {
			other, _ := wire.NewAgentID("impostor")
			envelope.Source = other
		},
		"another conversation": func(envelope *wire.AgentEnvelope) {
			envelope.Conversation = wire.ConversationID{
				ID: wire.IDFromBytes([16]byte{15: 9})}
		},
		"an added operation": func(envelope *wire.AgentEnvelope) {
			operation := "quarantine"
			envelope.Operation = &operation
		},
	} {
		t.Run(name, func(t *testing.T) {
			envelope := signedEnvelope(t)
			signature, err := key.Sign(envelope)
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			envelope.Signature = signature
			tamper(&envelope)

			_, err = registry.Verify(envelope)

			var failure *laser.SignatureError
			if !errors.As(err, &failure) {
				t.Fatalf("want a SignatureError, got %T: %v", err, err)
			}
		})
	}
}

// A signature that does not hold is refused, whatever is wrong with it.
func TestGivenAnUntrustworthySignatureWhenVerifiedThenShouldRefuseIt(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	stranger, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.Enroll("user-7", key.VerifyingKey())

	for name, break_ := range map[string]func(*wire.AgentEnvelope){
		"no signature at all": func(envelope *wire.AgentEnvelope) {
			envelope.Signature = nil
		},
		"a key nobody enrolled": func(envelope *wire.AgentEnvelope) {
			signature, _ := stranger.Sign(*envelope)
			envelope.Signature = signature
		},
		"a scheme this client does not implement": func(envelope *wire.AgentEnvelope) {
			envelope.Signature.Scheme = 99
		},
		"a signature of the wrong length": func(envelope *wire.AgentEnvelope) {
			envelope.Signature.Bytes = envelope.Signature.Bytes[:32]
		},
		"no signature bytes at all": func(envelope *wire.AgentEnvelope) {
			envelope.Signature.Bytes = nil
		},
		"bytes that are not the signature": func(envelope *wire.AgentEnvelope) {
			envelope.Signature.Bytes[0] ^= 0xff
		},
	} {
		t.Run(name, func(t *testing.T) {
			envelope := signedEnvelope(t)
			signature, err := key.Sign(envelope)
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			envelope.Signature = signature
			break_(&envelope)

			_, err = registry.Verify(envelope)
			var failure *laser.SignatureError
			if !errors.As(err, &failure) {
				t.Fatalf("want a SignatureError, got %T: %v", err, err)
			}
			// A signature of the wrong shape never reached the check, so it
			// is reported as malformed rather than as one that did not hold.
			malformed := envelope.Signature != nil &&
				len(envelope.Signature.Bytes) != laser.SignatureBytes &&
				envelope.Signature.Scheme == wire.SignatureSchemeEd25519
			if malformed && failure.Reason != "malformed Ed25519 signature" {
				t.Errorf("want the malformed reason, got %q", failure.Reason)
			}
		})
	}
}

// A key has a lifetime, so a rotation can overlap rather than cut over, and a
// compromised key stops verifying at once.
func TestGivenAKeyLifecycleWhenVerifiedAtATimeThenShouldHoldItToItsWindow(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	envelope := signedEnvelope(t)
	signature, err := key.Sign(envelope)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	envelope.Signature = signature

	windowed, err := laser.AgentKey("user-7", key.VerifyingKey()).
		ValidWindow(100, ptr(uint64(200)))
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	registry := laser.NewKeyRegistry()
	registry.EnrollRecord(windowed)

	if _, err := registry.VerifyAt(envelope, 150); err != nil {
		t.Errorf("a key inside its window verifies: %v", err)
	}
	if _, err := registry.VerifyAt(envelope, 99); err == nil {
		t.Error("a key before its window must not verify")
	}
	// The end is exclusive, so a record at the boundary belongs to the next
	// key rather than to both.
	if _, err := registry.VerifyAt(envelope, 200); err == nil {
		t.Error("a key at its end must not verify")
	}
	// Without a clock the window is not consulted at all, so the same
	// signature still checks out cryptographically.
	if _, err := registry.Verify(envelope); err != nil {
		t.Errorf("a windowless check reads no clock: %v", err)
	}

	// Revocation disables the key whatever the window says.
	registry.EnrollRecord(windowed.Revoke())
	if _, err := registry.VerifyAt(envelope, 150); err == nil {
		t.Error("a revoked key must not verify inside its window")
	}
}

// A window that ends before it starts describes a key that could never
// verify, so it is refused where it is built rather than where it fails.
func TestGivenAnImpossibleWindowWhenBuiltThenShouldRefuseIt(t *testing.T) {
	key, _ := laser.GenerateSigningKey()

	_, err := laser.AgentKey("user-7", key.VerifyingKey()).
		ValidWindow(200, ptr(uint64(200)))

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// An operator key may sign privileged control facts and an agent key may not,
// so the kind comes back with the principal.
func TestGivenAnOperatorKeyWhenVerifiedThenShouldReportItsKind(t *testing.T) {
	operator, _ := laser.GenerateSigningKey()
	agent, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.EnrollOperator("ops-1", operator.VerifyingKey())
	registry.Enroll("user-7", agent.VerifyingKey())

	for name, want := range map[string]struct {
		key  *laser.SigningKey
		kind wire.KeyKind
	}{
		"an operator": {key: operator, kind: wire.KeyOperator},
		"an agent":    {key: agent, kind: wire.KeyAgent},
	} {
		t.Run(name, func(t *testing.T) {
			envelope := signedEnvelope(t)
			signature, err := want.key.Sign(envelope)
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			envelope.Signature = signature

			verified, err := registry.VerifyAt(envelope, 1)
			if err != nil {
				t.Fatalf("verify: %v", err)
			}
			if verified.Kind != want.kind {
				t.Errorf("want kind %q, got %q", want.kind, verified.Kind)
			}
		})
	}
}

// A signature that bound no context reproduces the preimage a signature
// written before contexts existed covered, so an old record still verifies.
func TestGivenASignatureWithoutAContextWhenVerifiedThenShouldStillHold(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.Enroll("user-7", key.VerifyingKey())
	envelope := signedEnvelope(t)
	signature, err := key.Sign(envelope)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	envelope.Signature = signature

	if signature.Context != nil {
		t.Error("a plain signature binds no context")
	}
	if _, err := registry.Verify(envelope); err != nil {
		t.Errorf("a contextless signature must verify: %v", err)
	}
}

// Without binding the interpretation attributes, an intermediary could flip
// the codec on a signed record and the same bytes would read as something
// else entirely, so a bound context is held to what was observed.
func TestGivenARewrittenContextWhenVerifiedThenShouldRefuseIt(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.Enroll("user-7", key.VerifyingKey())
	envelope := signedEnvelope(t)
	json, cbor := uint8(1), uint8(3)
	version := uint32(wire.AgentOpVersion)
	context := wire.SignatureContext{ContentType: &json, AgentVersion: &version}
	signature, err := key.SignWithContext(envelope, context)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	envelope.Signature = signature

	if _, err := registry.VerifyObservedAt(envelope, context, 1); err != nil {
		t.Errorf("a signature matching what was observed verifies: %v", err)
	}

	// The record arrived stamped as something else than it was signed as.
	rewritten := wire.SignatureContext{ContentType: &cbor, AgentVersion: &version}
	_, err = registry.VerifyObservedAt(envelope, rewritten, 1)
	var failure *laser.SignatureError
	if !errors.As(err, &failure) {
		t.Fatalf("want a SignatureError, got %T: %v", err, err)
	}

	// A signature that bound nothing cannot be checked against what was
	// observed, so it is refused rather than accepted as unbound.
	plain := signedEnvelope(t)
	unbound, err := key.Sign(plain)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	plain.Signature = unbound
	if _, err := registry.VerifyObservedAt(plain, context, 1); err == nil {
		t.Error("an unbound signature cannot answer for the headers observed")
	}
}

// A signed record's claim about whom it acted for rides inside the signed
// span, so a signer cannot forge it and an intermediary cannot rewrite it.
func TestGivenADelegatedEnvelopeWhenVerifiedThenShouldNameBothPrincipals(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.Enroll("agent-7", key.VerifyingKey())
	envelope := signedEnvelope(t)
	envelope.Metadata = map[string]wire.Value{
		wire.MetadataDelegatedBy: wire.StrValue("user-3"),
	}
	version := uint32(wire.AgentOpVersion)
	context := wire.SignatureContext{AgentVersion: &version}
	signature, err := key.SignWithContext(envelope, context)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	envelope.Signature = signature

	delegation, err := laser.VerifyDelegation(registry, envelope, context, 1)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if delegation == nil {
		t.Fatal("want a delegation")
	}
	if delegation.Signer != "agent-7" || delegation.User != "user-3" {
		t.Errorf("want agent-7 acting for user-3, got %+v", delegation)
	}

	// Rewriting the claim breaks the signature, because it is inside it.
	envelope.Metadata[wire.MetadataDelegatedBy] = wire.StrValue("user-9")
	if _, err := laser.VerifyDelegation(registry, envelope, context, 1); err == nil {
		t.Error("a rewritten delegation must not verify")
	}
}

// An envelope carrying no delegation is not a failed one: it simply acts for
// nobody but itself.
func TestGivenNoDelegationWhenVerifiedThenShouldReportNone(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.Enroll("agent-7", key.VerifyingKey())
	version := uint32(wire.AgentOpVersion)
	context := wire.SignatureContext{AgentVersion: &version}

	for name, metadata := range map[string]map[string]wire.Value{
		"no metadata at all": nil,
		"other metadata": {
			wire.MetadataPurpose: wire.StrValue("support"),
		},
		"a delegation that is not a name": {
			wire.MetadataDelegatedBy: wire.IntValue(7),
		},
	} {
		t.Run(name, func(t *testing.T) {
			envelope := signedEnvelope(t)
			envelope.Metadata = metadata
			signature, err := key.SignWithContext(envelope, context)
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			envelope.Signature = signature

			delegation, err := laser.VerifyDelegation(registry, envelope, context, 1)
			if err != nil {
				t.Fatalf("verify: %v", err)
			}
			if delegation != nil {
				t.Errorf("want no delegation, got %+v", delegation)
			}
		})
	}
}

// A key rebuilt from its seed is the same key, which is what makes a stored
// secret a key rather than a one-session accident.
func TestGivenAStoredSeedWhenRebuiltThenShouldBeTheSameKey(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	rebuilt, err := laser.SigningKeyFromSeed(key.Seed())
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	if !bytes.Equal(rebuilt.KeyID(), key.KeyID()) {
		t.Error("a key rebuilt from its seed has the same identity")
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("user-7", key.VerifyingKey())
	envelope := signedEnvelope(t)
	signature, err := rebuilt.Sign(envelope)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	envelope.Signature = signature
	if _, err := registry.Verify(envelope); err != nil {
		t.Errorf("the rebuilt key signs for the same enrollment: %v", err)
	}
	// The seed is copied out, so a caller cannot edit the key it holds.
	key.Seed()[0] ^= 0xff
	if !bytes.Equal(key.Seed(), rebuilt.Seed()) {
		t.Error("a key must not hand out its own secret storage")
	}
}

// A seed of the wrong length is not a key, so it is refused where it is read.
func TestGivenAMalformedSeedWhenReadThenShouldRefuseIt(t *testing.T) {
	_, err := laser.SigningKeyFromSeed([]byte("too short"))

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// A key id is derived from the public key, so two clients name one key the
// same way without exchanging anything.
func TestGivenAPublicKeyWhenIdentifiedThenShouldDeriveItsID(t *testing.T) {
	key, _ := laser.GenerateSigningKey()

	derived := laser.KeyIDOf(key.VerifyingKey())

	if len(derived) != laser.KeyIDBytes {
		t.Fatalf("want %d bytes, got %d", laser.KeyIDBytes, len(derived))
	}
	if !bytes.Equal(derived, key.KeyID()) {
		t.Error("a key names itself by what its public key derives to")
	}
	other, _ := laser.GenerateSigningKey()
	if bytes.Equal(laser.KeyIDOf(other.VerifyingKey()), derived) {
		t.Error("two keys must not share an id")
	}
}

// A raw public key read from somewhere else enrolls like any other, and one
// that is not a key is refused.
func TestGivenARawPublicKeyWhenEnrolledThenShouldAcceptOnlyAValidOne(t *testing.T) {
	key, _ := laser.GenerateSigningKey()

	record, err := laser.KeyRecordFromBytes(
		"user-7", key.VerifyingKey(), wire.KeyAgent)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !bytes.Equal(record.KeyID(), key.KeyID()) {
		t.Error("the record names the key it holds")
	}

	if _, err := laser.KeyRecordFromBytes(
		"user-7", []byte("not a key"), wire.KeyAgent); err == nil {
		t.Error("a public key of the wrong length must be refused")
	}
}

// An agent send signs itself, which is what turns its authorship claim into
// something a reader can check.
func TestGivenASignedSendWhenPublishedThenShouldCarryAVerifiableSignature(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.Enroll("user-7", key.VerifyingKey())
	server := &publishServer{}
	handle := governedHandle(server)
	source, _ := wire.NewAgentID("planner")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}

	if _, err := handle.Agdx(source, conversation).
		Emit([]byte("the claim")).Broadcast().Signed(key).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	published := server.published(t)
	if len(published) != 1 {
		t.Fatalf("want one record, got %d", len(published))
	}
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(published[0].Payload, &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if envelope.Signature == nil {
		t.Fatal("a signed send carries its signature on the log")
	}
	// The signature binds the attributes the record is stamped with, so the
	// codec cannot be flipped under it.
	if envelope.Signature.Context == nil ||
		envelope.Signature.Context.ContentType == nil {
		t.Fatal("want the interpretation attributes bound")
	}
	if _, err := registry.VerifyObservedAt(
		envelope, *envelope.Signature.Context, 1); err != nil {
		t.Errorf("a published signature must verify off the log: %v", err)
	}
}

// A governor replacing the body runs before the signature, so the signature
// always covers what the log actually carries.
func TestGivenAGovernedReplacementWhenSignedThenShouldCoverTheReplacement(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	registry := laser.NewKeyRegistry()
	registry.Enroll("user-7", key.VerifyingKey())
	governor := &scriptedGovernor{decision: laser.Modify([]byte("redacted"))}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	source, _ := wire.NewAgentID("planner")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}

	if _, err := handle.Agdx(source, conversation).
		Emit([]byte("the card number")).Broadcast().Signed(key).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(server.published(t)[0].Payload, &envelope); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(envelope.Body) != "redacted" {
		t.Fatalf("want the replacement on the log, got %q", envelope.Body)
	}
	if _, err := registry.Verify(envelope); err != nil {
		t.Errorf("the signature must cover the replacement: %v", err)
	}
	// The policy is told the record will be signed, which is a fact about
	// this client rather than a claim the envelope carries.
	asked := governor.asked()
	if len(asked) != 1 || !asked[0].Signed {
		t.Errorf("want the signing reported, got %+v", asked)
	}
	// What it was offered is the body, not the whole envelope.
	if string(asked[0].Payload) != "the card number" {
		t.Errorf("want the body offered, got %q", asked[0].Payload)
	}
}

// A declared purpose and classification are claims a policy reads, which is
// why they ride inside the envelope rather than on a header.
func TestGivenDeclaredMetadataWhenGovernedThenShouldOfferItAsClaims(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Allow()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	source, _ := wire.NewAgentID("planner")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}

	if _, err := handle.Agdx(source, conversation).
		Emit([]byte("work")).Broadcast().
		WithMetadata(wire.MetadataPurpose, wire.StrValue("support")).
		WithMetadata(wire.MetadataDataClassification, wire.StrValue("pii")).
		WithMetadata(wire.MetadataDelegatedBy, wire.StrValue("user-3")).
		Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}

	asked := governor.asked()
	if len(asked) != 1 {
		t.Fatalf("want one decision, got %d", len(asked))
	}
	if asked[0].Purpose != "support" || asked[0].DataClassification != "pii" {
		t.Errorf("want the declarations offered, got %+v", asked[0])
	}
	if asked[0].OnBehalfOf != "user-3" {
		t.Errorf("want the delegation offered, got %q", asked[0].OnBehalfOf)
	}
	if asked[0].Signed {
		t.Error("an unsigned send must not report itself signed")
	}
}

// Keys enrolled through the platform are read back into the registry a
// verifier folds with, so a running client sees an enrollment without a
// restart.
func TestGivenEnrolledKeysWhenSnapshotThenShouldVerifyAgainstThem(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	store := newKeyStore()
	handle := kvHandle(store, laser.Options{})

	if err := handle.KeyRegistry().Enroll(
		context.Background(), "user-7", key.VerifyingKey()); err != nil {
		t.Fatalf("enroll: %v", err)
	}
	registry, err := handle.KeyRegistry().Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	envelope := signedEnvelope(t)
	signature, err := key.Sign(envelope)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	envelope.Signature = signature
	principal, err := registry.Verify(envelope)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if principal != "user-7" {
		t.Errorf("want the enrolled principal, got %q", principal)
	}
	// It is stored under the canonical key id, so two clients find it the
	// same way.
	if _, held := store.entry(hex.EncodeToString(key.KeyID())); !held {
		t.Error("a key is stored under the id its public key derives to")
	}
}

// Revoking a key stops it verifying everywhere that reads the registry.
func TestGivenARevokedKeyWhenSnapshotThenShouldNoLongerVerify(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	store := newKeyStore()
	handle := kvHandle(store, laser.Options{})
	registry := handle.KeyRegistry()
	if err := registry.Enroll(
		context.Background(), "user-7", key.VerifyingKey()); err != nil {
		t.Fatalf("enroll: %v", err)
	}

	if _, err := registry.Revoke(context.Background(), key.KeyID()); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	snapshot, err := registry.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	envelope := signedEnvelope(t)
	signature, _ := key.Sign(envelope)
	envelope.Signature = signature
	if _, err := snapshot.Verify(envelope); err == nil {
		t.Error("a revoked key must not verify")
	}
	// Revoking what is already revoked is what the caller asked for, so it
	// is not an error, and it writes nothing: the key is already off.
	writes := len(store.conditions)
	version, err := registry.Revoke(context.Background(), key.KeyID())
	if err != nil {
		t.Fatalf("revoking twice must succeed: %v", err)
	}
	if len(store.conditions) != writes {
		t.Error("revoking an already revoked key must write nothing")
	}
	held, _ := store.entry(hex.EncodeToString(key.KeyID()))
	if version != held.Version {
		t.Errorf("want the version it already had, got %d", version)
	}
}

// Revoking something that was never enrolled, or naming an id that is not
// one, is refused rather than silently doing nothing.
func TestGivenAnUnenrolledKeyWhenRevokedThenShouldRefuseIt(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	handle := kvHandle(newKeyStore(), laser.Options{})

	if _, err := handle.KeyRegistry().Revoke(
		context.Background(), key.KeyID()); err == nil {
		t.Error("revoking a key nobody enrolled must be refused")
	}
	_, err := handle.KeyRegistry().Revoke(context.Background(), []byte{1, 2, 3})
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if invalid.Reason != "an Ed25519 key id must be 8 bytes" {
		t.Errorf("want the key id width named, got %q", invalid.Reason)
	}
}

// A record stored under an id that is not its own is one somebody moved, so
// revoking it in place would disable the wrong key.
func TestGivenAMisfiledRecordWhenRevokedThenShouldRefuseIt(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	other, _ := laser.GenerateSigningKey()
	store := newKeyStore()
	handle := kvHandle(store, laser.Options{})
	misfiled := wire.NewKeyRecord("impostor", laser.KeyIDOf(other.VerifyingKey()),
		other.VerifyingKey(), wire.KeyAgent)
	encoded, err := wire.Marshal(misfiled)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	store.put(hex.EncodeToString(key.KeyID()), encoded)

	_, err = handle.KeyRegistry().Revoke(context.Background(), key.KeyID())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if invalid.Reason != "the key record identifier differs from its storage key" {
		t.Errorf("want the mismatch named, got %q", invalid.Reason)
	}
}

// One corrupt enrollment must not stop every other principal's records from
// verifying, so a stored entry that does not hold up is skipped.
func TestGivenACorruptEnrollmentWhenSnapshotThenShouldSkipItAlone(t *testing.T) {
	good, _ := laser.GenerateSigningKey()
	other, _ := laser.GenerateSigningKey()
	store := newKeyStore()
	handle := kvHandle(store, laser.Options{})
	if err := handle.KeyRegistry().Enroll(
		context.Background(), "user-7", good.VerifyingKey()); err != nil {
		t.Fatalf("enroll: %v", err)
	}

	// A value that is not a record at all.
	store.put("0011223344556677", []byte("not a key record"))
	// A record filed under an id that is not its own, which is how a forged
	// entry would try to answer for somebody else's key.
	misfiled := wire.NewKeyRecord("impostor", laser.KeyIDOf(other.VerifyingKey()),
		other.VerifyingKey(), wire.KeyAgent)
	encoded, err := wire.Marshal(misfiled)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	store.put("7766554433221100", encoded)
	// A record whose named id is not what its public key derives to.
	forged := wire.NewKeyRecord("impostor", []byte{1, 2, 3, 4, 5, 6, 7, 8},
		other.VerifyingKey(), wire.KeyAgent)
	encoded, err = wire.Marshal(forged)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	store.put("0102030405060708", encoded)
	// A record that decodes but does not hold up: no principal is bound to
	// it, so there is nothing for a verification to return.
	nameless, _ := laser.GenerateSigningKey()
	unbound := wire.NewKeyRecord("", laser.KeyIDOf(nameless.VerifyingKey()),
		nameless.VerifyingKey(), wire.KeyAgent)
	encoded, err = wire.Marshal(unbound)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	store.put(hex.EncodeToString(unbound.KeyID), encoded)

	registry, err := handle.KeyRegistry().Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if _, held := registry.Enrolled(good.KeyID()); !held {
		t.Error("the sound enrollment must survive its neighbours")
	}
	if _, held := registry.Enrolled(laser.KeyIDOf(other.VerifyingKey())); held {
		t.Error("a misfiled record must not enroll")
	}
	if _, held := registry.Enrolled([]byte{1, 2, 3, 4, 5, 6, 7, 8}); held {
		t.Error("a record naming an id its key does not derive to must not enroll")
	}
	if _, held := registry.Enrolled(laser.KeyIDOf(nameless.VerifyingKey())); held {
		t.Error("a record bound to no principal must not enroll")
	}
}

// An enrollment that races another under the same id must not silently win,
// so the write is conditional on what is there.
func TestGivenARacingEnrollmentWhenWrittenThenShouldBeConditional(t *testing.T) {
	key, _ := laser.GenerateSigningKey()
	store := newKeyStore()
	handle := kvHandle(store, laser.Options{})

	if err := handle.KeyRegistry().Enroll(
		context.Background(), "user-7", key.VerifyingKey()); err != nil {
		t.Fatalf("enroll: %v", err)
	}

	// The first write expects nothing to be there, the second expects the
	// version it read.
	if store.conditions[0] != "absent" {
		t.Errorf("want the first write conditional on absence, got %q",
			store.conditions[0])
	}
	windowed, err := laser.AgentKey("user-7", key.VerifyingKey()).
		ValidWindow(100, nil)
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	if _, err := handle.KeyRegistry().EnrollRecord(
		context.Background(), windowed); err != nil {
		t.Fatalf("re-enroll: %v", err)
	}
	if store.conditions[1] != "version" {
		t.Errorf("want the second write conditional on the version, got %q",
			store.conditions[1])
	}
}

// The registry uses a reserved namespace, and a deployment can put it
// somewhere else.
func TestGivenAKeyNamespaceWhenOpenedThenShouldDefaultAndBeOverridable(t *testing.T) {
	handle := kvHandle(newKeyStore(), laser.Options{})

	if handle.KeyRegistry().Namespace() != laser.DefaultKeyNamespace {
		t.Errorf("want the reserved namespace, got %q",
			handle.KeyRegistry().Namespace())
	}
	if handle.KeyRegistryIn("tenant.keys").Namespace() != "tenant.keys" {
		t.Error("a deployment can put its keys somewhere else")
	}
}

// Epoch micros is the clock every key lifecycle and record timestamp is
// measured in, so a caller holding a time can ask the registry directly.
func TestGivenAnInstantWhenConvertedThenShouldBeEpochMicros(t *testing.T) {
	at := time.Unix(1700000000, 123456000).UTC()

	if laser.MicrosOf(at) != 1700000000123456 {
		t.Errorf("want epoch micros, got %d", laser.MicrosOf(at))
	}
}

// ed25519 is what the contract names, so a key of another kind is not one.
func TestGivenTheSigningSchemeWhenInspectedThenShouldBeEd25519(t *testing.T) {
	key, _ := laser.GenerateSigningKey()

	if len(key.VerifyingKey()) != ed25519.PublicKeySize {
		t.Errorf("want a 32 byte public key, got %d", len(key.VerifyingKey()))
	}
	if len(key.Seed()) != laser.SigningSeedBytes {
		t.Errorf("want a 32 byte seed, got %d", len(key.Seed()))
	}
	envelope := signedEnvelope(t)
	signature, err := key.Sign(envelope)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if len(signature.Bytes) != laser.SignatureBytes {
		t.Errorf("want a 64 byte signature, got %d", len(signature.Bytes))
	}
}

// keyStore is an in-memory key-value backend, so the managed key registry is
// exercised against a store that really holds what it wrote.
type keyStore struct {
	iggcon.Client
	guard   sync.Mutex
	entries map[string]wire.KvEntry
	version uint64
	// conditions records what each write was conditional on, so a test can
	// see that an enrollment never overwrites blindly.
	conditions []string
}

func newKeyStore() *keyStore {
	return &keyStore{entries: map[string]wire.KvEntry{}}
}

func (store *keyStore) Close() error { return nil }

// entry is what the store holds under a key.
func (store *keyStore) entry(key string) (wire.KvEntry, bool) {
	store.guard.Lock()
	defer store.guard.Unlock()
	held, present := store.entries[key]
	return held, present
}

// put writes a value directly, for a test seeding something the registry
// would never write itself.
func (store *keyStore) put(key string, value []byte) {
	store.guard.Lock()
	defer store.guard.Unlock()
	store.version++
	store.entries[key] = wire.KvEntry{
		Key: []byte(key), Value: value, Version: store.version,
	}
}

func (store *keyStore) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	body := payload
	if wire.IsIdempotentManagedRequest(code) {
		var envelope wire.ManagedRequestEnvelope
		if err := wire.Unmarshal(payload, &envelope); err != nil {
			return nil, err
		}
		body = envelope.Payload
	}
	outcome, failure := store.apply(code, body)
	if failure != nil {
		return wire.Marshal(wire.ErrReply(*failure))
	}
	return wire.Marshal(wire.OkReply(outcome))
}

func (store *keyStore) apply(code uint32, body []byte) (wire.KvOutcome, *wire.KvError) {
	store.guard.Lock()
	defer store.guard.Unlock()
	switch code {
	case wire.AgdxKvGetCode:
		var request wire.KvGet
		if wire.Unmarshal(body, &request) != nil {
			return wire.KvOutcome{}, &wire.KvError{Kind: wire.KvInvalidKey}
		}
		held, present := store.entries[string(request.Key)]
		if !present {
			return wire.KvOutcome{Kind: wire.KvValue}, nil
		}
		return wire.KvOutcome{Kind: wire.KvValue, Entry: &held}, nil
	case wire.AgdxKvCasCode:
		var request wire.KvCas
		if wire.Unmarshal(body, &request) != nil {
			return wire.KvOutcome{}, &wire.KvError{Kind: wire.KvInvalidKey}
		}
		key := string(request.Key)
		held, present := store.entries[key]
		switch request.Expect.Kind {
		case wire.CasAbsent:
			store.conditions = append(store.conditions, "absent")
			if present {
				return wire.KvOutcome{}, &wire.KvError{Kind: wire.KvVersionConflict}
			}
		default:
			store.conditions = append(store.conditions, "version")
			if !present || held.Version != request.Expect.Version {
				return wire.KvOutcome{}, &wire.KvError{Kind: wire.KvVersionConflict}
			}
		}
		store.version++
		store.entries[key] = wire.KvEntry{
			Key: request.Key, Value: request.Value, Version: store.version,
		}
		return wire.KvOutcome{Kind: wire.KvCommitted, Version: store.version}, nil
	case wire.AgdxKvScanCode:
		items := make([]wire.KvEntry, 0, len(store.entries))
		for _, held := range store.entries {
			items = append(items, held)
		}
		sort.Slice(items, func(left, right int) bool {
			return string(items[left].Key) < string(items[right].Key)
		})
		return wire.KvOutcome{Kind: wire.KvPageOutcome, Page: &wire.KvPage{
			Items: items, Page: 1, TotalPages: 1,
			TotalResults: uint32(len(items)),
		}}, nil
	default:
		return wire.KvOutcome{}, &wire.KvError{Kind: wire.KvUnsupported}
	}
}

// The preimage a signature covers is the contract's, not this client's: the
// domain separator, then the bound context, then the envelope with its
// signature cleared. These vectors come from the Rust client signing the same
// envelope with the same seed, so a change to any part of the preimage here
// shows up as a signature no other client can verify.
func TestGivenTheSharedSigningVectorWhenSignedThenShouldMatchTheRustClient(t *testing.T) {
	seed := bytes.Repeat([]byte{7}, laser.SigningSeedBytes)
	key, err := laser.SigningKeyFromSeed(seed)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if hex.EncodeToString(key.KeyID()) != "fe812c12f3ab4ce6" {
		t.Fatalf("want the shared key id, got %s", hex.EncodeToString(key.KeyID()))
	}
	if hex.EncodeToString(key.VerifyingKey()) !=
		"ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c" {
		t.Fatalf("want the shared public key, got %s",
			hex.EncodeToString(key.VerifyingKey()))
	}

	envelope := sharedVectorEnvelope(t)

	plain, err := key.Sign(envelope)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if hex.EncodeToString(plain.Bytes) !=
		"97599d474e006ba3fa620a4d53feb053c8d2762181b3a15657cc0e495c1c56af"+
			"b8428b8032abad02bb5beb127dcf6643d4e63d664a8ffbe05820a2e9fac34203" {
		t.Errorf("the unbound preimage differs from the shared one, got %s",
			hex.EncodeToString(plain.Bytes))
	}

	contentType, version := uint8(1), uint32(1)
	bound, err := key.SignWithContext(envelope, wire.SignatureContext{
		ContentType: &contentType, AgentVersion: &version,
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if hex.EncodeToString(bound.Bytes) !=
		"e0073f1869f180b6c0586a5844e2f8b87128b92a8924ea33042917084d39d78b"+
			"9278da1dfd160a7b6c5ddc3e97c8be25bf5a7969338793cfab498018cbed8606" {
		t.Errorf("the bound preimage differs from the shared one, got %s",
			hex.EncodeToString(bound.Bytes))
	}
}

// sharedVectorEnvelope is the envelope the shared vector was signed over.
func sharedVectorEnvelope(t *testing.T) wire.AgentEnvelope {
	t.Helper()
	source, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	record := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 1})}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes([16]byte{15: 3})}
	return wire.AgentEnvelope{
		Kind:         wire.KindCommand,
		Record:       &record,
		Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 2})},
		Source:       source,
		Correlation:  &correlation,
		Body:         []byte("{}"),
	}
}
