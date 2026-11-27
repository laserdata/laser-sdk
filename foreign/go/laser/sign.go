package laser

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The signing constants, pinned by the contract.
const (
	// KeyIDBytes is how much of a public key's SHA-256 names it.
	KeyIDBytes = wire.KeyIDBytes
	// SignatureBytes is the Ed25519 detached-signature length.
	SignatureBytes = ed25519.SignatureSize
	// SigningSeedBytes is the Ed25519 secret seed length.
	SigningSeedBytes = ed25519.SeedSize
	// DefaultKeyNamespace is the key-value namespace a managed registry uses.
	DefaultKeyNamespace = "agent.keys"
)

// SignatureError reports a signature that could not be produced or trusted.
//
// It is its own kind because an unverifiable signature is neither a transport
// problem nor a malformed value: it is a claim that did not hold.
type SignatureError struct{ Reason string }

func (e *SignatureError) Error() string { return "signature: " + e.Reason }

// signingInput is the canonical bytes a signature covers: the domain
// separator, then the context when one is bound, then the named-field
// encoding of the envelope with its signature cleared.
//
// Signing and verifying build the identical input, so a decode and re-encode
// has to be byte-identical, which is the round-trip property the fixture
// corpus pins. The context is prepended rather than appended so a signed
// record's interpretation attributes cannot be flipped by an intermediary,
// and an absent context reproduces the pre-context preimage exactly, so a
// signature written before contexts existed still verifies.
func signingInput(
	envelope wire.AgentEnvelope, context *wire.SignatureContext,
) ([]byte, error) {
	bare := envelope
	bare.Signature = nil
	body, err := wire.Marshal(bare)
	if err != nil {
		return nil, &CodecError{Reason: "encode the envelope to sign", Cause: err}
	}
	var contextBytes []byte
	if context != nil {
		contextBytes, err = wire.Marshal(*context)
		if err != nil {
			return nil, &CodecError{
				Reason: "encode the signature context", Cause: err}
		}
	}
	input := make([]byte, 0,
		len(wire.SignatureDomain)+len(contextBytes)+len(body))
	input = append(input, wire.SignatureDomain...)
	input = append(input, contextBytes...)
	return append(input, body...), nil
}

// KeyIDOf is a public key's identity: the first bytes of its SHA-256.
func KeyIDOf(verifying ed25519.PublicKey) []byte {
	digest := sha256.Sum256(verifying)
	return digest[:KeyIDBytes]
}

// SigningKey is an Ed25519 signing key.
//
// Sign an envelope to produce the detached signature to attach to it. The
// seed is the secret: store it the way any secret is stored, and enroll only
// the verifying key.
type SigningKey struct {
	private ed25519.PrivateKey
	keyID   []byte
}

// GenerateSigningKey mints a key from the operating system's random source.
func GenerateSigningKey() (*SigningKey, error) {
	var seed [SigningSeedBytes]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, &SignatureError{
			Reason: "the operating system random source failed: " + err.Error()}
	}
	return SigningKeyFromSeed(seed[:])
}

// SigningKeyFromSeed rebuilds a key from its secret seed.
func SigningKeyFromSeed(seed []byte) (*SigningKey, error) {
	if len(seed) != SigningSeedBytes {
		return nil, &InvalidError{
			Reason: "an Ed25519 seed requires exactly 32 bytes"}
	}
	private := ed25519.NewKeyFromSeed(seed)
	return &SigningKey{
		private: private,
		keyID:   KeyIDOf(private.Public().(ed25519.PublicKey)),
	}, nil
}

// Seed is the secret this key rebuilds from.
func (key *SigningKey) Seed() []byte { return bytes.Clone(key.private.Seed()) }

// KeyID is this key's identity, the one it stamps on every signature.
func (key *SigningKey) KeyID() []byte { return bytes.Clone(key.keyID) }

// VerifyingKey is the public key to enroll in a registry.
func (key *SigningKey) VerifyingKey() ed25519.PublicKey {
	return key.private.Public().(ed25519.PublicKey)
}

// Sign signs an envelope, covering the envelope body alone.
func (key *SigningKey) Sign(envelope wire.AgentEnvelope) (*wire.Signature, error) {
	return key.sign(envelope, nil)
}

// SignWithContext signs an envelope binding its interpretation attributes,
// the content type and wire version, into the preimage.
//
// Without that binding an intermediary could flip the codec or the wire
// version on a signed record and the signature would still verify, which
// would let the same bytes be read as something else entirely. The context is
// stored on the signature so a verifier rebuilds the same preimage.
func (key *SigningKey) SignWithContext(
	envelope wire.AgentEnvelope, context wire.SignatureContext,
) (*wire.Signature, error) {
	return key.sign(envelope, &context)
}

func (key *SigningKey) sign(
	envelope wire.AgentEnvelope, context *wire.SignatureContext,
) (*wire.Signature, error) {
	input, err := signingInput(envelope, context)
	if err != nil {
		return nil, err
	}
	return &wire.Signature{
		Scheme:  wire.SignatureSchemeEd25519,
		KeyID:   bytes.Clone(key.keyID),
		Bytes:   ed25519.Sign(key.private, input),
		Context: context,
	}, nil
}

// VerifiedPrincipal is a verified signer: the enrolled principal and the kind
// of key it holds.
type VerifiedPrincipal struct {
	// Principal is the authenticated identity the key was enrolled under.
	Principal string
	// Kind says whether the key may sign privileged control facts.
	Kind wire.KeyKind
}

// KeyRecord is an enrolled key with its lifecycle.
//
// The window bounds when the key may sign, and revocation disables it
// whatever the window says. A rotation enrols the new key while the old one's
// window still covers the records already in flight, which is what makes a
// rotation something other than an outage.
type KeyRecord struct {
	// Principal is the authenticated identity this key is bound to.
	Principal string
	// Verifying is the public key.
	Verifying ed25519.PublicKey
	// Kind says whether this is an operator or an agent key.
	Kind wire.KeyKind
	// ValidFromMicros is when the key starts being usable.
	ValidFromMicros uint64
	// ValidToMicros is when it stops, nil for open-ended.
	ValidToMicros *uint64
	// Revoked disables the key whatever its window says.
	Revoked bool
}

// KeyID is the identity derived from this record's public key.
func (record KeyRecord) KeyID() []byte { return KeyIDOf(record.Verifying) }

// AgentKey is an always-valid key that may sign only its own agent messages.
func AgentKey(principal string, verifying ed25519.PublicKey) KeyRecord {
	return KeyRecord{Principal: principal, Verifying: verifying, Kind: wire.KeyAgent}
}

// OperatorKey is an always-valid key that may also sign privileged control
// facts.
//
// Quarantining an agent is such a fact, so an enrolled agent key cannot
// quarantine another agent: the kind is what separates the two.
func OperatorKey(principal string, verifying ed25519.PublicKey) KeyRecord {
	return KeyRecord{Principal: principal, Verifying: verifying, Kind: wire.KeyOperator}
}

// KeyRecordFromBytes builds a record from a raw public key.
func KeyRecordFromBytes(
	principal string, verifying []byte, kind wire.KeyKind,
) (KeyRecord, error) {
	if len(verifying) != ed25519.PublicKeySize {
		return KeyRecord{}, &InvalidError{
			Reason: "an Ed25519 public key requires exactly 32 bytes"}
	}
	return KeyRecord{
		Principal: principal,
		Verifying: ed25519.PublicKey(bytes.Clone(verifying)),
		Kind:      kind,
	}, nil
}

// ValidWindow bounds when the key may sign. An end at or before the start is
// refused, since such a key could never verify at all.
func (record KeyRecord) ValidWindow(from uint64, to *uint64) (KeyRecord, error) {
	if to != nil && *to <= from {
		return KeyRecord{}, &InvalidError{
			Reason: "a key validity window must end after it starts"}
	}
	record.ValidFromMicros, record.ValidToMicros = from, to
	return record, nil
}

// Revoke marks the key disabled.
func (record KeyRecord) Revoke() KeyRecord {
	record.Revoked = true
	return record
}

// KeyRegistry holds enrolled verifying keys, each bound to the authenticated
// principal that enrolled it.
//
// Verifying a signed envelope returns the bound principal, which a caller
// then compares against the server-stamped identity. That comparison is the
// authorship binding: a key proves the principal it was enrolled under, never
// the source an envelope claims for itself.
//
// A registry is shared state, so enrolling a revoked record reaches every
// verifier using it at once rather than on the next restart.
type KeyRegistry struct {
	guard sync.RWMutex
	keys  map[string]KeyRecord
}

// NewKeyRegistry opens an empty registry.
func NewKeyRegistry() *KeyRegistry {
	return &KeyRegistry{keys: map[string]KeyRecord{}}
}

// Enroll adds an always-valid agent key bound to a principal.
func (registry *KeyRegistry) Enroll(principal string, verifying ed25519.PublicKey) {
	registry.EnrollRecord(AgentKey(principal, verifying))
}

// EnrollOperator adds an always-valid operator key.
func (registry *KeyRegistry) EnrollOperator(
	principal string, verifying ed25519.PublicKey,
) {
	registry.EnrollRecord(OperatorKey(principal, verifying))
}

// EnrollRecord adds a key with an explicit kind and lifecycle. Enrolling
// under an id already held replaces it, which is how a key is revoked or its
// window changed.
func (registry *KeyRegistry) EnrollRecord(record KeyRecord) {
	registry.guard.Lock()
	defer registry.guard.Unlock()
	if registry.keys == nil {
		registry.keys = map[string]KeyRecord{}
	}
	registry.keys[string(record.KeyID())] = record
}

// Enrolled is the record held under one key id, and whether one is held.
func (registry *KeyRegistry) Enrolled(keyID []byte) (KeyRecord, bool) {
	registry.guard.RLock()
	defer registry.guard.RUnlock()
	record, held := registry.keys[string(keyID)]
	return record, held
}

// Verify checks an envelope's signature and returns the enrolled principal.
//
// It ignores the validity window, because it reads no clock. A check against
// a key's lifetime is VerifyAt.
func (registry *KeyRegistry) Verify(envelope wire.AgentEnvelope) (string, error) {
	verified, err := registry.check(envelope, nil)
	if err != nil {
		return "", err
	}
	return verified.Principal, nil
}

// VerifyAt checks a signature as of an instant, so a key outside its window
// fails even though its bytes are still correct.
func (registry *KeyRegistry) VerifyAt(
	envelope wire.AgentEnvelope, atMicros uint64,
) (*VerifiedPrincipal, error) {
	return registry.check(envelope, &atMicros)
}

// VerifyObservedAt checks a log-resident envelope against the interpretation
// headers the record actually carried, at the record's own timestamp.
//
// A signature whose bound context differs from what was observed is refused
// before any cryptography runs: the bytes may well verify, but they verify a
// record that is being presented as something it is not.
func (registry *KeyRegistry) VerifyObservedAt(
	envelope wire.AgentEnvelope, observed wire.SignatureContext, atMicros uint64,
) (*VerifiedPrincipal, error) {
	if envelope.Signature == nil || envelope.Signature.Context == nil {
		return nil, &SignatureError{
			Reason: "the signature does not bind the record headers"}
	}
	if !signatureContextsEqual(*envelope.Signature.Context, observed) {
		return nil, &SignatureError{
			Reason: "the signature context differs from the record headers"}
	}
	return registry.check(envelope, &atMicros)
}

// signatureContextsEqual compares two contexts by the values their optional
// fields point to, not by pointer identity.
//
// A plain "!=" on wire.SignatureContext compares its *uint8/*uint32 fields as
// addresses, so two contexts built independently -- one at signing time, one
// reconstructed from a record's own headers -- would always differ even
// when they carry the same value, since they are never the same allocation.
func signatureContextsEqual(first, second wire.SignatureContext) bool {
	return equalPtr(first.ContentType, second.ContentType) &&
		equalPtr(first.AgentVersion, second.AgentVersion)
}

// equalPtr compares two optional values: equal when both are nil, unequal
// when only one is, and compared by value when both are set.
func equalPtr[T comparable](first, second *T) bool {
	if first == nil || second == nil {
		return first == second
	}
	return *first == *second
}

func (registry *KeyRegistry) check(
	envelope wire.AgentEnvelope, atMicros *uint64,
) (*VerifiedPrincipal, error) {
	signature := envelope.Signature
	if signature == nil {
		return nil, &SignatureError{Reason: "the envelope is not signed"}
	}
	if signature.Scheme != wire.SignatureSchemeEd25519 {
		return nil, &SignatureError{Reason: "unsupported signature scheme " +
			itoa(int(signature.Scheme))}
	}
	record, enrolled := registry.Enrolled(signature.KeyID)
	if !enrolled {
		return nil, &SignatureError{Reason: "the signing key is not enrolled"}
	}
	if record.Revoked {
		return nil, &SignatureError{Reason: "the signing key is revoked"}
	}
	if atMicros != nil {
		if *atMicros < record.ValidFromMicros {
			return nil, &SignatureError{Reason: "the signing key is not yet valid"}
		}
		if record.ValidToMicros != nil && *atMicros >= *record.ValidToMicros {
			return nil, &SignatureError{Reason: "the signing key has expired"}
		}
	}
	if len(signature.Bytes) != SignatureBytes {
		return nil, &SignatureError{Reason: "malformed Ed25519 signature"}
	}
	input, err := signingInput(envelope, signature.Context)
	if err != nil {
		return nil, err
	}
	if !ed25519.Verify(record.Verifying, input, signature.Bytes) {
		return nil, &SignatureError{Reason: "the signature does not verify"}
	}
	return &VerifiedPrincipal{Principal: record.Principal, Kind: record.Kind}, nil
}

// Delegation is a signed on-behalf-of claim: who signed, and whom they acted
// for.
type Delegation struct {
	// Signer is the principal whose key signed the record.
	Signer string
	// User is whom the signer claims to have acted for.
	User string
}

// VerifyDelegation checks an on-behalf-of envelope and reads whom it acted
// for, nil when it carries no delegation.
//
// The claim rides inside the signed span rather than on a header, so a signer
// cannot forge whom it acted for and an intermediary cannot rewrite it. The
// server authorizes only the authenticated caller, so a caller intersects the
// two principals' grants itself: an agent must never exceed the user it acts
// for.
func VerifyDelegation(
	registry *KeyRegistry, envelope wire.AgentEnvelope,
	observed wire.SignatureContext, atMicros uint64,
) (*Delegation, error) {
	verified, err := registry.VerifyObservedAt(envelope, observed, atMicros)
	if err != nil {
		return nil, err
	}
	if envelope.Metadata == nil {
		return nil, nil
	}
	value, named := envelope.Metadata[wire.MetadataDelegatedBy]
	if !named || value.Kind != wire.ValueStr {
		return nil, nil
	}
	return &Delegation{Signer: verified.Principal, User: value.Str}, nil
}

// KvKeyRegistry is the managed sibling of KeyRegistry: lifecycle-aware key
// records in a reserved key-value namespace, keyed by their canonical id.
//
// Enroll keys through the platform, then snapshot them into the in-memory
// registry a verifier folds with. It needs the key-value surface, so a
// deployment without one is refused rather than silently verifying nothing.
type KvKeyRegistry struct {
	laser     *Laser
	namespace string
}

// KeyRegistry opens the managed key registry over the default namespace.
func (laser *Laser) KeyRegistry() *KvKeyRegistry {
	return laser.KeyRegistryIn(DefaultKeyNamespace)
}

// KeyRegistryIn opens the managed key registry over a namespace of its own.
func (laser *Laser) KeyRegistryIn(namespace string) *KvKeyRegistry {
	return &KvKeyRegistry{laser: laser, namespace: namespace}
}

// Namespace is where this registry stores its records.
func (registry *KvKeyRegistry) Namespace() string { return registry.namespace }

// Enroll stores a principal's agent key.
func (registry *KvKeyRegistry) Enroll(
	ctx context.Context, principal string, verifying ed25519.PublicKey,
) error {
	_, err := registry.EnrollRecord(ctx, AgentKey(principal, verifying))
	return err
}

// EnrollRecord stores or replaces a lifecycle-aware key record.
//
// The write is conditional on what is already there, so two administrators
// enrolling under one key id cannot both believe they won.
func (registry *KvKeyRegistry) EnrollRecord(
	ctx context.Context, record KeyRecord,
) (uint64, error) {
	stored := storedKeyRecord(record)
	if err := stored.Validate(); err != nil {
		return 0, &InvalidError{Reason: err.Error()}
	}
	payload, err := wire.Marshal(stored)
	if err != nil {
		return 0, &CodecError{Reason: "encode the key record", Cause: err}
	}
	kv := registry.laser.Kv(registry.namespace)
	key := []byte(hex.EncodeToString(stored.KeyID))
	current, err := kv.GetEntry(ctx, key)
	if err != nil {
		return 0, err
	}
	write := kv.Set(key).Bytes(payload)
	if current == nil {
		return write.ExpectAbsent().Commit(ctx)
	}
	return write.ExpectVersion(current.Version).Commit(ctx)
}

// Revoke disables an enrolled key.
//
// Revoking one already revoked is not an error: the caller wanted it off, and
// it is off.
func (registry *KvKeyRegistry) Revoke(
	ctx context.Context, keyID []byte,
) (uint64, error) {
	if len(keyID) != KeyIDBytes {
		return 0, &InvalidError{Reason: "an Ed25519 key id must be 8 bytes"}
	}
	kv := registry.laser.Kv(registry.namespace)
	key := []byte(hex.EncodeToString(keyID))
	entry, err := kv.GetEntry(ctx, key)
	if err != nil {
		return 0, err
	}
	if entry == nil {
		return 0, &InvalidError{Reason: "the signing key is not enrolled"}
	}
	var stored wire.KeyRecord
	if err := wire.Unmarshal(entry.Value, &stored); err != nil {
		return 0, &CodecError{Reason: "decode the key record", Cause: err}
	}
	if err := stored.Validate(); err != nil {
		return 0, &InvalidError{Reason: err.Error()}
	}
	// A record filed under another id is a record somebody moved, so it is
	// refused rather than revoked in place.
	if !bytes.Equal(stored.KeyID, keyID) {
		return 0, &InvalidError{
			Reason: "the key record identifier differs from its storage key"}
	}
	if stored.Revoked {
		return entry.Version, nil
	}
	stored.Revoked = true
	payload, err := wire.Marshal(stored)
	if err != nil {
		return 0, &CodecError{Reason: "encode the key record", Cause: err}
	}
	return kv.Set(key).Bytes(payload).
		ExpectVersion(entry.Version).Commit(ctx)
}

// Snapshot reads every enrolled key into an in-memory registry.
//
// An entry that does not hold a valid record is skipped rather than failing
// the read, because one corrupt enrollment must not stop every other
// principal's records from verifying.
func (registry *KvKeyRegistry) Snapshot(ctx context.Context) (*KeyRegistry, error) {
	entries, err := registry.laser.Kv(registry.namespace).Scan().All(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := NewKeyRegistry()
	for _, entry := range entries {
		record, usable := keyRecordOf(entry)
		if usable {
			snapshot.EnrollRecord(record)
		}
	}
	return snapshot, nil
}

// keyRecordOf reads one stored entry, reporting whether it is usable.
//
// Every check here is one a forged or corrupt entry would fail: the record
// has to decode, validate, be filed under its own id, hold a real public key,
// and name the id that key actually derives to.
func keyRecordOf(entry wire.KvEntry) (KeyRecord, bool) {
	var stored wire.KeyRecord
	if err := wire.Unmarshal(entry.Value, &stored); err != nil {
		return KeyRecord{}, false
	}
	if stored.Validate() != nil {
		return KeyRecord{}, false
	}
	if string(entry.Key) != hex.EncodeToString(stored.KeyID) {
		return KeyRecord{}, false
	}
	if len(stored.VerifyingKey) != ed25519.PublicKeySize {
		return KeyRecord{}, false
	}
	verifying := ed25519.PublicKey(bytes.Clone(stored.VerifyingKey))
	if !bytes.Equal(stored.KeyID, KeyIDOf(verifying)) {
		return KeyRecord{}, false
	}
	return KeyRecord{
		Principal:       stored.Principal,
		Verifying:       verifying,
		Kind:            stored.Kind,
		ValidFromMicros: stored.ValidFromMicros,
		ValidToMicros:   stored.ValidToMicros,
		Revoked:         stored.Revoked,
	}, true
}

// storedKeyRecord is the record as the registry topic carries it.
func storedKeyRecord(record KeyRecord) wire.KeyRecord {
	stored := wire.NewKeyRecord(record.Principal, record.KeyID(),
		bytes.Clone(record.Verifying), record.Kind)
	stored.ValidFromMicros = record.ValidFromMicros
	stored.ValidToMicros = record.ValidToMicros
	stored.Revoked = record.Revoked
	return stored
}

// Signed signs this message with a key, binding the interpretation attributes
// the record will carry.
//
// Signing is what turns an authorship claim into an authorship proof: a
// record on a shared topic says who sent it, and only a signature makes that
// something a reader can check.
func (send *AgdxSend) Signed(key *SigningKey) *AgdxSend {
	send.signer = key
	return send
}

// signContext is what a signature binds beyond the body: the content type and
// wire version the record is stamped with.
//
// A content type the contract has no code for binds nothing, because there is
// nothing stable to bind: the signature then covers the body alone.
func (send *AgdxSend) signContext() wire.SignatureContext {
	context := wire.SignatureContext{AgentVersion: ptrOf(uint32(wire.AgentOpVersion))}
	if code, named := send.contentType.Code(); named {
		context.ContentType = &code
	}
	return context
}

// ptrOf is the address of a value, for the optional fields the contract
// carries as pointers.
func ptrOf[T any](value T) *T { return &value }

// MicrosOf is the epoch microseconds of an instant, the clock every key
// lifecycle and record timestamp is measured in.
func MicrosOf(at time.Time) uint64 { return uint64(at.UnixMicro()) }
