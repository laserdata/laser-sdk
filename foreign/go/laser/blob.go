package laser

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// BlobStore is where claim-checked bodies live.
//
// No store ships with this client. The deployment chooses one: an
// S3-compatible bucket, the key-value surface for small overflow, a
// filesystem in development. A default would be wrong everywhere, because a
// body externalized to the wrong place is a body nobody can read back.
type BlobStore interface {
	// Put stores a payload and returns the reference a capsule will carry.
	Put(ctx context.Context, payload []byte) (string, error)
	// Get fetches the bytes behind a reference.
	Get(ctx context.Context, reference string) ([]byte, error)
}

// IntegrityError reports a store that returned something other than what the
// producer externalized.
//
// It is its own kind because the alternative is handing a caller unverified
// bytes: a body that fails its digest is not a transport problem to retry, it
// is a claim that did not hold.
type IntegrityError struct{ Reference string }

func (e *IntegrityError) Error() string {
	return "the body behind " + e.Reference +
		" is not what its capsule says it is"
}

// CheckIn externalizes a payload at or over a threshold, replacing it with
// the capsule that names where it went.
//
// Under the threshold the payload comes back untouched, byte-identical to no
// claim check at all, so turning the feature on changes nothing for the
// records that never needed it.
func CheckIn(
	ctx context.Context, store BlobStore, thresholdBytes int, payload []byte,
) ([]byte, *wire.ContentType, error) {
	if store == nil {
		return nil, nil, &InvalidError{
			Reason: "a claim check needs a blob store to externalize to"}
	}
	if len(payload) < thresholdBytes {
		return payload, nil, nil
	}
	digest := sha256.Sum256(payload)
	size := uint64(len(payload))
	reference, err := store.Put(ctx, payload)
	if err != nil {
		return nil, nil, err
	}
	capsule := wire.NewBodyRef(reference, size, digest)
	encoded, err := wire.Marshal(capsule)
	if err != nil {
		return nil, nil, &CodecError{Reason: "encode the body reference", Cause: err}
	}
	contentType := wire.ContentBodyRef
	return encoded, &contentType, nil
}

// ResolveBody fetches a claim-checked body and verifies it against its
// capsule.
//
// The digest is the reader's proof that the store returned what the producer
// externalized. A mismatch is refused rather than handed over, because a body
// nobody can vouch for is worse than no body at all.
func ResolveBody(
	ctx context.Context, store BlobStore, payload []byte,
) ([]byte, error) {
	if store == nil {
		return nil, &InvalidError{
			Reason: "a claim-checked body needs a blob store to resolve it"}
	}
	var capsule wire.BodyRef
	if err := wire.Unmarshal(payload, &capsule); err != nil {
		return nil, &CodecError{Reason: "decode the body reference", Cause: err}
	}
	// The capsule came off the log, so its reference is untrusted input to
	// whatever store the caller supplied. The contract's bounds are enforced
	// before it is handed over.
	if err := capsule.Validate(); err != nil {
		return nil, &InvalidError{Reason: "body reference: " + err.Error()}
	}
	body, err := store.Get(ctx, capsule.Reference)
	if err != nil {
		return nil, err
	}
	// The capsule states the size it externalized, so a length that disagrees
	// is already a failed claim check, caught before hashing anything.
	if uint64(len(body)) != capsule.SizeBytes {
		return nil, &IntegrityError{Reference: capsule.Reference}
	}
	digest := sha256.Sum256(body)
	if !bytes.Equal(digest[:], capsule.SHA256) {
		return nil, &IntegrityError{Reference: capsule.Reference}
	}
	return body, nil
}

// ClaimCheck externalizes this send's body when it reaches a threshold.
//
// It runs after the governor and before signing, so a policy decides on the
// body the producer meant and a signature covers the capsule the log actually
// carries.
func (send *AgdxSend) ClaimCheck(store BlobStore, thresholdBytes int) *AgdxSend {
	send.blobStore, send.claimThreshold = store, thresholdBytes
	return send
}

// ResolveBody is the real body of a claim-checked message.
//
// A body stamped as a reference is decoded, fetched and verified. Any other
// content type is the body itself, so a caller resolves uniformly without
// having to ask first.
func (message *AgdxMessage) ResolveBody(
	ctx context.Context, store BlobStore,
) ([]byte, error) {
	if message.ContentType() != wire.ContentBodyRef {
		return bytes.Clone(message.Envelope.Body), nil
	}
	return ResolveBody(ctx, store, message.Envelope.Body)
}
