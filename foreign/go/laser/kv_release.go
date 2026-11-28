package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// PreparedRelease retains the payload and operation identity for a repeatable lease release.
type PreparedRelease struct {
	client  *Laser
	payload []byte
	id      wire.U128
}

// PrepareRelease validates and encodes a release without sending it.
func (kv *Kv) PrepareRelease(key []byte, holder string, token uint64) (*PreparedRelease, error) {
	if err := kv.requireFencedLeases(); err != nil {
		return nil, err
	}
	if err := wire.ValidateKvNamespace(kv.namespace); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	request := wire.KvRelease{V: wire.KvLeaseOpVersion, Namespace: kv.ResourceNamespace(), Key: key, HolderID: holder, LeaseToken: token}
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode lease release", Cause: err}
	}
	return &PreparedRelease{client: kv.laser, payload: payload, id: NewOperationID()}, nil
}

// OperationID identifies this release across attempts.
func (release *PreparedRelease) OperationID() wire.U128 { return release.id }

// Execute releases the lease. Call Execute again on this value after an ambiguous outcome.
func (release *PreparedRelease) Execute(ctx context.Context) (bool, error) {
	if release == nil || release.client == nil {
		return false, &InvalidError{Reason: "lease release has not been prepared"}
	}
	raw, err := release.client.SendManagedWithOperation(ctx, wire.AgdxKvReleaseCode, release.payload, release.id)
	if err != nil {
		return false, err
	}
	outcome, err := decodeKvOutcome(raw)
	if err != nil {
		return false, err
	}
	if outcome.Kind != wire.KvReleased {
		return false, unexpectedKvOutcome("release", outcome.Kind)
	}
	return outcome.Released, nil
}
