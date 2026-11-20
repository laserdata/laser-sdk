package laser

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// scratchRandomBytes is four bytes, which renders as the eight lowercase hex
// characters every client appends to a scratch prefix.
const scratchRandomBytes = 4

// ScratchStream is a stream that lives for one program run, built with
// Laser.ScratchStream. Laser addresses it as the default stream and Delete
// removes it with every topic and message in it.
//
// Unlike the Rust and Python clients, nothing deletes the stream when the
// handle goes out of scope: Go has no destructor, and a background delete
// would need a context the caller never gave. Pair the call with a deferred
// Delete instead, so an abandoned stream is a visible omission rather than a
// cleanup that silently did not run.
type ScratchStream struct {
	laser   *Laser
	name    string
	deleted atomic.Bool
}

// ScratchStream creates a uniquely named stream for one program run, prefix
// followed by a hyphen and eight lowercase hex characters, and returns it
// with a handle whose default stream it is.
//
// A stream of that name that already exists is refused rather than reused,
// because reuse would hand a caller another run's records as if they were
// its own.
func (laser *Laser) ScratchStream(
	ctx context.Context, prefix string,
) (*ScratchStream, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	random := make([]byte, scratchRandomBytes)
	if _, err := rand.Read(random); err != nil {
		return nil, &TransportError{
			Reason: "draw a scratch stream name", Cause: err}
	}
	name := prefix + "-" + hex.EncodeToString(random)
	if _, err := iggcon.NewIdentifier(name); err != nil {
		return nil, &InvalidError{
			Reason: "scratch stream name `" + name + "`: " + err.Error()}
	}
	if _, err := client.CreateStream(ctx, name, &iggcon.StreamOptions{}); err != nil {
		return nil, &TransportError{Reason: "create the scratch stream", Cause: err}
	}
	return &ScratchStream{laser: laser.WithDefaultStream(name), name: name}, nil
}

// Name is the stream's unique name.
func (scratch *ScratchStream) Name() string { return scratch.name }

// Laser is a handle on the same connection whose default stream is this one.
func (scratch *ScratchStream) Laser() *Laser { return scratch.laser }

// Delete removes the stream. It is idempotent, so a deferred call after an
// explicit one is not an error, and neither is a stream something else
// already removed.
func (scratch *ScratchStream) Delete(ctx context.Context) error {
	if scratch.deleted.Load() {
		return nil
	}
	if _, err := scratch.laser.Stream(scratch.name).Delete(ctx); err != nil {
		return err
	}
	// Recorded only once the server confirmed, so a failed delete stays
	// retryable rather than being remembered as done.
	scratch.deleted.Store(true)
	return nil
}
