package laser

import "context"

// ResolveOperation proves the outcome of a request this client could not
// confirm, naming the id a TransportError reported through
// UncertainOperation.
//
// It replays the retained mutation until the server gives its final result
// or ctx ends. The point is that the outcome is settled rather than guessed:
// a mutation whose reply was lost may have applied, and the only honest way
// to find out is to ask the deployment that holds it.
//
// The transport keeps the outcomes its replays proved, so asking again for
// the same operation answers from that record without another request. An
// operation the client never proved, because a session reset or an abandon
// came first or the record aged out, is reported as not retained.
func (laser *Laser) ResolveOperation(ctx context.Context, operationID uint64) error {
	client, err := laser.connected()
	if err != nil {
		return err
	}
	if err := client.ResolveOperation(ctx, operationID); err != nil {
		return &TransportError{Reason: "resolve the operation", Cause: err}
	}
	return nil
}

// AbandonOperation gives up on a request whose outcome was never proved,
// freeing the lane it blocks.
//
// It forgets local recovery state and cannot undo anything the server did:
// an abandoned operation may well have applied, and nothing afterwards will
// say whether it did. That is the trade for unblocking the lane, so it is
// the second choice after ResolveOperation rather than the quick way out.
func (laser *Laser) AbandonOperation(ctx context.Context, operationID uint64) error {
	client, err := laser.connected()
	if err != nil {
		return err
	}
	if err := client.AbandonOperation(ctx, operationID); err != nil {
		return &TransportError{Reason: "abandon the operation", Cause: err}
	}
	return nil
}
