package laser

import (
	"context"
	"strings"
	"sync"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// snapshotEveryDeltas is how many deltas a writer appends before it writes a
// snapshot.
//
// A reader folds from the newest snapshot forward, so without one a long-
// lived session's state costs more to read every time it is written to.
const snapshotEveryDeltas = 64

// stateCursor is the revision one writer continues from.
//
// It is shared by a session handle and every handle derived from it, so the
// writes of one process land in revision order rather than racing each other
// to the same base.
type stateCursor struct {
	mutex    sync.Mutex
	revision uint64
	// head is the operation id of the delta that produced revision, which is
	// what tells a delta continuing its own chain from one that resumed
	// another's.
	head *string
	// document is nil for a handle that has not written yet, or whose last
	// write failed and left the lane revision unknown.
	document      *wire.DynValue
	sinceSnapshot uint32
}

// SessionState is the session's state document: RFC 6902 patches on the
// session lane, applied in lane order, with snapshots as checkpoints.
type SessionState struct {
	session *Session
}

// State opens this session's state document.
func (session *Session) State() *SessionState {
	return &SessionState{session: session}
}

// Set sets one key to a value.
func (state *SessionState) Set(
	ctx context.Context, key string, value wire.DynValue,
) (*AgdxReceipt, error) {
	return state.Patch(ctx, []wire.PatchOperation{
		wire.AddOperation(statePointer(key), value),
	})
}

// Patch applies a patch atomically against the revision this handle last saw.
//
// A successful append does not prove the patch applied: another writer may
// have reached the same revision first, and a reader learns the outcome from
// the folded state rather than from this receipt.
func (state *SessionState) Patch(
	ctx context.Context, patch []wire.PatchOperation,
) (*AgdxReceipt, error) {
	cursor := state.session.cursor()
	cursor.mutex.Lock()
	defer cursor.mutex.Unlock()
	if err := state.seed(ctx, cursor); err != nil {
		return nil, err
	}
	return state.writeDelta(ctx, cursor, patch)
}

// Replace replaces the whole document, as one revision-guarded patch that
// removes the keys it drops and sets every key it holds.
func (state *SessionState) Replace(
	ctx context.Context, document wire.DynValue,
) (*AgdxReceipt, error) {
	next, object := documentFields(document)
	if !object {
		return nil, &InvalidError{Reason: "a state document is a JSON object"}
	}
	cursor := state.session.cursor()
	cursor.mutex.Lock()
	defer cursor.mutex.Unlock()
	if err := state.seed(ctx, cursor); err != nil {
		return nil, err
	}
	var patch []wire.PatchOperation
	if cursor.document != nil {
		current, held := documentFields(*cursor.document)
		if held {
			// Removals first, so a key the new document drops is gone before
			// the keys it keeps are set.
			for _, key := range sortedKeys(current) {
				if _, kept := next[key]; !kept {
					patch = append(patch, wire.RemoveOperation(statePointer(key)))
				}
			}
		}
	}
	for _, key := range sortedKeys(next) {
		patch = append(patch, wire.AddOperation(statePointer(key), next[key]))
	}
	if len(patch) == 0 {
		return nil, &InvalidError{
			Reason: "replacing an empty document with an empty one changes nothing"}
	}
	return state.writeDelta(ctx, cursor, patch)
}

// Snapshot writes the whole document this handle holds.
//
// It fails with a stale session error, writing nothing, when the session's
// document is no longer the one this handle holds, so a handle whose earlier
// write did not apply never overwrites a later change.
func (state *SessionState) Snapshot(ctx context.Context) (*AgdxReceipt, error) {
	cursor := state.session.cursor()
	cursor.mutex.Lock()
	defer cursor.mutex.Unlock()
	current, err := state.holdsCurrent(ctx, cursor)
	if err != nil {
		return nil, err
	}
	if !current {
		return nil, &SessionError{Failure: wire.SessionError{
			Kind:    wire.SessionStale,
			Message: "the session state changed since this handle's last write",
		}}
	}
	return state.writeSnapshot(ctx, cursor)
}

// Get folds the state from the whole retained session lane: the newest
// snapshot plus the deltas after it.
//
// Complete is false when retention removed the records the document starts
// from, which is the difference between a document that is empty and one
// whose beginning is gone.
func (state *SessionState) Get(ctx context.Context) (wire.SessionStateView, error) {
	envelopes, err := state.session.laneEnvelopes(ctx)
	if err != nil {
		return wire.SessionStateView{}, err
	}
	return FoldSessionState(envelopes), nil
}

// writeDelta appends one delta and advances the cursor.
//
// The cursor lock is held across the publish, so the writes of one handle and
// its clones land in revision order. A failed publish leaves the lane
// revision unknown, so the cursor is cleared and the next write reseeds from
// the folded lane rather than naming a revision nothing was written at.
func (state *SessionState) writeDelta(
	ctx context.Context, cursor *stateCursor, patch []wire.PatchOperation,
) (*AgdxReceipt, error) {
	base := wire.NewDynValue(cborform.Map{})
	if cursor.document != nil {
		base = *cursor.document
	}
	next, err := wire.ApplyJSONPatch(base, patch)
	if err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	delta := wire.StateDelta{
		BaseRevision: cursor.revision,
		Patch:        patch,
		OpID:         wire.IDFromBytes(NewOperationID()).String(),
		BaseOpID:     cursor.head,
	}
	if err := delta.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	body, err := wire.Marshal(delta)
	if err != nil {
		return nil, &CodecError{Reason: "encode the state delta", Cause: err}
	}
	receipt, err := state.session.writeEvent(ctx, wire.OperationStateDelta, body)
	if err != nil {
		cursor.document = nil
		cursor.head = nil
		return nil, err
	}
	cursor.revision++
	head := delta.OpID
	cursor.head = &head
	cursor.document = &next
	cursor.sinceSnapshot++
	if cursor.sinceSnapshot >= snapshotEveryDeltas {
		// A failed automatic snapshot is not a failed write: the delta is on
		// the log, and the next write retries the checkpoint.
		if current, err := state.holdsCurrent(ctx, cursor); err == nil && current {
			_, _ = state.writeSnapshot(ctx, cursor)
		}
	}
	return receipt, nil
}

func (state *SessionState) writeSnapshot(
	ctx context.Context, cursor *stateCursor,
) (*AgdxReceipt, error) {
	document := wire.NewDynValue(cborform.Map{})
	if cursor.document != nil {
		document = *cursor.document
	}
	body, err := wire.Marshal(wire.StateSnapshot{
		BaseRevision: cursor.revision,
		Document:     document,
		BaseOpID:     cursor.head,
	})
	if err != nil {
		return nil, &CodecError{Reason: "encode the state snapshot", Cause: err}
	}
	receipt, err := state.session.writeEvent(ctx, wire.OperationStateSnapshot, body)
	if err != nil {
		return nil, err
	}
	cursor.sinceSnapshot = 0
	return receipt, nil
}

// seed starts a handle that has not written yet, or whose last write failed,
// from the folded lane, so its next delta names the revision the lane is at.
func (state *SessionState) seed(ctx context.Context, cursor *stateCursor) error {
	if cursor.document != nil {
		return nil
	}
	view, err := state.Get(ctx)
	if err != nil {
		return err
	}
	cursor.revision = view.Revision
	cursor.head = view.HeadOpID
	document := view.Document
	if document.IsZero() {
		document = wire.NewDynValue(cborform.Map{})
	}
	cursor.document = &document
	return nil
}

// holdsCurrent reports whether the session's current document is the one this
// handle holds.
//
// A delta written against a stale base never applied, yet the handle counted
// it, so its revision can name the revision the session reached through
// another writer with a document nobody applied. When both name the delta
// behind the revision it must match, since a stale delta can leave an equal
// document. An unseeded handle is seeded and holds the current document by
// construction.
//
// The three conditions are over-determined against a correct fold: a
// divergence in any one of them implies a divergence in the others, so no
// test can isolate them. They are kept because the Rust client keeps them,
// and because a fold that ever did let them disagree would be caught here
// rather than overwriting somebody's change.
func (state *SessionState) holdsCurrent(
	ctx context.Context, cursor *stateCursor,
) (bool, error) {
	if cursor.document == nil {
		return true, state.seed(ctx, cursor)
	}
	view, err := state.Get(ctx)
	if err != nil {
		return false, err
	}
	sameHead := cursor.head == nil || view.HeadOpID == nil ||
		*cursor.head == *view.HeadOpID
	if view.Revision == cursor.revision && sameHead &&
		cursor.document.Equal(view.Document) {
		return true, nil
	}
	cursor.document = nil
	cursor.head = nil
	return false, nil
}

// FoldSessionState folds a session's state records, in lane order, into the
// document they describe.
//
// A record that does not decode is skipped, as the session index skips it: a
// reader that stopped at one would lose every later record to a single bad
// one.
func FoldSessionState(envelopes []wire.AgentEnvelope) wire.SessionStateView {
	document := wire.NewDynValue(cborform.Map{})
	var revision uint64
	var head *string
	complete := false
	for _, envelope := range envelopes {
		if envelope.Operation == nil {
			continue
		}
		switch *envelope.Operation {
		case wire.OperationStateSnapshot:
			var snapshot wire.StateSnapshot
			if err := wire.Unmarshal(envelope.Body, &snapshot); err != nil {
				continue
			}
			if snapshot.Validate() != nil {
				continue
			}
			// The first snapshot is the baseline and carries the head. A
			// later one counts only at the revision the fold has reached and
			// with the document it holds, so a stale snapshot from a second
			// writer changes nothing and the fold keeps its own head.
			if complete {
				if snapshot.BaseRevision != revision ||
					!snapshot.Document.Equal(document) {
					continue
				}
			} else {
				head = snapshot.BaseOpID
			}
			document = snapshot.Document
			revision = snapshot.BaseRevision
			complete = true
		case wire.OperationStateDelta:
			var delta wire.StateDelta
			if err := wire.Unmarshal(envelope.Body, &delta); err != nil {
				continue
			}
			if delta.Validate() != nil {
				continue
			}
			// A delta on an empty document at revision zero is the document's
			// own beginning, so the fold is complete from here.
			if delta.BaseRevision == 0 && revision == 0 {
				complete = true
			}
			// A writer that lost a race names a revision that was reached,
			// but not the delta that reached it.
			staleParent := delta.BaseOpID != nil && head != nil &&
				*delta.BaseOpID != *head
			if delta.BaseRevision != revision || staleParent {
				continue
			}
			next, err := wire.ApplyJSONPatch(document, delta.Patch)
			if err != nil {
				continue
			}
			document = next
			revision++
			applied := delta.OpID
			head = &applied
		}
	}
	return wire.SessionStateView{
		Revision: revision, Document: document,
		Complete: complete, HeadOpID: head,
	}
}

// statePointer is the JSON pointer naming one top-level key, with the two
// characters a pointer escapes spelled out.
func statePointer(key string) string {
	escaped := strings.ReplaceAll(key, "~", "~0")
	return "/" + strings.ReplaceAll(escaped, "/", "~1")
}

// documentFields reads a document's top-level fields, and false when it is
// not an object.
func documentFields(document wire.DynValue) (map[string]wire.DynValue, bool) {
	entries, object := document.Item().(cborform.Map)
	if !object {
		return nil, false
	}
	fields := make(map[string]wire.DynValue, len(entries))
	for _, entry := range entries {
		key, text := entry.Key.(cborform.Text)
		if !text {
			return nil, false
		}
		fields[string(key)] = wire.NewDynValue(entry.Value)
	}
	return fields, true
}

// sortedKeys orders a document's keys, so a replacement patch is the same
// patch whichever process builds it.
func sortedKeys(fields map[string]wire.DynValue) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	for outer := 1; outer < len(keys); outer++ {
		value := keys[outer]
		inner := outer - 1
		for inner >= 0 && keys[inner] > value {
			keys[inner+1] = keys[inner]
			inner--
		}
		keys[inner+1] = value
	}
	return keys
}
