package laser

import (
	"sync"

	"github.com/zeebo/blake3"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// approvalCorrelationTag marks a correlation a parked approval gate minted,
// in the top two bytes.
//
// A parked gate is resumed by a reply the runtime has to recognize before it
// knows which record the reply belongs to, and it cannot keep a waiter in
// memory to match against, because the process that sent the prompt may be gone.
// Tagging the correlation lets any later process tell an approval decision
// from every other reply on the topic, and deriving the rest of it from the
// handled record's own position lets it find which parked record that is.
//
// The same value and derivation as Rust's APPROVAL_CORRELATION_TAG and
// approval_correlation (sdk/src/agent/ctx.rs:844), so a decision minted by
// one client is recognized by the other.
const approvalCorrelationTag = 0xa6a6

// approvalCorrelation is the correlation an approval gate on the record at
// `at` prompts under. Deterministic, so a replay of the same record derives
// the same one rather than minting a second prompt nobody answers.
func approvalCorrelation(at wire.LogPosition) wire.CorrelationID {
	hasher := blake3.New()
	// Domain-separated: the digest of a bare position must not collide with
	// any other use of the same bytes.
	_, _ = hasher.Write([]byte("laser.approval"))
	packed := at.PackedBytes()
	_, _ = hasher.Write(packed[:])
	sum := hasher.Sum(nil)

	var id [16]byte
	copy(id[:], sum[:16])
	id[0] = byte(approvalCorrelationTag >> 8)
	id[1] = byte(approvalCorrelationTag & 0xff)
	return wire.CorrelationID{ID: wire.IDFromBytes(id)}
}

// isApprovalCorrelation reports a correlation a parked approval gate minted.
func isApprovalCorrelation(correlation wire.CorrelationID) bool {
	id := correlation.Bytes()
	return id[0] == byte(approvalCorrelationTag>>8) &&
		id[1] == byte(approvalCorrelationTag&0xff)
}

// approvalInterrupt is what a parked approval gate leaves behind for the
// runtime: where the prompt landed, and when the decision stops being
// waited for.
type approvalInterrupt struct {
	prompt         wire.LogPosition
	deadlineMicros uint64
}

// approvalResume is what a replayed approval gate answers with: the
// approver's decision, or nothing when the deadline passed first.
type approvalResume struct {
	// decided is the correlated reply. nil means the approval expired.
	decided *AgdxMessage
}

// approvalSlot is the channel between one handler's approval gate and the
// runtime that parks for it.
//
// A gate cannot park on its own: parking is a decision about the record,
// which only the runtime holds. So the gate publishes its prompt, writes
// what it did here, and defers; the runtime reads it after the handler
// returns and parks the record. On the replay, the decision is written here
// first and the gate returns it instead of prompting again.
type approvalSlot struct {
	mutex sync.Mutex
	// parkingTopic is the consumer topic a decision has to arrive on for
	// this runtime to be able to park. Empty when the runtime cannot park
	// at all, which is what makes the gate wait in place instead.
	parkingTopic string
	interrupt    *approvalInterrupt
	resume       *approvalResume
}

// parksOn reports whether a gate whose decision comes back on topic can be
// parked by this runtime.
func (slot *approvalSlot) parksOn(topic string) bool {
	if slot == nil {
		return false
	}
	slot.mutex.Lock()
	defer slot.mutex.Unlock()
	return slot.parkingTopic != "" && slot.parkingTopic == topic
}

// takeResume takes the decision a replay was started with, once.
func (slot *approvalSlot) takeResume() *approvalResume {
	if slot == nil {
		return nil
	}
	slot.mutex.Lock()
	defer slot.mutex.Unlock()
	resume := slot.resume
	slot.resume = nil
	return resume
}

// raise records that the gate prompted and deferred.
func (slot *approvalSlot) raise(interrupt approvalInterrupt) {
	if slot == nil {
		return
	}
	slot.mutex.Lock()
	defer slot.mutex.Unlock()
	slot.interrupt = &interrupt
}

// takeInterrupt takes what the gate left behind, once.
func (slot *approvalSlot) takeInterrupt() *approvalInterrupt {
	if slot == nil {
		return nil
	}
	slot.mutex.Lock()
	defer slot.mutex.Unlock()
	interrupt := slot.interrupt
	slot.interrupt = nil
	return interrupt
}

// ApprovalExpiredError reports an approval whose deadline passed with no
// decision. Returned from the gate on the replay, so a handler can decide
// what an unanswered approval means for its own work.
type ApprovalExpiredError struct{ Approver wire.AgentID }

func (e *ApprovalExpiredError) Error() string {
	return "the approval of " + e.Approver.String() + " expired undecided"
}
