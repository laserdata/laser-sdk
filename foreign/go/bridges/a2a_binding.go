package bridges

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
	"github.com/zeebo/blake3"
)

// taskBindingDomain separates this MAC from every other use of the same key.
var taskBindingDomain = []byte("laserdata a2a task v1\x00")

// taskBinding scopes a task id to the caller that created it.
type taskBinding struct {
	key [32]byte
	ttl time.Duration
}

// WithTaskBinding scopes every task id this bridge mints to the caller that
// created it, for ttl.
//
// Without it a task id is a bare conversation, and anyone who learns or
// guesses one can read and cancel that task: the log is the authority on
// what happened, and it has no opinion on who may ask. The id becomes
// `<task>.<expiry>.<mac>`, and an id that is not this caller's, has expired,
// or was minted on an earlier incarnation of the stream reads as a task that
// does not exist.
//
// The caller is named by A2ABridge.Caller, so every call that mints or opens
// an id has one. A bridge that binds tasks and is called without a caller
// refuses rather than falling back to an unbound id.
func (bridge *A2ABridge) WithTaskBinding(key [32]byte, ttl time.Duration) *A2ABridge {
	derived := *bridge
	derived.binding = &taskBinding{key: key, ttl: ttl}
	return &derived
}

// TaskBindingMAC is the keyed digest a bound task id carries.
//
// The caller's length is written before the caller itself, so two different
// callers cannot be framed into the same preimage. The stream generation is
// in it because a recreated stream is a different log: an id minted on the
// old one names a conversation whose records are gone.
func TaskBindingMAC(
	key [32]byte, caller string, streamGeneration uint64,
	task wire.ConversationID, expiresMicros uint64,
) []byte {
	hasher, err := blake3.NewKeyed(key[:])
	if err != nil {
		// A 32-byte key is the only shape this takes, and the type says so.
		panic("bridges: the task binding key must be 32 bytes: " + err.Error())
	}
	hasher.Write(taskBindingDomain)
	hasher.Write(leUint64(uint64(len(caller))))
	hasher.Write([]byte(caller))
	hasher.Write(leUint64(streamGeneration))
	hasher.Write(leUint128(task.ID))
	hasher.Write(leUint64(expiresMicros))
	return hasher.Sum(nil)[:32]
}

func leUint64(value uint64) []byte {
	octets := make([]byte, 8)
	binary.LittleEndian.PutUint64(octets, value)
	return octets
}

// leUint128 writes an id as the little-endian u128 the other clients hash.
func leUint128(id wire.ID) []byte {
	octets := id.Bytes()
	reversed := make([]byte, len(octets))
	for index, octet := range octets {
		reversed[len(octets)-1-index] = octet
	}
	return reversed
}

// unboundCaller reports a bound bridge asked to act without a caller.
func unboundCaller() error {
	return &BridgeError{Reason: "this bridge binds task ids to a caller, " +
		"so the call must name one with Caller"}
}

// mintTaskID is the id a new task is known by.
func (bridge *A2ABridge) mintTaskID(
	ctx context.Context, task wire.ConversationID, caller string,
) (string, error) {
	if bridge.binding == nil {
		return task.String(), nil
	}
	if caller == "" {
		return "", unboundCaller()
	}
	generation, err := bridge.streamGeneration(ctx)
	if err != nil {
		return "", err
	}
	expires := uint64(time.Now().Add(bridge.binding.ttl).UnixMicro())
	mac := TaskBindingMAC(bridge.binding.key, caller, generation, task, expires)
	return fmt.Sprintf("%s.%d.%s", task, expires, hex.EncodeToString(mac)), nil
}

// openTaskID reads the conversation a task id names, refusing one that is
// not this caller's, has expired, or came from another stream incarnation.
func (bridge *A2ABridge) openTaskID(
	ctx context.Context, id, caller string,
) (wire.ConversationID, error) {
	if bridge.binding == nil {
		return openTask(id)
	}
	if caller == "" {
		return wire.ConversationID{}, unboundCaller()
	}
	invalid := func() error {
		return &BridgeError{Reason: fmt.Sprintf("invalid task id %q", id)}
	}
	parts := strings.Split(id, ".")
	if len(parts) != 3 {
		return wire.ConversationID{}, invalid()
	}
	task, err := openTask(parts[0])
	if err != nil {
		return wire.ConversationID{}, invalid()
	}
	expires, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return wire.ConversationID{}, invalid()
	}
	mac, err := hex.DecodeString(parts[2])
	if err != nil || len(mac) != 32 {
		return wire.ConversationID{}, invalid()
	}
	if expires < uint64(time.Now().UnixMicro()) {
		return wire.ConversationID{}, invalid()
	}
	generation, err := bridge.streamGeneration(ctx)
	if err != nil {
		return wire.ConversationID{}, err
	}
	expected := TaskBindingMAC(bridge.binding.key, caller, generation, task, expires)
	// Compared whole rather than early-exiting, so the comparison tells an
	// attacker nothing about how much of a forged digest was right.
	if !constantTimeEqual(expected, mac) {
		return wire.ConversationID{}, invalid()
	}
	return task, nil
}

// constantTimeEqual compares two digests without leaking where they differ.
func constantTimeEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

// streamGeneration is when the bridge's stream was created, which tells a
// recreated stream apart from the one a task id was minted on.
func (bridge *A2ABridge) streamGeneration(ctx context.Context) (uint64, error) {
	stream := bridge.laser.DefaultStream()
	if stream == "" {
		return 0, &BridgeError{Reason: errNoStream.Error()}
	}
	client := bridge.laser.Client()
	if client == nil {
		return 0, &BridgeError{Reason: "the A2A bridge is not connected"}
	}
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return 0, &BridgeError{Reason: "stream name: " + err.Error()}
	}
	details, err := client.GetStream(ctx, streamID)
	if err != nil {
		return 0, &BridgeError{
			Reason: "read the bridge stream: " + err.Error()}
	}
	if details == nil {
		return 0, &BridgeError{Reason: "the bridge stream does not exist"}
	}
	return details.CreatedAt, nil
}

// A2ACallerPrincipal is the authenticated caller of an A2A request. A host
// that binds tasks names it from its own authentication.
type A2ACallerPrincipal string

// A2ACaller is the bridge acting for one authenticated caller.
type A2ACaller struct {
	bridge    *A2ABridge
	principal string
}

// Caller binds this bridge's calls to one authenticated principal, which is
// what a bound task id is scoped to.
func (bridge *A2ABridge) Caller(principal string) *A2ACaller {
	return &A2ACaller{bridge: bridge, principal: principal}
}

// Principal is who this caller is.
func (caller *A2ACaller) Principal() string { return caller.principal }

// Submit publishes a task owned by this caller.
func (caller *A2ACaller) Submit(
	ctx context.Context, paramsJSON []byte,
) (*A2ATask, error) {
	return caller.bridge.submitFor(
		ctx, paramsJSON, caller.bridge.target, caller.bridge.hops, caller.principal)
}

// SubmitTo addresses one task owned by this caller to one agent, overriding
// the bridge's own WithTarget.
func (caller *A2ACaller) SubmitTo(
	ctx context.Context, target wire.AgentID, paramsJSON []byte,
) (*A2ATask, error) {
	return caller.bridge.submitAddressed(
		ctx, paramsJSON, &target, caller.bridge.hops, caller.principal, nil, nil)
}

// SubmitWithHops submits work owned by this caller that already crossed
// other bridges, carrying its hop path with this bridge appended.
func (caller *A2ACaller) SubmitWithHops(
	ctx context.Context, previous []string, target *wire.AgentID, paramsJSON []byte,
) (*A2ATask, error) {
	hops, err := CallHops(caller.bridge.hops, previous)
	if err != nil {
		return nil, err
	}
	if target == nil {
		target = caller.bridge.target
	}
	return caller.bridge.submitAddressed(
		ctx, paramsJSON, target, hops, caller.principal, nil, nil)
}

// SubmitIn submits a task owned by this caller as a child session of parent,
// in the tree rooted at root. The command is addressed like Submit.
func (caller *A2ACaller) SubmitIn(
	ctx context.Context, parent, root wire.ConversationID, paramsJSON []byte,
) (*A2ATask, error) {
	return caller.bridge.submitAddressed(ctx, paramsJSON,
		caller.bridge.target, caller.bridge.hops, caller.principal, &parent, &root)
}

// SubmitInTo is SubmitIn addressed to target.
func (caller *A2ACaller) SubmitInTo(
	ctx context.Context, target wire.AgentID, parent, root wire.ConversationID,
	paramsJSON []byte,
) (*A2ATask, error) {
	return caller.bridge.submitAddressed(ctx, paramsJSON,
		&target, caller.bridge.hops, caller.principal, &parent, &root)
}

// Task and Cancel act on a task of this caller's. An id belonging to another
// caller reads as one that does not exist.
func (caller *A2ACaller) Task(ctx context.Context, id string) (*A2ATask, error) {
	return caller.bridge.taskFor(ctx, id, caller.principal)
}

func (caller *A2ACaller) Cancel(ctx context.Context, id string) (*A2ATask, error) {
	return caller.bridge.cancelFor(ctx, id, caller.principal)
}

// HandleRPC answers a JSON-RPC call as this caller.
func (caller *A2ACaller) HandleRPC(
	ctx context.Context, request []byte,
) JSONRPCResponse {
	return caller.bridge.handleRPCFor(ctx, request, caller.principal)
}
