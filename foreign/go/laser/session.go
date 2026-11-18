package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Derived conversation ids are versioned so they stay stable across builds.
//
// Bumping deriveVersion deliberately remaps every derived conversation, which
// is why it is a constant to change on purpose rather than an implementation
// detail to drift.
const (
	deriveVersion byte   = 1
	fnvOffset     uint64 = 0xcbf2_9ce4_8422_2325
	fnvPrime      uint64 = 0x0000_0100_0000_01b3
)

// NewConversationID mints a fresh conversation. It is time-ordered, so two
// conversations sort by when they began.
func NewConversationID() wire.ConversationID {
	return wire.ConversationID{ID: wire.IDFromBytes(NewOperationID())}
}

// DeriveConversationID derives a stable conversation from a seed.
//
// The same seed always reaches the same conversation, which gives per-seed
// ordering and isolation without any coordination: two processes that know the
// seed address one conversation without talking to each other.
func DeriveConversationID(seed string) wire.ConversationID {
	high := deriveHash(0x1d, seed)
	low := deriveHash(0x9e, seed)
	var raw [16]byte
	for index := 7; index >= 0; index-- {
		raw[index] = byte(high)
		high >>= 8
	}
	for index := 15; index >= 8; index-- {
		raw[index] = byte(low)
		low >>= 8
	}
	return wire.ConversationID{ID: wire.IDFromBytes(raw)}
}

// DeriveSessionID is the session a label reaches in a stream under a
// namespace.
//
// All three are part of the seed, so two applications sharing a stream share a
// session only when they deliberately pick the same namespace and label.
func DeriveSessionID(stream, namespace, label string) wire.ConversationID {
	return DeriveConversationID(stream + "\x1f" + namespace + "\x1f" + label)
}

// deriveHash is FNV-1a over the version, a salt, and the seed.
//
// It is written out rather than taken from a library because the result is a
// wire-visible identity: a different hash is a different conversation, so the
// algorithm is pinned here and versioned above.
func deriveHash(salt byte, seed string) uint64 {
	hash := fnvOffset
	mix := func(value byte) {
		hash ^= uint64(value)
		hash *= fnvPrime
	}
	mix(deriveVersion)
	mix(salt)
	for index := 0; index < len(seed); index++ {
		mix(seed[index])
	}
	return hash
}

// SessionPolicy decides how a key maps to a conversation.
type SessionPolicy int

const (
	// PerCall opens a fresh conversation each time, which is what a one-shot
	// request wants.
	PerCall SessionPolicy = iota
	// PerUser derives a stable conversation from the key, so one user's work
	// stays on one partition and therefore in order.
	PerUser
)

// ConversationFor is the conversation this policy gives a key.
func (policy SessionPolicy) ConversationFor(key string) wire.ConversationID {
	if policy == PerUser {
		return DeriveConversationID(key)
	}
	return NewConversationID()
}

// SessionConfig is how a session behaves.
type SessionConfig struct {
	// Stream is where session records ride. Empty takes the handle's default.
	Stream string
	// Namespace separates one application's sessions from another's inside one
	// stream, and seeds a derived session id.
	Namespace string
	// IdleTimeout is how long a session may go quiet before the deployment may
	// consider it abandoned. Zero leaves it to the deployment.
	IdleTimeout time.Duration
	// MemoryNamespace is where this session's memory lives. Empty takes the
	// session namespace.
	MemoryNamespace string
	// ContextTokens bounds the context a session assembles.
	ContextTokens int
	// SDK names this client in the session start record, so an operator can
	// tell which client opened a session.
	SDK wire.SdkInfo
}

// DefaultSessionContextTokens bounds an assembled context when the caller
// names no bound.
const DefaultSessionContextTokens = 4000

func (config SessionConfig) withDefaults(laser *Laser) SessionConfig {
	if config.Stream == "" {
		config.Stream = laser.defaultStream
	}
	if config.Namespace == "" {
		config.Namespace = "default"
	}
	if config.MemoryNamespace == "" {
		config.MemoryNamespace = config.Namespace
	}
	if config.ContextTokens == 0 {
		config.ContextTokens = DefaultSessionContextTokens
	}
	if config.SDK.Language == "" {
		config.SDK = wire.SdkInfo{Language: "go", Version: ClientVersion}
	}
	return config
}

// ClientVersion is what this client reports in a session start record.
const ClientVersion = "0.10.2"

// Create opens a session whose id is derived from its label.
//
// The same label in the same stream and namespace always reaches the same
// session, so a job that runs nightly resumes its session rather than starting
// a new one each time.
func (sessions *Sessions) Create(label string) *Session {
	return sessions.Open(DeriveSessionID(
		sessions.config.Stream, sessions.config.Namespace, label), label)
}

// Start opens a fresh session, which is what a one-shot exchange wants.
func (sessions *Sessions) Start() *Session {
	return sessions.Open(NewConversationID(), "")
}

// Open addresses an existing session by its conversation.
func (sessions *Sessions) Open(conversation wire.ConversationID, label string) *Session {
	return &Session{
		laser:        sessions.laser,
		config:       sessions.config,
		conversation: conversation,
		label:        label,
	}
}

// Session is one agent session: a conversation with a lifecycle on the log.
type Session struct {
	laser        *Laser
	config       SessionConfig
	conversation wire.ConversationID
	label        string
	agent        *wire.AgentID
}

// Conversation is the session's identity, and the partition key its records
// ride under.
func (session *Session) Conversation() wire.ConversationID { return session.conversation }

// Label is the name this session was created under, empty for a fresh one.
func (session *Session) Label() string { return session.label }

// As names the agent writing this session's records.
func (session *Session) As(agent wire.AgentID) *Session {
	narrowed := *session
	narrowed.agent = &agent
	return &narrowed
}

// Reference is the session as another surface names it, for linking a memory
// or a graph fact back to the session that produced it.
func (session *Session) Reference() wire.SessionRef {
	return wire.SessionRef{Stream: session.config.Stream, Session: session.conversation}
}

// Memory is this session's memory, scoped to the conversation so a session's
// own memories are separate from the namespace's shared ones.
func (session *Session) Memory() *Memory {
	conversation := session.conversation
	return session.laser.Memory(session.config.MemoryNamespace).
		For(MemoryScope{Conversation: &conversation})
}

// Context assembles this session's history under its configured token bound.
func (session *Session) Context() *ContextAssembler {
	return Assemble(session.conversation).
		WithPolicy(TokenBudget{MaxTokens: session.config.ContextTokens})
}

// Agdx is the agent messaging surface for this session.
func (session *Session) Agdx() (*Agdx, error) {
	if session.agent == nil {
		return nil, &InvalidError{Reason: "a session writes as an agent: " +
			"name one with Session.As before writing"}
	}
	return session.laser.AgdxOn(session.config.Stream, wire.AgentSessions,
		*session.agent, session.conversation), nil
}

// Begin writes the session's start record, opening it on the log.
//
// The ancestry a session carries is the envelope's, so a sub-session records
// the exchange it was spawned from and a reader can walk the tree.
func (session *Session) Begin(ctx context.Context) (*AgdxReceipt, error) {
	agdx, err := session.Agdx()
	if err != nil {
		return nil, err
	}
	start := wire.SessionStart{
		Agent: *session.agent,
		SDK:   session.config.SDK,
	}
	if session.label != "" {
		label := session.label
		start.Label = &label
	}
	if session.config.Namespace != "" {
		namespace := session.config.Namespace
		start.Namespace = &namespace
	}
	if session.config.IdleTimeout > 0 {
		micros := uint64(session.config.IdleTimeout.Microseconds())
		start.IdleTimeoutMicros = &micros
	}
	return agdx.SessionStarted(start).Send(ctx)
}

// Progress records the session moving along.
func (session *Session) Progress(
	ctx context.Context, transition wire.SessionTransition,
) (*AgdxReceipt, error) {
	agdx, err := session.Agdx()
	if err != nil {
		return nil, err
	}
	return agdx.SessionProgressed(transition).Send(ctx)
}

// End closes the session in a terminal state.
func (session *Session) End(
	ctx context.Context, state wire.TaskState, end wire.SessionEnd,
) (*AgdxReceipt, error) {
	agdx, err := session.Agdx()
	if err != nil {
		return nil, err
	}
	return agdx.SessionEnded(state, end).Send(ctx)
}

// Complete closes the session successfully, the common ending.
func (session *Session) Complete(ctx context.Context, reason string) (*AgdxReceipt, error) {
	end := wire.SessionEnd{}
	if reason != "" {
		end.Reason = &reason
	}
	return session.End(ctx, wire.TaskCompleted, end)
}

// Fail closes the session with the error that ended it, so a reader sees why
// rather than only that it stopped.
func (session *Session) Fail(
	ctx context.Context, failure wire.AgentErrorBody,
) (*AgdxReceipt, error) {
	return session.End(ctx, wire.TaskFailed, wire.SessionEnd{Error: &failure})
}
