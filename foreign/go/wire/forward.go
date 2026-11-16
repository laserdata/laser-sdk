package wire

// ForwardedScope is the trusted stream identity a server stamps on a forwarded
// request, carried through durable replay.
type ForwardedScope struct {
	StreamID             uint32 `cbor:"stream_id"`
	Stream               string `cbor:"stream"`
	StreamCreatedAtMicro uint64 `cbor:"stream_created_at_micros"`
}

// ForwardedQuery is one query the streaming server hands to the managed
// backend.
//
// The identity fields are stamped by the server, never by the client: the
// backend trusts them because the client could not have set them. The query
// envelope itself rides opaque, so the server never has to decode what it
// forwards.
type ForwardedQuery struct {
	// UserID is the authenticated identity.
	UserID uint32 `cbor:"user_id"`
	// ClientID is the originating connection, for logging and audit only.
	ClientID U128 `cbor:"client_id"`
	// Correlation is echoed for the audit log, never used to route.
	Correlation *string `cbor:"correlation"`
	// QueryEnvelope is the opaque request the client sent.
	QueryEnvelope []byte `cbor:"query_envelope"`
	// Grants are the caller's effective capabilities, stamped by the server.
	// Empty means authorization is not in effect.
	Grants []Grant `cbor:"grants,omitempty"`
	// Tenancy is how the server's replicated authorization state is scoped.
	// The backend refuses a frame whose tenancy differs from its own.
	Tenancy Tenancy `cbor:"tenancy,omitempty"`
	// Scope is the stream of the queried index, stamped once the caller's
	// native read authority over it was checked. The backend refuses a
	// stream-scoped query that arrives without it or with another.
	Scope *ForwardedScope `cbor:"scope,omitempty"`
}

// ForwardedCommand is one keyed managed command the streaming server hands to
// the managed backend.
//
// Unlike a forwarded query, several operation kinds share one path, so the
// frame carries the command code and the backend dispatches on it. The payload
// rides opaque.
type ForwardedCommand struct {
	// Caller is the client that asked, stamped by the server so an SDK cannot
	// set it. The backend records it and authorizes with the grants. Absent
	// on a request the server issues on its own.
	Caller *Caller `cbor:"caller,omitempty"`
	// ClientID is the caller's consensus client id, for logs and correlation
	// only, never identity.
	ClientID    U128    `cbor:"client_id"`
	Correlation *string `cbor:"correlation"`
	// OperationID is the stable identity the originating client minted for a
	// logical mutation. A read-only command leaves it absent.
	OperationID *U128 `cbor:"operation_id,omitempty"`
	// ReadAll is vestigial. The server stamps it false, the plane ignores it,
	// and the caller's grants govern access. It stays on the wire as the
	// contract's current structure, never as a widening flag.
	ReadAll bool `cbor:"read_all"`
	// Scope is the server-stamped stream identity. A session command without
	// it fails closed.
	Scope       *ForwardedScope `cbor:"scope,omitempty"`
	CommandCode uint32          `cbor:"command_code"`
	Payload     []byte          `cbor:"payload"`
	Grants      []Grant         `cbor:"grants,omitempty"`
	// ReadBarrier is set when the client wrapped this read in a barrier, so
	// the backend answers only once its fold has reached the log head.
	ReadBarrier bool `cbor:"read_barrier,omitempty"`
	// MinPosition is the position the backend's fold must have applied before
	// it answers.
	MinPosition *MutationPosition `cbor:"min_position,omitempty"`
	// PositionedReply asks the backend to answer with its applied frontier.
	PositionedReply bool `cbor:"positioned_reply,omitempty"`
	// MinSessions are the session positions a session read must find applied.
	MinSessions []SessionPosition `cbor:"min_sessions,omitempty"`
	Tenancy     Tenancy           `cbor:"tenancy,omitempty"`
}

// Caller is who asked for a managed record: the authenticated user, plus the
// two claims an intermediary may make about whom it acted for.
//
// The claims are recorded, never verified, so a reader treats them as
// provenance rather than as identity.
type Caller struct {
	// UserID is the authenticated user. The root user is id zero.
	UserID uint32 `cbor:"user_id"`
	// Agent is the agent the caller's connection declared in its presence. It
	// is a claim the client makes.
	Agent *AgentID `cbor:"agent,omitempty"`
	// OnBehalfOf is the end user an intermediary wrote for, as it named that
	// user in the on-behalf-of header.
	OnBehalfOf *string `cbor:"on_behalf_of,omitempty"`
}
