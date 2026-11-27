package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Sessions reads the deployment's session index for one stream.
//
// Every request names the stream before anything else, because the index is
// per stream: a session id alone does not say which stream's index to look in.
type Sessions struct {
	laser  *Laser
	config SessionConfig
	// lane is the lane-identity pin every session opened from this factory
	// shares. A new factory gets a new pin, which is what lets a caller
	// recover from a replaced lane by opening a fresh one.
	lane *laneGuard
}

// Sessions opens the session surface of the handle's default stream, under the
// default configuration.
func (laser *Laser) Sessions() *Sessions {
	return laser.SessionsWith(SessionConfig{})
}

// SessionsIn opens the session surface of a named stream.
func (laser *Laser) SessionsIn(stream string) *Sessions {
	return laser.SessionsWith(SessionConfig{Stream: stream})
}

// SessionsWith opens the session surface under an explicit configuration. It
// is free to build: nothing is sent until a verb is called.
func (laser *Laser) SessionsWith(config SessionConfig) *Sessions {
	resolved := config.withDefaults(laser)
	scoped := laser
	if resolved.Stream != laser.defaultStream {
		scoped = laser.WithDefaultStream(resolved.Stream)
	}
	// A declared layout holds for every later send on the stream through this
	// connection, so an agent and a lens opened from different handles route
	// one agent's work to the same place.
	if resolved.Layout.Kind() != LayoutShared {
		scoped.declareLayout(resolved.Stream, resolved.Layout)
	}
	return &Sessions{laser: scoped, config: resolved, lane: &laneGuard{}}
}

// Stream is the stream these sessions ride.
func (sessions *Sessions) Stream() string { return sessions.config.Stream }

// Config is how these sessions behave.
func (sessions *Sessions) Config() SessionConfig { return sessions.config }

// Get reads one session's summary.
func (sessions *Sessions) Get(
	ctx context.Context, id wire.ConversationID,
) (*wire.SessionInfo, error) {
	outcome, err := sessions.read(ctx, wire.AgdxSessionGetCode, func(stream string) any {
		return wire.SessionGetRequest{Stream: stream, ID: id}
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.SessionInfoOutcome {
		return nil, unexpectedSessionOutcome("get", outcome.Kind)
	}
	return outcome.Info, nil
}

// State reads a session's folded state document with up to historyLimit
// history rows. Zero leaves the history size to the server.
func (sessions *Sessions) State(
	ctx context.Context, id wire.ConversationID, historyLimit uint32,
) (*wire.SessionStateView, error) {
	outcome, err := sessions.read(ctx, wire.AgdxSessionStateCode, func(stream string) any {
		return wire.SessionStateRequest{Stream: stream, ID: id, HistoryLimit: historyLimit}
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.SessionStateOutcome {
		return nil, unexpectedSessionOutcome("state", outcome.Kind)
	}
	return outcome.State, nil
}

// Links reads the resources a session touched, narrowed to one surface when
// named.
func (sessions *Sessions) Links(
	ctx context.Context, id wire.ConversationID, surface *wire.LinkSurface,
) (*wire.SessionLinksView, error) {
	outcome, err := sessions.read(ctx, wire.AgdxSessionLinksCode, func(stream string) any {
		return wire.SessionLinksRequest{Stream: stream, ID: id, Surface: surface}
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.SessionLinksOutcome {
		return nil, unexpectedSessionOutcome("links", outcome.Kind)
	}
	return outcome.Links, nil
}

// Sources reads the partitions holding a session's records, which is what
// makes a timeline resumable.
func (sessions *Sessions) Sources(
	ctx context.Context, id wire.ConversationID,
) (*wire.SessionSources, error) {
	return sessions.sources(ctx, id, false)
}

// LaneSources reads only the registered lane identity, for a native lane
// writer that does not need the rest.
func (sessions *Sessions) LaneSources(
	ctx context.Context, id wire.ConversationID,
) (*wire.SessionSources, error) {
	return sessions.sources(ctx, id, true)
}

func (sessions *Sessions) sources(
	ctx context.Context, id wire.ConversationID, laneOnly bool,
) (*wire.SessionSources, error) {
	outcome, err := sessions.read(ctx, wire.AgdxSessionSourcesCode, func(stream string) any {
		return wire.SessionSourcesRequest{Stream: stream, ID: id, LaneOnly: laneOnly}
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.SessionSourcesOutcome {
		return nil, unexpectedSessionOutcome("sources", outcome.Kind)
	}
	return outcome.Sources, nil
}

// Changes reads a page of the stream's change feed.
//
// Page p of size r holds the changes with sequence (p-1)*r+1 through p*r, so
// appends and pruning never shift a page out from under a reader.
func (sessions *Sessions) Changes(
	ctx context.Context, page, results uint32,
) (*wire.SessionChanges, error) {
	outcome, err := sessions.read(ctx, wire.AgdxSessionChangesCode, func(stream string) any {
		return wire.SessionChangesRequest{Stream: stream, Page: page, Results: results}
	})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.SessionChangesOutcome {
		return nil, unexpectedSessionOutcome("changes", outcome.Kind)
	}
	return outcome.Changes, nil
}

// SessionBound is the walk boundary a paged read carries.
//
// A list or timeline walked page by page would otherwise shift under appends.
// The first page returns the boundary, and later pages pass it back so
// anything folded after it stays out of the walk.
type SessionBound struct {
	AsOf   uint64
	Series uint64
}

// SessionListRequest lists a stream's sessions, newest created first.
type SessionListRequest struct {
	sessions *Sessions
	request  wire.SessionListRequest
}

// List begins a listing of this stream's sessions.
func (sessions *Sessions) List() *SessionListRequest {
	return &SessionListRequest{sessions: sessions}
}

// Status keeps only sessions in one lifecycle state.
func (request *SessionListRequest) Status(status wire.SessionStatus) *SessionListRequest {
	request.request.Status = &status
	return request
}

// Root keeps only sessions in the tree rooted at one session.
func (request *SessionListRequest) Root(root wire.ConversationID) *SessionListRequest {
	request.request.Root = &root
	return request
}

// LabelPrefix keeps only sessions whose label starts with a prefix.
func (request *SessionListRequest) LabelPrefix(prefix string) *SessionListRequest {
	request.request.LabelPrefix = &prefix
	return request
}

// Agent keeps only sessions one agent took part in.
func (request *SessionListRequest) Agent(agent wire.AgentID) *SessionListRequest {
	request.request.Agent = &agent
	return request
}

// Text keeps only sessions matching a free-text search.
func (request *SessionListRequest) Text(text string) *SessionListRequest {
	request.request.Text = &text
	return request
}

// Page selects a 1-based page. Zero reads the first.
func (request *SessionListRequest) Page(page uint32) *SessionListRequest {
	request.request.Page = page
	return request
}

// Results sets the sessions per page. Zero leaves it to the server.
func (request *SessionListRequest) Results(results uint32) *SessionListRequest {
	request.request.Results = results
	return request
}

// Bound pins the walk to the boundary an earlier page returned, so sessions
// folded after it stay out.
func (request *SessionListRequest) Bound(bound SessionBound) *SessionListRequest {
	request.request.AsOf, request.request.Series = &bound.AsOf, &bound.Series
	return request
}

// Fetch reads one page.
func (request *SessionListRequest) Fetch(ctx context.Context) (*wire.SessionPage, error) {
	outcome, err := request.sessions.read(ctx, wire.AgdxSessionListCode,
		func(stream string) any {
			sent := request.request
			sent.Stream = stream
			return sent
		})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.SessionPageOutcome {
		return nil, unexpectedSessionOutcome("list", outcome.Kind)
	}
	return outcome.Page, nil
}

// SessionEventsRequest reads one session's timeline in server append order.
type SessionEventsRequest struct {
	sessions *Sessions
	request  wire.SessionEventsRequest
}

// Events begins reading one session's timeline.
func (sessions *Sessions) Events(id wire.ConversationID) *SessionEventsRequest {
	return &SessionEventsRequest{
		sessions: sessions,
		request:  wire.SessionEventsRequest{ID: id},
	}
}

// Page selects a 1-based page. Zero reads the first.
func (request *SessionEventsRequest) Page(page uint32) *SessionEventsRequest {
	request.request.Page = page
	return request
}

// Results sets the events per page. Zero leaves it to the server.
func (request *SessionEventsRequest) Results(results uint32) *SessionEventsRequest {
	request.request.Results = results
	return request
}

// Bound pins the walk to the boundary an earlier page returned.
func (request *SessionEventsRequest) Bound(bound SessionBound) *SessionEventsRequest {
	request.request.AsOf, request.request.Series = &bound.AsOf, &bound.Series
	return request
}

// Fetch reads one page.
func (request *SessionEventsRequest) Fetch(
	ctx context.Context,
) (*wire.SessionEventsPage, error) {
	outcome, err := request.sessions.read(ctx, wire.AgdxSessionEventsCode,
		func(stream string) any {
			sent := request.request
			sent.Stream = stream
			return sent
		})
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.SessionEventsOutcome {
		return nil, unexpectedSessionOutcome("events", outcome.Kind)
	}
	return outcome.Events, nil
}

// read sends one session index request and opens its reply.
//
// The stream is resolved here rather than when the request was built, because
// a request with no stream has nowhere to look: the index is per stream.
func (sessions *Sessions) read(
	ctx context.Context, code uint32, build func(stream string) any,
) (*wire.SessionOutcome, error) {
	if !sessions.laser.capabilities.Managed || !sessions.laser.capabilities.Sessions {
		return nil, &UnsupportedError{Surface: "managed session reads"}
	}
	if sessions.config.Stream == "" {
		return nil, &InvalidError{Reason: "session reads need a stream: " +
			"pin one with Options.DefaultStream or use SessionsIn"}
	}
	payload, err := wire.Marshal(build(sessions.config.Stream))
	if err != nil {
		return nil, &CodecError{Reason: "encode session request", Cause: err}
	}
	raw, err := sessions.laser.SendManaged(ctx, code, payload)
	if err != nil {
		return nil, err
	}
	var reply wire.SessionReply
	if err := wire.Unmarshal(raw, &reply); err != nil {
		return nil, &CodecError{Reason: "decode session reply", Cause: err}
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{
				Reason: "session reply carried neither an outcome nor an error"}
		}
		return nil, &SessionError{Failure: *reply.Err}
	}
	return reply.Outcome, nil
}

// SessionError reports a session read the server refused.
type SessionError struct{ Failure wire.SessionError }

func (e *SessionError) Error() string { return "sessions: " + e.Failure.Error() }

// ResultCode is the shared code this failure projects onto.
func (e *SessionError) ResultCode() wire.ResultCode { return e.Failure.ResultCode() }

func unexpectedSessionOutcome(operation string, kind wire.SessionOutcomeKind) error {
	return &CodecError{Reason: "session " + operation +
		" answered with an unexpected outcome: " + itoa(int(kind))}
}
