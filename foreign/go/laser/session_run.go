package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Append appends one typed envelope to the session lane. The envelope must name this session as its conversation.
func (session *Session) Append(ctx context.Context, envelope wire.AgentEnvelope) (*AgdxReceipt, error) {
	if envelope.Conversation != session.conversation {
		return nil, &InvalidError{Reason: "an appended envelope must belong to this session"}
	}
	lane := session.laser.AgdxOn(session.config.Stream, wire.AgentSessions, envelope.Source, envelope.Conversation)
	if session.lane != nil {
		lane = lane.withLaneGuard(session.lane)
	}
	return lane.publishEnvelope(ctx, envelope)
}

// Run runs work inside the session, releases lease, and ends the session completed on success or failed on an error or a panic, which is raised again.
func (session *Session) Run(
	ctx context.Context, lease *SessionLease, work func(context.Context, *Session) error,
) (err error) {
	if lease != nil {
		defer lease.Release()
	}
	defer func() {
		raised := recover()
		if raised == nil {
			return
		}
		body := sessionErrorBody(panicText(raised))
		body.Detail = map[string]wire.Value{"panic": wire.BoolValue(true)}
		_, _ = session.Fail(context.WithoutCancel(ctx), body)
		panic(raised)
	}()
	if failure := work(ctx, session); failure != nil {
		_, _ = session.Fail(context.WithoutCancel(ctx), sessionErrorBody(failure.Error()))
		return failure
	}
	_, err = session.Complete(ctx, "")
	return err
}

// Status reads this session's summary from the deployment's session index.
func (session *Session) Status(ctx context.Context) (*wire.SessionInfo, error) {
	sessions := &Sessions{laser: session.laser, config: session.config, lane: session.lane}
	return sessions.Get(ctx, session.conversation)
}

func sessionErrorBody(message string) wire.AgentErrorBody {
	return wire.AgentErrorBody{Code: wire.ErrorInternal, Message: &message}
}

func panicText(raised any) string {
	switch value := raised.(type) {
	case string:
		return value
	case error:
		return value.Error()
	default:
		return "the session work panicked"
	}
}
