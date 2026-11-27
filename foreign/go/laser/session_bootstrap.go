package laser

import (
	"context"
	"errors"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// registrationPollInterval is how often a source registration is looked for
// before the publish timeout gives up on it.
const registrationPollInterval = 100 * time.Millisecond

// SessionBootstrap is what Sessions.Bootstrap set up.
type SessionBootstrap struct {
	// Registered reports whether the stream is now indexed as a session
	// source. False is not a failure: a deployment that does not index
	// sessions, or an account without session administration rights, relies
	// on provisioning to register the stream.
	Registered bool
}

// Bootstrap creates the agent topics, then, on a deployment that serves
// sessions, registers the stream as a session source.
//
// The layout decides the shape. A single-partition layout creates every topic
// with one partition, whatever partition count the caller asked for, because
// the point of that layout is total order. A per-agent topic layout also
// creates each declared topic with the lane's partition count and retention,
// since each one carries a share of the same work.
//
// A refused registration is reported as Registered false and never as an
// error: the topics are there, and an operator can register the stream
// separately.
func (sessions *Sessions) Bootstrap(
	ctx context.Context, partitions uint32, retention TopicRetention,
) (SessionBootstrap, error) {
	if err := sessions.config.Layout.Validate(); err != nil {
		return SessionBootstrap{}, err
	}
	if sessions.config.Layout.Kind() == LayoutSinglePartition {
		partitions = 1
	}
	declared, err := sessions.config.Layout.DeclaredTopics()
	if err != nil {
		return SessionBootstrap{}, err
	}
	if err := sessions.laser.Bootstrap(ctx, partitions, retention); err != nil {
		return SessionBootstrap{}, err
	}
	for _, topic := range declared {
		if err := sessions.laser.ensureRetained(
			ctx, topic, retention.options(partitions)); err != nil {
			return SessionBootstrap{}, err
		}
	}
	if !sessions.laser.capabilities.Sessions {
		return SessionBootstrap{}, nil
	}
	if err := sessions.laser.PublishControlIn(ctx, wire.ControlCommand{
		Kind:   wire.CommandRegisterSessionSource,
		Stream: sessions.config.Stream,
		Topics: ptr(wire.AllSessionTopics()),
	}, sessions.config.Stream); err != nil {
		// An account without session administration rights still has its
		// topics, so the registration is reported rather than raised.
		return SessionBootstrap{Registered: false}, nil
	}
	return SessionBootstrap{Registered: sessions.registrationVisible(ctx)}, nil
}

// Unregister stops the deployment indexing this stream's sessions.
//
// The index is dropped once the control record folds. A later Bootstrap
// registers the stream again.
func (sessions *Sessions) Unregister(ctx context.Context) error {
	if !sessions.laser.capabilities.Sessions {
		return &UnsupportedError{Surface: "sessions"}
	}
	return sessions.laser.PublishControlIn(ctx, wire.ControlCommand{
		Kind:   wire.CommandRemoveSessionSource,
		Stream: sessions.config.Stream,
	}, sessions.config.Stream)
}

// registrationVisible waits until the index answers for this stream.
//
// A registration is a control record the deployment folds, so it is not
// visible the instant it is published. The wait is bounded by the publish
// timeout: past it the registration is reported as not visible rather than
// waited on forever, because the topics are usable either way.
func (sessions *Sessions) registrationVisible(ctx context.Context) bool {
	deadline := time.Now().Add(sessions.laser.publishSchedule().Timeout)
	for time.Now().Before(deadline) {
		attempt, cancel := context.WithDeadline(ctx, deadline)
		_, err := sessions.Changes(attempt, 0, 1)
		cancel()
		switch {
		case err == nil:
			return true
		case indexCatchingUp(err):
			// The index has not folded the registration yet, which is the
			// normal answer for the first moments after one.
		default:
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(registrationPollInterval):
		}
	}
	return false
}

// indexCatchingUp reports a read the session index could not answer yet,
// which is what a registration looks like before it folds.
func indexCatchingUp(err error) bool {
	var failure *SessionError
	if errors.As(err, &failure) {
		switch failure.Failure.Kind {
		case wire.SessionNotRegistered, wire.SessionStale:
			return true
		}
	}
	var command *CommandFailure
	if errors.As(err, &command) {
		switch command.Failure.Code {
		case wire.ResultUnavailable, wire.ResultStale:
			return true
		}
	}
	return false
}

// ptr is the address of a value, for the optional contract fields that carry
// a pointer.
func ptr[T any](value T) *T { return &value }
