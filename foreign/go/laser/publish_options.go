package laser

import (
	"context"
	"errors"
	"strconv"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

// The publish recovery defaults, shared with every other Laser client and
// documented in docs/publish-recovery.md.
const (
	// DefaultPublishTimeout bounds one attempt.
	DefaultPublishTimeout = 60 * time.Second
	// DefaultPublishMaxRetries is how many attempts follow the first. Zero
	// disables resending.
	DefaultPublishMaxRetries uint32 = 3
	// DefaultPublishRetryBackoff is the first retry delay. Each later one
	// doubles it, up to MaxPublishRetryDelay.
	DefaultPublishRetryBackoff = 250 * time.Millisecond
	// MaxPublishRetryDelay caps the doubling, so a long outage does not pace
	// retries minutes apart.
	MaxPublishRetryDelay = 30 * time.Second
	// MaxConfigurableMillis is the longest any configured duration may be.
	MaxConfigurableMillis = 2147483647
)

// PublishOptions is how a publish is retried.
//
// Publishing is at-least-once: a reply lost after the server applied the
// append cannot be told from one it never received, so a resend can duplicate
// a record. A caller that cannot tolerate that sets MaxRetries to zero and
// settles the outcome itself.
type PublishOptions struct {
	// Timeout bounds one attempt, including the reconnect before a retry.
	Timeout time.Duration
	// MaxRetries is how many attempts follow the first.
	MaxRetries uint32
	// RetryBackoff is the first retry delay.
	RetryBackoff time.Duration
}

// DefaultPublishOptions is the shared schedule.
func DefaultPublishOptions() PublishOptions {
	return PublishOptions{
		Timeout:      DefaultPublishTimeout,
		MaxRetries:   DefaultPublishMaxRetries,
		RetryBackoff: DefaultPublishRetryBackoff,
	}
}

// Validate refuses a schedule a deployment could not honour.
//
// It is checked before connecting rather than at the first publish, because a
// misconfigured client that connects and then fails on every send is harder
// to diagnose than one that refuses to start.
func (options PublishOptions) Validate() error {
	if err := boundedMillis("publish timeout", options.Timeout); err != nil {
		return err
	}
	return boundedMillis("publish retry backoff", options.RetryBackoff)
}

// boundedMillis holds a configured duration to what the wire can carry.
func boundedMillis(what string, value time.Duration) error {
	milliseconds := value.Milliseconds()
	if milliseconds < 1 || milliseconds > MaxConfigurableMillis {
		return configError(what + " must be between 1 and " +
			strconv.Itoa(MaxConfigurableMillis) + " milliseconds")
	}
	return nil
}

// publishOptions resolves the schedule, preferring the caller's values over
// the environment over the shared defaults.
func (options Options) publishOptions() (PublishOptions, error) {
	resolved := DefaultPublishOptions()
	if options.Publish != nil {
		resolved = *options.Publish
	} else {
		timeout, err := options.millis("LASER_PUBLISH_TIMEOUT_MS",
			DefaultPublishTimeout)
		if err != nil {
			return PublishOptions{}, err
		}
		backoff, err := options.millis("LASER_PUBLISH_RETRY_BACKOFF_MS",
			DefaultPublishRetryBackoff)
		if err != nil {
			return PublishOptions{}, err
		}
		retries, err := options.count("LASER_PUBLISH_MAX_RETRIES",
			DefaultPublishMaxRetries)
		if err != nil {
			return PublishOptions{}, err
		}
		resolved = PublishOptions{
			Timeout: timeout, MaxRetries: retries, RetryBackoff: backoff,
		}
	}
	if err := resolved.Validate(); err != nil {
		return PublishOptions{}, err
	}
	return resolved, nil
}

// millis reads a duration from the environment.
func (options Options) millis(
	name string, fallback time.Duration,
) (time.Duration, error) {
	raw, present := options.environment()(name)
	if !present || raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, configError(name + " must be an integer")
	}
	// The bound itself is Validate's, so one place decides what a schedule
	// may ask for whether it came from a caller or from the environment.
	return time.Duration(value) * time.Millisecond, nil
}

// count reads a retry count from the environment. Zero is meaningful here,
// since it is how resending is turned off.
func (options Options) count(name string, fallback uint32) (uint32, error) {
	raw, present := options.environment()(name)
	if !present || raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, configError(name + " must be an integer that fits in 32 bits")
	}
	return uint32(value), nil
}

// RetryDelay is the pause before one retry, counting from zero: the backoff
// doubled per attempt, capped, and spread over its upper half so clients that
// failed together do not come back together.
func (options PublishOptions) RetryDelay(attempt uint32) time.Duration {
	shift := min(attempt, 16)
	delay := options.RetryBackoff * time.Duration(1<<shift)
	if delay <= 0 || delay > MaxPublishRetryDelay {
		delay = MaxPublishRetryDelay
	}
	return Jittered(delay)
}

// RetryablePublish reports a publish failure the identical attempt may
// survive.
//
// Only a lost or stalled link and an expired session qualify. A refusal the
// server reasoned about, a malformed request or a missing topic will fail the
// same way however many times it is sent, and retrying it only delays the
// report.
func RetryablePublish(err error) bool {
	if timedOut(err) {
		return true
	}
	return transientTransport(err) || needsReauthentication(err)
}

// timedOut reports a deadline rather than a fault, whichever wait set it.
func timedOut(err error) bool {
	var deadline interface{ Timeout() bool }
	return errors.As(err, &deadline) && deadline.Timeout()
}

// transientTransport reports a link that dropped or was never established.
func transientTransport(err error) bool {
	for _, transient := range []error{
		ierror.Disconnected{}, ierror.TcpError{}, ierror.QuicError{},
		ierror.StaleClient{}, ierror.NotConnected{}, ierror.ConnectionClosed{},
		ierror.CannotEstablishConnection{},
		ierror.CannotSendMessagesDueToClientDisconnection{},
		ierror.TransientNotAccepted{}, ierror.TransientNotCommitted{},
		ierror.CannotReadFile{}, ierror.CannotReadPartitions{},
	} {
		if errors.Is(err, transient) {
			return true
		}
	}
	return false
}

// needsReauthentication reports a session the server no longer accepts, which
// a reconnect fixes.
func needsReauthentication(err error) bool {
	for _, expired := range []error{
		ierror.Unauthenticated{}, ierror.InvalidCredentials{},
		ierror.StaleClient{},
	} {
		if errors.Is(err, expired) {
			return true
		}
	}
	return false
}

// StageTimeoutError reports a stage that did not finish in the time it was
// given.
//
// It names the stage rather than only the duration, because "the server never
// accepted the connection" and "the server never answered the login" call for
// different fixes.
type StageTimeoutError struct{ Stage string }

func (e *StageTimeoutError) Error() string {
	return "timed out waiting for " + e.Stage
}

// Timeout marks this as a deadline rather than a fault, the same way the
// reply wait does, so a caller branches on one interface for both.
func (e *StageTimeoutError) Timeout() bool { return true }

// runWithRecovery runs one publish under the configured schedule, reconnecting
// between attempts.
//
// One deadline covers the recovery and the attempt together, so a reconnect
// that hangs cannot eat the whole budget and leave no time to send. A timed
// out attempt is followed by a recovery whatever happens next, because a
// cancelled read must not leave its late reply on a shared connection for the
// next caller to decode as its own.
func runWithRecovery[T any](
	ctx context.Context, options PublishOptions,
	send func(context.Context) (T, error),
	recover func(context.Context) error,
) (T, error) {
	var zero T
	needsRecovery := false
	for attempt := uint32(0); ; attempt++ {
		bounded, cancel := context.WithTimeout(ctx, options.Timeout)
		var err error
		if needsRecovery {
			err = recover(bounded)
		}
		var value T
		if err == nil {
			value, err = send(bounded)
		}
		cancel()
		if err == nil {
			return value, nil
		}
		err = asTimeout(ctx, err, "the publish reply")
		timeout := timedOut(err)
		if !RetryablePublish(err) || attempt >= options.MaxRetries {
			if timeout {
				// The attempt was abandoned mid-read, so the connection is
				// cleaned up even though nothing will be retried on it.
				recoverWithin(ctx, options.Timeout, recover)
			}
			return zero, err
		}
		needsRecovery = true
		if timeout {
			needsRecovery = !recoverWithin(ctx, options.Timeout, recover)
		}
		if err := sleepWithin(ctx, options.RetryDelay(attempt)); err != nil {
			return zero, err
		}
	}
}

// asTimeout names a deadline this client set, leaving a cancellation by the
// caller as it is.
func asTimeout(ctx context.Context, err error, stage string) error {
	if ctx.Err() != nil {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &StageTimeoutError{Stage: stage}
	}
	return err
}

// recoverWithin reconnects under its own bound, reporting whether it worked.
func recoverWithin(
	ctx context.Context, within time.Duration, recover func(context.Context) error,
) bool {
	bounded, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	return recover(bounded) == nil
}
