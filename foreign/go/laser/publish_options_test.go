package laser_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// recoveringServer fails a scripted number of appends, counting how often it
// was reconnected.
type recoveringServer struct {
	clusterOfOne
	guard       sync.Mutex
	fail        []error
	attempts    int
	reconnects  int
	hangOnFirst bool
}

func (server *recoveringServer) Close() error { return nil }

func (server *recoveringServer) Connect(context.Context) error {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.reconnects++
	return nil
}

func (server *recoveringServer) SendMessages(
	ctx context.Context, _ iggcon.Identifier, _ iggcon.Identifier,
	_ iggcon.Partitioning, _ []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.guard.Lock()
	server.attempts++
	attempt := server.attempts
	hang := server.hangOnFirst && attempt == 1
	var failure error
	if len(server.fail) > 0 {
		failure = server.fail[0]
		server.fail = server.fail[1:]
	}
	server.guard.Unlock()
	if hang {
		// A server that accepts the write and never answers is what the
		// attempt timeout exists for.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if failure != nil {
		return nil, failure
	}
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *recoveringServer) counts() (attempts, reconnects int) {
	server.guard.Lock()
	defer server.guard.Unlock()
	return server.attempts, server.reconnects
}

func recoveringHandle(
	server iggcon.Client, publish laser.PublishOptions,
) *laser.Laser {
	return laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Publish: &publish,
	})
}

// A dropped connection is the one publish failure a resend reliably fixes, so
// it is retried after a reconnect.
func TestGivenADroppedLinkWhenPublishingThenShouldReconnectAndRetry(t *testing.T) {
	server := &recoveringServer{fail: []error{ierror.Disconnected{}}}
	handle := recoveringHandle(server, laser.PublishOptions{
		Timeout: 2 * time.Second, MaxRetries: 3, RetryBackoff: time.Millisecond,
	})

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	attempts, reconnects := server.counts()
	if attempts != 2 {
		t.Errorf("want one retry, got %d attempts", attempts)
	}
	if reconnects != 1 {
		t.Errorf("want a reconnect before the retry, got %d", reconnects)
	}
}

// Only a lost link or an expired session is retried. A refusal the server
// reasoned about will fail the same way however many times it is sent, and
// retrying it only delays the report.
func TestGivenAPermanentRefusalWhenPublishingThenShouldNotRetry(t *testing.T) {
	server := &recoveringServer{fail: []error{ierror.TopicIdNotFound{}}}
	handle := recoveringHandle(server, laser.PublishOptions{
		Timeout: 2 * time.Second, MaxRetries: 5, RetryBackoff: time.Millisecond,
	})

	_, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(context.Background())

	if err == nil {
		t.Fatal("a refused publish must fail")
	}
	attempts, reconnects := server.counts()
	if attempts != 1 {
		t.Errorf("want no retry, got %d attempts", attempts)
	}
	if reconnects != 0 {
		t.Errorf("want no reconnect, got %d", reconnects)
	}
}

// An expired session is fixed by reconnecting, so it is retried like a
// dropped link rather than reported as a refusal.
func TestGivenAnExpiredSessionWhenPublishingThenShouldReconnectAndRetry(t *testing.T) {
	server := &recoveringServer{fail: []error{ierror.Unauthenticated{}}}
	handle := recoveringHandle(server, laser.PublishOptions{
		Timeout: 2 * time.Second, MaxRetries: 3, RetryBackoff: time.Millisecond,
	})

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if _, reconnects := server.counts(); reconnects != 1 {
		t.Errorf("want a reconnect, got %d", reconnects)
	}
}

// Retries stop at the configured limit, so a long outage fails rather than
// retrying forever.
func TestGivenAnOutageWhenRetriesExhaustThenShouldStopAtTheLimit(t *testing.T) {
	server := &recoveringServer{fail: []error{
		ierror.Disconnected{}, ierror.Disconnected{}, ierror.Disconnected{},
		ierror.Disconnected{}, ierror.Disconnected{},
	}}
	handle := recoveringHandle(server, laser.PublishOptions{
		Timeout: 2 * time.Second, MaxRetries: 2, RetryBackoff: time.Millisecond,
	})

	_, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(context.Background())

	if err == nil {
		t.Fatal("an exhausted outage must fail")
	}
	if attempts, _ := server.counts(); attempts != 3 {
		t.Errorf("want the first attempt and two retries, got %d", attempts)
	}

	// The handle stays usable: a later call on a recovered server succeeds.
	if _, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(context.Background()); err != nil {
		t.Errorf("want the handle still usable, got %v", err)
	}
}

// Setting retries to zero turns resending off, for a caller that cannot
// tolerate the duplicate a lost reply can cause.
func TestGivenNoRetriesWhenPublishingThenShouldSendOnce(t *testing.T) {
	server := &recoveringServer{fail: []error{ierror.Disconnected{}}}
	handle := recoveringHandle(server, laser.PublishOptions{
		Timeout: 2 * time.Second, MaxRetries: 0, RetryBackoff: time.Millisecond,
	})

	_, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(context.Background())

	if err == nil {
		t.Fatal("without retries a dropped link fails the publish")
	}
	if attempts, _ := server.counts(); attempts != 1 {
		t.Errorf("want exactly one attempt, got %d", attempts)
	}
}

// A server that accepts the write and never answers is what the attempt
// timeout exists for, and the connection is cleaned up afterwards because a
// cancelled read must not leave its late reply for the next caller.
func TestGivenAStalledAttemptWhenTimedOutThenShouldRecoverAndRetry(t *testing.T) {
	server := &recoveringServer{hangOnFirst: true}
	handle := recoveringHandle(server, laser.PublishOptions{
		Timeout: 60 * time.Millisecond, MaxRetries: 2,
		RetryBackoff: time.Millisecond,
	})

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	attempts, reconnects := server.counts()
	if attempts != 2 {
		t.Errorf("want the stalled attempt and a retry, got %d", attempts)
	}
	if reconnects == 0 {
		t.Error("want the abandoned connection cleaned up")
	}
}

// A stall with no retries left still cleans up, because the abandoned read
// outlives the attempt whether or not anything follows it.
func TestGivenAStallWithNoRetriesWhenTimedOutThenShouldStillCleanUp(t *testing.T) {
	server := &recoveringServer{hangOnFirst: true}
	handle := recoveringHandle(server, laser.PublishOptions{
		Timeout: 60 * time.Millisecond, MaxRetries: 0,
		RetryBackoff: time.Millisecond,
	})

	_, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(context.Background())

	var timeout *laser.StageTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want a StageTimeoutError, got %T: %v", err, err)
	}
	if !timeout.Timeout() {
		t.Error("a deadline is a timeout rather than a fault")
	}
	if _, reconnects := server.counts(); reconnects == 0 {
		t.Error("want the abandoned connection cleaned up")
	}
}

// A caller that gives up stops the publish rather than running out its
// retries.
func TestGivenACancelledCallerWhenPublishingThenShouldStop(t *testing.T) {
	server := &recoveringServer{fail: []error{
		ierror.Disconnected{}, ierror.Disconnected{}, ierror.Disconnected{},
	}}
	handle := recoveringHandle(server, laser.PublishOptions{
		Timeout: time.Second, MaxRetries: 5, RetryBackoff: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(ctx)

	// The caller's own deadline is reported as such rather than as a stage
	// this client timed out, because retrying is not the answer to it.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the caller's deadline, got %T: %v", err, err)
	}
	// It gave up on the caller's deadline rather than waiting out the hour.
	if attempts, _ := server.counts(); attempts > 2 {
		t.Errorf("want the cancellation to stop it, got %d attempts", attempts)
	}
}

// A caller's own deadline cutting an attempt short is reported as the
// caller's, not as a stage this client timed out: retrying is not the answer
// to a caller that has stopped waiting.
func TestGivenACallerDeadlineInsideAnAttemptWhenCutShortThenShouldReportItAsTheirs(t *testing.T) {
	server := &recoveringServer{hangOnFirst: true}
	// The attempt has an hour, so only the caller's deadline can end it, and
	// no retry follows to replace the error it ends with.
	handle := recoveringHandle(server, laser.PublishOptions{
		Timeout: time.Hour, MaxRetries: 0, RetryBackoff: time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the caller's deadline, got %T: %v", err, err)
	}
	var stage *laser.StageTimeoutError
	if errors.As(err, &stage) {
		t.Error("a caller that stopped waiting is not a stage that timed out")
	}
	if attempts, _ := server.counts(); attempts != 1 {
		t.Errorf("want no retry of a cancelled caller, got %d attempts", attempts)
	}
}

// The delay doubles per attempt, stops at the cap, and is spread over its
// upper half so clients that failed together do not come back together.
func TestGivenRetryDelaysWhenComputedThenShouldDoubleCapAndSpread(t *testing.T) {
	options := laser.PublishOptions{
		Timeout: time.Minute, MaxRetries: 10,
		RetryBackoff: 250 * time.Millisecond,
	}

	for attempt, ceiling := range map[uint32]time.Duration{
		0: 250 * time.Millisecond,
		1: 500 * time.Millisecond,
		2: time.Second,
		3: 2 * time.Second,
	} {
		delay := options.RetryDelay(attempt)
		if delay < ceiling/2 || delay > ceiling {
			t.Errorf("attempt %d: want a delay in [%v, %v], got %v",
				attempt, ceiling/2, ceiling, delay)
		}
	}
	// The spread is what keeps clients that failed together from coming back
	// together, so two delays at one attempt are not the same delay.
	spread := map[time.Duration]struct{}{}
	for range 200 {
		spread[options.RetryDelay(3)] = struct{}{}
	}
	if len(spread) < 2 {
		t.Error("a delay that is always the same spreads nothing")
	}

	// A long outage is capped rather than paced minutes apart.
	for _, attempt := range []uint32{10, 20, 1 << 20} {
		delay := options.RetryDelay(attempt)
		if delay > laser.MaxPublishRetryDelay {
			t.Errorf("attempt %d: want the cap, got %v", attempt, delay)
		}
		if delay < laser.MaxPublishRetryDelay/2 {
			t.Errorf("attempt %d: want the capped delay spread, got %v",
				attempt, delay)
		}
	}
}

// A misconfigured schedule is refused before anything is dialed, because a
// client that connects and then fails on every send is harder to diagnose
// than one that refuses to start.
func TestGivenAnInvalidScheduleWhenResolvedThenShouldRefuseIt(t *testing.T) {
	for name, options := range map[string]laser.PublishOptions{
		"a timeout of zero": {
			Timeout: 0, RetryBackoff: time.Second,
		},
		"a timeout past the wire bound": {
			Timeout: 30 * 24 * time.Hour, RetryBackoff: time.Second,
		},
		"a backoff of zero": {
			Timeout: time.Minute, RetryBackoff: 0,
		},
		"a backoff past the wire bound": {
			Timeout: time.Minute, RetryBackoff: 30 * 24 * time.Hour,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := options.Validate(); err == nil {
				t.Fatal("an unhonourable schedule must be refused")
			}
			pinned := options
			resolved, err := laser.Options{Publish: &pinned}.Resolve()
			if err == nil {
				t.Fatalf("a connect must refuse it too, got %+v", resolved.Publish)
			}
			var config *laser.ConfigError
			if !errors.As(err, &config) {
				t.Fatalf("want a ConfigError, got %T: %v", err, err)
			}
		})
	}
}

// The schedule is the one every Laser client shares, read from the same
// environment variables.
func TestGivenTheEnvironmentWhenResolvedThenShouldReadTheSharedVariables(t *testing.T) {
	resolved, err := laser.Options{Environment: laser.EnvMap(map[string]string{
		"LASER_PUBLISH_TIMEOUT_MS":       "90000",
		"LASER_PUBLISH_MAX_RETRIES":      "5",
		"LASER_PUBLISH_RETRY_BACKOFF_MS": "500",
	})}.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if resolved.Publish == nil {
		t.Fatal("want a resolved schedule")
	}
	if resolved.Publish.Timeout != 90*time.Second {
		t.Errorf("want the timeout read, got %v", resolved.Publish.Timeout)
	}
	if resolved.Publish.MaxRetries != 5 {
		t.Errorf("want the retries read, got %d", resolved.Publish.MaxRetries)
	}
	if resolved.Publish.RetryBackoff != 500*time.Millisecond {
		t.Errorf("want the backoff read, got %v", resolved.Publish.RetryBackoff)
	}

	// Nothing set falls back to the shared defaults.
	bare, err := laser.Options{
		Environment: laser.EnvMap(map[string]string{}),
	}.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if *bare.Publish != laser.DefaultPublishOptions() {
		t.Errorf("want the shared defaults, got %+v", bare.Publish)
	}
	// An explicit schedule wins over the environment, because configuration a
	// caller wrote is never overridden by what is ambient.
	pinned := laser.PublishOptions{
		Timeout: time.Minute, MaxRetries: 1, RetryBackoff: time.Second,
	}
	explicit, err := laser.Options{
		Publish: &pinned,
		Environment: laser.EnvMap(map[string]string{
			"LASER_PUBLISH_TIMEOUT_MS": "90000",
		}),
	}.Resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if *explicit.Publish != pinned {
		t.Errorf("want the pinned schedule, got %+v", explicit.Publish)
	}
}

// A variable that is not a number is a configuration mistake, named so the
// caller knows which one.
func TestGivenAMalformedVariableWhenResolvedThenShouldNameIt(t *testing.T) {
	for _, name := range []string{
		"LASER_PUBLISH_TIMEOUT_MS",
		"LASER_PUBLISH_MAX_RETRIES",
		"LASER_PUBLISH_RETRY_BACKOFF_MS",
	} {
		_, err := laser.Options{
			Environment: laser.EnvMap(map[string]string{name: "soon"}),
		}.Resolve()

		var config *laser.ConfigError
		if !errors.As(err, &config) {
			t.Fatalf("%s: want a ConfigError, got %T: %v", name, err, err)
		}
		if !strings.Contains(err.Error(), name) {
			t.Errorf("want %s named, got %q", name, err.Error())
		}
	}
	// A value past the wire bound is refused where it is read.
	if _, err := (laser.Options{
		Environment: laser.EnvMap(map[string]string{
			"LASER_PUBLISH_TIMEOUT_MS": "99999999999",
		}),
	}).Resolve(); err == nil {
		t.Error("a timeout past the wire bound must be refused")
	}
}

// A handle can take its own schedule, and one it could not honour is refused
// rather than stored.
func TestGivenAHandleScheduleWhenDerivedThenShouldValidateIt(t *testing.T) {
	handle := recoveringHandle(&recoveringServer{}, laser.DefaultPublishOptions())

	derived, err := handle.WithPublishOptions(laser.PublishOptions{
		Timeout: 5 * time.Second, MaxRetries: 1, RetryBackoff: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if derived == handle {
		t.Error("a derived handle is its own handle")
	}

	if _, err := handle.WithPublishOptions(laser.PublishOptions{}); err == nil {
		t.Error("a schedule the client cannot honour must be refused")
	}
}

// A handle built as a zero value still publishes, under the shared defaults
// rather than a zero timeout that would fail every attempt.
func TestGivenAZeroHandleWhenPublishingThenShouldUseTheSharedDefaults(t *testing.T) {
	server := &recoveringServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("x")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if attempts, _ := server.counts(); attempts != 1 {
		t.Errorf("want one attempt, got %d", attempts)
	}
}
