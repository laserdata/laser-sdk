package laser_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// A deployment reached from the environment is how a program moves between
// local, staging and Cloud with no code change, so a missing variable names
// itself rather than failing as a bad connection string.
func TestGivenNoConnectionStringInTheEnvironmentWhenResolvedThenShouldNameTheVariable(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset": {},
		"empty": {"LASER_CONNECTION_STRING": ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := laser.ConnectionFromEnv(laser.Options{Environment: laser.EnvMap(env)})
			if err == nil {
				t.Fatal("a missing connection string must be refused")
			}
			var config *laser.ConfigError
			if !errors.As(err, &config) {
				t.Fatalf("want a ConfigError, got %T", err)
			}
			if !strings.Contains(err.Error(), "LASER_CONNECTION_STRING") {
				t.Errorf("want the variable named, got %v", err)
			}
		})
	}
}

func TestGivenAStreamInTheEnvironmentWhenResolvedThenShouldPinItAsTheDefault(t *testing.T) {
	connectionString, resolved, err := laser.ConnectionFromEnv(laser.Options{
		Environment: laser.EnvMap(map[string]string{
			"LASER_CONNECTION_STRING": "iggy://user:password@host",
			"LASER_STREAM":            "agent-telemetry",
		}),
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if connectionString != "iggy://user:password@host" {
		t.Errorf("want the connection string carried, got %q", connectionString)
	}
	if resolved.DefaultStream != "agent-telemetry" {
		t.Errorf("want the stream pinned, got %q", resolved.DefaultStream)
	}
}

// An explicit setting is never overridden by ambient configuration.
func TestGivenAPinnedStreamWhenTheEnvironmentNamesAnotherThenShouldKeepThePinned(t *testing.T) {
	_, resolved, err := laser.ConnectionFromEnv(laser.Options{
		DefaultStream: "chosen",
		Environment: laser.EnvMap(map[string]string{
			"LASER_CONNECTION_STRING": "iggy://user:password@host",
			"LASER_STREAM":            "ambient",
		}),
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved.DefaultStream != "chosen" {
		t.Errorf("want the pinned stream kept, got %q", resolved.DefaultStream)
	}
}

func TestGivenAConnectTimeoutWhenResolvedThenShouldPreferTheCallerThenTheEnvironment(t *testing.T) {
	pinned, err := laser.Options{
		Timeout:     5 * time.Second,
		Environment: laser.EnvMap(map[string]string{"LASER_CONNECT_TIMEOUT_MS": "100"}),
	}.Resolve()
	if err != nil || pinned.Timeout != 5*time.Second {
		t.Errorf("want the caller's deadline, got %v (%v)", pinned.Timeout, err)
	}

	fromEnv, err := laser.Options{
		Environment: laser.EnvMap(map[string]string{"LASER_CONNECT_TIMEOUT_MS": "250"}),
	}.Resolve()
	if err != nil || fromEnv.Timeout != 250*time.Millisecond {
		t.Errorf("want the environment's deadline, got %v (%v)", fromEnv.Timeout, err)
	}

	fallback, err := laser.Options{Environment: noEnv()}.Resolve()
	if err != nil || fallback.Timeout != laser.DefaultConnectTimeout {
		t.Errorf("want the default deadline, got %v (%v)", fallback.Timeout, err)
	}
}

func TestGivenAMalformedConnectTimeoutWhenResolvedThenShouldRefuseIt(t *testing.T) {
	for _, value := range []string{"soon", "-1", "0", "1.5", "+30"} {
		_, err := laser.Options{
			Environment: laser.EnvMap(map[string]string{"LASER_CONNECT_TIMEOUT_MS": value}),
		}.Resolve()
		if err == nil {
			t.Errorf("LASER_CONNECT_TIMEOUT_MS=%q must be refused", value)
		}
	}
}

// A connection string that cannot be parsed fails before anything is dialed,
// so a typo costs no network round trip and no deadline.
func TestGivenABadConnectionStringWhenConnectingThenShouldFailBeforeDialing(t *testing.T) {
	started := time.Now()
	_, err := laser.ConnectWith(context.Background(), "iggy+quic://user:password@host",
		laser.Options{Timeout: 30 * time.Second, Environment: noEnv()})
	if err == nil {
		t.Fatal("a QUIC connection string must be refused")
	}
	var config *laser.ConfigError
	if !errors.As(err, &config) {
		t.Fatalf("want a ConfigError, got %T", err)
	}
	// The refusal has to name what was wrong, not merely be the right type: a
	// caller fixes a connection string from the message.
	if !strings.Contains(err.Error(), "TCP, plain or TLS") {
		t.Errorf("want the transport named, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Errorf("a parse failure must not wait on the deadline, took %v", elapsed)
	}
}

// A refused connection is a transport failure, and its report must never carry
// the password the connection string held.
func TestGivenARefusedConnectionWhenReportedThenShouldNotEchoTheCredential(t *testing.T) {
	const secret = "sup3rs3cr3t"
	// Port zero on the loopback, bound and immediately released, is an address
	// nothing is listening on.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release the port: %v", err)
	}

	_, err = laser.ConnectWith(context.Background(),
		"iggy://user:"+secret+"@"+address,
		laser.Options{Timeout: 3 * time.Second, Environment: noEnv()})
	if err == nil {
		t.Fatal("connecting to a closed port must fail")
	}
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if strings.Contains(cause.Error(), secret) {
			t.Fatalf("credential leaked: %v", cause)
		}
	}
	var transport *laser.TransportError
	if !errors.As(err, &transport) {
		t.Errorf("want a TransportError, got %T: %v", err, err)
	}
}

// Every stage shares one deadline, so a server that accepts the socket and
// then says nothing cannot hold the caller past the budget it set.
func TestGivenAServerThatAcceptsAndStallsWhenConnectingThenShouldStopAtTheDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	accepted := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			close(accepted)
			return
		}
		close(accepted)
		// Hold the socket open without answering, which is the stall this test
		// is about.
		<-time.After(10 * time.Second)
		conn.Close()
	}()

	// The connect runs on its own goroutine so a deadline that is not honored
	// fails this test quickly instead of hanging the suite until Go's global
	// timeout, which would report as a panic rather than as this assertion.
	type outcome struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan outcome, 1)
	go func() {
		started := time.Now()
		_, err := laser.ConnectWith(context.Background(),
			"iggy://user:password@"+listener.Addr().String(),
			laser.Options{Timeout: 750 * time.Millisecond, Environment: noEnv()})
		done <- outcome{err: err, elapsed: time.Since(started)}
	}()

	select {
	case result := <-done:
		if result.err == nil {
			t.Fatal("a stalled connect must fail")
		}
		if result.elapsed > 5*time.Second {
			t.Errorf("want the deadline honored, took %v", result.elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the connect ignored its deadline against a server that never answered")
	}
	<-accepted
}

// fakeClient stands in for a connected Apache Iggy client. The embedded
// interface supplies the methods this test never calls, so only the two that
// matter here need a body.
type fakeClient struct {
	clusterOfOne
	closes int
}

func (client *fakeClient) Close() error {
	client.closes++
	return nil
}

// A deferred Close must be safe wherever it is written, so closing twice is not
// an error and the second call reaches no closed connection.
func TestGivenAConnectedClientWhenClosedTwiceThenShouldReleaseItOnce(t *testing.T) {
	client := &fakeClient{}
	handle := laser.FromClient(client, laser.Options{})

	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Errorf("closing twice must succeed, got %v", err)
	}
	if client.closes != 1 {
		t.Errorf("want the connection released once, got %d", client.closes)
	}
	if handle.Client() != nil {
		t.Error("a closed handle must not hand out its connection")
	}
}

// A handle that never connected still closes cleanly.
func TestGivenAnUnconnectedHandleWhenClosedThenShouldStaySilent(t *testing.T) {
	var handle laser.Laser
	if err := handle.Close(); err != nil {
		t.Errorf("closing an unconnected handle must succeed, got %v", err)
	}
	if handle.Capabilities().Hello != laser.HelloUnknown {
		t.Error("a handle that never probed establishes nothing")
	}
}

// A caller-supplied connection is never probed, because this package did not
// open it and cannot assume what it reaches.
func TestGivenACallerSuppliedClientWhenWrappedThenShouldNotProbeIt(t *testing.T) {
	pinned := laser.OpenCapabilities()
	pinned.A2AGateway = true
	handle := laser.FromClient(&fakeClient{},
		laser.Options{Capabilities: &pinned, DefaultStream: "agents"})

	if handle.Capabilities().Hello != laser.HelloUnknown {
		t.Errorf("want nothing established, got %s", handle.Capabilities().Hello)
	}
	if !handle.Capabilities().A2AGateway {
		t.Error("the pinned capability set must be kept as given")
	}
	if handle.DefaultStream() != "agents" {
		t.Errorf("want the pinned stream, got %q", handle.DefaultStream())
	}
}
