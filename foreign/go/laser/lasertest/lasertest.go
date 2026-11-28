// Package lasertest builds the values an agent handler receives, so a handler
// is unit tested without the reliable consumer or a live server.
package lasertest

import (
	"sync/atomic"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// AgentCtxOptions name the agent a test context acts as and where it replies.
type AgentCtxOptions struct {
	// Agent is the identity the handler runs as.
	Agent wire.AgentID
	// RespondOn is the topic a reply goes to. Empty refuses a reply.
	RespondOn string
}

// AgentMessage builds a plain message carrying payload and provenance, seated
// at the start of partition 0.
func AgentMessage(payload []byte, provenance *laser.Provenance) *laser.AgentMessage {
	return &laser.AgentMessage{Record: &laser.Record{
		Message:    iggcon.IggyMessage{Payload: append([]byte(nil), payload...)},
		Provenance: provenance,
	}}
}

// AgentCtx builds the handler context of message over a caller-owned client,
// which supplies whatever IO the context helpers use.
func AgentCtx(
	client *laser.Laser, message *laser.AgentMessage, options AgentCtxOptions,
) (*laser.AgentCtx, error) {
	stream := client.DefaultStream()
	if stream == "" {
		stream = "lasertest"
	}
	runtime, err := client.AgentRuntime(
		client.Agent(options.Agent, laser.AgentOptions{}),
		laser.RuntimeOptions{Topic: wire.AgentSessions, Stream: stream, RespondOn: options.RespondOn})
	if err != nil {
		return nil, err
	}
	return runtime.HandlerContext(message), nil
}

// TestClock is a clock a test sets and advances, for AgentOptions.Now.
type TestClock struct{ micros atomic.Int64 }

// NewTestClock starts a test clock at start, kept to the microsecond.
func NewTestClock(start time.Time) *TestClock {
	clock := &TestClock{}
	clock.Set(start)
	return clock
}

// Now reads the clock.
func (clock *TestClock) Now() time.Time { return time.UnixMicro(clock.micros.Load()) }

// Advance moves the clock forward.
func (clock *TestClock) Advance(by time.Duration) { clock.micros.Add(by.Microseconds()) }

// Set moves the clock to an absolute time.
func (clock *TestClock) Set(now time.Time) { clock.micros.Store(now.UnixMicro()) }
