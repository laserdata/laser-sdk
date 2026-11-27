//go:build integration

// Live integration tests, run against a real Apache Iggy deployment rather
// than a fixture.
//
// They sit behind the `integration` build tag because the default gate has to
// stay hermetic: a developer with no server running must still get a green
// `go test ./...`. Run these with
//
//	go test ./laser/ -tags integration -count=1
//
// against a server the Go client can actually speak to. The client is built on
// the Apache Iggy Go SDK v0.10.0, whose binary protocol a v0.9.0 server does
// not answer, so the deployment has to be v0.10.x. See the live suite's README
// note for the container that serves it.
//
// Every test skips rather than fails when no server answers, because a missing
// deployment is missing infrastructure and not a defect in the client. Once a
// connection is open, every assertion past it is a hard failure: a live test
// that quietly passed without exercising the server would be worse than no
// test at all.
package laser_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// liveDefaultConnectionString is the deployment the live suite reaches when
// the environment names none.
const liveDefaultConnectionString = "iggy://iggy:iggy@127.0.0.1:8090"

// liveConnectTimeout bounds the dial that decides whether to skip. It is short
// because the answer here is only "is anything listening", and a developer
// with no server should learn that in a moment rather than after the default
// connect budget.
const liveConnectTimeout = 5 * time.Second

// liveOperationTimeout bounds one test's work against the server.
const liveOperationTimeout = 30 * time.Second

// livePollWait is how long a consumer waits for a record. It is well above the
// round trip a local container needs, so a slow machine does not read empty
// and fail a round trip that actually worked.
const livePollWait = 5 * time.Second

// liveConnectionString is the deployment to run against.
//
// LASER_LIVE_CONNECTION_STRING is read first so the live suite can be pointed
// somewhere other than whatever LASER_CONNECTION_STRING an example or a shell
// already exports, which matters when the conventional port is taken by
// another container.
func liveConnectionString() string {
	for _, name := range []string{"LASER_LIVE_CONNECTION_STRING", "LASER_CONNECTION_STRING"} {
		if value, present := os.LookupEnv(name); present && value != "" {
			return value
		}
	}
	return liveDefaultConnectionString
}

// dialLive opens a connection, skipping the test when nothing answers.
//
// The skip covers the dial alone. Anything that fails after a connection is
// open is reported as a failure, because at that point the server is reachable
// and the client is the thing under test.
func dialLive(t *testing.T) *laser.Laser {
	t.Helper()
	target := liveConnectionString()

	ctx, cancel := context.WithTimeout(context.Background(), liveConnectTimeout)
	defer cancel()

	client, err := laser.ConnectWith(ctx, target, laser.Options{Timeout: liveConnectTimeout})
	if err != nil {
		t.Skipf("no reachable deployment at %s (%v): start one and set "+
			"LASER_LIVE_CONNECTION_STRING to run the live suite", target, err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close the connection: %v", err)
		}
	})
	return client
}

// liveContext bounds one test's work against the server.
func liveContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
	t.Cleanup(cancel)
	return ctx
}

// liveName mints a name unique to this run, so two runs against one long lived
// container never collide and a leftover from a crashed run is never mistaken
// for this run's resource.
func liveName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// liveTopic creates a stream and a single-partition topic, removing the stream
// when the test ends.
//
// One partition is deliberate: a standalone consumer has to name the
// partitions it reads, and with one partition every record this test publishes
// lands on that one partition whatever the partition key hashes to. The
// ordering rule is not what these tests are checking, so spreading records
// across partitions would only make the round trip racy.
func liveTopic(t *testing.T, client *laser.Laser, ctx context.Context) *laser.Topic {
	t.Helper()
	scratch := liveScratch(t, client, ctx, "laser-live")
	topic := scratch.Laser().Topic("records")
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		t.Fatalf("create a topic on %s: %v", scratch.Name(), err)
	}
	return topic
}

// liveScratch opens a scratch stream and removes it when the test ends.
//
// The scratch stream is what mints a name free on the server, so two runs
// against one long lived container never read each other's records. Nothing
// deletes it on its own, by design: Go has no destructor, so the cleanup is
// registered here where a reader can see it rather than left to a finalizer
// that might never run.
func liveScratch(
	t *testing.T, client *laser.Laser, ctx context.Context, prefix string,
) *laser.ScratchStream {
	t.Helper()
	scratch, err := client.ScratchStream(ctx, prefix)
	if err != nil {
		t.Fatalf("create a scratch stream under %s: %v", prefix, err)
	}
	t.Cleanup(func() {
		// A fresh context, because the test's own may already be done and the
		// stream would then outlive the run and leak into the next one.
		cleanup, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
		defer cancel()
		if err := scratch.Delete(cleanup); err != nil {
			t.Errorf("delete the scratch stream %s: %v", scratch.Name(), err)
		}
	})
	return scratch
}

// livePartitions is the partition ids the server actually assigned a topic.
//
// They are read back rather than assumed, because the partition numbering base
// is the server's to choose and a hard-coded guess fails as a silent timeout:
// a consumer pointed at a partition that does not exist simply never reads
// anything, which looks exactly like a publish that never arrived.
func livePartitions(t *testing.T, topic *laser.Topic, ctx context.Context) []uint32 {
	t.Helper()
	partitions, err := topic.Partitions(ctx)
	if err != nil {
		t.Fatalf("read the partitions of %s: %v", topic.Name(), err)
	}
	if len(partitions) == 0 {
		t.Fatalf("topic %s was created with partitions but the server reports none",
			topic.Name())
	}
	ids := make([]uint32, 0, len(partitions))
	for _, partition := range partitions {
		ids = append(ids, partition.ID)
	}
	return ids
}

// liveConsumer opens a standalone consumer reading every partition of the
// topic from the start of the log.
func liveConsumer(
	t *testing.T, topic *laser.Topic, ctx context.Context, partitions []uint32,
) *laser.Consumer {
	t.Helper()
	start := iggcon.FirstPollingStrategy()
	consumer, err := topic.Consumer(ctx, liveName("reader"), laser.ConsumerOptions{
		Partitions: partitions,
		Start:      &start,
		PollWait:   livePollWait,
		Count:      10,
	})
	if err != nil {
		t.Fatalf("open a consumer on %s: %v", topic.Name(), err)
	}
	t.Cleanup(func() {
		closing, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
		defer cancel()
		if err := consumer.Close(closing); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("close the consumer: %v", err)
		}
	})
	return consumer
}

// TestGivenLiveServerWhenConnectingThenShouldNegotiateCapabilities proves the
// connect path end to end: the dial, the login and the capability probe.
//
// The probe's outcome is asserted rather than its content, because what a
// deployment serves is the deployment's business. What the client must get
// right is that the probe ran and settled on an answer: a HelloUnknown here
// would mean negotiation never happened, and a client that never negotiated
// cannot know which surfaces it may use.
func TestGivenLiveServerWhenConnectingThenShouldNegotiateCapabilities(t *testing.T) {
	client := dialLive(t)

	capabilities := client.Capabilities()
	if capabilities.Hello == laser.HelloUnknown {
		t.Fatal("the connect-time probe established nothing: a live connect must negotiate")
	}
	t.Logf("deployment answered the probe with %s, managed=%v",
		capabilities.Hello, capabilities.Managed)

	connection := client.Connection()
	if connection == nil {
		t.Fatal("a connected client must report the connection string it dialed")
	}
	if connection.Host == "" || connection.Port == 0 {
		t.Fatalf("the parsed connection string lost its address: %+v", connection)
	}

	// The underlying client is what a caller reaches for an operation this
	// package does not wrap, so a connected handle must hand one out.
	if client.Client() == nil {
		t.Fatal("a connected client must expose the underlying Iggy client")
	}
}

// TestGivenLiveServerWhenManagingStreamsAndTopicsThenShouldCreateListAndDelete
// walks the administrative lifecycle against the server.
//
// Create, read back, list and delete are asserted as one sequence because each
// step is only meaningful given the last: a create that does not show up in a
// list has not really created anything, and a delete is only proven by the
// absence that follows it.
func TestGivenLiveServerWhenManagingStreamsAndTopicsThenShouldCreateListAndDelete(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)

	streamName := liveName("laser-live-admin")
	stream := client.Stream(streamName)
	topic := stream.Topic("events")

	if err := topic.Ensure(ctx, 2, nil); err != nil {
		t.Fatalf("create stream %s and topic events: %v", streamName, err)
	}
	// The stream is removed even if an assertion below fails, so a failing run
	// does not leave the container dirty for the next one.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
		defer cancel()
		if _, err := client.Stream(streamName).Delete(cleanup); err != nil {
			t.Errorf("delete stream %s: %v", streamName, err)
		}
	}()

	// Ensure is the create path every client shares, and it has to be safe to
	// repeat: a second call must leave the first call's topic alone rather
	// than failing on the name already being taken.
	if err := topic.Ensure(ctx, 2, nil); err != nil {
		t.Fatalf("Ensure must be repeatable, the second call failed: %v", err)
	}

	exists, err := stream.Exists(ctx)
	if err != nil {
		t.Fatalf("read whether stream %s exists: %v", streamName, err)
	}
	if !exists {
		t.Fatal("the stream created a moment ago is not there")
	}

	streams, err := client.Streams(ctx)
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if !containsStream(streams, streamName) {
		t.Fatalf("stream %s is missing from the server's listing of %d streams",
			streamName, len(streams))
	}

	topics, err := stream.Topics(ctx)
	if err != nil {
		t.Fatalf("list topics on %s: %v", streamName, err)
	}
	found := false
	for _, info := range topics {
		if info.Name != "events" {
			continue
		}
		found = true
		if info.PartitionsCount != 2 {
			t.Errorf("topic events was created with 2 partitions, the server reports %d",
				info.PartitionsCount)
		}
	}
	if !found {
		t.Fatalf("topic events is missing from the listing of %d topics on %s",
			len(topics), streamName)
	}

	// Deleting reports whether something was there to delete, so the first
	// call is true and a repeat is false. That is what lets a caller tell "I
	// removed it" from "it was already gone" without racing a read.
	removed, err := topic.Delete(ctx)
	if err != nil {
		t.Fatalf("delete topic events: %v", err)
	}
	if !removed {
		t.Fatal("deleting an existing topic must report that one was removed")
	}
	removed, err = topic.Delete(ctx)
	if err != nil {
		t.Fatalf("deleting an absent topic must be an absence, not a failure: %v", err)
	}
	if removed {
		t.Fatal("deleting an absent topic must report that nothing was removed")
	}

	topicExists, err := topic.Exists(ctx)
	if err != nil {
		t.Fatalf("read whether the deleted topic exists: %v", err)
	}
	if topicExists {
		t.Fatal("the deleted topic is still there")
	}
}

func containsStream(streams []laser.StreamInfo, name string) bool {
	for _, info := range streams {
		if info.Name == name {
			return true
		}
	}
	return false
}

// TestGivenPublishedRecordWhenConsumingThenShouldRoundTripPayload proves the
// data path: a record published through the Laser producer comes back through
// the Laser consumer with its body and its plain user headers intact.
func TestGivenPublishedRecordWhenConsumingThenShouldRoundTripPayload(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	topic := liveTopic(t, client, ctx)

	// The consumer is opened before the publish so the topic exists and the
	// reader is positioned at the first record, which is what makes the read
	// below deterministic rather than a race against retention.
	partitions := livePartitions(t, topic, ctx)
	consumer := liveConsumer(t, topic, ctx, partitions)

	payload := []byte("the live round trip body")
	if _, err := topic.Publish().
		Payload(payload).
		Header("x-live-test", "round-trip").
		Key("live").
		Send(ctx); err != nil {
		t.Fatalf("publish a record: %v", err)
	}

	record, err := consumer.Next(ctx)
	if err != nil {
		t.Fatalf("read the record back: %v", err)
	}
	if record == nil {
		t.Fatal("the server returned no record for one that was just published")
	}
	if got := string(record.Payload()); got != string(payload) {
		t.Fatalf("payload changed over the round trip: sent %q, read %q", payload, got)
	}
	if record.PartitionID != partitions[0] {
		t.Errorf("a single-partition topic must deliver on partition %d, got %d",
			partitions[0], record.PartitionID)
	}

	// A record with no provenance headers must decode as having none, rather
	// than as a broken spine: a plain data topic is a supported shape.
	if record.Provenance != nil {
		t.Errorf("a record published without provenance must carry none, got %+v",
			record.Provenance)
	}

	if value, ok := userHeader(t, record, "x-live-test"); !ok {
		t.Error("the plain user header did not survive the round trip")
	} else if value != "round-trip" {
		t.Errorf("the user header changed over the round trip: %q", value)
	}
}

// userHeader reads one string user header off a record.
//
// The record carries its headers as the raw encoded block, so they are parsed
// here the same way the provenance decoder parses them. Reading them through
// the shipped deserializer is the point: it proves the block the server handed
// back is one this client can actually parse, not merely bytes that came home.
func userHeader(t *testing.T, record *laser.Record, name string) (string, bool) {
	t.Helper()
	entries, err := iggcon.DeserializeHeaders(record.Message.UserHeaders)
	if err != nil {
		t.Fatalf("parse the record's user headers: %v", err)
	}
	for _, entry := range entries {
		if string(entry.Key.Value) == name {
			return string(entry.Value.Value), true
		}
	}
	return "", false
}

// TestGivenProvenanceHeadersWhenRoundTrippingThenShouldPreserveEverySpineField
// publishes a fully populated provenance block and reads it back off the live
// server.
//
// Every optional field is set, because the header block is encoded and decoded
// key by key and a field nobody exercises is exactly where an encoding bug
// survives. The fixture tests prove the client agrees with itself about the
// bytes. This proves the bytes also survive a real server, which is the step
// that a fixture can never take: a server that rejected, reordered or dropped
// a header would look identical to a fixture round trip.
func TestGivenProvenanceHeadersWhenRoundTrippingThenShouldPreserveEverySpineField(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	topic := liveTopic(t, client, ctx)
	consumer := liveConsumer(t, topic, ctx, livePartitions(t, topic, ctx))

	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	parent := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 8})}
	root := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 9})}

	author, err := wire.NewAgentID("live-author")
	if err != nil {
		t.Fatalf("build the author id: %v", err)
	}
	target, err := wire.NewAgentID("live-target")
	if err != nil {
		t.Fatalf("build the target id: %v", err)
	}

	causalParent := laser.NewMessageID(1, 42)
	inputTokens, outputTokens := uint64(1024), uint64(256)
	costUSD := 0.0125
	// Truncated to microseconds, because that is the resolution the header
	// carries. Comparing a nanosecond value against what came back would fail
	// on the encoding rather than on anything the server did.
	deadline := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	idempotencyKey := "live-idempotency-key"
	correlationID := "live-correlation-id"
	fenceToken := uint64(99)

	sent := &laser.Provenance{
		Conversation:       conversation,
		CausalParent:       &causalParent,
		ParentConversation: &parent,
		RootConversation:   &root,
		Agent:              &author,
		TargetAgent:        &target,
		Usage: &laser.LlmUsage{
			InputTokens:  &inputTokens,
			OutputTokens: &outputTokens,
			CostUSD:      &costUSD,
		},
		DeadlineAt:     &deadline,
		IdempotencyKey: &idempotencyKey,
		CorrelationID:  &correlationID,
		FenceToken:     &fenceToken,
	}

	if _, err := topic.Publish().
		Payload([]byte("provenance round trip")).
		Provenance(sent).
		Send(ctx); err != nil {
		t.Fatalf("publish a record carrying provenance: %v", err)
	}

	record, err := consumer.Next(ctx)
	if err != nil {
		t.Fatalf("read the record back: %v", err)
	}
	if record.Provenance == nil {
		t.Fatal("the provenance spine did not survive the round trip: the record came " +
			"back with no headers the client could read")
	}
	read := record.Provenance

	if read.Conversation != conversation {
		t.Errorf("conversation changed: sent %s, read %s",
			conversation, read.Conversation)
	}
	assertConversation(t, "parent conversation", sent.ParentConversation, read.ParentConversation)
	assertConversation(t, "root conversation", sent.RootConversation, read.RootConversation)

	if read.Agent == nil {
		t.Error("the authoring agent did not survive the round trip")
	} else if *read.Agent != author {
		t.Errorf("authoring agent changed: sent %v, read %v", author, *read.Agent)
	}
	if read.TargetAgent == nil {
		t.Error("the addressee did not survive the round trip")
	} else if *read.TargetAgent != target {
		t.Errorf("addressee changed: sent %v, read %v", target, *read.TargetAgent)
	}

	if read.CausalParent == nil {
		t.Error("the causal parent did not survive the round trip")
	} else if *read.CausalParent != causalParent {
		t.Errorf("causal parent changed: sent %s, read %s",
			causalParent, *read.CausalParent)
	}

	assertString(t, "idempotency key", sent.IdempotencyKey, read.IdempotencyKey)
	assertString(t, "correlation id", sent.CorrelationID, read.CorrelationID)
	assertUint64(t, "fence token", sent.FenceToken, read.FenceToken)

	if read.DeadlineAt == nil {
		t.Error("the deadline did not survive the round trip")
	} else if !read.DeadlineAt.Equal(deadline) {
		t.Errorf("deadline changed: sent %s, read %s",
			deadline, read.DeadlineAt.UTC())
	}

	if read.Usage == nil {
		t.Error("the usage block did not survive the round trip")
	} else {
		assertUint64(t, "input tokens", sent.Usage.InputTokens, read.Usage.InputTokens)
		assertUint64(t, "output tokens", sent.Usage.OutputTokens, read.Usage.OutputTokens)
		if read.Usage.CostUSD == nil {
			t.Error("the cost did not survive the round trip")
		} else if *read.Usage.CostUSD != costUSD {
			t.Errorf("cost changed: sent %v, read %v", costUSD, *read.Usage.CostUSD)
		}
	}
}

func assertConversation(t *testing.T, label string, sent, read *wire.ConversationID) {
	t.Helper()
	switch {
	case read == nil:
		t.Errorf("the %s did not survive the round trip", label)
	case *read != *sent:
		t.Errorf("%s changed: sent %s, read %s", label, *sent, *read)
	}
}

func assertString(t *testing.T, label string, sent, read *string) {
	t.Helper()
	switch {
	case read == nil:
		t.Errorf("the %s did not survive the round trip", label)
	case *read != *sent:
		t.Errorf("%s changed: sent %q, read %q", label, *sent, *read)
	}
}

func assertUint64(t *testing.T, label string, sent, read *uint64) {
	t.Helper()
	switch {
	case read == nil:
		t.Errorf("the %s did not survive the round trip", label)
	case *read != *sent:
		t.Errorf("%s changed: sent %d, read %d", label, *sent, *read)
	}
}
