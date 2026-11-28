package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example       = "event-analytics"
	topicName     = "clickstream"
	guardedTopic  = "clickstream_guarded"
	checkpointKey = "clickstream-export-cursor"
	liveGroup     = "event-analytics-live"
	liveTimeout   = 15 * time.Second
	baseMicros    = 1_900_000_000_000_000
	minuteMicros  = 60_000_000

	userID      = "user_id"
	messageType = "message_type"
	route       = "route"
	latencyMS   = "latency_ms"
	timestamp   = "ts"
	count       = "count"
	windowStart = "window_start"

	clickEventSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object",` +
		`"additionalProperties":false,"required":["user_id","message_type","route","latency_ms","ts"],` +
		`"properties":{"user_id":{"type":"string"},"message_type":{"enum":["request","retry","error"]},` +
		`"route":{"type":"string"},"latency_ms":{"type":"integer"},"ts":{"type":"integer"}}}`
	malformedEvent = `{"user_id":"mallory","message_type":"error","route":"/api/v1/jobs","latency_ms":"fast","ts":1}`
)

var (
	columns  = []string{userID, messageType, route, latencyMS, timestamp}
	visitors = []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi", "ivan", "judy", "mallory", "oscar"}
	routes   = []string{"/healthz", "/api/v1/hosts/42", "/api/v1/jobs/7", "/api/v1/metrics", "/api/v1/hosts", "/api/v1/jobs", "/login", "/docs"}
)

type clickEvent struct {
	UserID      string `json:"user_id"`
	MessageType string `json:"message_type"`
	Route       string `json:"route"`
	LatencyMS   int    `json:"latency_ms"`
	TS          int64  `json:"ts"`
}

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) (err error) {
	total := examples.Messages(180)
	events := clickstream(total)
	capabilities := client.Capabilities()
	// Index names carry this run's token, so a rerun or another language's
	// example on the same deployment never shares their rows.
	index := examples.IndexFor(topicName)

	examples.Phase("warming up")
	topic := client.Topic(topicName)
	if err := topic.Ensure(ctx, examples.Partitions, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", topicName, err)
	}
	if capabilities.Query.Available {
		projection := examples.ViewProjection(index, wire.ContentJSON, false, columns...)
		if err := examples.RegisterView(ctx, client, topicName, projection, wire.InvalidRecordBlock); err != nil {
			return err
		}
	}

	live, err := topic.Group(liveGroup).Consumer(ctx, laser.ConsumerOptions{Count: 100})
	if err != nil {
		return fmt.Errorf("join %s: %w", liveGroup, err)
	}
	defer func() { err = errors.Join(err, live.Close(ctx)) }()
	publishing := make(chan error, 1)
	go func() { publishing <- publishClickstream(ctx, topic, events) }()

	examples.Phase("hot path: a live reader tails the stream while the producer runs")
	seen, failures, err := liveFold(ctx, live, total)
	if err != nil {
		return errors.Join(err, <-publishing)
	}
	if err := <-publishing; err != nil {
		return err
	}
	fmt.Printf("live fold: %d events, %d errors\n", seen, failures)

	examples.Phase("read model: a resumable downstream reader")
	if err := checkpointedExport(ctx, topic); err != nil {
		return err
	}

	if !examples.ManagedGate(capabilities, examples.FeatureQuery, example) {
		return nil
	}
	examples.Phase("read model: ad-hoc analytics over the managed query layer")
	if _, err := examples.WaitForProjection(ctx, client, index, uint64(total)); err != nil {
		return err
	}
	if err := runAnalytics(ctx, client, index); err != nil {
		return err
	}
	examples.Phase("validated ingest: a JSON Schema guards the index")
	return guardedIngest(ctx, client, events[0])
}

func publishClickstream(ctx context.Context, topic *laser.Topic, events []clickEvent) error {
	chunkSize := min(examples.BatchSize(30), len(events))
	for start := 0; start < len(events); start += chunkSize {
		chunk := events[start:min(start+chunkSize, len(events))]
		batch := topic.PublishBatch().InlinePayload()
		for _, event := range chunk {
			batch.AddJSON(event)
		}
		if _, err := batch.Send(ctx); err != nil {
			return fmt.Errorf("publish to %s: %w", topicName, err)
		}
		fmt.Printf("published %d/%d events to `%s`\n", start+len(chunk), len(events), topicName)
	}
	return nil
}

func liveFold(ctx context.Context, live *laser.Consumer, total int) (seen, failures int, err error) {
	for seen < total {
		read, cancel := context.WithTimeout(ctx, liveTimeout)
		record, err := live.Next(read)
		cancel()
		if err != nil {
			return seen, failures, fmt.Errorf("live read after %d/%d: %w", seen, total, err)
		}
		var event clickEvent
		if err := json.Unmarshal(record.Payload(), &event); err != nil {
			return seen, failures, fmt.Errorf("decode offset %d: %w", record.Offset(), err)
		}
		seen++
		if event.MessageType == "error" {
			failures++
		}
	}
	return seen, failures, nil
}

// checkpointedExport reads the head of every partition, saves where it
// stopped, and resumes from the saved position without replaying a record.
func checkpointedExport(ctx context.Context, topic *laser.Topic) error {
	counter, err := topic.ReplayAs("event-analytics-count")
	if err != nil {
		return err
	}
	everything, err := counter.Poll(ctx)
	if err != nil {
		return fmt.Errorf("count %s: %w", topicName, err)
	}

	// A poll reads through the tail, so the export stops itself after the
	// first record of each partition and checkpoints one past it.
	export, err := topic.ReplayAs("event-analytics-export")
	if err != nil {
		return err
	}
	head, err := export.Poll(ctx)
	if err != nil {
		return fmt.Errorf("export %s: %w", topicName, err)
	}
	offsets := make([]uint64, len(export.Offsets()))
	first := 0
	for _, record := range head {
		if offsets[record.PartitionID] == 0 {
			offsets[record.PartitionID] = record.Offset() + 1
			first++
		}
	}
	saved, err := json.Marshal(offsets)
	if err != nil {
		return err
	}
	state := laser.NewInMemoryStore()
	if err := state.Set(ctx, checkpointKey, saved); err != nil {
		return fmt.Errorf("save the checkpoint: %w", err)
	}

	restored, found, err := state.Get(ctx, checkpointKey)
	if err != nil {
		return fmt.Errorf("read the checkpoint: %w", err)
	}
	if !found {
		return errors.New("checkpoint was not persisted")
	}
	var resumeAt []uint64
	if err := json.Unmarshal(restored, &resumeAt); err != nil {
		return fmt.Errorf("decode the checkpoint: %w", err)
	}
	resumed, err := topic.ReplayAs("event-analytics-tail")
	if err != nil {
		return err
	}
	tail, err := resumed.FromOffsets(resumeAt).Poll(ctx)
	if err != nil {
		return fmt.Errorf("resume the export: %w", err)
	}
	if first+len(tail) != len(everything) {
		return errors.New("checkpoint resume lost or duplicated records")
	}
	fmt.Printf("checkpointed export resumed with %d/%d records, no replay duplication\n",
		first+len(tail), len(everything))
	return nil
}

func runAnalytics(ctx context.Context, client *laser.Laser, index string) error {
	byKind, err := client.Query(index).Count().GroupBy(messageType).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("count by event: %w", err)
	}
	examples.PrintTable(table(byKind, []string{"event", "count"}, func(row laser.QueryRow) []string {
		return []string{text(row, messageType, "?"), text(row, count, "0")}
	}))

	slowest, err := client.Query(index).OrderDesc(latencyMS).Limit(3).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("slowest routes: %w", err)
	}
	fmt.Println("slowest 3 routes")
	examples.PrintTable(table(slowest, []string{"route", "latency"}, func(row laser.QueryRow) []string {
		return []string{text(row, route, "?"), text(row, latencyMS, "?") + "ms"}
	}))

	failures, err := client.Query(index).MessageType("error").Count().Fetch(ctx)
	if err != nil {
		return fmt.Errorf("count errors: %w", err)
	}
	fmt.Printf("errors: %s\n", scalar(failures))

	firstWindow, err := client.Query(index).TimeRange(baseMicros, baseMicros+5*minuteMicros).Count().Fetch(ctx)
	if err != nil {
		return fmt.Errorf("count the first window: %w", err)
	}
	fmt.Printf("events in the first 5 minutes: %s\n", scalar(firstWindow))

	perMinute, err := client.Query(index).Count().Window(timestamp, time.Minute).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("count per minute: %w", err)
	}
	fmt.Println("events per minute")
	examples.PrintTable(table(perMinute, []string{"window start", "count"}, func(row laser.QueryRow) []string {
		return []string{text(row, windowStart, "?"), text(row, count, "0")}
	}))

	metrics, err := client.Query(index).Avg(latencyMS).CountDistinct(route).GroupBy(messageType).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("latency by event: %w", err)
	}
	fmt.Println("latency and route breadth by event")
	examples.PrintTable(table(metrics, []string{"event", "avg latency", "routes"}, func(row laser.QueryRow) []string {
		return []string{text(row, messageType, "?"), text(row, "avg", "?") + "ms", text(row, "count_distinct", "?")}
	}))

	var payload clickEvent
	found, err := client.Query(index).FetchOne(ctx, laser.JSONCodec{}, &payload)
	if err != nil {
		return fmt.Errorf("read a payload from %s: %w", index, err)
	}
	if !found {
		return errors.New("materialized clickstream returned no payload")
	}
	fmt.Printf("payload round trip: %s on %s\n", payload.MessageType, payload.Route)
	return nil
}

// guardedIngest registers a JSON Schema as the writer schema of a second
// topic. A malformed event is refused before it is sent, and one that reaches
// the topic anyway is dead-lettered so the valid ones keep materializing.
func guardedIngest(ctx context.Context, client *laser.Laser, sample clickEvent) error {
	schemaID, err := client.Schemas().Register(wire.JSONSchema(clickEventSchema)).
		Name("clickstream_event").Version(1).Send(ctx)
	if err != nil {
		return fmt.Errorf("register the ClickEvent schema: %w", err)
	}
	fmt.Printf("allocated writer-schema id %d for the ClickEvent guard\n", schemaID)
	topic := client.Topic(guardedTopic)
	if err := topic.Ensure(ctx, examples.Partitions, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", guardedTopic, err)
	}
	guardedIndex := examples.IndexFor(guardedTopic)
	projection := examples.ViewProjection(guardedIndex, wire.ContentJSON, true, columns...)
	if err := examples.RegisterView(ctx, client, guardedTopic, projection, wire.InvalidRecordDeadLetter); err != nil {
		return err
	}
	if err := examples.WaitForSchema(ctx, client, schemaID); err != nil {
		return err
	}
	guarded, err := laser.TypedSchema[map[string]any](ctx, topic, schemaID)
	if err != nil {
		return fmt.Errorf("open the guarded topic: %w", err)
	}
	valid := map[string]any{
		userID: sample.UserID, messageType: sample.MessageType, route: sample.Route,
		latencyMS: sample.LatencyMS, timestamp: sample.TS,
	}
	if _, err := guarded.Publish(valid).Send(ctx); err != nil {
		return fmt.Errorf("publish the valid event: %w", err)
	}
	malformed := map[string]any{
		userID: sample.UserID, messageType: sample.MessageType, route: sample.Route,
		latencyMS: "slow", timestamp: sample.TS,
	}
	if _, err := guarded.Publish(malformed).Send(ctx); err == nil {
		return errors.New("the malformed event unexpectedly passed schema validation")
	}
	_, err = topic.Publish().RawBytes([]byte(malformedEvent), wire.ContentJSON).SchemaID(schemaID).Send(ctx)
	if err != nil {
		return fmt.Errorf("publish the malformed event past the guard: %w", err)
	}
	if _, err := examples.WaitForProjection(ctx, client, guardedIndex, 1); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Second):
	}
	result, err := client.Query(guardedIndex).WithTotal().Fetch(ctx)
	if err != nil {
		return fmt.Errorf("query %s: %w", guardedIndex, err)
	}
	if result.Page.Total == nil || *result.Page.Total != 1 {
		return errors.New("the malformed event must not materialize")
	}
	fmt.Printf("guarded JSON rows: %d\n", *result.Page.Total)
	return nil
}

// clickstream is the deterministic feed every language's example publishes.
func clickstream(total int) []clickEvent {
	rng := examples.NewRng(0x123456789abcdef0)
	at := int64(baseMicros)
	events := make([]clickEvent, total)
	for index := range events {
		kind := "error"
		if roll := rng.Below(100); roll < 70 {
			kind = "request"
		} else if roll < 92 {
			kind = "retry"
		}
		event := clickEvent{MessageType: kind, TS: at}
		event.UserID = examples.Pick(rng, visitors)
		event.Route = examples.Pick(rng, routes)
		event.LatencyMS = 30 + rng.Below(600)
		events[index] = event
		at += int64(1 + rng.Below(30_000_000))
	}
	return events
}

func table(result *wire.QueryResult, header []string, cells func(laser.QueryRow) []string) [][]string {
	rows := [][]string{header}
	for _, row := range laser.QueryRowsFromPage(result.Fields, result.Rows) {
		rows = append(rows, cells(row))
	}
	return rows
}

func scalar(result *wire.QueryResult) string {
	rows := laser.QueryRowsFromPage(result.Fields, result.Rows)
	if len(rows) == 0 {
		return "0"
	}
	return text(rows[0], count, "0")
}

func text(row laser.QueryRow, field, missing string) string {
	value, found := row.Value(field)
	if !found {
		return missing
	}
	return value.DiagnosticText()
}
