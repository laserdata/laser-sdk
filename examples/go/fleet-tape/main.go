package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example         = "fleet-tape"
	feedTopic       = "metrics_feed"
	tapeTopic       = "readings"
	avroTapeTopic   = "readings_avro"
	degradedCPU     = 80
	baseMicros      = 1_900_000_000_000_000
	liveTimeout     = 15 * time.Second
	feedPace        = 120 * time.Millisecond
	idlePause       = 5 * time.Millisecond
	avroReadingsCap = 500

	host      = "host"
	samples   = "samples"
	cpuTotal  = "cpu_total"
	sumColumn = "sum"

	avroSchema = `{"type":"record","name":"HostReading","fields":[{"name":"host","type":"string"},` +
		`{"name":"cpu","type":"long"},{"name":"samples","type":"int"},{"name":"level","type":"string"},` +
		`{"name":"cpu_total","type":"long"},{"name":"message_type","type":"string"},{"name":"ts","type":"long"}]}`
)

var columns = []string{host, "cpu", samples, "level", cpuTotal, "message_type", "ts"}

type reading struct {
	Host        string `json:"host"`
	CPU         int    `json:"cpu"`
	Samples     int    `json:"samples"`
	Level       string `json:"level"`
	CPUTotal    int64  `json:"cpu_total"`
	MessageType string `json:"message_type"`
	TS          int64  `json:"ts"`
}

type opening struct {
	host string
	cpu  int
}

// hostLoad is the live view's row for one host.
type hostLoad struct {
	samples, cpuTotal int64
	last, degraded    int
}

var openings = []opening{{"node-1", 42}, {"node-2", 57}, {"node-3", 31}, {"node-4", 68}, {"node-5", 49}}

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	total := examples.Messages(2_000)
	chunk := min(examples.BatchSize(100), total)
	values := readings(total)
	capabilities := client.Capabilities()
	// Index names carry this run's token, so a rerun or another language's
	// example on the same deployment never shares their rows.
	tapeIndex := examples.IndexFor(tapeTopic)

	examples.Phase("warming up")
	for _, topic := range []string{feedTopic, tapeTopic} {
		if err := client.Topic(topic).Ensure(ctx, examples.Partitions, nil); err != nil {
			return fmt.Errorf("ensure %s: %w", topic, err)
		}
	}
	if capabilities.Query.Available {
		projection := examples.ViewProjection(tapeIndex, wire.ContentJSON, false, columns...)
		if err := examples.RegisterView(ctx, client, tapeTopic, projection, wire.InvalidRecordBlock); err != nil {
			return err
		}
	}

	examples.Phase("streaming a live telemetry feed")
	fmt.Printf("%d readings across %d hosts\n", total, len(openings))
	if err := streamLiveView(ctx, client, values, min(chunk, 40)); err != nil {
		return err
	}

	examples.Phase("publishing the readings to the durable reading tape")
	tape := laser.TypedJSON[reading](client.Topic(tapeTopic))
	offsets, err := tapeHead(ctx, tape)
	if err != nil {
		return err
	}
	if err := publishTape(ctx, tape.Topic(), values, chunk); err != nil {
		return err
	}

	if examples.ManagedGate(capabilities, examples.FeatureQuery, example) {
		if _, err := examples.WaitForProjection(ctx, client, tapeIndex, uint64(len(values))); err != nil {
			return err
		}
		examples.Phase("reading-tape analytics")
		if err := reportSamplesAndMean(ctx, client, tapeIndex); err != nil {
			return err
		}
	}

	examples.Phase("typed tape audit: replay the log as Reading values")
	if err := auditTape(ctx, tape, values, offsets); err != nil {
		return err
	}

	if !capabilities.Managed {
		fmt.Println("writer schemas need Laser Stack or LaserData Cloud, skipping the Avro tape")
		return nil
	}
	examples.Phase("schema-first tape: Avro readings decoded by a registered writer schema")
	return publishAvroTape(ctx, client, values, chunk)
}

// streamLiveView folds the feed into a per-host view while the producer runs.
func streamLiveView(ctx context.Context, client *laser.Laser, values []reading, size int) error {
	feed := laser.TypedJSON[reading](client.Topic(feedTopic))
	cursor, err := feed.Topic().ReplayAs("fleet-tape-builder")
	if err != nil {
		return err
	}
	cursor.Batch(uint32(max(size, 256)))
	publishing := make(chan error, 1)
	go func() { publishing <- publishFeed(ctx, feed, values, size) }()

	view := map[string]hostLoad{}
	seen, lastProgress := 0, time.Now()
	for seen < len(values) {
		read, err := drain(ctx, feed, cursor, func(value reading) {
			load := view[value.Host]
			load.samples += int64(value.Samples)
			load.cpuTotal += value.CPUTotal
			load.last = value.CPU
			if value.Level == "degraded" {
				load.degraded++
			}
			view[value.Host] = load
		})
		if err != nil {
			return errors.Join(err, <-publishing)
		}
		seen += read
		if read > 0 {
			lastProgress = time.Now()
			continue
		}
		if time.Since(lastProgress) >= liveTimeout {
			return errors.Join(fmt.Errorf("no reading arrived for %s after %d/%d", liveTimeout, seen, len(values)), <-publishing)
		}
		if err := pause(ctx, idlePause); err != nil {
			return errors.Join(err, <-publishing)
		}
	}
	if err := <-publishing; err != nil {
		return err
	}
	printView(view)
	return nil
}

func publishFeed(ctx context.Context, feed *laser.TypedTopic[reading], values []reading, size int) error {
	for start := 0; start < len(values); start += size {
		if _, err := feed.PublishBatch(values[start:min(start+size, len(values))]).Send(ctx); err != nil {
			return fmt.Errorf("publish to %s: %w", feedTopic, err)
		}
		if err := pause(ctx, feedPace); err != nil {
			return err
		}
	}
	return nil
}

// tapeHead is where the tape ends before this run publishes to it.
func tapeHead(ctx context.Context, tape *laser.TypedTopic[reading]) ([]uint64, error) {
	cursor, err := tape.Topic().ReplayAs("fleet-tape-head")
	if err != nil {
		return nil, err
	}
	for {
		read, err := drain(ctx, tape, cursor, func(reading) {})
		if err != nil {
			return nil, err
		}
		if read == 0 {
			return slices.Clone(cursor.Offsets()), nil
		}
	}
}

func publishTape(ctx context.Context, topic *laser.Topic, values []reading, size int) error {
	for start := 0; start < len(values); start += size {
		chunk := values[start:min(start+size, len(values))]
		batch := topic.PublishBatch().InlinePayload()
		for _, value := range chunk {
			batch.AddJSON(value)
		}
		if _, err := batch.Send(ctx); err != nil {
			return fmt.Errorf("publish to %s: %w", tapeTopic, err)
		}
		fmt.Printf("indexed %d/%d readings to `%s`\n", start+len(chunk), len(values), tapeTopic)
	}
	return nil
}

func reportSamplesAndMean(ctx context.Context, client *laser.Laser, index string) error {
	sampled, err := groupTotals(ctx, client, index, samples)
	if err != nil {
		return err
	}
	weighted, err := groupTotals(ctx, client, index, cpuTotal)
	if err != nil {
		return err
	}
	rows := [][]string{{"host", "samples", "mean cpu"}}
	for _, name := range slices.Sorted(maps.Keys(sampled)) {
		mean := int64(0)
		if sampled[name] != 0 {
			mean = weighted[name] / sampled[name]
		}
		rows = append(rows, []string{name, strconv.FormatInt(sampled[name], 10), fmt.Sprintf("%d%%", mean)})
	}
	examples.PrintTable(rows)

	var payload reading
	found, err := client.Query(index).FetchOne(ctx, laser.JSONCodec{}, &payload)
	if err != nil {
		return fmt.Errorf("read a payload from %s: %w", index, err)
	}
	if !found {
		return errors.New("materialized reading tape returned no payload")
	}
	fmt.Printf("payload round trip: %s cpu %d%% %s\n", payload.Host, payload.CPU, payload.Level)
	return nil
}

// auditTape replays this run's part of the tape as typed readings and checks
// every host's weighted CPU total against what was published.
func auditTape(ctx context.Context, tape *laser.TypedTopic[reading], values []reading, offsets []uint64) error {
	cursor, err := tape.Topic().ReplayAs("fleet-tape-audit")
	if err != nil {
		return err
	}
	cursor.FromOffsets(offsets)
	actual := map[string]int64{}
	audited, lastProgress := 0, time.Now()
	for audited < len(values) && time.Since(lastProgress) < liveTimeout {
		read, err := drain(ctx, tape, cursor, func(value reading) { actual[value.Host] += value.CPUTotal })
		if err != nil {
			return err
		}
		audited += read
		if read > 0 {
			lastProgress = time.Now()
			continue
		}
		if err := pause(ctx, idlePause); err != nil {
			return err
		}
	}
	expected := map[string]int64{}
	for _, value := range values {
		expected[value.Host] += value.CPUTotal
	}
	for name, total := range expected {
		if actual[name] != total {
			return fmt.Errorf("typed tape audit disagrees for %s", name)
		}
	}
	if audited != len(values) {
		return fmt.Errorf("typed tape audit read %d/%d readings", audited, len(values))
	}
	fmt.Printf("audited %d readings, every host's weighted CPU total matches\n", audited)
	return nil
}

// publishAvroTape registers a writer schema, publishes readings the schema
// encodes, and reads their totals back from the view the backend decoded.
func publishAvroTape(ctx context.Context, client *laser.Laser, values []reading, size int) error {
	schemaID, err := client.Schemas().Register(wire.AvroSchema(avroSchema)).Name("fleet_reading").Version(1).Send(ctx)
	if err != nil {
		return fmt.Errorf("register the Avro schema: %w", err)
	}
	topic := client.Topic(avroTapeTopic)
	if err := topic.Ensure(ctx, examples.Partitions, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", avroTapeTopic, err)
	}
	index := examples.IndexFor(avroTapeTopic)
	projection := examples.ViewProjection(index, wire.ContentAvro, true, columns...)
	if err := examples.RegisterView(ctx, client, avroTapeTopic, projection, wire.InvalidRecordBlock); err != nil {
		return err
	}
	if err := examples.WaitForSchema(ctx, client, schemaID); err != nil {
		return err
	}
	typed, err := laser.TypedSchema[reading](ctx, topic, schemaID)
	if err != nil {
		return fmt.Errorf("open the Avro tape: %w", err)
	}
	subset := values[:min(avroReadingsCap, len(values))]
	for start := 0; start < len(subset); start += size {
		if _, err := typed.PublishBatch(subset[start:min(start+size, len(subset))]).Send(ctx); err != nil {
			return fmt.Errorf("publish to %s: %w", avroTapeTopic, err)
		}
	}
	if _, err := examples.WaitForProjection(ctx, client, index, uint64(len(subset))); err != nil {
		return err
	}
	totals, err := groupTotals(ctx, client, index, cpuTotal)
	if err != nil {
		return err
	}
	rows := [][]string{{"host", "Avro weighted CPU total"}}
	for _, name := range slices.Sorted(maps.Keys(totals)) {
		rows = append(rows, []string{name, strconv.FormatInt(totals[name], 10)})
	}
	examples.PrintTable(rows)
	return nil
}

// groupTotals sums one column per host.
func groupTotals(ctx context.Context, client *laser.Laser, index, column string) (map[string]int64, error) {
	result, err := client.Query(index).Sum(column).GroupBy(host).Fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("sum %s by host: %w", column, err)
	}
	totals := map[string]int64{}
	for _, row := range laser.QueryRowsFromPage(result.Fields, result.Rows) {
		name, named := row.ValueText(host)
		total, summed := row.Value(sumColumn)
		if !named || !summed {
			continue
		}
		parsed, err := strconv.ParseInt(total.DiagnosticText(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("sum of %s for %s: %w", column, name, err)
		}
		totals[name] = parsed
	}
	return totals, nil
}

// drain visits every reading appended since the cursor's last read.
func drain(
	ctx context.Context, topic *laser.TypedTopic[reading], cursor *laser.Cursor, visit func(reading),
) (int, error) {
	read := 0
	var undecodable error
	err := topic.Records(ctx, cursor, func(record *laser.TypedRecord[reading]) bool {
		visit(record.Body)
		read++
		return true
	}, func(failure *laser.TypedDecodeError) { undecodable = errors.Join(undecodable, failure) })
	return read, errors.Join(err, undecodable)
}

func printView(view map[string]hostLoad) {
	rows := [][]string{{"host", "last cpu", "mean cpu", "samples", "degraded"}}
	for _, name := range slices.Sorted(maps.Keys(view)) {
		load := view[name]
		mean := int64(0)
		if load.samples != 0 {
			mean = load.cpuTotal / load.samples
		}
		rows = append(rows, []string{
			name, fmt.Sprintf("%d%%", load.last), fmt.Sprintf("%d%%", mean),
			strconv.FormatInt(load.samples, 10), strconv.Itoa(load.degraded),
		})
	}
	examples.PrintTable(rows)
}

// readings is the deterministic feed every language's example publishes.
func readings(total int) []reading {
	rng := examples.NewRng(0x123456789abcdef0)
	load := map[string]int{}
	for _, start := range openings {
		load[start.host] = start.cpu
	}
	at := int64(baseMicros)
	values := make([]reading, total)
	for index := range values {
		name := examples.Pick(rng, openings).host
		cpu := min(100, max(0, load[name]+rng.Below(15)-7))
		count := 1 + rng.Below(500)
		at += int64(1 + rng.Below(50_000))
		load[name] = cpu
		level := "ok"
		if cpu >= degradedCPU {
			level = "degraded"
		}
		values[index] = reading{
			Host: name, CPU: cpu, Samples: count, Level: level,
			CPUTotal: int64(cpu) * int64(count), MessageType: "reading", TS: at,
		}
	}
	return values
}

func pause(ctx context.Context, wait time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}
