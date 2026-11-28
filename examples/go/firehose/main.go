package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example    = "firehose"
	baseMicros = 1_900_000_000_000_000
)

var (
	services = []string{"api", "storage", "metrics", "worker"}
	regions  = []string{"us-east", "us-west", "eu-central"}
	columns  = []string{"org", "service", "region", "status", "latency_ms", "ts"}
)

type telemetry struct {
	Org       string `json:"org"`
	Service   string `json:"service"`
	Region    string `json:"region"`
	Status    int    `json:"status"`
	LatencyMS int    `json:"latency_ms"`
	TS        int64  `json:"ts"`
	Payload   string `json:"payload"`
}

type settings struct {
	orgs, messages, concurrency, payloadBytes, chunk, partitions int
}

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	config := settings{
		orgs: max(1, examples.EnvInt("LASER_FIREHOSE_ORGS", 8)),
		// The total is split across the organizations, the same as the peers.
		messages:     max(1, examples.EnvInt("LASER_FIREHOSE_MESSAGES", 20_000)),
		concurrency:  max(1, examples.EnvInt("LASER_FIREHOSE_CONCURRENCY", 4)),
		payloadBytes: max(0, examples.EnvInt("LASER_FIREHOSE_PAYLOAD_BYTES", 4096)),
		chunk:        max(1, examples.EnvInt("LASER_FIREHOSE_BATCH", examples.BatchSize(500))),
		partitions:   max(1, examples.EnvInt("LASER_FIREHOSE_PARTITIONS", 8)),
	}
	capabilities := client.Capabilities()
	topics := make([]string, config.orgs)
	for index := range topics {
		topics[index] = fmt.Sprintf("org_%02d", index)
	}

	examples.Phase("firehose: warming up")
	fmt.Printf("%d records across %d orgs, batch %d, concurrency %d, payload %d bytes\n",
		config.messages, config.orgs, config.chunk, config.concurrency, config.payloadBytes)
	if examples.EnvBool("LASER_FIREHOSE_REGISTER", true) &&
		examples.ManagedGate(capabilities, examples.FeatureQuery, example) {
		examples.Phase("provisioning topics and indexes")
		for _, topic := range topics {
			// A binding names an existing topic, so the topic comes first.
			if err := client.Topic(topic).Ensure(ctx, uint32(config.partitions), nil); err != nil {
				return fmt.Errorf("ensure %s: %w", topic, err)
			}
			projection := examples.ViewProjection(examples.IndexFor(topic), wire.ContentJSON, false, columns...)
			if err := examples.RegisterView(ctx, client, topic, projection, wire.InvalidRecordBlock); err != nil {
				return err
			}
		}
	}

	examples.Phase("firing the hose")
	perOrg, remainder := config.messages/config.orgs, config.messages%config.orgs
	expected := func(org int) int {
		if org < remainder {
			return perOrg + 1
		}
		return perOrg
	}
	started := time.Now()
	total, err := fire(ctx, client, config, expected)
	if err != nil {
		return err
	}
	seconds := max(time.Since(started).Seconds(), 0.001)
	fmt.Printf("published %d records across %d orgs in %.2fs (%.0f records/s)\n",
		total, config.orgs, seconds, float64(total)/seconds)

	if !capabilities.Query.Available || !examples.EnvBool("LASER_FIREHOSE_QUERY", true) {
		return nil
	}
	for org, topic := range topics {
		if count := expected(org); count > 0 {
			if _, err := examples.WaitForProjection(ctx, client, examples.IndexFor(topic), uint64(count)); err != nil {
				return err
			}
		}
	}
	examples.Phase("sample analytics over the firehose")
	// Index names carry this run's token, so another run or another language's
	// firehose never shares an index with this one.
	index := examples.IndexFor(topics[0])
	sample, err := client.Query(index).WithTotal().Limit(5).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("query %s: %w", index, err)
	}
	var indexed uint64
	if sample.Page.Total != nil {
		indexed = *sample.Page.Total
	}
	fmt.Printf("sample index total: %d\n", indexed)
	var payload telemetry
	found, err := client.Query(index).FetchOne(ctx, laser.JSONCodec{}, &payload)
	if err != nil {
		return fmt.Errorf("read a payload from %s: %w", index, err)
	}
	if !found {
		return errors.New("materialized telemetry returned no payload")
	}
	fmt.Printf("sample payload: %s/%s in %s\n", payload.Org, payload.Service, payload.Region)
	return nil
}

// fire publishes every organization's share with a bounded number of workers.
func fire(ctx context.Context, client *laser.Laser, config settings, expected func(int) int) (int, error) {
	work, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	orgs := make(chan int)
	var (
		workers sync.WaitGroup
		mutex   sync.Mutex
		total   int
	)
	for range min(config.concurrency, config.orgs) {
		workers.Go(func() {
			for org := range orgs {
				sent, err := publishOrg(work, client, org, expected(org), config)
				mutex.Lock()
				total += sent
				mutex.Unlock()
				if err != nil {
					stop(err)
				}
			}
		})
	}
	for org := 0; org < config.orgs && work.Err() == nil; org++ {
		select {
		case orgs <- org:
		case <-work.Done():
		}
	}
	close(orgs)
	workers.Wait()
	if err := context.Cause(work); err != nil {
		return total, err
	}
	return total, nil
}

func publishOrg(ctx context.Context, client *laser.Laser, orgIndex, count int, config settings) (int, error) {
	org := fmt.Sprintf("org_%02d", orgIndex)
	topic := client.Topic(org)
	if err := topic.Ensure(ctx, uint32(config.partitions), nil); err != nil {
		return 0, fmt.Errorf("ensure %s: %w", org, err)
	}
	rng := examples.NewRng(0x1000 + uint64(orgIndex))
	filler := strings.Repeat("x", config.payloadBytes)
	sent := 0
	for sent < count {
		size := min(config.chunk, count-sent)
		batch := topic.PublishBatch().InlinePayload()
		for offset := range size {
			batch.AddJSON(sample(org, sent+offset, filler, rng))
		}
		if _, err := batch.Send(ctx); err != nil {
			return sent, fmt.Errorf("publish to %s: %w", org, err)
		}
		sent += size
	}
	return sent, nil
}

func sample(org string, sequence int, filler string, rng *examples.Rng) telemetry {
	record := telemetry{Org: org, TS: baseMicros + int64(sequence), Payload: filler}
	record.Service = examples.Pick(rng, services)
	record.Region = examples.Pick(rng, regions)
	record.Status = 200
	if rng.Below(100) < 4 {
		record.Status = 500
	}
	record.LatencyMS = 5 + rng.Below(1_500)
	return record
}
