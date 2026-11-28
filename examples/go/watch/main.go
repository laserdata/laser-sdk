package main

import (
	"context"
	"fmt"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

const (
	example       = "watch"
	topicName     = "readings"
	changeTimeout = 10 * time.Second
)

type reading struct {
	Host   string `json:"host"`
	CPU    int    `json:"cpu"`
	Status string `json:"status"`
}

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	capabilities := client.Capabilities()
	if !examples.ManagedGate(capabilities, examples.FeatureQuery, example) ||
		!examples.ManagedGate(capabilities, examples.FeatureWatch, example) {
		return nil
	}
	index := examples.IndexFor("readings_v1")

	examples.Phase("watch a view, then publish something that advances it")
	topic := client.Topic(topicName)
	if err := topic.Ensure(ctx, examples.Partitions, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", topicName, err)
	}
	// The same view shape the query example declares, under this run's own
	// name, so this program runs on its own with no shared state.
	if err := examples.EnsureView(ctx, client, topicName, index, "host", "cpu", "status"); err != nil {
		return err
	}

	// Open at the feed's end before publishing, so the reader sees this run's
	// change and none of the history before it.
	feed, err := client.Watch().Index(index).FromEnd().Records(ctx)
	if err != nil {
		return fmt.Errorf("watch %s: %w", index, err)
	}

	if _, err := topic.Publish().JSON(reading{Host: "node-4", CPU: 88, Status: "degraded"}).Send(ctx); err != nil {
		return fmt.Errorf("publish node-4: %w", err)
	}

	changes, err := feed.PollWithin(ctx, changeTimeout)
	if err != nil {
		return fmt.Errorf("read the change feed: %w", err)
	}
	if len(changes) == 0 {
		return fmt.Errorf("no change on `%s` arrived in time", index)
	}
	for _, change := range changes {
		fmt.Printf("  view advanced: %d row(s), source offsets %d..%d\n",
			change.Change.Rows, change.Change.FromOffset, change.Change.ToOffset)
	}
	if missed := feed.Missed(); missed > 0 {
		fmt.Printf("  the feed lost %d change(s) to retention, re-read the view\n", missed)
	}
	return nil
}
