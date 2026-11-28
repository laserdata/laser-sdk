package main

import (
	"context"
	"fmt"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

const (
	example       = "log"
	topicName     = "readings"
	partitions    = 2
	replayTimeout = 10 * time.Second
)

type reading struct {
	Host string `json:"host"`
	CPU  int    `json:"cpu"`
}

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	examples.Phase("write two messages, then read them back")
	topic := client.Topic(topicName)
	if err := topic.Ensure(ctx, partitions, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", topicName, err)
	}
	samples := []reading{{Host: "node-1", CPU: 42}, {Host: "node-2", CPU: 91}}
	for _, sample := range samples {
		if _, err := topic.Publish().JSON(sample).Send(ctx); err != nil {
			return fmt.Errorf("publish %s: %w", sample.Host, err)
		}
	}

	// One typed handle pins the contract: a reading in on publish, a reading
	// out on replay, read from offset 0 with the offsets staying caller-owned.
	readings := laser.TypedJSON[reading](topic)
	cursor, err := topic.ReplayAs("log-example")
	if err != nil {
		return fmt.Errorf("open the replay: %w", err)
	}
	replayed, err := drain(ctx, readings, cursor, len(samples))
	if err != nil {
		return err
	}
	for _, value := range replayed {
		fmt.Printf("  reading %s cpu %d\n", value.Host, value.CPU)
	}
	return nil
}

// drain collects through the current tail. A poll reads a bounded batch per
// partition, so a larger replay still needs the loop.
func drain(
	ctx context.Context, readings *laser.TypedTopic[reading], cursor *laser.Cursor, expected int,
) ([]reading, error) {
	deadline := time.Now().Add(replayTimeout)
	var replayed []reading
	for {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("only %d record(s) replayed", len(replayed))
		}
		read := 0
		err := readings.Records(ctx, cursor, func(record *laser.TypedRecord[reading]) bool {
			replayed = append(replayed, record.Body)
			read++
			return true
		}, nil)
		if err != nil {
			return nil, fmt.Errorf("replay %s: %w", topicName, err)
		}
		if len(replayed) >= expected && read == 0 {
			return replayed, nil
		}
	}
}
