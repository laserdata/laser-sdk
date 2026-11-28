package main

import (
	"context"
	"fmt"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example   = "query"
	topicName = "readings"
)

type reading struct {
	Host   string `json:"host"`
	CPU    int    `json:"cpu"`
	Status string `json:"status"`
}

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	if !examples.ManagedGate(client.Capabilities(), examples.FeatureQuery, example) {
		return nil
	}
	index := examples.IndexFor("readings_v1")
	readings := []reading{
		{Host: "node-1", CPU: 42, Status: "ok"},
		{Host: "node-2", CPU: 91, Status: "degraded"},
		{Host: "node-3", CPU: 17, Status: "ok"},
	}

	examples.Phase("keep a queryable view of a topic, then query it")
	topic := client.Topic(topicName)
	if err := topic.Ensure(ctx, examples.Partitions, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", topicName, err)
	}
	// Declare this run's view over the topic. From here it maintains itself:
	// every record published to the topic lands in the table, and the per-run
	// name means the counts below are this run's alone.
	if err := examples.EnsureView(ctx, client, topicName, index, "host", "cpu", "status"); err != nil {
		return err
	}

	for _, value := range readings {
		if _, err := topic.Publish().JSON(value).Send(ctx); err != nil {
			return fmt.Errorf("publish %s: %w", value.Host, err)
		}
	}
	if _, err := examples.WaitForProjection(ctx, client, index, uint64(len(readings))); err != nil {
		return err
	}

	// WhereEq matches an indexed key, the cheap path a projection's key columns
	// answer directly. Filter and Predicate cover the rest.
	degraded, err := client.Query(index).WhereEq("status", wire.StringTyped("degraded")).Limit(10).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("query %s: %w", index, err)
	}

	fmt.Printf("  %d of %d hosts are degraded\n", len(degraded.Rows), len(readings))
	for _, row := range laser.QueryRowsFromPage(degraded.Fields, degraded.Rows) {
		fmt.Printf("    host %s cpu %s\n", text(row, "host"), text(row, "cpu"))
	}
	return nil
}

func text(row laser.QueryRow, field string) string {
	value, found := row.Value(field)
	if !found {
		return "?"
	}
	return value.DiagnosticText()
}
