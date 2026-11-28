package main

import (
	"context"
	"fmt"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example   = "graph"
	graphName = "kg"
	host      = "host:node-7"
	relation  = "runs"
)

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	if !examples.ManagedGate(client.Capabilities(), examples.FeatureGraph, example) {
		return nil
	}
	graph := client.Graph(graphName)

	examples.Phase("relate entities, then traverse from one of them")
	// Link upserts both content-addressed entity nodes and the typed edge
	// between them, so linking the same triple again converges.
	for _, service := range []string{"service:auth", "service:metrics"} {
		if err := graph.Link(ctx, host, relation, service); err != nil {
			return fmt.Errorf("link %s: %w", service, err)
		}
	}

	// The same id Link derived, rebuilt locally: a node is addressed by its
	// content, never by a server-assigned key.
	start := wire.EntityNode("host", "node-7")
	services, err := graph.Neighbors(ctx, start.ID, wire.EdgeOut, relation, 1)
	if err != nil {
		return fmt.Errorf("read the neighbors of %s: %w", host, err)
	}

	// The reply carries the start node beside the nodes it reaches.
	fmt.Printf("  %s %s:\n", host, relation)
	for _, node := range services.Nodes {
		if entity := entityOf(node); entity != host {
			fmt.Printf("    %s\n", entity)
		}
	}
	return nil
}

func entityOf(node wire.GraphNode) string {
	label, value := "entity", "?"
	if len(node.Labels) > 0 {
		label = node.Labels[0]
	}
	for _, attr := range node.Attrs {
		if attr.Key == "value" {
			value = attr.Value.Str
		}
	}
	return label + ":" + value
}
