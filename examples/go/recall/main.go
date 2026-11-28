package main

import (
	"context"
	"fmt"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

const (
	example   = "recall"
	namespace = "host:node-7"
	fact      = "node-7 sits in the eu-west pool, rotates keys monthly"
)

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	// Memory records ride the agent memory topic, created once here.
	if err := client.Bootstrap(ctx, examples.Partitions, laser.ExpireAfter(examples.SessionRetention)); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	conversation := laser.NewConversationID()

	examples.Phase("all four verbs: remember, recall, improve, forget")
	memory := client.Context(conversation).Memory(namespace)

	remembered, err := memory.Remember(ctx, laser.MemoryFact, []byte(fact))
	if err != nil {
		return fmt.Errorf("remember: %w", err)
	}

	// Durable log memory recalls the newest matching facts. Similarity ranking
	// is the vector and reranker path shown in the full memory example.
	hits, err := memory.RecallFolded(ctx, laser.MemoryQuery{Strategy: laser.RecallRecent, Limit: 5})
	if err != nil {
		return fmt.Errorf("recall: %w", err)
	}

	fmt.Println("  newest recalled fact(s):")
	for _, hit := range hits {
		fmt.Printf("    %s\n", hit.Text())
	}

	// Reinforce what was useful, then retire it. Both are records on the memory
	// topic, so the store stays an auditable history, not a mutable cell.
	if err := memory.Improve(ctx, laser.Feedback{Target: remembered, Weight: 1}); err != nil {
		return fmt.Errorf("improve: %w", err)
	}
	if err := memory.Forget(ctx, remembered); err != nil {
		return fmt.Errorf("forget: %w", err)
	}
	fmt.Printf("  reinforced then forgot %s\n", remembered)
	return nil
}
