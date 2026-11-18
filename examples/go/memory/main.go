// Agentic memory: remember facts, recall them by word overlap, and render the
// result into a prompt under a token budget.
//
// Ranking here is what a client can do with no model: overlap on words. A
// deployment with embeddings ranks better server-side, and this is what is
// available when one is not there.
package main

import (
	"context"
	"fmt"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func main() { examples.Run("memory", run) }

func run(ctx context.Context, client *laser.Laser) error {
	memory := client.Memory("prefs").For(laser.MemoryScope{User: "ana"})

	examples.Phase("remember a few things about one user")
	facts := []string{
		"the user is allergic to peanuts",
		"green tea every morning",
		"the user lives in Lisbon",
	}
	remembered := make([]laser.MemoryItem, 0, len(facts))
	for _, fact := range facts {
		id, err := memory.Remember(ctx, laser.MemoryFact, []byte(fact))
		if err != nil {
			return fmt.Errorf("remember: %w", err)
		}
		fmt.Printf("  %s  %s\n", id, fact)
		remembered = append(remembered, laser.MemoryItem{
			ID: id, Body: []byte(fact), Kind: laser.MemoryFact,
		})
	}

	examples.Phase("remembering the same fact again converges on one item")
	again, err := memory.Remember(ctx, laser.MemoryFact, []byte(facts[0]))
	if err != nil {
		return fmt.Errorf("remember: %w", err)
	}
	if again != remembered[0].ID {
		return fmt.Errorf("want one item for one fact, got %s then %s",
			remembered[0].ID, again)
	}
	fmt.Println("the id is derived from the owner, kind and body, so it is the same item")

	examples.Phase("recall by word overlap")
	ranked := laser.Rank(remembered, laser.MemoryQuery{
		Query: "which city does the user live in",
	})
	for _, item := range ranked {
		fmt.Printf("  %.2f  %s\n", item.Score, item.Text())
	}

	examples.Phase("render it into a prompt under a budget")
	fmt.Println(laser.ToContextBlock(ranked, 40))

	examples.Phase("a different user cannot address the same memory")
	// The owner is part of the id, so two users never share an item.
	theirs := laser.ContentMemoryID(
		laser.MemoryScope{Stream: memory.Scope().Stream, User: "bo"},
		laser.MemoryFact, []byte(facts[0]))
	fmt.Printf("  ana: %s\n  bo:  %s\n", remembered[0].ID, theirs)

	examples.Phase("forget one")
	if err := memory.Forget(ctx, remembered[0].ID); err != nil {
		return fmt.Errorf("forget: %w", err)
	}
	fmt.Println("a tombstone was written: what is recalled changed, " +
		"and what happened is still on the log")
	return nil
}
