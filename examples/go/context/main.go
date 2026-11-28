package main

import (
	"context"
	"fmt"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example     = "context"
	lastN       = 20
	tokenBudget = 4_000
)

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	// Conversation turns ride the shared session topic, created once here.
	if err := client.Bootstrap(ctx, examples.Partitions, laser.ExpireAfter(examples.SessionRetention)); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	conversation := laser.NewConversationID()

	examples.Phase("append a conversation, then assemble it under a budget")
	scope := client.Context(conversation)
	for _, turn := range []string{"drain node-7", "drained, 0 connections left"} {
		if err := scope.Append(ctx, wire.AgentSessions, []byte(turn)); err != nil {
			return fmt.Errorf("append a turn: %w", err)
		}
	}

	// The shape of a prompt's context is a declared policy, not slicing logic
	// spread through the application: cap the turns, then fit the budget.
	policy := laser.Chain{laser.LastN(lastN), laser.TokenBudget{MaxTokens: tokenBudget}}
	turns, err := scope.FetchWith(ctx, []string{wire.AgentSessions}, policy)
	if err != nil {
		return fmt.Errorf("assemble the context: %w", err)
	}

	fmt.Printf("  %d turn(s) within %d tokens:\n", len(turns), tokenBudget)
	for _, turn := range turns {
		fmt.Printf("    %s\n", turn.Body())
	}
	return nil
}
