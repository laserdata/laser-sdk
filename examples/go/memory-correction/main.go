// go get github.com/laserdata/laser-sdk/foreign/go/laser@latest
// Needs only LASER_CONNECTION_STRING, which ConnectEnv reads. No server to start when it is set.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

const (
	user  = "user-42"
	query = "Where does the user live, and what should I know before booking dinner?"
)

type recall struct {
	User  string   `json:"user"`
	Query string   `json:"query"`
	Facts []string `json:"facts"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) (err error) {
	root, err := laser.ConnectEnv(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	scratch, err := root.ScratchStream(ctx, "memory")
	if err != nil {
		return err
	}
	result, err := correctAndRecall(ctx, scratch.Laser())
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err = errors.Join(err, scratch.Delete(cleanup)); err != nil {
		return err
	}
	printed, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(printed))
	return nil
}

func correctAndRecall(ctx context.Context, client *laser.Laser) (recall, error) {
	memory := client.Memory("assistant")

	// Conversation 1: the user shares three facts.
	first := laser.NewConversationID()
	shared := memory.For(laser.MemoryScope{User: user, Conversation: &first})
	city, err := shared.Remember(ctx, laser.MemoryFact, []byte("Lives in Gdansk"))
	if err != nil {
		return recall{}, err
	}
	for _, fact := range []string{"Allergic to peanuts", "Prefers a window seat"} {
		if _, err := shared.Remember(ctx, laser.MemoryFact, []byte(fact)); err != nil {
			return recall{}, err
		}
	}

	// Conversation 2: the user moved. Supersede stores the new fact and forgets the old one.
	second := laser.NewConversationID()
	moved := memory.For(laser.MemoryScope{User: user, Conversation: &second})
	if _, err := moved.Supersede(ctx, city, laser.MemoryFact, []byte("Lives in Lisbon")); err != nil {
		return recall{}, err
	}

	// Recall for the user across every conversation.
	items, err := memory.For(laser.MemoryScope{User: user}).
		Recall(ctx, laser.MemoryQuery{Query: query, Strategy: laser.RecallSemantic, Limit: 10})
	if err != nil {
		return recall{}, err
	}
	facts := make([]string, 0, len(items))
	for _, item := range items {
		facts = append(facts, item.Text())
	}
	return recall{User: user, Query: query, Facts: facts}, nil
}
