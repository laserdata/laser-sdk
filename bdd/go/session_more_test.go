package bdd_test

import (
	"context"
	"fmt"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The session steps added after the first slice: the context checkpoint and
// the session's linked memory.

// takeCheckpoint records where the lane stood once it holds the named number
// of this session's records.
//
// The count is waited for rather than assumed, because a checkpoint taken
// before the earlier records landed would be a checkpoint of a different
// moment than the scenario describes.
func takeCheckpoint(ctx context.Context, state *world, want int) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	scope := state.client.Context(session.Conversation())
	deadline := time.Now().Add(20 * time.Second)
	for {
		displays, err := laneDisplays(ctx, state)
		if err != nil {
			return err
		}
		if len(displays) == want {
			checkpoint, err := scope.Checkpoint(ctx, []string{wire.AgentSessions})
			if err != nil {
				return err
			}
			state.put("checkpoint", checkpoint)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want %d records before the checkpoint, got %d",
				want, len(displays))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// sinceCheckpoint checks only what arrived after the checkpoint is read back.
func sinceCheckpoint(ctx context.Context, state *world, want []string) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	value, found := state.get("checkpoint")
	if !found {
		return fmt.Errorf("the scenario took no checkpoint")
	}
	checkpoint := value.(*laser.ContextCheckpoint)
	scope := state.client.Context(session.Conversation())
	deadline := time.Now().Add(20 * time.Second)
	for {
		messages, err := scope.FetchSince(
			ctx, checkpoint, []string{wire.AgentSessions}, nil)
		if err != nil {
			return err
		}
		var bodies []string
		for _, message := range messages {
			bodies = append(bodies, string(message.Body()))
		}
		if len(bodies) == len(want) {
			for index, body := range want {
				if bodies[index] != body {
					return fmt.Errorf("want %v since the checkpoint, got %v",
						want, bodies)
				}
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want %v since the checkpoint, got %v", want, bodies)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// rememberThroughSession writes one fact through the session's linked memory.
func rememberThroughSession(
	ctx context.Context, state *world, arguments []string,
) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one fact, got %v", arguments)
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	if err := state.topic(wire.AgentMemory).Ensure(ctx, 1, nil); err != nil {
		return err
	}
	_, err = session.Memory().Remember(
		ctx, laser.MemoryFact, []byte(arguments[0]))
	return err
}

// rememberedProducer reads the memory topic back and checks who it says wrote
// the item.
//
// The producer is what lets a reader tell one agent's memory from another's
// long after the fact, so it must name the session's agent rather than the
// process or the SDK alone.
func rememberedProducer(
	ctx context.Context, state *world, arguments []string,
) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one producer, got %v", arguments)
	}
	deadline := time.Now().Add(20 * time.Second)
	var seen []string
	for {
		records, err := state.replay(ctx, wire.AgentMemory)
		if err != nil {
			return err
		}
		seen = nil
		for _, record := range records {
			// A memory record rides the topic as itself rather than inside an
			// agent envelope, so the payload is read directly.
			var item wire.MemoryRecord
			if wire.Unmarshal(record.Payload(), &item) != nil {
				continue
			}
			if item.Producer == nil {
				// The record is there and names nobody, so nothing further is
				// coming and the gap is named at once.
				return blocked("a session's linked memory does not stamp its " +
					"agent as producer: Session.Memory scopes the item to the " +
					"conversation only, where the Rust reference stamps " +
					"sdk:<agent> and the current record as origin " +
					"(sdk/src/agent/session_ops.rs linked_memory), so a reader " +
					"cannot tell which agent remembered an item")
			}
			seen = append(seen, item.Producer.Name)
			if item.Producer.Name == arguments[0] {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want the producer %q, the memory topic names %v",
				arguments[0], seen)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// atCheckpoint asserts what the lane held at the moment the checkpoint was
// taken, which is the complement of sinceCheckpoint: one reproduces
// everything before the point, the other resumes after it.
func atCheckpoint(ctx context.Context, state *world, want []string) error {
	if len(want) != 1 {
		return fmt.Errorf("want one record body, got %v", want)
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	value, found := state.get("checkpoint")
	if !found {
		return fmt.Errorf("the scenario took no checkpoint")
	}
	checkpoint := value.(*laser.ContextCheckpoint)
	scope := state.client.Context(session.Conversation())
	messages, err := scope.FetchUntil(
		ctx, checkpoint, []string{wire.AgentSessions}, nil)
	if err != nil {
		return err
	}
	if len(messages) == 0 {
		return fmt.Errorf("want %q at the checkpoint, the read held nothing", want[0])
	}
	// The last one, not the whole list: the checkpoint names a moment, and
	// what the scenario asserts is the record that moment ended on.
	last := string(messages[len(messages)-1].Body())
	if last != want[0] {
		return fmt.Errorf("want %q as the last record at the checkpoint, got %q",
			want[0], last)
	}
	return nil
}
