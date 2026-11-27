package bdd

import (
	"encoding/binary"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Reference agent-memory engine: the executable specification of the recall
// contract every LaserData SDK and the managed backend must reproduce —
// content-addressed dedup, recency order, feedback re-ranking, and forget.
// Pure and transport-free, mirroring the Rust BDD crate's MemoryEngine and
// bdd/python/reference.py's.
//
// The content id comes from the client's own ContentMemoryID rather than a
// hash re-implemented here, so a deduped id is identical across the clients
// by construction instead of by a second implementation agreeing.

// memoryItem is one remembered body under its content id.
type memoryItem struct {
	id   laser.MemoryID
	body []byte
}

// MemoryEngine is an in-memory agent-memory store under one durable owner.
//
// Items keep arrival order, so the most recent are the tail.
type MemoryEngine struct {
	agent    wire.AgentID
	items    []memoryItem
	feedback map[laser.MemoryID]float64
	counter  uint64
}

// NewMemoryEngine opens an empty store owned by agent.
func NewMemoryEngine(agent wire.AgentID) *MemoryEngine {
	return &MemoryEngine{
		agent:    agent,
		feedback: map[laser.MemoryID]float64{},
	}
}

// scopeFor is the owner a content id is minted under: the engine's agent,
// and the conversation when the caller named one.
//
// The conversation is part of the id, which is what makes one fact
// remembered in two conversations two items rather than one.
func (engine *MemoryEngine) scopeFor(conversation string) laser.MemoryScope {
	scope := laser.MemoryScope{Agent: &engine.agent}
	if conversation == "" {
		return scope
	}
	if parsed, err := wire.ParseID(conversation); err == nil {
		id := wire.ConversationID{ID: parsed}
		scope.Conversation = &id
	}
	return scope
}

// Remember stores a body, returning its id.
//
// With dedup the id is the content's own, so remembering the same fact twice
// stores one item. Without it the body is salted with a counter, so each
// remembering is its own item.
func (engine *MemoryEngine) Remember(
	body []byte, dedup bool, conversation string,
) laser.MemoryID {
	scope := engine.scopeFor(conversation)
	if dedup {
		id := laser.ContentMemoryID(scope, laser.MemoryFact, body)
		for _, item := range engine.items {
			if item.id == id {
				return id
			}
		}
		engine.items = append(engine.items, memoryItem{id: id, body: body})
		return id
	}
	engine.counter++
	salt := make([]byte, 16)
	binary.BigEndian.PutUint64(salt[8:], engine.counter)
	id := laser.ContentMemoryID(scope, laser.MemoryFact, append(append([]byte{}, body...), salt...))
	engine.items = append(engine.items, memoryItem{id: id, body: body})
	return id
}

// Len is how many items the store holds.
func (engine *MemoryEngine) Len() int { return len(engine.items) }

// Improve reweights an item for later recall.
func (engine *MemoryEngine) Improve(target laser.MemoryID, weight float64) {
	engine.feedback[target] += weight
}

// IDOf is the id a body was stored under, searching from the most recent.
func (engine *MemoryEngine) IDOf(body string) (laser.MemoryID, bool) {
	for index := len(engine.items) - 1; index >= 0; index-- {
		if string(engine.items[index].body) == body {
			return engine.items[index].id, true
		}
	}
	return laser.MemoryID{}, false
}

// Forget drops an item. Remembering it again later brings it back, the way a
// write after a tombstone does.
func (engine *MemoryEngine) Forget(target laser.MemoryID) {
	kept := engine.items[:0]
	for _, item := range engine.items {
		if item.id != target {
			kept = append(kept, item)
		}
	}
	engine.items = kept
}

// Recall answers the most recent items first, re-ranked by feedback when any
// was given.
func (engine *MemoryEngine) Recall(limit int) []string {
	live := make([]memoryItem, 0, len(engine.items))
	for index := len(engine.items) - 1; index >= 0; index-- {
		live = append(live, engine.items[index])
	}
	if len(engine.feedback) > 0 {
		// A stable sort by weight, so items nobody judged keep their
		// recency order among themselves.
		for outer := 1; outer < len(live); outer++ {
			for inner := outer; inner > 0 &&
				engine.feedback[live[inner].id] > engine.feedback[live[inner-1].id]; inner-- {
				live[inner], live[inner-1] = live[inner-1], live[inner]
			}
		}
	}
	bodies := make([]string, 0, limit)
	for index := 0; index < len(live) && index < limit; index++ {
		bodies = append(bodies, string(live[index].body))
	}
	return bodies
}
