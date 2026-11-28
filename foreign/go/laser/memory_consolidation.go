package laser

import (
	"bytes"
	"context"
	"slices"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultConsolidator keeps recent items and can summarize the messages in each conversation.
type DefaultConsolidator struct {
	memory          *Memory
	maxItems        int
	summarizer      Summarizer
	pruneSummarized bool
}

// NewDefaultConsolidator keeps the newest maxItems from the latest 10,000 items in the scope.
func NewDefaultConsolidator(memory *Memory, maxItems int) *DefaultConsolidator {
	return &DefaultConsolidator{memory: memory, maxItems: maxItems}
}

// WithSummarizer folds each conversation's messages in recall order into a durable summary.
func (c *DefaultConsolidator) WithSummarizer(summarizer Summarizer) *DefaultConsolidator {
	configured := *c
	configured.summarizer = summarizer
	return &configured
}

// PruneSummarized removes the covered messages after every summary has been stored.
func (c *DefaultConsolidator) PruneSummarized() *DefaultConsolidator {
	configured := *c
	configured.pruneSummarized = true
	return &configured
}

// Consolidate runs a single pass. Nil uses the memory handle's existing scope.
func (c *DefaultConsolidator) Consolidate(ctx context.Context, scope *MemoryScope) (ConsolidationReport, error) {
	var report ConsolidationReport
	if c.memory == nil || c.maxItems < 0 {
		return report, &InvalidError{Reason: "consolidation needs memory and a nonnegative item limit"}
	}
	memory := c.memory
	if scope != nil {
		memory = memory.For(*scope)
	}
	items, err := memory.Recall(ctx, MemoryQuery{Limit: 10_000, Strategy: RecallRecent})
	if err != nil {
		return report, err
	}

	if c.summarizer != nil {
		count, err := summarizeConversations(ctx, memory, c.summarizer, items)
		if err != nil {
			return report, err
		}
		report.Summarized = count
		if c.pruneSummarized {
			kept := items[:0]
			for _, item := range items {
				if item.Kind != MemoryMessage {
					kept = append(kept, item)
					continue
				}
				if err := memory.Forget(ctx, item.ID); err == nil {
					report.Pruned++
				} else if ctx.Err() != nil {
					return report, ctx.Err()
				}
			}
			items = kept
		}
	}
	for i := len(items) - 1; i >= c.maxItems; i-- {
		if err := memory.Forget(ctx, items[i].ID); err == nil {
			report.Pruned++
		} else if ctx.Err() != nil {
			return report, ctx.Err()
		}
	}
	return report, nil
}

func summarizeConversations(ctx context.Context, memory *Memory, summarizer Summarizer, items []MemoryItem) (int, error) {
	byConversation := make(map[wire.ConversationID][]MemoryItem)
	var conversations []wire.ConversationID
	existing := make(map[MemoryID]struct{}, len(items))
	for _, item := range items {
		existing[item.ID] = struct{}{}
		if item.Kind != MemoryMessage {
			continue
		}
		conversation := memoryConversationOf(item.Scope)
		if _, ok := byConversation[conversation]; !ok {
			conversations = append(conversations, conversation)
		}
		byConversation[conversation] = append(byConversation[conversation], item)
	}
	summarized := 0
	for _, conversation := range conversations {
		turns := byConversation[conversation]
		scope := memory.scope
		scope.Conversation = &conversation
		id := summaryID(scope, turns)
		if _, stored := existing[id]; stored {
			continue
		}
		bodies := make([][]byte, len(turns))
		for i := range turns {
			bodies[i] = turns[i].Body
		}
		body, err := summarizer.Summarize(ctx, bodies)
		if err != nil {
			return summarized, err
		}
		if err := memory.For(scope).Append(ctx, id, MemorySummary, body); err != nil {
			return summarized, err
		}
		summarized += len(turns)
	}
	return summarized, nil
}

func summaryID(scope MemoryScope, turns []MemoryItem) MemoryID {
	ids := make([]wire.ID, len(turns))
	for i := range turns {
		ids[i] = turns[i].ID.ID
	}
	slices.SortFunc(ids, func(a, b wire.ID) int { return bytes.Compare(a[:], b[:]) })
	covered := make([]byte, 0, len(ids)*len(wire.ID{}))
	for _, id := range ids {
		covered = append(covered, id[:]...)
	}
	return ContentMemoryID(scope, MemorySummary, covered)
}

// Consolidate prunes the oldest items in this memory's scope.
func (memory *Memory) Consolidate(ctx context.Context, maxItems int) (ConsolidationReport, error) {
	return NewDefaultConsolidator(memory, maxItems).Consolidate(ctx, nil)
}

// ConsolidateWith summarizes this scope's conversations before applying its item limit.
func (memory *Memory) ConsolidateWith(ctx context.Context, maxItems int, summarizer Summarizer, pruneSummarized bool) (ConsolidationReport, error) {
	c := NewDefaultConsolidator(memory, maxItems).WithSummarizer(summarizer)
	if pruneSummarized {
		c = c.PruneSummarized()
	}
	return c.Consolidate(ctx, nil)
}
