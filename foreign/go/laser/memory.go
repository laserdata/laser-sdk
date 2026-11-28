package laser

import (
	"context"
	"sort"
	"strings"
	"time"
	"unicode"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// MemoryKind is what a remembered item is.
type MemoryKind string

const (
	// MemoryFact is a standalone fact. Semantic memory: what is known.
	MemoryFact MemoryKind = "fact"
	// MemoryMessage is a conversation turn. Episodic memory: what happened.
	MemoryMessage MemoryKind = "message"
	// MemorySummary is distilled from other items.
	MemorySummary MemoryKind = "summary"
	// MemoryEntity is an extracted entity, the body of a graph node.
	MemoryEntity MemoryKind = "entity"
	// MemoryFeedbackKind reweights an item's recall rank.
	MemoryFeedbackKind MemoryKind = "feedback"
	// MemoryProcedure is a reusable skill or workflow. Procedural memory: how
	// things are done.
	MemoryProcedure MemoryKind = "procedure"
)

// Code is the one-byte discriminator mixed into a content-addressed id, so the
// same body remembered under two kinds gets two ids rather than colliding.
func (kind MemoryKind) Code() byte {
	switch kind {
	case MemoryMessage:
		return 2
	case MemorySummary:
		return 3
	case MemoryEntity:
		return 4
	case MemoryFeedbackKind:
		return 5
	case MemoryProcedure:
		return 6
	default:
		return 1
	}
}

// MemoryKindFromWord reads the kind off a stored record.
//
// An unrecognised word reads as a fact, so a kind a newer build writes
// degrades here rather than failing the whole recall.
func MemoryKindFromWord(word string) MemoryKind {
	switch MemoryKind(word) {
	case MemoryMessage, MemorySummary, MemoryEntity, MemoryFeedbackKind, MemoryProcedure:
		return MemoryKind(word)
	default:
		return MemoryFact
	}
}

// MemoryClass is the converging taxonomy a kind falls into.
type MemoryClass string

const (
	// ClassEpisodic is what happened.
	ClassEpisodic MemoryClass = "episodic"
	// ClassSemantic is what is known.
	ClassSemantic MemoryClass = "semantic"
	// ClassProcedural is how things are done.
	ClassProcedural MemoryClass = "procedural"
)

// Class is the taxonomy this kind belongs to.
func (kind MemoryKind) Class() MemoryClass {
	switch kind {
	case MemoryMessage:
		return ClassEpisodic
	case MemoryProcedure:
		return ClassProcedural
	default:
		return ClassSemantic
	}
}

// MemoryScope is who a memory belongs to and how long it lives.
//
// The identity fields are also what recall filters on, so two users never see
// each other's memories and the same fact in two conversations is two items.
type MemoryScope struct {
	// Stream is the physical isolation boundary.
	Stream string
	// User is the identity the memory is about.
	User  string
	Agent *wire.AgentID
	// Conversation is the session layer.
	Conversation *wire.ConversationID
	// App is the application or organisation within the deployment.
	App string
	// Origin is the session record that motivated the item. It is written
	// with the item and never used to filter recall.
	Origin *wire.SourceRef
	// Producer is the component that produced the item, written with it and
	// never used to filter recall.
	Producer *wire.ProducerInfo
}

// ConsolidationReport is what one consolidation pass did: how many items it
// summarized into fewer, reweighted, pruned, or derived as new items.
type ConsolidationReport struct {
	Summarized int
	Reweighted int
	Pruned     int
	Derived    int
}

// Consolidator runs a memory maintenance pass. SpawnOptions.ConsolidateEvery
// runs the pass over the agent's scope on a background goroutine.
type Consolidator interface {
	Consolidate(ctx context.Context, scope *MemoryScope) (ConsolidationReport, error)
}

// MemoryID is a remembered item's identity.
type MemoryID struct{ wire.ID }

// ContentMemoryID derives an item's id from its owner, kind and body.
//
// The owner is part of the hash, so the same fact remembered by two users is
// two items and neither can address the other's. Every client hashes the same
// byte segments in the same order, so an id minted in one is found by another.
func ContentMemoryID(owner MemoryScope, kind MemoryKind, body []byte) MemoryID {
	agent := ""
	if owner.Agent != nil {
		agent = owner.Agent.String()
	}
	conversation := ""
	if owner.Conversation != nil {
		conversation = owner.Conversation.String()
	}
	separator := []byte{0}
	return MemoryID{ID: wire.ContentID(
		[]byte(owner.Stream), separator,
		[]byte(agent), separator,
		[]byte(owner.User), separator,
		[]byte(owner.App), separator,
		[]byte(conversation), separator,
		[]byte{kind.Code()},
		body,
	)}
}

// MemoryItem is a remembered item as recall hands it back.
type MemoryItem struct {
	ID   MemoryID
	Body []byte
	Kind MemoryKind
	// Score is the recall rank, set by the strategy that found it. Zero when
	// nothing ranked it.
	Score float64
	// Scope is what the item was stored under.
	Scope MemoryScope
	// Source identifies the record that produced this item in the managed view.
	Source *wire.SourceRef
	// At is when the item was remembered.
	At time.Time
	// Signals are the signals that surfaced this item and their standing
	// before fusion. Empty for an unranked recall.
	Signals []RecallSignal
}

// Text is the body as text, for an item remembered as one.
func (item MemoryItem) Text() string { return string(item.Body) }

// RecallStrategy is how recall finds items.
type RecallStrategy string

const (
	// RecallAuto lets the backend route from what it has: a query ranks by
	// overlap, no query returns the most recent. The default. It also labels
	// the feedback signal of a ranked recall.
	RecallAuto RecallStrategy = "auto"
	// RecallKeyword keeps only items sharing a word with the query.
	RecallKeyword RecallStrategy = "keyword"
	// RecallRecent returns the most recent items, ignoring any query.
	RecallRecent RecallStrategy = "recent"
	// RecallSemantic ranks by how close an item's embedding is to the
	// query's, which finds what a question means rather than which words it
	// used. It needs a backend with an embedder, such as VectorMemory.
	RecallSemantic RecallStrategy = "semantic"
	// RecallGraph traverses the knowledge graph from the query seed. The log
	// keeps only items sharing a word with the query.
	RecallGraph RecallStrategy = "graph"
	// RecallTemporal returns items inside the time range, most recent first.
	// The log keeps only items sharing a word with the query.
	RecallTemporal RecallStrategy = "temporal"
	// RecallHybrid fuses every available signal into one ranking: semantic
	// similarity, lexical overlap, and the feedback an agent gave. The
	// multi-signal default for quality recall.
	RecallHybrid RecallStrategy = "hybrid"
)

// validate refuses a strategy word no backend knows.
func (strategy RecallStrategy) validate() error {
	switch strategy {
	case "", RecallAuto, RecallRecent, RecallSemantic, RecallKeyword, RecallGraph, RecallTemporal, RecallHybrid:
		return nil
	default:
		return &InvalidError{Reason: "unknown recall strategy '" + string(strategy) +
			"': use one of auto, recent, semantic, keyword, graph, temporal, hybrid"}
	}
}

// RecallSignal is one signal's contribution to a recalled item: which
// strategy surfaced it, where it ranked within that signal, and what that
// signal scored it before fusion.
//
// It is what keeps a fused recall inspectable: a caller can see that an item
// came top because two signals agreed rather than because one was confident.
type RecallSignal struct {
	// Strategy is the signal that produced the candidate.
	Strategy RecallStrategy
	// Rank is where it placed within that signal, 0 being best.
	Rank int
	// Score is that signal's own score, before fusion.
	Score float64
}

// MemoryQuery is a recall request.
type MemoryQuery struct {
	// Limit is the most items to return. Zero takes the default.
	Limit int
	// TokenBudget bounds the rendered context block alongside Limit. It is
	// advisory: tokens are estimated rather than tokenized, because the client
	// carries no model.
	TokenBudget int
	// Query is the text to rank against.
	Query string
	// Strategy is how to find items.
	Strategy RecallStrategy
	// Kinds narrows recall to these kinds. Empty takes every kind.
	Kinds []MemoryKind
}

// DefaultRecallLimit is how many items a recall returns when it names no
// limit.
const DefaultRecallLimit = 50

// Feedback reweights a recalled item.
//
// A positive weight promotes the item in later recall and a negative one
// demotes it, which is how an agent's experience changes what it is shown
// without anything being rewritten.
type Feedback struct {
	Target MemoryID
	Weight float32
	Note   string
}

// Memory is the agentic memory surface over one namespace.
//
// Items ride the memory topic as an on-log audit of what was remembered, and a
// deployment folds them into a read view. Writing to the log rather than to a
// store is what makes memory replayable.
type Memory struct {
	laser     *Laser
	namespace string
	stream    string
	topic     string
	scope     MemoryScope
	// reader is the folded view recall is served from. It is shared by every
	// handle narrowed off this one, because narrowing changes which items a
	// recall returns and not which topic they were read from.
	reader *memoryReader
	// vector and custom replace the log as the store when set.
	vector *VectorMemory
	custom CustomMemory
	// reranker reorders what a recall with a query returns.
	reranker Reranker
}

// Memory opens the memory surface for a namespace on the handle's default
// stream.
func (laser *Laser) Memory(namespace string) *Memory {
	return laser.logMemory(namespace, wire.AgentMemory)
}

func (laser *Laser) logMemory(namespace, topic string) *Memory {
	return &Memory{
		laser:     laser,
		namespace: namespace,
		stream:    laser.defaultStream,
		topic:     topic,
		scope:     MemoryScope{Stream: laser.defaultStream},
		reader:    newMemoryReader(laser.defaultStream, laser.ResourceName(namespace)),
	}
}

// Namespace is the namespace these memories live in.
func (memory *Memory) Namespace() string { return memory.namespace }

// Scope is what this handle remembers under.
func (memory *Memory) Scope() MemoryScope { return memory.scope }

// For narrows the handle to one scope. The stream is kept, because it is the
// isolation boundary rather than a filter.
func (memory *Memory) For(scope MemoryScope) *Memory {
	narrowed := *memory
	if scope.Stream == "" {
		scope.Stream = memory.stream
	}
	narrowed.scope = scope
	return &narrowed
}

// Remember appends an item, returning the id it was stored under.
//
// The id is derived from the owner, kind and body, so remembering the same
// thing twice converges on one item rather than growing the store.
func (memory *Memory) Remember(
	ctx context.Context, kind MemoryKind, body []byte,
) (MemoryID, error) {
	if memory.custom != nil {
		return memory.custom.Remember(ctx, memory.scope, body)
	}
	id := ContentMemoryID(memory.scope, kind, body)
	if err := memory.Append(ctx, id, kind, body); err != nil {
		return MemoryID{}, err
	}
	return id, nil
}

// Append writes a memory item under a caller-supplied identity.
func (memory *Memory) Append(ctx context.Context, id MemoryID, kind MemoryKind, body []byte) error {
	switch {
	case memory.custom != nil:
		if appender, typed := memory.custom.(MemoryAppender); typed {
			_, err := appender.Append(ctx, memory.scope, id, kind, body)
			return err
		}
		_, err := memory.custom.Remember(ctx, memory.scope, body)
		return err
	case memory.vector != nil:
		return memory.vector.append(ctx, memory.scope, id, kind, body)
	}
	record := wire.RememberItem(id.String(), string(kind), body)
	record.Origin = memory.scope.Origin
	record.Producer = memory.scope.Producer
	return memory.write(ctx, record)
}

// Forget writes a tombstone removing an item from recall.
//
// The item stays on the log, because the log is the audit: forgetting changes
// what is recalled rather than erasing what happened.
func (memory *Memory) Forget(ctx context.Context, target MemoryID) error {
	switch {
	case memory.custom != nil:
		return memory.custom.Forget(ctx, memory.scope, target)
	case memory.vector != nil:
		return memory.vector.forget(ctx, memory.scope, target)
	}
	record := wire.ForgetItem(target.String())
	if memory.scope.Conversation != nil {
		conversation := memory.scope.Conversation.String()
		record.Conversation = &conversation
	}
	return memory.write(ctx, record)
}

// Supersede corrects a fact in one call: the new body is remembered, then the
// old item is forgotten in every conversation, so no later recall returns what
// was corrected.
//
// The tombstone carries no conversation on purpose. A correction learnt in one
// session is about the user rather than about the session, so leaving the old
// fact alive everywhere else would have a later conversation recall it again.
//
// The new fact is written first. A failure in between leaves both facts rather
// than neither, because a duplicate is recoverable and a lost fact is not.
func (memory *Memory) Supersede(
	ctx context.Context, old MemoryID, kind MemoryKind, body []byte,
) (MemoryID, error) {
	id, err := memory.Remember(ctx, kind, body)
	if err != nil {
		return MemoryID{}, err
	}
	everywhere := *memory
	everywhere.scope.Conversation = nil
	if err := everywhere.Forget(ctx, old); err != nil {
		return MemoryID{}, err
	}
	return id, nil
}

// Improve records feedback on a recalled item, reweighting it for later
// recall.
func (memory *Memory) Improve(ctx context.Context, feedback Feedback) error {
	switch {
	case memory.custom != nil:
		_, err := memory.custom.Improve(ctx, memory.scope, feedback)
		return err
	case memory.vector != nil:
		return memory.vector.improve(ctx, memory.scope, feedback)
	}
	record := wire.MemoryRecord{
		Kind:   wire.MemoryFeedback,
		Target: feedback.Target.String(),
		Weight: feedback.Weight,
	}
	if memory.scope.Conversation != nil {
		conversation := memory.scope.Conversation.String()
		record.Conversation = &conversation
	}
	return memory.write(ctx, record)
}

// write appends one memory record, partitioned by the conversation so a
// session's memory stays ordered.
func (memory *Memory) write(ctx context.Context, record wire.MemoryRecord) error {
	if memory.stream == "" {
		return errNoStream()
	}
	if memory.scope.Stream != "" && memory.scope.Stream != memory.stream {
		return &InvalidError{Reason: "memory scope names a different stream"}
	}
	if err := record.Validate(); err != nil {
		return &InvalidError{Reason: err.Error()}
	}
	scope := memory.scope
	if scope.Conversation == nil {
		scope.Conversation = &wire.ConversationID{ID: wire.IDFromBytes(NewOperationID())}
	}
	identity := record.ID
	if identity == "" {
		identity = wire.IDFromBytes(NewOperationID()).String()
	}
	headers, err := memoryHeaders(scope, memory.laser.ResourceName(memory.namespace), identity)
	if err != nil {
		return err
	}
	action := &GovernedAction{
		Kind: ActionMemoryWrite, Stream: memory.stream, Topic: memory.topic,
		Conversation: scope.Conversation,
	}
	if scope.Agent != nil {
		action.Source = scope.Agent.String()
	}
	var payload []byte
	if record.Kind == wire.MemoryItem {
		action.Payload = record.Body
		governed, err := memory.laser.govern(ctx, action)
		if err != nil {
			return err
		}
		if governed != nil {
			record.Body = governed
		}
		payload, err = wire.Marshal(record)
		if err != nil {
			return &CodecError{Reason: "encode memory item", Cause: err}
		}
	} else {
		payload, err = wire.Marshal(record)
		if err != nil {
			return &CodecError{Reason: "encode memory mutation", Cause: err}
		}
		action.Payload = payload
		governed, err := memory.laser.govern(ctx, action)
		if err != nil {
			return err
		}
		if governed != nil {
			payload = governed
		}
	}
	if err := memory.send(ctx, headers, scope.Conversation.String(), payload); err != nil {
		return err
	}
	memory.trackWrite(record, scope.Conversation)
	return nil
}

// send appends one encoded record, creating the stream and topic on a first write.
func (memory *Memory) send(ctx context.Context, headers []iggcon.HeaderEntry, key string, payload []byte) error {
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
	if err != nil {
		return &InvalidError{Reason: "build the memory record: " + err.Error()}
	}
	topic := memory.laser.Stream(memory.stream).Topic(memory.topic)
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return err
	}
	partition, err := partitioningFor(key, true)
	if err != nil {
		return err
	}
	batch := []iggcon.IggyMessage{message}
	_, err = topic.publishOnce(ctx, streamID, topicID, partition, batch)
	if isMissingResource(err) {
		if err := memory.laser.Stream(memory.stream).Ensure(ctx); err != nil {
			return err
		}
		if err := memory.laser.ensureRetained(ctx, memory.topic, ExpireAfter(DefaultMemoryTopicTTL).options(1)); err != nil {
			return err
		}
		_, err = topic.publishOnce(ctx, streamID, topicID, partition, batch)
	}
	return err
}

// Rank orders items against a query and trims them to the request.
//
// This is the ranking a client can do without a model: overlap on words. A
// deployment with embeddings ranks better server-side, and this is what is
// available when one is not there.
func Rank(items []MemoryItem, query MemoryQuery) []MemoryItem {
	limit := recallLimit(query)
	kept := make([]MemoryItem, 0, len(items))
	for _, item := range items {
		if !matchesKind(item, query.Kinds) {
			continue
		}
		kept = append(kept, item)
	}

	strategy := query.Strategy
	if strategy == "" || strategy == RecallAuto {
		strategy = RecallRecent
		if query.Query != "" {
			strategy = RecallKeyword
		}
	}

	switch strategy {
	case RecallKeyword:
		wanted := words(query.Query)
		scored := make([]MemoryItem, 0, len(kept))
		for _, item := range kept {
			overlap := overlap(words(item.Text()), wanted)
			if overlap == 0 {
				// Keyword recall keeps only what shares a word, so an item
				// matching nothing is not a weak answer but no answer.
				continue
			}
			item.Score = overlap
			scored = append(scored, item)
		}
		sort.SliceStable(scored, func(first, second int) bool {
			return scored[first].Score > scored[second].Score
		})
		kept = scored
	default:
		// Most recent first, which is what a reader without a query wants.
		sort.SliceStable(kept, func(first, second int) bool {
			return kept[first].At.After(kept[second].At)
		})
	}

	if len(kept) > limit {
		kept = kept[:limit]
	}
	return kept
}

func matchesKind(item MemoryItem, kinds []MemoryKind) bool {
	if len(kinds) == 0 {
		return true
	}
	for _, kind := range kinds {
		if item.Kind == kind {
			return true
		}
	}
	return false
}

// words splits text into lowercase words, which is the unit overlap is
// measured in.
func words(text string) map[string]struct{} {
	found := map[string]struct{}{}
	for _, word := range strings.FieldsFunc(strings.ToLower(text), func(character rune) bool {
		return !unicode.IsLetter(character) && !unicode.IsDigit(character)
	}) {
		found[word] = struct{}{}
	}
	return found
}

// overlap is the fraction of the query an item covers, from 0 to 1.
//
// It counts query words found rather than item words matched, which is what
// stops a long item outranking a precise one simply by holding more words. The
// division by the query length does not change the order, since it is the same
// for every item: it makes the score a fraction a caller can threshold on.
func overlap(item, query map[string]struct{}) float64 {
	if len(query) == 0 {
		return 0
	}
	shared := 0
	for word := range query {
		if _, held := item[word]; held {
			shared++
		}
	}
	return float64(shared) / float64(len(query))
}

// ToContextBlock renders items as a text block for a prompt, filling in rank
// order until the token budget is reached.
//
// The budget is advisory: tokens are estimated from characters rather than
// tokenized, because the client carries no model and should not pretend to.
func ToContextBlock(items []MemoryItem, tokenBudget int) string {
	var built strings.Builder
	used := 0
	for _, item := range items {
		line := item.Text()
		if line == "" {
			continue
		}
		cost := estimateTokens(line)
		if tokenBudget > 0 && used+cost > tokenBudget {
			// Filling in rank order means stopping at the first item that does
			// not fit, rather than skipping it for a smaller one further down:
			// a block ordered by rank is what a model reads best.
			break
		}
		if built.Len() > 0 {
			built.WriteString("\n")
		}
		built.WriteString(line)
		used += cost
	}
	return built.String()
}

// estimateTokens approximates a token count from a body's size.
//
// The estimate is the contract's own, not a second one written here: a budget
// that two clients spend differently is not one budget. It is deliberately an
// estimate, because a real count needs the model's tokenizer and carrying one
// would tie this client to a provider.
func estimateTokens(text string) int {
	return int(wire.EstimateTokens(len(text)))
}
