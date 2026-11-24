package laser

import (
	"context"
	"sort"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Assemble reads this session's own history under policy and turns what it
// keeps into a manifest-backed AssembledContext: every fragment names the
// exact log position it came from, so a caller can tell a contract later what
// a model call actually saw and re-fetch the same bytes.
//
// Ported from assemble in sdk/src/agent/session_ops.rs. Rust's Session owns
// its lane and reads it directly; Go's Session does the same by resolving
// which topics its own declared SessionLayout puts work on (the session lane
// itself, plus a per-agent-topic layout's declared topics), rather than
// asking the caller to name them the way ContextScope.Fetch does.
func (session *Session) Assemble(
	ctx context.Context, policy ContextPolicy,
) (*AssembledContext, error) {
	topics, err := session.assembledTopics()
	if err != nil {
		return nil, err
	}
	messages, err := session.laser.Context(session.conversation).
		FetchWith(ctx, topics, policy)
	if err != nil {
		return nil, err
	}
	streamID, locators, err := session.topicLocators(ctx, messages)
	if err != nil {
		return nil, err
	}

	conversation := session.conversation.String()
	type frontierKey struct {
		topic     uint32
		partition uint32
	}
	frontier := map[frontierKey]uint64{}
	var totalTokens, totalBytes uint64
	fragments := make([]wire.Fragment, 0, len(messages))
	for _, message := range messages {
		locator := locators[message.Topic]
		body := message.Body()
		size := uint64(len(body))
		tokens := wire.EstimateTokens(len(body))
		totalTokens += tokens
		totalBytes += size
		source := wire.NewMessageSource(wire.MessageSource{
			Stream: streamID, Topic: locator.topicID,
			Partition: message.Record.PartitionID, Offset: message.Record.Offset(),
			Generation: locator.generation, Conversation: &conversation,
		})
		fragments = append(fragments, wire.MessageFragment(
			source, uint32(tokens), uint32(size)))
		key := frontierKey{topic: locator.topicID, partition: message.Record.PartitionID}
		offset := message.Record.Offset()
		// A plain "offset > frontier[key]" would never record offset 0 for a
		// key seen for the first time, since a missing map entry already
		// reads as the zero value: the comma-ok form is what tells "never
		// seen" apart from "saw offset 0".
		if existing, seen := frontier[key]; !seen || offset > existing {
			frontier[key] = offset
		}
	}
	frontierPositions := make([]wire.FrontierPosition, 0, len(frontier))
	for key, offset := range frontier {
		frontierPositions = append(frontierPositions, wire.FrontierPosition{
			Topic: key.topic, Partition: key.partition, Offset: offset,
		})
	}
	sort.Slice(frontierPositions, func(first, second int) bool {
		left, right := frontierPositions[first], frontierPositions[second]
		if left.Topic != right.Topic {
			return left.Topic < right.Topic
		}
		return left.Partition < right.Partition
	})

	manifest := wire.ContextManifest{
		Policy:    policy.Name(),
		Fragments: fragments,
		Tokens:    totalTokens,
		Bytes:     totalBytes,
		Frontier:  frontierPositions,
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return &AssembledContext{Fragments: messages, Manifest: manifest}, nil
}

// assembledTopics is every topic this session's work can land on under its
// declared layout: the session lane itself, which a shared, per-agent-
// partition, or single-partition layout keeps everything on, plus a
// per-agent-topic layout's own declared topics.
func (session *Session) assembledTopics() ([]string, error) {
	topics := []string{wire.AgentSessions}
	if session.config.Layout.Kind() != LayoutPerAgentTopic {
		return topics, nil
	}
	declared, err := session.config.Layout.DeclaredTopics()
	if err != nil {
		return nil, err
	}
	return append(topics, declared...), nil
}

// topicLocator is a topic's numeric id and generation, the half a fragment's
// source needs beyond the partition and offset ContextMessage already
// carries. The zero value (a topic whose lookup failed) locates with topic id
// 0 rather than failing the whole assemble, the same "warn loudly, keep the
// rest right" tradeoff resolveSource makes for its own stream/topic lookup.
type topicLocator struct {
	topicID    uint32
	generation *uint64
}

// topicLocators resolves the numeric stream id, once, and each distinct
// topic named by messages, so every fragment's source locates fully without
// a round trip per fragment.
func (session *Session) topicLocators(
	ctx context.Context, messages []ContextMessage,
) (uint32, map[string]topicLocator, error) {
	client, err := session.laser.connected()
	if err != nil {
		return 0, nil, err
	}
	stream := session.config.Stream
	if stream == "" {
		stream = session.laser.defaultStream
	}
	if stream == "" {
		return 0, nil, errNoStream()
	}
	streamIdent, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return 0, nil, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	var streamID uint32
	if details, err := client.GetStream(ctx, streamIdent); err == nil && details != nil {
		streamID = details.Id
	}

	locators := make(map[string]topicLocator, len(messages))
	for _, message := range messages {
		if _, resolved := locators[message.Topic]; resolved {
			continue
		}
		topicIdent, err := iggcon.NewIdentifier(message.Topic)
		if err != nil {
			return 0, nil, &InvalidError{Reason: "topic name: " + err.Error()}
		}
		details, err := client.GetTopic(ctx, streamIdent, topicIdent)
		if err != nil || details == nil {
			continue
		}
		generation := details.CreatedAt
		locators[message.Topic] = topicLocator{topicID: details.Id, generation: &generation}
	}
	return streamID, locators, nil
}
