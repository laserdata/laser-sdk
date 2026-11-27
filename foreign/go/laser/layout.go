package laser

import (
	"sort"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// SessionLayout is where a stream's agents put their work.
//
// The default is one shared lane. The other three trade that for isolation:
// a declared topic or partition per agent keeps one agent's work out of
// another's read path, and a single partition gives total order at the cost
// of parallelism. A session's lifecycle and state records stay on the lane in
// every layout, so a reader always finds a session's spine in one place.
//
// The zero value is the shared layout, which is what a caller that never
// mentions layouts gets.
type SessionLayout struct {
	kind       string
	topics     map[wire.AgentID]string
	partitions map[wire.AgentID]uint32
}

// The layout kinds, as they are named in every client.
const (
	LayoutShared            = "shared"
	LayoutPerAgentTopic     = "per_agent_topic"
	LayoutPerAgentPartition = "per_agent_partition"
	LayoutSinglePartition   = "single_partition"
)

// SharedLayout puts every agent on one agent.sessions topic keyed by session.
func SharedLayout() SessionLayout { return SessionLayout{kind: LayoutShared} }

// PerAgentTopicLayout gives each declared agent its own topic, keyed by
// session.
//
// On an agent.sessions send, a command addressed to a declared agent goes to
// that agent's topic, and a response, error or chunk goes to its addressee's
// declared topic. Lifecycle, state, broadcast records, and records for an
// undeclared agent stay on the session lane. Bootstrap creates the declared
// topics with the lane's partition count and retention.
func PerAgentTopicLayout(topics map[wire.AgentID]string) SessionLayout {
	declared := make(map[wire.AgentID]string, len(topics))
	for agent, topic := range topics {
		declared[agent] = topic
	}
	return SessionLayout{kind: LayoutPerAgentTopic, topics: declared}
}

// PerAgentPartitionLayout keeps work on agent.sessions but on a declared
// partition per agent.
//
// A command lands on its addressee's partition and a reply on its requester's.
// Lifecycle, state, and records for an undeclared agent stay on the session's
// own partition.
func PerAgentPartitionLayout(partitions map[wire.AgentID]uint32) SessionLayout {
	declared := make(map[wire.AgentID]uint32, len(partitions))
	for agent, partition := range partitions {
		declared[agent] = partition
	}
	return SessionLayout{kind: LayoutPerAgentPartition, partitions: declared}
}

// SinglePartitionLayout puts everything on one partition: bootstrap creates
// every agent topic with a single partition, so the whole stream is totally
// ordered.
func SinglePartitionLayout() SessionLayout {
	return SessionLayout{kind: LayoutSinglePartition}
}

// Kind names this layout. The zero value reads as the shared layout.
func (layout SessionLayout) Kind() string {
	if layout.kind == "" {
		return LayoutShared
	}
	return layout.kind
}

// Topics is a copy of the declared agent topics, empty under any other
// layout.
func (layout SessionLayout) Topics() map[wire.AgentID]string {
	declared := make(map[wire.AgentID]string, len(layout.topics))
	for agent, topic := range layout.topics {
		declared[agent] = topic
	}
	return declared
}

// Partitions is a copy of the declared agent partitions, empty under any
// other layout.
func (layout SessionLayout) Partitions() map[wire.AgentID]uint32 {
	declared := make(map[wire.AgentID]uint32, len(layout.partitions))
	for agent, partition := range layout.partitions {
		declared[agent] = partition
	}
	return declared
}

// DeclaredTopics are the distinct topics a per-agent topic layout declares,
// in name order, and none under any other layout.
//
// The order is fixed so bootstrap creates them in the same order on every run
// and on every client, which is what makes a partial bootstrap resumable
// rather than a different half each time.
func (layout SessionLayout) DeclaredTopics() ([]string, error) {
	// Only a per-agent topic layout fills this map, so no kind check is
	// needed to keep the other three from declaring anything.
	seen := make(map[string]struct{}, len(layout.topics))
	declared := make([]string, 0, len(layout.topics))
	for _, topic := range layout.topics {
		// The layout routes around these two, so declaring one would send an
		// agent's work to the very lane the layout exists to keep it off.
		if topic == wire.AgentSessions || topic == wire.AgentControl {
			return nil, &InvalidError{
				Reason: "a per-agent topic layout cannot declare `" + topic + "`"}
		}
		if _, err := iggcon.NewIdentifier(topic); err != nil {
			return nil, &InvalidError{
				Reason: "a per-agent topic layout cannot declare `" + topic + "`: " +
					err.Error()}
		}
		if _, repeated := seen[topic]; repeated {
			continue
		}
		seen[topic] = struct{}{}
		declared = append(declared, topic)
	}
	sort.Strings(declared)
	return declared, nil
}

// Validate reports a layout this client will not route under.
func (layout SessionLayout) Validate() error {
	switch layout.Kind() {
	case LayoutShared, LayoutSinglePartition:
		return nil
	case LayoutPerAgentTopic:
		for agent := range layout.topics {
			if agent.IsZero() {
				return &InvalidError{
					Reason: "a per-agent topic layout must name every agent"}
			}
		}
		_, err := layout.DeclaredTopics()
		return err
	case LayoutPerAgentPartition:
		for agent := range layout.partitions {
			if agent.IsZero() {
				return &InvalidError{
					Reason: "a per-agent partition layout must name every agent"}
			}
		}
		return nil
	default:
		return &InvalidError{Reason: "unknown session layout " + layout.kind}
	}
}

// directed reports the kinds a layout moves off the lane: work addressed to
// one agent, and the replies to it. Lifecycle and state are not directed, so
// they stay where every reader looks for them.
func directed(kind wire.AgentKind) bool {
	switch kind {
	case wire.KindCommand, wire.KindResponse, wire.KindError, wire.KindChunk:
		return true
	default:
		return false
	}
}

// destinationTopic is the topic one agent record lands on under a declared
// per-agent topic layout, or the empty string to keep the topic it was sent
// on.
//
// Only agent.sessions sends move. A kind of "" is a plain record with no
// envelope, which is directed like a command: it carries an addressee and
// nothing else routes it.
func destinationTopic(
	layout SessionLayout, topic string, kind wire.AgentKind, target *wire.AgentID,
) string {
	if topic != wire.AgentSessions {
		return ""
	}
	if kind != "" && !directed(kind) {
		return ""
	}
	if target == nil {
		return ""
	}
	// Only a per-agent topic layout fills this map, and an agent it does not
	// declare is not in it, so one lookup answers both questions.
	return layout.topics[*target]
}

// declaredTopic is the topic agent reads instead of agent.sessions under a
// declared per-agent topic layout, the one its addressed work and replies
// land on.
func declaredTopic(layout SessionLayout, agent wire.AgentID) string {
	return layout.topics[agent]
}

// declaredPartition is the partition a record of kind addressed to target
// lands on under a declared per-agent partition layout.
//
// The second result is false for the session's own partition, which is where
// everything the layout does not move goes: the conversation keys it, so a
// session's records stay ordered among themselves.
func declaredPartition(
	layout SessionLayout, topic string, kind wire.AgentKind, target *wire.AgentID,
) (uint32, bool) {
	if topic != wire.AgentSessions {
		return 0, false
	}
	if !directed(kind) || target == nil {
		return 0, false
	}
	// Only a per-agent partition layout fills this map, so an undeclared
	// agent and every other layout answer the same way: the session's own
	// partition.
	partition, declared := layout.partitions[*target]
	return partition, declared
}
