// Memory recalls what is relevant and the graph shows how it connects.
//
// The first phases run in process on a vector memory and need no server. With
// LASER_CONNECTION_STRING set, the same facts become durable on the log, and a
// managed deployment adds the knowledge graph.
package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example           = "memory"
	graphName         = "ops"
	topology          = "topology"
	embeddingSize     = 64
	mitigationSinceUs = uint64(1_900_000_000_000_000)
)

var knowledge = []string{
	"gateway latency spikes are usually database connection pool exhaustion",
	"duplicate config pushes trace back to retries without an idempotency key",
	"search returning stale results means the nightly index rebuild failed",
	"gateway pages recover fastest by failing over to the read replica",
	"auth token errors after a deploy come from the rotated signing key",
	"storage read misses climb when the cache eviction rate is set too aggressive",
	"metrics gaps are the message queue dropping sample events",
	"recommendation gaps appear when the search index lags behind the source data",
}

// entities are the graph nodes as label and value.
var entities = [][2]string{
	{"Service", "gateway"}, {"Service", "config"}, {"Service", "search"}, {"Service", "storage"},
	{"Service", "auth"}, {"Service", "recommendations"}, {"Service", "metrics"}, {"Service", "notifications"},
	{"Component", "hosts-db"}, {"Component", "db-pool"}, {"Component", "read-replica"},
	{"Component", "search-index"}, {"Component", "signing-key"}, {"Component", "cache"},
	{"Component", "token-service"}, {"Component", "message-queue"},
	{"Team", "identity"}, {"Team", "search-platform"}, {"Team", "core-platform"},
	{"Incident", "INC-101"}, {"Incident", "INC-102"},
}

// relationships are the graph edges as from, edge type and to.
var relationships = [][3]string{
	{"gateway", "depends_on", "hosts-db"}, {"gateway", "depends_on", "db-pool"},
	{"gateway", "depends_on", "token-service"}, {"gateway", "mitigated_by", "read-replica"},
	{"config", "depends_on", "hosts-db"}, {"config", "depends_on", "signing-key"},
	{"config", "depends_on", "token-service"}, {"search", "depends_on", "search-index"},
	{"search", "depends_on", "cache"}, {"search", "mitigated_by", "cache"},
	{"storage", "depends_on", "cache"}, {"storage", "depends_on", "hosts-db"},
	{"auth", "depends_on", "signing-key"}, {"recommendations", "depends_on", "search-index"},
	{"recommendations", "depends_on", "cache"}, {"metrics", "depends_on", "hosts-db"},
	{"metrics", "depends_on", "message-queue"}, {"notifications", "depends_on", "message-queue"},
	{"read-replica", "replicates", "hosts-db"}, {"identity", "owns", "gateway"},
	{"identity", "owns", "config"}, {"search-platform", "owns", "search"},
	{"search-platform", "owns", "recommendations"}, {"core-platform", "owns", "auth"},
	{"core-platform", "owns", "storage"}, {"core-platform", "owns", "metrics"},
	{"core-platform", "owns", "notifications"}, {"INC-101", "affected", "gateway"},
	{"INC-101", "affected", "db-pool"}, {"INC-102", "affected", "search"},
	{"INC-102", "affected", "search-index"},
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", example, err)
		os.Exit(1)
	}
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conversation := laser.NewConversationID()
	if err := vectorPhase(ctx, conversation); err != nil {
		return err
	}
	if strings.TrimSpace(os.Getenv("LASER_CONNECTION_STRING")+os.Getenv("LASER_SERVER")) == "" {
		fmt.Println("\nSet LASER_CONNECTION_STRING to use durable memory and the knowledge graph.")
		return nil
	}
	client, err := examples.ConnectExample(ctx, example)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	if err := managedPhase(ctx, client, conversation); err != nil {
		return err
	}
	examples.Phase("done")
	fmt.Println("memory recalls what is relevant, the graph shows how it connects")
	return nil
}

func vectorPhase(ctx context.Context, conversation wire.ConversationID) error {
	memory := laser.NewVectorMemory(deterministicEmbedder{})
	scope := laser.MemoryScope{Conversation: &conversation}

	examples.Phase("Remember")
	var replicaNote, staleNote *laser.MemoryID
	for _, fact := range knowledge {
		id, err := memory.Remember(ctx, scope, laser.MemoryFact, []byte(fact))
		if err != nil {
			return fmt.Errorf("remember: %w", err)
		}
		if strings.Contains(fact, "read replica") {
			replicaNote = &id
		}
		if strings.Contains(fact, "index rebuild") {
			staleNote = &id
		}
	}
	fmt.Printf("remembered %d facts\n", len(knowledge))

	examples.Phase("Recall")
	question := "gateway is slow during the rollout"
	ask := laser.MemoryQuery{Query: question, Strategy: laser.RecallSemantic, Limit: 3}
	initial, err := memory.Recall(ctx, scope, ask)
	if err != nil {
		return fmt.Errorf("recall: %w", err)
	}
	printHits(fmt.Sprintf("recall for %q", question), initial)

	if replicaNote == nil {
		return errors.New("the read-replica note was not remembered")
	}
	examples.Phase("Improve")
	if err := memory.Improve(ctx, laser.Feedback{Target: *replicaNote, Weight: 1, Note: "resolved the incident"}); err != nil {
		return fmt.Errorf("improve: %w", err)
	}
	improved, err := memory.Recall(ctx, scope, ask)
	if err != nil {
		return fmt.Errorf("recall after feedback: %w", err)
	}
	printHits(fmt.Sprintf("recall after feedback for %q", question), improved)
	if len(improved) == 0 || improved[0].ID != *replicaNote {
		return errors.New("feedback did not rank the read-replica note first")
	}
	fmt.Println("the upvoted note now ranks first")

	if staleNote == nil {
		return errors.New("the index-rebuild note was not remembered")
	}
	examples.Phase("Forget")
	if err := memory.Forget(ctx, *staleNote); err != nil {
		return fmt.Errorf("forget: %w", err)
	}
	remaining, err := memory.Recall(ctx, scope,
		laser.MemoryQuery{Query: "search results are stale", Strategy: laser.RecallSemantic, Limit: 3})
	if err != nil {
		return fmt.Errorf("recall after forget: %w", err)
	}
	if slices.ContainsFunc(remaining, func(item laser.MemoryItem) bool { return item.ID == *staleNote }) {
		return errors.New("forgotten memory still appears in recall")
	}
	fmt.Println("forgot the superseded index-rebuild note, it no longer recalls")
	return nil
}

func managedPhase(ctx context.Context, client *laser.Laser, conversation wire.ConversationID) error {
	capabilities := client.Capabilities()
	if !examples.ManagedGate(capabilities, examples.FeatureGraph, example) ||
		!examples.ManagedGate(capabilities, examples.FeatureKv, example) {
		return nil
	}
	if err := client.Bootstrap(ctx, examples.Partitions, laser.ExpireAfter(examples.SessionRetention)); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	if err := durablePhase(ctx, client, conversation); err != nil {
		return err
	}
	return graphPhase(ctx, client)
}

func durablePhase(ctx context.Context, client *laser.Laser, conversation wire.ConversationID) error {
	examples.Phase("Remember durable facts")
	incidents, err := client.MemoryTopic("incidents").Partitions(examples.Partitions).TTL(24 * time.Hour).Build(ctx)
	if err != nil {
		return fmt.Errorf("build the incidents memory topic: %w", err)
	}
	durable := incidents.For(laser.MemoryScope{Conversation: &conversation})
	for _, fact := range knowledge {
		if _, err := durable.Remember(ctx, laser.MemoryFact, []byte(fact)); err != nil {
			return fmt.Errorf("remember a durable fact: %w", err)
		}
	}
	// The managed read view folds the memory topic asynchronously, so wait until
	// it holds the new facts instead of reading it once.
	hits, err := waitForRecall(ctx, durable, 3)
	if err != nil {
		return err
	}
	fmt.Printf("stored %d durable facts, recalled %d most-recent\n", len(knowledge), len(hits))
	for _, hit := range hits {
		if hit.Source != nil && hit.Source.Message != nil {
			at := hit.Source.Message
			fmt.Printf("  recalled from source %d/%d partition %d offset %d\n", at.Stream, at.Topic, at.Partition, at.Offset)
		}
	}

	examples.Phase("Scope the session: messages and memory under one conversation")
	session := client.Context(conversation)
	if err := session.Append(ctx, wire.AgentAudit, []byte("incident opened: gateway slow")); err != nil {
		return fmt.Errorf("append to the audit trail: %w", err)
	}
	scoped, err := durable.Recall(ctx, laser.MemoryQuery{Limit: 3})
	if err != nil {
		return fmt.Errorf("scoped recall: %w", err)
	}
	trail, err := session.Fetch(ctx, []string{wire.AgentAudit}, 8, 0)
	if err != nil {
		return fmt.Errorf("read the audit trail: %w", err)
	}
	fmt.Printf("one scope recalled %d durable facts and read back %d of the conversation's messages\n",
		len(scoped), len(trail))
	return nil
}

func graphPhase(ctx context.Context, client *laser.Laser) error {
	examples.Phase("Build the knowledge graph")
	if err := registerGraph(ctx, client); err != nil {
		return err
	}
	registry := client.Kv(topology)
	nodes := make(map[string]wire.GraphNode, len(entities))
	ordered := make([]wire.GraphNode, 0, len(entities))
	for _, entity := range entities {
		label, value := entity[0], entity[1]
		if err := registry.Set([]byte(value)).JSON(map[string]string{"kind": label, "value": value}).Send(ctx); err != nil {
			return fmt.Errorf("register %s: %w", value, err)
		}
		node := wire.EntityNode(label, value)
		source := wire.NewKvSource(topology, value)
		node.Source = &source
		nodes[value] = node
		ordered = append(ordered, node)
	}
	edges := make([]wire.GraphEdge, 0, len(relationships))
	bitemporal := 0
	for _, relationship := range relationships {
		from, to := nodes[relationship[0]], nodes[relationship[2]]
		edge := wire.RelateNodes(from, relationship[1], to)
		source := wire.NewKvSource(topology, relationship[0])
		edge.Source = &source
		if relationship[1] == "mitigated_by" {
			since := mitigationSinceUs
			edge.ValidFrom = &since
			bitemporal++
		}
		edges = append(edges, edge)
	}
	graph := client.Graph(graphName)
	gateway, incident := nodes["gateway"], nodes["INC-101"]
	if err := graph.Upsert(ctx, ordered, edges); err != nil {
		return fmt.Errorf("upsert the graph: %w", err)
	}
	fmt.Printf("upserted %d nodes and %d edges (%d bitemporal)\n", len(ordered), len(edges), bitemporal)

	examples.Phase("Read a node's neighbors")
	around, err := graph.Neighbors(ctx, gateway.ID, wire.EdgeOut, "", 1)
	if err != nil {
		return fmt.Errorf("read gateway's neighbors: %w", err)
	}
	printNodes("gateway's one-hop neighborhood", around.Nodes)

	examples.Phase("Traverse from a predicate")
	dependencies, err := client.Graph(graphName).
		StartMatch(wire.Pred("label", wire.CmpEq, wire.StringTyped("Service"))).
		Out("depends_on").Limit(100).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("traverse the service dependencies: %w", err)
	}
	printNodes("components every Service depends on", labelled(dependencies.Nodes, "Component"))

	examples.Phase("Trace an incident's blast radius")
	blast, err := client.Graph(graphName).StartIDs(incident.ID).Out("affected").Fetch(ctx)
	if err != nil {
		return fmt.Errorf("trace INC-101: %w", err)
	}
	fmt.Printf("what INC-101 affected: %s\n", strings.Join(values(except(blast.Nodes, incident.ID)), ", "))

	examples.Phase("Trace a node back to its source")
	for _, node := range around.Nodes {
		if node.ID == gateway.ID && node.Source != nil && node.Source.Kv != nil {
			fmt.Printf("gateway's source record is %s/%s\n", node.Source.Kv.Namespace, node.Source.Kv.Key)
		}
	}

	examples.Phase("Read the graph as of a point in time")
	before, err := client.Graph(graphName).StartIDs(gateway.ID).Out("mitigated_by").AsOf(mitigationSinceUs - 1).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("read the mitigations before the rollout: %w", err)
	}
	after, err := client.Graph(graphName).StartIDs(gateway.ID).Out("mitigated_by").AsOf(mitigationSinceUs + 1).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("read the mitigations after the rollout: %w", err)
	}
	fmt.Printf("gateway mitigations before the rollout: %d, after: %d\n",
		len(except(before.Nodes, gateway.ID)), len(except(after.Nodes, gateway.ID)))

	examples.Phase("Return whole paths")
	paths, err := client.Graph(graphName).StartIDs(incident.ID).Out("affected").ReturnPaths().Fetch(ctx)
	if err != nil {
		return fmt.Errorf("read the paths from INC-101: %w", err)
	}
	fmt.Printf("INC-101 reaches %d components by a traced path\n", len(paths.Paths))
	return nil
}

// registerGraph declares the graph projection and waits until the catalog serves it.
func registerGraph(ctx context.Context, client *laser.Laser) error {
	id := wire.ProjectionID(graphName + ".v1")
	projection := wire.Projection{
		ID: id, Name: graphName, Version: 1,
		Kind: wire.ProjectionGraph, History: wire.AppendHistory(), ContentType: wire.ContentJSON,
		EntitySchema: &wire.EntitySchema{
			Nodes: []wire.NodeExtract{
				{Label: "Service", ValuePointer: "/service"},
				{Label: "Component", ValuePointer: "/component"},
			},
			Edges: []wire.EdgeExtract{{EdgeType: "depends_on", FromPointer: "/service", ToPointer: "/component"}},
		},
	}
	if err := client.Projections().RegisterGraph(ctx, projection); err != nil {
		return fmt.Errorf("register the graph: %w", err)
	}
	deadline := time.Now().Add(examples.ProjectorTimeout)
	for {
		registered, err := client.Projections().Get(ctx, string(id))
		if err != nil && !laser.IsNotFound(err) {
			return fmt.Errorf("read the graph projection: %w", err)
		}
		if registered != nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("graph `%s` was never registered", graphName)
		}
		if err := wait(ctx, examples.ProjectionPoll); err != nil {
			return err
		}
	}
}

// waitForRecall polls the read view until it returns expected items or the deadline passes.
func waitForRecall(ctx context.Context, memory *laser.Memory, expected int) ([]laser.MemoryItem, error) {
	deadline := time.Now().Add(examples.ProjectorTimeout)
	for {
		hits, err := memory.Recall(ctx, laser.MemoryQuery{Limit: expected})
		if err != nil {
			return nil, fmt.Errorf("recall the durable facts: %w", err)
		}
		if len(hits) >= expected || time.Now().After(deadline) {
			return hits, nil
		}
		if err := wait(ctx, examples.ProjectionPoll); err != nil {
			return nil, err
		}
	}
}

// deterministicEmbedder hashes words into a fixed vector, so recall ranks the same on every run.
type deterministicEmbedder struct{}

func (deterministicEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	dimensions := make([]float64, embeddingSize)
	for _, word := range strings.Fields(strings.ToLower(text)) {
		token := strings.TrimFunc(word, func(r rune) bool { return (r < '0' || r > '9') && (r < 'a' || r > 'z') })
		if token == "" {
			continue
		}
		var hash uint32
		for _, b := range []byte(token) {
			hash = hash*31 + uint32(b)
		}
		dimensions[hash%embeddingSize]++
	}
	var sum float64
	for _, value := range dimensions {
		sum += value * value
	}
	norm := math.Sqrt(sum)
	if norm == 0 {
		norm = 1
	}
	embedding := make([]float32, embeddingSize)
	for index, value := range dimensions {
		embedding[index] = float32(value / norm)
	}
	return embedding, nil
}

func printHits(label string, hits []laser.MemoryItem) {
	fmt.Println(label)
	for index, hit := range hits {
		fmt.Printf("  %d. (%.3f) %s\n", index+1, hit.Score, hit.Text())
	}
}

func printNodes(label string, nodes []wire.GraphNode) {
	fmt.Printf("%s: %s\n", label, strings.Join(values(nodes), ", "))
}

// values are the sorted entity values of nodes.
func values(nodes []wire.GraphNode) []string {
	found := make([]string, 0, len(nodes))
	for _, node := range nodes {
		value := "?"
		for _, attr := range node.Attrs {
			if attr.Key == "value" && attr.Value.Str != "" {
				value = attr.Value.Str
			}
		}
		found = append(found, value)
	}
	sort.Strings(found)
	return found
}

func labelled(nodes []wire.GraphNode, label string) []wire.GraphNode {
	return slices.DeleteFunc(slices.Clone(nodes), func(node wire.GraphNode) bool {
		return !slices.Contains(node.Labels, label)
	})
}

func except(nodes []wire.GraphNode, id wire.NodeID) []wire.GraphNode {
	return slices.DeleteFunc(slices.Clone(nodes), func(node wire.GraphNode) bool { return node.ID == id })
}

func wait(ctx context.Context, delay time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}
