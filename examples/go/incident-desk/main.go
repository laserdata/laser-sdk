// A support desk over the log: a ticket firehose materialized into a queryable
// index, four agents that triage, diagnose, approve and resolve, capacity
// grants applied effectively once, a speculative plan in a fork, and the
// incident rebuilt from its records alone.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example        = "incident-desk"
	ticketsTopic   = "support_tickets"
	plan           = "bulk-resolve-plan"
	embeddingSize  = 64
	requestTimeout = 15 * time.Second
	settleTimeout  = time.Minute
)

var (
	angles = []string{"most likely root cause", "fastest mitigation", "blast radius"}
	notes  = []string{
		"auth latency usually traces to database pool exhaustion",
		"config push retries require an idempotency key",
		"critical auth incidents recover by failing over the read replica",
	}
	grants = []grant{
		{Key: "gr-1", Cluster: "east-1", Units: 150},
		{Key: "gr-2", Cluster: "west-2", Units: 50},
		{Key: "gr-3", Cluster: "eu-1", Units: 80},
	}
)

type ticket struct {
	TicketID    string `json:"ticket_id"`
	MessageType string `json:"message_type"`
	Cluster     string `json:"cluster"`
	Component   string `json:"component"`
	Severity    string `json:"severity"`
	Status      string `json:"status"`
	TS          uint64 `json:"ts"`
}

type grant struct {
	Key     string `json:"key"`
	Cluster string `json:"cluster"`
	Units   int    `json:"units"`
}

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) (err error) {
	examples.Phase("warming up")
	if err := client.Bootstrap(ctx, examples.Partitions, laser.ExpireAfter(examples.SessionRetention)); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	if err := client.Topic(ticketsTopic).Ensure(ctx, examples.Partitions, nil); err != nil {
		return fmt.Errorf("ensure the ticket topic: %w", err)
	}
	capabilities := client.Capabilities()
	if !examples.ManagedGate(capabilities, examples.FeatureQuery, example) ||
		!examples.ManagedGate(capabilities, examples.FeatureKvCas, example) ||
		!examples.ManagedGate(capabilities, examples.FeatureForks, example) {
		return nil
	}
	// The index carries this run's token, so a rerun or another language's desk on
	// the same deployment never shares its rows.
	index := examples.IndexFor(ticketsTopic)

	examples.Phase("registering the ticket index")
	projection := examples.ViewProjection(index, wire.ContentJSON, false,
		"ticket_id", "message_type", "cluster", "component", "severity", "status", "ts")
	if err := examples.RegisterView(ctx, client, ticketsTopic, projection, wire.InvalidRecordBlock); err != nil {
		return err
	}
	examples.Phase("ingesting the ticket firehose")
	if err := ingest(ctx, client, index, examples.Messages(2_000)); err != nil {
		return err
	}

	examples.Phase("seeding semantic memory with past resolutions")
	memory := laser.NewVectorMemory(simpleEmbedder{})
	for _, note := range notes {
		if _, err := memory.Remember(ctx, laser.MemoryScope{}, laser.MemoryFact, []byte(note)); err != nil {
			return fmt.Errorf("remember a past resolution: %w", err)
		}
	}
	runID := laser.NewConversationID().String()
	desk := &desk{
		client: client, memory: memory, llm: examples.DefaultLLM(),
		grants: client.Kv("desk-grants-" + runID), seen: client.Kv("desk-dedup-" + runID),
	}
	ids, err := examples.AgentIDs("triage", "specialist", "resolver", "approver", "orchestrator")
	if err != nil {
		return err
	}
	desk.triage, desk.specialist, desk.resolver, desk.approver, desk.orchestrator = ids[0], ids[1], ids[2], ids[3], ids[4]

	examples.Phase("spawning the desk: triage, specialist, resolver, approver")
	agents, err := desk.spawn(ctx)
	defer func() {
		for _, handle := range slices.Backward(agents) {
			err = errors.Join(err, handle.Shutdown(context.WithoutCancel(ctx)))
		}
	}()
	if err != nil {
		return err
	}

	examples.Phase("triaging the incident through the desk")
	incident := laser.NewConversationID()
	diagnosis, err := client.Contract(laser.To(desk.triage), wire.AgentSessions).
		From(desk.orchestrator).
		Conversation(incident).
		Payload([]byte("auth is slow for several clusters")).
		InboxRoute(laser.FixedInbox(wire.AgentSessions)).
		Deadline(time.Minute).
		Send(ctx)
	if err != nil {
		return fmt.Errorf("triage contract: %w", err)
	}
	if diagnosis.State != laser.ContractCompleted {
		return fmt.Errorf("diagnosis ended as %s", diagnosis.State)
	}
	text := string(diagnosis.Reply.Envelope.Body)
	if _, err := memory.Remember(ctx, laser.MemoryScope{}, laser.MemorySummary, []byte(text)); err != nil {
		return fmt.Errorf("remember the diagnosis: %w", err)
	}
	fmt.Printf("diagnosis: %s\n", text)

	examples.Phase("executing capacity grants effectively once")
	if err := desk.applyGrants(ctx, incident); err != nil {
		return err
	}
	fmt.Println("duplicate deliveries produced exact cluster grant totals")

	examples.Phase("speculating a bulk-resolve plan in a fork")
	fork := client.Fork(plan)
	if _, err := fork.Create().Tables(index).Send(ctx); err != nil {
		return fmt.Errorf("create the fork: %w", err)
	}
	if err := fork.PutRow(index, 0, 0).Field("status", "resolved").Send(ctx); err != nil {
		return fmt.Errorf("stage the resolution in the fork: %w", err)
	}
	if examples.EnvBool("LASER_APPLY_PLAN", false) {
		if _, err := fork.Promote(ctx); err != nil {
			return fmt.Errorf("promote the fork: %w", err)
		}
	}
	fmt.Printf("speculative fork: %s\n", plan)

	examples.Phase("rebuilding the incident from the log alone")
	rebuilt, err := laser.LoadConversationState(ctx, client, incident, []string{wire.AgentSessions},
		laser.ReplayBound{Kind: laser.ReplayFull}, 0, func(total int, _ laser.ContextMessage) int { return total + 1 })
	if err != nil {
		return fmt.Errorf("rebuild the incident: %w", err)
	}
	if rebuilt == 0 {
		return errors.New("conversation state rebuild returned no messages")
	}
	fmt.Printf("audit records rebuilt: %d\n", rebuilt)
	examples.Phase("done")
	fmt.Println("the desk resolved the incident with a replayable audit trail")
	return nil
}

// ingest publishes the ticket feed in batches, waits for the index and reads one payload back.
func ingest(ctx context.Context, client *laser.Laser, index string, count int) error {
	feed := tickets(count)
	for chunk := range slices.Chunk(feed, examples.BatchSize(200)) {
		bodies := make([]any, 0, len(chunk))
		for _, ticket := range chunk {
			bodies = append(bodies, ticket)
		}
		if _, err := client.Topic(ticketsTopic).PublishBatch().InlinePayload().ExtendJSON(bodies...).Send(ctx); err != nil {
			return fmt.Errorf("publish tickets: %w", err)
		}
	}
	if _, err := examples.WaitForProjection(ctx, client, index, uint64(count)); err != nil {
		return err
	}
	var payload ticket
	found, err := client.Query(index).FetchOne(ctx, wire.JSONCodec, &payload)
	if err != nil {
		return fmt.Errorf("read a ticket payload: %w", err)
	}
	if !found {
		return errors.New("materialized tickets returned no payload")
	}
	fmt.Printf("ticket payload round trip: %s/%s\n", payload.TicketID, payload.Component)
	return nil
}

// tickets is the feed every language's desk publishes from the same seed.
func tickets(count int) []ticket {
	rng := examples.NewRng(0xc0ffee42)
	clusters := []string{"east-1", "west-2", "eu-1", "ap-1", "lab"}
	components := []string{"auth", "config", "storage", "metrics", "gateway"}
	severities := []string{"low", "medium", "high", "critical"}
	feed := make([]ticket, 0, count)
	for index := range count {
		feed = append(feed, ticket{
			TicketID:    fmt.Sprintf("ticket-%07d", index),
			MessageType: "ticket",
			Cluster:     examples.Pick(rng, clusters),
			Component:   examples.Pick(rng, components),
			Severity:    examples.Pick(rng, severities),
			Status:      "open",
			TS:          1_900_000_000_000_000 + uint64(index),
		})
	}
	return feed
}

type desk struct {
	client                                               *laser.Laser
	memory                                               *laser.VectorMemory
	llm                                                  examples.LLM
	grants, seen                                         *laser.Kv
	triage, specialist, resolver, approver, orchestrator wire.AgentID
}

func (desk *desk) spawn(ctx context.Context) ([]*laser.AgentHandle, error) {
	answering := laser.RuntimeOptions{Topic: wire.AgentSessions, RespondOn: wire.AgentSessions}
	workers := []struct {
		id      wire.AgentID
		runtime laser.RuntimeOptions
		dedup   laser.DedupStore
		handle  laser.HandlerFunc
	}{
		{desk.triage, answering, nil, desk.triageIncident},
		{desk.specialist, answering, nil, desk.investigate},
		{desk.resolver, laser.RuntimeOptions{Topic: wire.AgentSessions}, kvDeduplicator{seen: desk.seen}, desk.applyGrant},
		// The approver answers only the approval requests addressed to it. Every
		// desk agent reads the shared session topic, so an unaddressed prompt
		// would be work for all of them.
		{desk.approver, answering, nil, func(ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx) error {
			return agent.Respond(ctx, []byte("approved"))
		}},
	}
	handles := make([]*laser.AgentHandle, 0, len(workers))
	for _, worker := range workers {
		handle, err := desk.client.SpawnAgent(ctx, worker.id, laser.SpawnOptions{
			AgentOptions: laser.AgentOptions{Dedup: worker.dedup}, RuntimeOptions: worker.runtime,
		}, worker.handle)
		if err != nil {
			return handles, fmt.Errorf("spawn %s: %w", worker.id, err)
		}
		handles = append(handles, handle)
	}
	for _, handle := range handles {
		if err := handle.Ready(ctx); err != nil {
			return handles, fmt.Errorf("%s ready: %w", handle.Identity(), err)
		}
	}
	return handles, nil
}

// triageIncident asks the specialist about each angle and answers with the model's summary.
func (desk *desk) triageIncident(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
	incident := string(message.Body())
	findings := make([]string, 0, len(angles))
	for _, angle := range angles {
		finding, err := ask(ctx, agent, desk.specialist, angle+": "+incident)
		if err != nil {
			return fmt.Errorf("ask the specialist: %w", err)
		}
		findings = append(findings, finding)
	}
	summary, err := desk.llm.Complete(ctx, incident+"\n"+strings.Join(findings, "\n"))
	if err != nil {
		return err
	}
	return agent.Respond(ctx, []byte(summary))
}

// investigate answers one angle with what memory recalls about it.
func (desk *desk) investigate(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
	prompt := string(message.Body())
	recalled, err := desk.memory.Recall(ctx, laser.MemoryScope{},
		laser.MemoryQuery{Query: prompt, Strategy: laser.RecallSemantic, Limit: 2})
	if err != nil {
		return fmt.Errorf("recall past resolutions: %w", err)
	}
	lines := []string{prompt}
	for _, item := range recalled {
		lines = append(lines, item.Text())
	}
	answer, err := desk.llm.Complete(ctx, strings.Join(lines, "\n"))
	if err != nil {
		return err
	}
	return agent.Respond(ctx, []byte(answer))
}

// applyGrant adds one capacity grant to its cluster total. The resolver's deduplicator drops a repeated key before it.
func (desk *desk) applyGrant(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
	var request grant
	if err := json.Unmarshal(message.Body(), &request); err != nil {
		return fmt.Errorf("decode the grant: %w", err)
	}
	if request.Units >= 100 {
		prompt := fmt.Sprintf("approve a %d unit capacity grant to %s?", request.Units, request.Cluster)
		decision, err := ask(ctx, agent, desk.approver, prompt)
		if err != nil {
			return fmt.Errorf("ask the approver: %w", err)
		}
		if decision != "approved" {
			return nil
		}
	}
	current, err := desk.total(ctx, request.Cluster)
	if err != nil {
		return err
	}
	if err := desk.grants.Set([]byte(request.Cluster)).Bytes([]byte(strconv.Itoa(current + request.Units))).Send(ctx); err != nil {
		return fmt.Errorf("write the grant total of %s: %w", request.Cluster, err)
	}
	return nil
}

// ask sends one agent a question in a subconversation of the handled one and returns its answer.
func ask(ctx context.Context, agent *laser.AgentCtx, target wire.AgentID, prompt string) (string, error) {
	provenance, err := agent.SpawnSubconversation()
	if err != nil {
		return "", err
	}
	reply, err := agent.Request(ctx, wire.AgentSessions, []byte(prompt), laser.RequestOptions{
		Target: target, ReplyTopic: wire.AgentSessions, Provenance: provenance, Timeout: requestTimeout,
	})
	if err != nil {
		return "", err
	}
	return string(reply.Envelope.Body), nil
}

// kvDeduplicator keeps handled keys in managed KV, so a redelivery after a restart is still skipped.
type kvDeduplicator struct{ seen *laser.Kv }

// Observe records the key and reports whether it is new. A store failure processes the record.
func (dedup kvDeduplicator) Observe(ctx context.Context, key string) bool {
	_, err := dedup.seen.Set([]byte(key)).Bytes([]byte{1}).ExpectAbsent().TTL(time.Hour).Commit(ctx)
	return !laser.IsVersionConflict(err)
}

// applyGrants sends every grant twice and waits until each cluster holds its exact total.
func (desk *desk) applyGrants(ctx context.Context, incident wire.ConversationID) error {
	for _, request := range slices.Concat(grants, grants) {
		body, err := json.Marshal(request)
		if err != nil {
			return err
		}
		key := request.Key
		_, err = desk.client.SendAgent(ctx, wire.AgentSessions, body, &laser.Provenance{
			Conversation: incident, Agent: &desk.orchestrator, TargetAgent: &desk.resolver, IdempotencyKey: &key,
		})
		if err != nil {
			return fmt.Errorf("send grant %s: %w", request.Key, err)
		}
	}
	deadline := time.Now().Add(settleTimeout)
	for {
		settled := true
		for _, request := range grants {
			actual, err := desk.total(ctx, request.Cluster)
			if err != nil {
				return err
			}
			if actual == request.Units {
				continue
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("grant total for %s is %d", request.Cluster, actual)
			}
			settled = false
		}
		if settled {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (desk *desk) total(ctx context.Context, cluster string) (int, error) {
	value, found, err := desk.grants.Get(ctx, []byte(cluster))
	if err != nil {
		return 0, fmt.Errorf("read the grant total of %s: %w", cluster, err)
	}
	if !found {
		return 0, nil
	}
	total, err := strconv.Atoi(string(value))
	if err != nil {
		return 0, fmt.Errorf("grant total of %s is not a number: %w", cluster, err)
	}
	return total, nil
}

// simpleEmbedder counts bytes into a fixed vector, enough to rank the seeded notes.
type simpleEmbedder struct{}

func (simpleEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	values := make([]float32, embeddingSize)
	for _, b := range []byte(strings.ToLower(text)) {
		values[int(b)%embeddingSize]++
	}
	return values, nil
}
