package bdd_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// allPartitions is every partition a topic currently holds, so a standalone
// consumer reads the whole topic rather than one hard-coded slice: the
// session lane runs four partitions in the shared background and one under
// the single-partition layout, and a responder spawned in either must see
// whichever its scenario actually bootstrapped.
func allPartitions(ctx context.Context, state *world, topic string) ([]uint32, error) {
	info, err := state.topic(topic).Get(ctx)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("topic %q does not exist", topic)
	}
	partitions := make([]uint32, info.PartitionsCount)
	for index := range partitions {
		partitions[index] = uint32(index)
	}
	return partitions, nil
}

// recordingAgent records the body of every command dispatched to it, for the
// scenarios proving what does and does not reach an agent as work.
type recordingAgent struct {
	mutex sync.Mutex
	seen  []string
}

func (agent *recordingAgent) handle(
	_ context.Context, message *laser.AgentMessage, _ *laser.AgentCtx,
) error {
	agent.mutex.Lock()
	agent.seen = append(agent.seen, string(message.Body()))
	agent.mutex.Unlock()
	return nil
}

func (agent *recordingAgent) snapshot() []string {
	agent.mutex.Lock()
	defer agent.mutex.Unlock()
	return append([]string(nil), agent.seen...)
}

// spawnRecordingAgentPair spawns both names of "agents X and Y each record
// the commands they handle", each serving every operation.
func spawnRecordingAgentPair(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want two agents, got %v", arguments)
	}
	for _, name := range arguments {
		if err := spawnRecordingAgent(ctx, state, name); err != nil {
			return err
		}
	}
	return nil
}

func spawnRecordingAgent(ctx context.Context, state *world, name string) error {
	identity, err := bridgeAgent(name)
	if err != nil {
		return err
	}
	gate := state.client.Agent(identity, laser.AgentOptions{})
	runtime, err := state.client.AgentRuntime(gate, laser.RuntimeOptions{Topic: wire.AgentSessions})
	if err != nil {
		return err
	}
	partitions, err := allPartitions(ctx, state, wire.AgentSessions)
	if err != nil {
		return err
	}
	source, err := state.topic(wire.AgentSessions).Consumer(
		ctx, fmt.Sprintf("bdd-recording-%s-%d", name, time.Now().UnixNano()),
		laser.ConsumerOptions{Partitions: partitions},
	)
	if err != nil {
		return err
	}
	agent := &recordingAgent{}
	runAgentRuntime(state, runtime, source, laser.HandlerFunc(agent.handle), name)
	state.put("recording:"+name, agent)
	return nil
}

func checkHandledExactly(state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want an agent, got %v", arguments)
	}
	agentAny, found := state.get("recording:" + arguments[0])
	agent, ok := agentAny.(*recordingAgent)
	if !found || !ok {
		return fmt.Errorf("no recording agent %q was spawned", arguments[0])
	}
	want := arguments[1:]
	// A sibling's records share the partition, so a wrongly delivered one is
	// given a moment to show up rather than checked the instant want is seen.
	deadline := time.Now().Add(5 * time.Second)
	var got []string
	for {
		got = agent.snapshot()
		if equalStrings(got, want) || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	got = agent.snapshot()
	if !equalStrings(got, want) {
		return fmt.Errorf("want %v, got %v", want, got)
	}
	return nil
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range want {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

// endsEverySession answers any work with ending the session it belongs to,
// ignoring what the work actually says: the point of every scenario that
// spawns it is what reaches the lane around the ending, not the ending
// itself.
type endsEverySession struct{}

func (endsEverySession) handle(
	ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx,
) error {
	_, err := agent.Session().Complete(ctx, "")
	return err
}

func endsEverySessionStep(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one agent, got %v", arguments)
	}
	return spawnEndsEverySessionAgent(ctx, state, arguments[0])
}

// spawnEndsEverySessionAgent spawns "agent X ends every session it handles",
// returning the runtime's own control book so a later step can check what it
// folded without needing to be inside a handler to ask.
func spawnEndsEverySessionAgent(ctx context.Context, state *world, name string) error {
	identity, err := bridgeAgent(name)
	if err != nil {
		return err
	}
	gate := state.client.Agent(identity, laser.AgentOptions{})
	runtime, err := state.client.AgentRuntime(gate, laser.RuntimeOptions{Topic: wire.AgentSessions})
	if err != nil {
		return err
	}
	partitions, err := allPartitions(ctx, state, wire.AgentSessions)
	if err != nil {
		return err
	}
	source, err := state.topic(wire.AgentSessions).Consumer(
		ctx, fmt.Sprintf("bdd-ends-%s-%d", name, time.Now().UnixNano()),
		laser.ConsumerOptions{Partitions: partitions},
	)
	if err != nil {
		return err
	}
	runAgentRuntime(state, runtime, source, laser.HandlerFunc(endsEverySession{}.handle), name)
	state.put("control:"+name, runtime.Control())
	return nil
}

// runAgentRuntime starts runtime.Run in the background and registers it to
// stop with the scenario.
//
// The Go client has no AgentHandle.ready() to await, unlike the Rust and
// TypeScript runtimes: a standalone consumer with no stored offset starts
// from the first record either way, so this waits a fixed interval instead,
// covering the time between opening the reader and its first poll actually
// reaching the server.
func runAgentRuntime(
	state *world, runtime *laser.AgentRuntime, source laser.RecordSource,
	handler laser.AgentHandler, name string,
) {
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx, source, handler) }()
	state.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			state.t.Logf("agent %s did not stop", name)
		}
	})
	time.Sleep(responderStarted)
}

// sendWorkToAgent sends a plain command, with no operation, to target in the
// scenario's current session: the shape every "X sends work to agent Y in the
// session" step shares.
func sendWorkToAgent(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want a sender and a target, got %v", arguments)
	}
	sender, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[1])
	if err != nil {
		return err
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
	_, err = state.client.Agdx(sender, session.Conversation()).
		Command(correlation, []byte("work")).
		WithTarget(target).
		Send(ctx)
	return err
}

// misplacedControl writes control-shaped commands on the session lane rather
// than on agent.control, the unauthorized write the scenario expects to go
// nowhere.
func misplacedControl(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 3 {
		return fmt.Errorf("want a sender, operations and a target, got %v", arguments)
	}
	sender, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[len(arguments)-1])
	if err != nil {
		return err
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	for _, operation := range arguments[1 : len(arguments)-1] {
		correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
		if _, err := state.client.Agdx(sender, session.Conversation()).
			Command(correlation, []byte("{}")).
			WithOperation(operation).
			WithTarget(target).
			Send(ctx); err != nil {
			return err
		}
	}
	return nil
}

func checkNoPendingControl(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want an agent, got %v", arguments)
	}
	agent, err := wire.NewAgentID(arguments[0])
	if err != nil {
		return err
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	lens := state.client.SessionsWith(session.Config()).Open(session.Conversation(), "").As(agent)
	pending, err := lens.PendingControl(ctx)
	if err != nil {
		return err
	}
	if pending.PauseRequested || pending.CancelRequested {
		return fmt.Errorf("want no pending control, a misplaced write applied: %+v", pending)
	}
	canceled, err := lens.CancelRequested(ctx)
	if err != nil {
		return err
	}
	if canceled {
		return fmt.Errorf("want no cancel requested, a misplaced write applied")
	}
	return nil
}

// singlePartitionLayoutStream opens a fresh stream under the single-partition
// session layout: the layout the "two agents on one partition" scenario needs
// to prove its addressing works while sharing the exact log the responders
// are standalone consumers of.
func singlePartitionLayoutStream(ctx context.Context, state *world) error {
	if err := state.freshStream(ctx); err != nil {
		return err
	}
	sessions := state.client.SessionsWith(laser.SessionConfig{Layout: laser.SinglePartitionLayout()})
	_, err := sessions.Bootstrap(ctx, 4, laser.ExpireAfter(24*time.Hour))
	return err
}

// sendSeparateSessions sends a command per (body, target) pair, each on its
// own fresh conversation, and records which partition each landed on.
func sendSeparateSessions(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 3 || len(arguments)%2 != 1 {
		return fmt.Errorf("want a sender and body/target pairs, got %v", arguments)
	}
	sender, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	var partitions []uint32
	for index := 1; index+1 < len(arguments); index += 2 {
		body, targetName := arguments[index], arguments[index+1]
		target, err := bridgeAgent(targetName)
		if err != nil {
			return err
		}
		correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
		receipt, err := state.client.Agdx(sender, laser.NewConversationID()).
			Command(correlation, []byte(body)).
			WithTarget(target).
			Send(ctx)
		if err != nil {
			return err
		}
		if receipt.Response == nil || len(receipt.Response.Confirmations) == 0 {
			return fmt.Errorf("the send confirmed no partition for %q", body)
		}
		partitions = append(partitions, receipt.Response.Confirmations[0].PartitionId)
	}
	state.put("sentPartitions", partitions)
	return nil
}

func checkSamePartition(state *world) error {
	partitionsAny, found := state.get("sentPartitions")
	partitions, ok := partitionsAny.([]uint32)
	if !found || !ok || len(partitions) == 0 {
		return fmt.Errorf("no commands were sent")
	}
	first := partitions[0]
	for _, partition := range partitions[1:] {
		if partition != first {
			return fmt.Errorf("want every command on one partition, got %v", partitions)
		}
	}
	return nil
}

// submitSessionTo submits a session to target, the "X submits a session to
// agent Y" step every submission scenario shares.
func submitSessionTo(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want a submitter and a target, got %v", arguments)
	}
	submitter, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[1])
	if err != nil {
		return err
	}
	submitted, err := state.client.Sessions().Submit(target, []byte("{}")).From(submitter).Send(ctx)
	if err != nil {
		return err
	}
	state.put("otherSession", submitted.Session)
	return nil
}

func checkSubmittedReaches(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want a start and an end display, got %v", arguments)
	}
	conversationAny, found := state.get("otherSession")
	conversation, ok := conversationAny.(wire.ConversationID)
	if !found || !ok {
		return fmt.Errorf("no session was submitted")
	}
	deadline := time.Now().Add(20 * time.Second)
	var displays []string
	for {
		var err error
		displays, err = laneDisplaysFor(ctx, state, conversation)
		if err != nil {
			return err
		}
		for _, display := range displays {
			if display != arguments[1] {
				continue
			}
			if len(displays) == 0 || displays[0] != arguments[0] {
				return fmt.Errorf("want the first record %q, got %v", arguments[0], displays)
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want the lane to reach %q, got %v", arguments[1], displays)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// laneDisplaysFor is laneDisplays for a conversation other than the
// scenario's own current session, which a submission reaches without ever
// becoming it.
func laneDisplaysFor(
	ctx context.Context, state *world, conversation wire.ConversationID,
) ([]string, error) {
	records, err := state.replay(ctx, wire.AgentSessions)
	if err != nil {
		return nil, err
	}
	var displays []string
	for _, record := range records {
		message, err := laser.OpenAgdx(record)
		if err != nil || message.Envelope.Conversation != conversation {
			continue
		}
		envelope := message.Envelope
		displays = append(displays, string(wire.DisplayTypeOf(
			wire.EnvelopeRecord(&envelope), wire.AgentSessions)))
	}
	return displays, nil
}

// opensSessionLens opens a session as a lens: no Begin, no lease, no
// heartbeat, as AgentCtx.Session() opens one for a running handler.
func opensSessionLens(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want an agent and a label, got %v", arguments)
	}
	agent, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	conversation := state.client.Sessions().Create(arguments[1]).Conversation()
	session := state.client.Sessions().Open(conversation, arguments[1]).As(agent)
	state.put("session", session)
	state.conversation = conversation
	return nil
}

// heartbeatEvery records the interval a later lease would beat at.
//
// Nothing here takes a lease: it only stores the interval so a scenario that
// does can read it back. The scenario this closes, a session lens never
// starting a heartbeat, takes none either, so storing the number is enough to
// make the Given step honest without claiming to wire an interval the Go
// client applies nowhere yet.
func heartbeatEvery(step bdd.Step, state *world) error {
	counts := numbersOf(step)
	if len(counts) != 1 {
		return fmt.Errorf("want one interval, got %v", counts)
	}
	state.put("heartbeatInterval", time.Duration(counts[0])*time.Millisecond)
	return nil
}

func checkNoHeartbeat(ctx context.Context, state *world, step bdd.Step) error {
	counts := numbersOf(step)
	wait := time.Second
	if len(counts) == 1 {
		wait = time.Duration(counts[0]) * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		records, err := state.replay(ctx, wire.AgentHeartbeats)
		if err != nil {
			return err
		}
		if len(records) > 0 {
			return fmt.Errorf("want no heartbeat, the topic holds %d records", len(records))
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
