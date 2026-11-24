package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// --- refusing publish policy ---

// refusingGovernor blocks the next session-status publish, or the next state
// snapshot publish, once asked to. Both counters start at zero, so nothing is
// refused until a step arms one.
type refusingGovernor struct {
	statusRemaining   atomic.Int32
	snapshotRemaining atomic.Int32
}

// takeOne decrements counter if it holds a token, reporting whether it took
// one. A compare-and-swap loop rather than a plain decrement, so a counter
// armed exactly once never goes negative under a concurrent decider.
func takeOne(counter *atomic.Int32) bool {
	for {
		current := counter.Load()
		if current <= 0 {
			return false
		}
		if counter.CompareAndSwap(current, current-1) {
			return true
		}
	}
}

func (governor *refusingGovernor) Decide(
	_ context.Context, action *laser.GovernedAction,
) (*laser.ActionDecision, error) {
	if action.Kind == laser.ActionStatus && action.Operation == wire.OperationSession &&
		takeOne(&governor.statusRemaining) {
		return laser.Block("injected publish refusal"), nil
	}
	if action.Operation == wire.OperationStateSnapshot && takeOne(&governor.snapshotRemaining) {
		return laser.Block("injected publish refusal"), nil
	}
	return laser.Allow(), nil
}

func installRefusingPolicy(state *world) error {
	governor := &refusingGovernor{}
	state.client = state.client.WithGovernor(governor, laser.GovernorEnforce)
	state.put("refusingGovernor", governor)
	return nil
}

func refusingPolicy(state *world) (*refusingGovernor, error) {
	governorAny, found := state.get("refusingGovernor")
	governor, ok := governorAny.(*refusingGovernor)
	if !found || !ok {
		return nil, fmt.Errorf("no refusing policy installed")
	}
	return governor, nil
}

func refuseNextStatusPublish(state *world) error {
	governor, err := refusingPolicy(state)
	if err != nil {
		return err
	}
	governor.statusRemaining.Store(1)
	return nil
}

func refuseNextSnapshotPublish(state *world) error {
	governor, err := refusingPolicy(state)
	if err != nil {
		return err
	}
	governor.snapshotRemaining.Store(1)
	return nil
}

func checkRejectedByPolicy(state *world) error {
	var refused *laser.PolicyBlockedError
	if !errors.As(state.failure, &refused) {
		return fmt.Errorf("want a policy refusal, got %v", state.failure)
	}
	return nil
}

// endSessionCaptured is "I end the session": unlike endSession, the outcome
// is stored rather than returned as a step failure, since the point of the
// scenarios that use it is reading what a refused publish did, not that the
// call succeeded.
func endSessionCaptured(ctx context.Context, state *world) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	_, state.failure = session.Complete(ctx, "")
	if state.failure == nil {
		ended, _ := state.get("ended")
		count, _ := ended.(int)
		state.put("ended", count+1)
	}
	return nil
}

// writeStateSnapshotCaptured is "I write a state snapshot", captured the same
// way: a refused snapshot is what the scenario is proving happens, not a
// runner error.
func writeStateSnapshotCaptured(ctx context.Context, state *world) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	_, state.failure = session.State().Snapshot(ctx)
	return nil
}

// checkOneTerminalRecordID proves a terminal verb retried after a refused
// publish rewrites the same record rather than appending a second one: every
// terminal record on the lane must share one record id.
func checkOneTerminalRecordID(ctx context.Context, state *world) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		records, err := state.replay(ctx, wire.AgentSessions)
		if err != nil {
			return err
		}
		ids := map[string]bool{}
		count := 0
		for _, record := range records {
			message, openErr := laser.OpenAgdx(record)
			if openErr != nil || message.Envelope.Conversation != session.Conversation() {
				continue
			}
			envelope := message.Envelope
			display := string(wire.DisplayTypeOf(wire.EnvelopeRecord(&envelope), wire.AgentSessions))
			if !terminalDisplay(display) {
				continue
			}
			count++
			if envelope.Record != nil {
				ids[envelope.Record.String()] = true
			}
		}
		if count > 0 {
			if len(ids) != 1 {
				return fmt.Errorf("want one terminal record id across %d terminal records, got %d",
					count, len(ids))
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no terminal record appeared")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// --- declared layouts ---

func declaredPartitionLayoutStep(step bdd.Step, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want two agents, got %v", arguments)
	}
	counts := numbersOf(step)
	if len(counts) != 2 {
		return fmt.Errorf("want two partitions, got %v", counts)
	}
	first, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	second, err := bridgeAgent(arguments[1])
	if err != nil {
		return err
	}
	layout := laser.PerAgentPartitionLayout(map[wire.AgentID]uint32{
		first: uint32(counts[0]), second: uint32(counts[1]),
	})
	state.client.SessionsWith(laser.SessionConfig{Layout: layout})
	return nil
}

func declaredTopicLayoutStep(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 4 {
		return fmt.Errorf("want two agent/topic pairs, got %v", arguments)
	}
	first, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	second, err := bridgeAgent(arguments[2])
	if err != nil {
		return err
	}
	layout := laser.PerAgentTopicLayout(map[wire.AgentID]string{
		first: arguments[1], second: arguments[3],
	})
	sessions := state.client.SessionsWith(laser.SessionConfig{Layout: layout})
	_, err = sessions.Bootstrap(ctx, 1, laser.ExpireAfter(24*time.Hour))
	return err
}

// routedExchange sends a command addressed to target and a reply addressed
// back to requester, both on a fresh conversation, raw rather than through a
// running responder: the point is where the layout routes the send, not that
// a handler picked the work up.
func routedExchange(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 3 {
		return fmt.Errorf("want a requester, a target and a responder, got %v", arguments)
	}
	requester, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[1])
	if err != nil {
		return err
	}
	session := laser.NewConversationID()
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
	command, err := state.client.Agdx(requester, session).
		Command(correlation, []byte("{}")).
		WithTarget(target).
		Send(ctx)
	if err != nil {
		return err
	}
	reply, err := state.client.Agdx(target, session).
		Respond(correlation, []byte("{}")).
		WithTarget(requester).
		Send(ctx)
	if err != nil {
		return err
	}
	state.put("routedCommand", command)
	state.put("routedReply", reply)
	state.conversation = session
	return nil
}

func routedPartition(state *world, key string) (uint32, error) {
	receiptAny, found := state.get(key)
	receipt, ok := receiptAny.(*laser.AgdxReceipt)
	if !found || !ok || receipt.Response == nil || len(receipt.Response.Confirmations) == 0 {
		return 0, fmt.Errorf("no routed exchange ran")
	}
	return receipt.Response.Confirmations[0].PartitionId, nil
}

func checkRoutedPartitions(state *world, step bdd.Step) error {
	counts := numbersOf(step)
	if len(counts) != 2 {
		return fmt.Errorf("want two partitions, got %v", counts)
	}
	commandPartition, err := routedPartition(state, "routedCommand")
	if err != nil {
		return err
	}
	replyPartition, err := routedPartition(state, "routedReply")
	if err != nil {
		return err
	}
	if uint64(commandPartition) != counts[0] || uint64(replyPartition) != counts[1] {
		return fmt.Errorf("want the command on %d and the reply on %d, got %d and %d",
			counts[0], counts[1], commandPartition, replyPartition)
	}
	return nil
}

func recordsOnTopic(
	ctx context.Context, state *world, topic string, conversation wire.ConversationID,
) ([]wire.AgentEnvelope, error) {
	cursor, err := state.topic(topic).Replay()
	if err != nil {
		return nil, err
	}
	records, err := cursor.Poll(ctx)
	if err != nil {
		return nil, err
	}
	var envelopes []wire.AgentEnvelope
	for _, record := range records {
		message, openErr := laser.OpenAgdx(record)
		if openErr != nil || message.Envelope.Conversation != conversation {
			continue
		}
		envelopes = append(envelopes, message.Envelope)
	}
	return envelopes, nil
}

func checkTopicHoldsCommand(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want two topics, got %v", arguments)
	}
	commandEnvelopes, err := recordsOnTopic(ctx, state, arguments[0], state.conversation)
	if err != nil {
		return err
	}
	hasCommand := false
	for _, envelope := range commandEnvelopes {
		if envelope.Kind == wire.KindCommand {
			hasCommand = true
		}
	}
	if !hasCommand {
		return fmt.Errorf("the topic %q holds no command", arguments[0])
	}
	replyEnvelopes, err := recordsOnTopic(ctx, state, arguments[1], state.conversation)
	if err != nil {
		return err
	}
	hasReply := false
	for _, envelope := range replyEnvelopes {
		if envelope.Kind == wire.KindResponse {
			hasReply = true
		}
	}
	if !hasReply {
		return fmt.Errorf("the topic %q holds no reply", arguments[1])
	}
	return nil
}

func checkSessionLaneHoldsNeither(ctx context.Context, state *world) error {
	envelopes, err := recordsOnTopic(ctx, state, wire.AgentSessions, state.conversation)
	if err != nil {
		return err
	}
	if len(envelopes) > 0 {
		return fmt.Errorf("want the session lane to hold neither, it holds %d records", len(envelopes))
	}
	return nil
}

// --- a second handle on one session ---

func secondHandleOf(state *world) (*laser.Session, error) {
	value, found := state.get("secondHandle")
	if !found {
		return nil, fmt.Errorf("no second handle was opened")
	}
	return value.(*laser.Session), nil
}

func openSecondHandle(state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want an agent, got %v", arguments)
	}
	agent, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	second := state.client.Sessions().Open(session.Conversation(), "").As(agent)
	state.put("secondHandle", second)
	return nil
}

func secondStateSetStep(ctx context.Context, state *world, text string, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a key, got %v", arguments)
	}
	second, err := secondHandleOf(state)
	if err != nil {
		return err
	}
	return setStateOn(ctx, second, text, arguments)
}

// jsonAfter is the JSON blob a step carries after prefix, for the "replaced
// with {...}" steps: the document is not quoted, so argumentsOf's quoted-only
// extraction cannot read it.
func jsonAfter(text, prefix string) (string, error) {
	index := strings.Index(text, prefix)
	if index < 0 {
		return "", fmt.Errorf("want %q in %q", prefix, text)
	}
	return strings.TrimSpace(text[index+len(prefix):]), nil
}

func stateReplace(ctx context.Context, state *world, text string) error {
	raw, err := jsonAfter(text, "replaced with ")
	if err != nil {
		return err
	}
	var document wire.DynValue
	if err := wire.UnmarshalJSON([]byte(raw), &document); err != nil {
		return err
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	_, err = session.State().Replace(ctx, document)
	return err
}

func secondStateReplace(ctx context.Context, state *world, text string) error {
	raw, err := jsonAfter(text, "replaced with ")
	if err != nil {
		return err
	}
	var document wire.DynValue
	if err := wire.UnmarshalJSON([]byte(raw), &document); err != nil {
		return err
	}
	second, err := secondHandleOf(state)
	if err != nil {
		return err
	}
	_, err = second.State().Replace(ctx, document)
	return err
}

func secondHandleEnds(ctx context.Context, state *world) error {
	second, err := secondHandleOf(state)
	if err != nil {
		return err
	}
	_, err = second.Complete(ctx, "")
	return err
}

func secondHandleSnapshot(ctx context.Context, state *world) error {
	second, err := secondHandleOf(state)
	if err != nil {
		return err
	}
	_, snapshotErr := second.State().Snapshot(ctx)
	state.put("secondSnapshotErr", snapshotErr)
	return nil
}

func checkSecondSnapshotStale(state *world) error {
	errAny, found := state.get("secondSnapshotErr")
	if !found {
		return fmt.Errorf("the second handle never wrote a snapshot")
	}
	snapshotErr, _ := errAny.(error)
	var stale *laser.SessionError
	if !errors.As(snapshotErr, &stale) || stale.Failure.Kind != wire.SessionStale {
		return fmt.Errorf("want a stale session refusal, got %v", snapshotErr)
	}
	return nil
}

// foldedDocumentIs is "the folded session state is {...} at revision N": a
// whole-document comparison, unlike foldedState's field-by-field check.
func foldedDocumentIs(ctx context.Context, state *world, step bdd.Step) error {
	text := step.Text
	const prefix = "the folded session state is "
	const suffix = " at revision "
	start := strings.Index(text, prefix)
	end := strings.LastIndex(text, suffix)
	if start < 0 || end < 0 || end <= start {
		return fmt.Errorf("want %q then %q in %q", prefix, suffix, text)
	}
	var expected wire.DynValue
	if err := wire.UnmarshalJSON([]byte(text[start+len(prefix):end]), &expected); err != nil {
		return err
	}
	counts := numbersOf(step)
	if len(counts) != 1 {
		return fmt.Errorf("want one revision, got %v", counts)
	}
	revision := counts[0]
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		view, err := session.State().Get(ctx)
		if err == nil && view.Revision == revision {
			if !view.Document.Equal(expected) {
				return fmt.Errorf("the folded document does not match %s", text[start+len(prefix):end])
			}
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("want revision %d, got %d", revision, view.Revision)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// --- heartbeats and leases ---

// startLeasedSession starts a labeled session and takes an explicit lease on
// it: the Go client splits what Rust's begin() bundles, since Session.Begin
// writes no lease of its own.
func startLeasedSession(
	ctx context.Context, client *laser.Laser, sessions *laser.Sessions,
	interval time.Duration, owner, label string,
) (wire.ConversationID, *laser.SessionLease, error) {
	agentID, err := bridgeAgent(owner)
	if err != nil {
		return wire.ConversationID{}, nil, err
	}
	session := sessions.Create(label).As(agentID)
	if _, err := session.Begin(ctx); err != nil {
		return wire.ConversationID{}, nil, err
	}
	conversation := session.Conversation()
	lease, err := client.LeaseSession(ctx, conversation, agentID, interval)
	if err != nil {
		return wire.ConversationID{}, nil, err
	}
	return conversation, lease, nil
}

func configuredHeartbeatInterval(state *world) time.Duration {
	intervalAny, _ := state.get("heartbeatInterval")
	interval, _ := intervalAny.(time.Duration)
	return interval
}

func startTwoSessions(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 3 {
		return fmt.Errorf("want an agent and two labels, got %v", arguments)
	}
	interval := configuredHeartbeatInterval(state)
	labels := map[string]wire.ConversationID{}
	leases := map[string]*laser.SessionLease{}
	for _, label := range arguments[1:] {
		conversation, lease, err := startLeasedSession(
			ctx, state.client, state.client.Sessions(), interval, arguments[0], label)
		if err != nil {
			return err
		}
		labels[label] = conversation
		leases[label] = lease
	}
	state.put("sessionLabels", labels)
	state.put("sessionLeases", leases)
	return nil
}

func readHeartbeats(ctx context.Context, state *world, stream string) ([]wire.SessionHeartbeat, error) {
	cursor, err := state.client.Stream(stream).Topic(wire.AgentHeartbeats).Replay()
	if err != nil {
		return nil, err
	}
	records, err := cursor.Poll(ctx)
	if err != nil {
		return nil, err
	}
	var beats []wire.SessionHeartbeat
	for _, record := range records {
		message, err := laser.OpenAgdx(record)
		if err != nil {
			continue
		}
		var beat wire.SessionHeartbeat
		if err := wire.Unmarshal(message.Envelope.Body, &beat); err != nil {
			continue
		}
		beats = append(beats, beat)
	}
	return beats, nil
}

func sessionSet(sessions []wire.ConversationID) map[string]bool {
	set := make(map[string]bool, len(sessions))
	for _, session := range sessions {
		set[session.String()] = true
	}
	return set
}

func sameSessionSet(sessions []wire.ConversationID, expected map[string]bool) bool {
	got := sessionSet(sessions)
	if len(got) != len(expected) {
		return false
	}
	for session := range expected {
		if !got[session] {
			return false
		}
	}
	return true
}

func checkHeartbeatLists(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want session labels, got %v", arguments)
	}
	labelsAny, found := state.get("sessionLabels")
	labels, ok := labelsAny.(map[string]wire.ConversationID)
	if !found || !ok {
		return fmt.Errorf("no leased sessions were started")
	}
	expected := map[string]bool{}
	for _, label := range arguments {
		conversation, has := labels[label]
		if !has {
			return fmt.Errorf("no started session %q", label)
		}
		expected[conversation.String()] = true
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		beats, err := readHeartbeats(ctx, state, state.stream)
		if err != nil {
			return err
		}
		for _, beat := range beats {
			if beat.Stream == state.stream && sameSessionSet(beat.Sessions, expected) {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no heartbeat listed exactly %v", arguments)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func releaseLease(state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want a label, got %v", arguments)
	}
	leasesAny, found := state.get("sessionLeases")
	leases, ok := leasesAny.(map[string]*laser.SessionLease)
	if !found || !ok {
		return fmt.Errorf("no leased sessions were started")
	}
	lease, has := leases[arguments[0]]
	if !has {
		return fmt.Errorf("no leased session %q", arguments[0])
	}
	lease.Release()
	delete(leases, arguments[0])
	return nil
}

func checkHeartbeatsStop(ctx context.Context, state *world) error {
	// A beat already in flight when the last lease dropped may still land.
	time.Sleep(500 * time.Millisecond)
	beats, err := readHeartbeats(ctx, state, state.stream)
	if err != nil {
		return err
	}
	settled := len(beats)
	if settled == 0 {
		return fmt.Errorf("no heartbeat was ever published")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Second):
	}
	after, err := readHeartbeats(ctx, state, state.stream)
	if err != nil {
		return err
	}
	if len(after) != settled {
		return fmt.Errorf("want the heartbeats to stay at %d, got %d", settled, len(after))
	}
	return nil
}

func secondStreamSameConnection(ctx context.Context, state *world) error {
	name := state.stream + "-second"
	if err := state.client.Stream(name).Ensure(ctx); err != nil {
		return err
	}
	sessions := state.client.SessionsWith(laser.SessionConfig{Stream: name})
	if _, err := sessions.Bootstrap(ctx, 4, laser.ExpireAfter(24*time.Hour)); err != nil {
		return err
	}
	state.put("secondStream", name)
	client := state.client
	state.t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := client.Stream(name).Delete(cleanup); err != nil {
			state.t.Logf("could not delete %s: %v", name, err)
		}
	})
	return nil
}

func startOnTwoStreams(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 3 {
		return fmt.Errorf("want an agent and two labels, got %v", arguments)
	}
	owner, here, there := arguments[0], arguments[1], arguments[2]
	interval := configuredHeartbeatInterval(state)
	hereConversation, _, err := startLeasedSession(
		ctx, state.client, state.client.Sessions(), interval, owner, here)
	if err != nil {
		return err
	}
	secondAny, found := state.get("secondStream")
	secondStream, ok := secondAny.(string)
	if !found || !ok {
		return fmt.Errorf("no second stream was bootstrapped")
	}
	secondClient := state.client.WithDefaultStream(secondStream)
	thereConversation, _, err := startLeasedSession(
		ctx, secondClient, secondClient.Sessions(), interval, owner, there)
	if err != nil {
		return err
	}
	state.put("streamSessions", map[string]wire.ConversationID{
		state.stream: hereConversation, secondStream: thereConversation,
	})
	return nil
}

func checkHeartbeatsPerStream(ctx context.Context, state *world) error {
	mapAny, found := state.get("streamSessions")
	streamSessions, ok := mapAny.(map[string]wire.ConversationID)
	if !found || !ok || len(streamSessions) != 2 {
		return fmt.Errorf("want one session started on each of two streams")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ready := true
		for stream := range streamSessions {
			beats, err := readHeartbeats(ctx, state, stream)
			if err != nil {
				return err
			}
			if len(beats) < 2 {
				ready = false
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("heartbeats did not accumulate on both streams")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	for stream, conversation := range streamSessions {
		beats, err := readHeartbeats(ctx, state, stream)
		if err != nil {
			return err
		}
		for _, beat := range beats {
			if beat.Stream != stream {
				return fmt.Errorf("want stream %q, got %q", stream, beat.Stream)
			}
			if len(beat.Sessions) != 1 || beat.Sessions[0] != conversation {
				return fmt.Errorf("want only session %s on %s, got %v",
					conversation, stream, beat.Sessions)
			}
		}
	}
	return nil
}

// --- the context manifest ---

// buildAssembledContext reads what the session already holds and turns each
// record into a message fragment addressed by its exact log position: what a
// caller builds by hand today, since the Go client has no Session.Assemble of
// its own yet to do it.
func buildAssembledContext(
	ctx context.Context, state *world, session *laser.Session, want int,
) (*laser.AssembledContext, error) {
	deadline := time.Now().Add(20 * time.Second)
	var messages []laser.ContextMessage
	for {
		var err error
		messages, err = state.client.Context(session.Conversation()).
			Fetch(ctx, []string{wire.AgentSessions}, 100, 0)
		if err == nil && len(messages) == want {
			break
		}
		if time.Now().After(deadline) {
			if err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("want %d records to assemble, got %d", want, len(messages))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
	streamInfo, err := state.client.Stream(state.stream).Get(ctx)
	if err != nil {
		return nil, err
	}
	if streamInfo == nil {
		return nil, fmt.Errorf("the stream %q does not exist", state.stream)
	}
	topicInfo, err := state.topic(wire.AgentSessions).Get(ctx)
	if err != nil {
		return nil, err
	}
	if topicInfo == nil {
		return nil, fmt.Errorf("the topic %q does not exist", wire.AgentSessions)
	}
	conversationString := session.Conversation().String()
	generation := topicInfo.CreatedAtMicros
	fragments := make([]wire.Fragment, 0, len(messages))
	var totalBytes uint64
	for _, message := range messages {
		source := wire.NewMessageSource(wire.MessageSource{
			Stream: streamInfo.ID, Topic: topicInfo.ID,
			Partition: message.Record.PartitionID, Offset: message.Record.Offset(),
			Generation: &generation, Conversation: &conversationString,
		})
		body := message.Body()
		fragments = append(fragments, wire.Fragment{
			Kind: wire.FragmentMessage, At: &source,
			Tokens: uint32(wire.EstimateTokens(len(body))), Bytes: uint32(len(body)),
		})
		totalBytes += uint64(len(body))
	}
	manifest := wire.ContextManifest{
		Policy: "last_n", PolicyVersion: "1", Fragments: fragments, Bytes: totalBytes,
	}
	return &laser.AssembledContext{Fragments: messages, Manifest: manifest}, nil
}

func recordModelCallAssembled(
	ctx context.Context, state *world, session *laser.Session, text string, arguments []string,
) error {
	counts := numbersOf(bdd.Step{Text: text})
	if len(counts) != 1 {
		return fmt.Errorf("want one record count, got %v", counts)
	}
	assembled, err := buildAssembledContext(ctx, state, session, int(counts[0]))
	if err != nil {
		return err
	}
	call, err := session.Model(ctx, laser.ModelRequest{
		Model: arguments[0], Body: []byte(`{"prompt":"hi"}`),
	}, assembled)
	if err != nil {
		return err
	}
	if _, err := call.Complete(ctx, laser.ModelResponse{Body: []byte("hello")}); err != nil {
		return err
	}
	state.put("assembled", assembled)
	return nil
}

func checkManifestAddresses(ctx context.Context, state *world) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	assembledAny, found := state.get("assembled")
	assembled, ok := assembledAny.(*laser.AssembledContext)
	if !found || !ok {
		return fmt.Errorf("no context was assembled")
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		records, err := state.replay(ctx, wire.AgentSessions)
		if err != nil {
			return err
		}
		var manifestBody []byte
		for _, record := range records {
			message, openErr := laser.OpenAgdx(record)
			if openErr != nil || message.Envelope.Conversation != session.Conversation() {
				continue
			}
			envelope := message.Envelope
			if wire.DisplayTypeOf(wire.EnvelopeRecord(&envelope), wire.AgentSessions) ==
				wire.DisplayContextAssembled {
				manifestBody = envelope.Body
			}
		}
		if manifestBody != nil {
			var manifest wire.ContextManifest
			if err := wire.Unmarshal(manifestBody, &manifest); err != nil {
				return err
			}
			if len(manifest.Fragments) != len(assembled.Fragments) {
				return fmt.Errorf("want %d fragments, got %d",
					len(assembled.Fragments), len(manifest.Fragments))
			}
			for index, fragment := range manifest.Fragments {
				if fragment.At == nil || fragment.At.Message == nil {
					return fmt.Errorf("fragment %d carries no message address", index)
				}
				want := assembled.Fragments[index].Record
				if fragment.At.Message.Partition != want.PartitionID ||
					fragment.At.Message.Offset != want.Offset() {
					return fmt.Errorf("fragment %d: want partition %d offset %d, got %d/%d",
						index, want.PartitionID, want.Offset(),
						fragment.At.Message.Partition, fragment.At.Message.Offset)
				}
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no context.assembled record appeared")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// --- concurrent terminal calls ---

type raceOutcome struct {
	Verb string
	Err  error
}

// racingClones runs enders calls to End and cancelers calls to Cancel at
// once, on clones of one session sharing its terminal latch: a Go struct
// copy shares the latch's pointer field, the same sharing a cloned Rust
// handle gets from its Arc.
func racingClones(ctx context.Context, state *world, step bdd.Step) error {
	counts := numbersOf(step)
	if len(counts) != 2 {
		return fmt.Errorf("want two counts, got %v", counts)
	}
	enders, cancelers := int(counts[0]), int(counts[1])
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	total := enders + cancelers
	var ready sync.WaitGroup
	ready.Add(total)
	outcomes := make([]raceOutcome, total)
	var wg sync.WaitGroup
	wg.Add(total)
	for index := 0; index < total; index++ {
		ends := index%2 == 0 && index/2 < enders || index/2 >= cancelers
		go func(index int, ends bool) {
			defer wg.Done()
			clone := *session
			ready.Done()
			ready.Wait()
			if ends {
				_, callErr := clone.Complete(ctx, "")
				outcomes[index] = raceOutcome{Verb: "end", Err: callErr}
			} else {
				_, callErr := clone.End(ctx, wire.TaskCanceled, wire.SessionEnd{})
				outcomes[index] = raceOutcome{Verb: "cancel", Err: callErr}
			}
		}(index, ends)
	}
	wg.Wait()
	state.put("racingOutcomes", outcomes)
	return nil
}

func checkOneVerbWins(state *world) error {
	outcomesAny, found := state.get("racingOutcomes")
	outcomes, ok := outcomesAny.([]raceOutcome)
	if !found || !ok {
		return fmt.Errorf("no racing clones ran")
	}
	winners := map[string]bool{}
	for _, outcome := range outcomes {
		if outcome.Err == nil {
			winners[outcome.Verb] = true
		}
	}
	if len(winners) != 1 {
		return fmt.Errorf("want exactly one winning verb, got %v", winners)
	}
	var winner string
	for verb := range winners {
		winner = verb
	}
	for _, outcome := range outcomes {
		if outcome.Err == nil {
			if outcome.Verb != winner {
				return fmt.Errorf("want only %q to succeed, %q also did", winner, outcome.Verb)
			}
			continue
		}
		if outcome.Verb == winner {
			return fmt.Errorf("the winning verb %q also failed: %v", winner, outcome.Err)
		}
		var invalid *laser.InvalidError
		if !errors.As(outcome.Err, &invalid) {
			return fmt.Errorf("want an invalid refusal for %q, got %v", outcome.Verb, outcome.Err)
		}
	}
	state.put("racingWinner", winner)
	return nil
}

func checkWinnerOnLane(ctx context.Context, state *world, step bdd.Step) error {
	counts := numbersOf(step)
	if len(counts) != 1 {
		return fmt.Errorf("want one count, got %v", counts)
	}
	winnerAny, found := state.get("racingWinner")
	winner, ok := winnerAny.(string)
	if !found || !ok {
		return fmt.Errorf("no verb won the race yet")
	}
	display := string(wire.DisplaySessionCompleted)
	if winner == "cancel" {
		display = string(wire.DisplaySessionCanceled)
	}
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	want := int(counts[0])
	deadline := time.Now().Add(20 * time.Second)
	for {
		records, err := state.replay(ctx, wire.AgentSessions)
		if err != nil {
			return err
		}
		ids := map[string]bool{}
		matching := 0
		for _, record := range records {
			message, openErr := laser.OpenAgdx(record)
			if openErr != nil || message.Envelope.Conversation != session.Conversation() {
				continue
			}
			envelope := message.Envelope
			if string(wire.DisplayTypeOf(wire.EnvelopeRecord(&envelope), wire.AgentSessions)) != display {
				continue
			}
			matching++
			if envelope.Record != nil {
				ids[envelope.Record.String()] = true
			}
		}
		if matching >= want {
			if matching != want {
				return fmt.Errorf("want %d terminal records, got %d", want, matching)
			}
			if len(ids) != 1 {
				return fmt.Errorf("want one record id, got %d", len(ids))
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want %d terminal records of %q, got %d", want, display, matching)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}
