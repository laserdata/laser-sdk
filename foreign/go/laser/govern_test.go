package laser_test

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
	"github.com/zeebo/blake3"
)

// publishServer records what reached the log.
type publishServer struct {
	clusterOfOne
	guard    sync.Mutex
	topics   []string
	messages [][]iggcon.IggyMessage
	fail     error
	// failAudit refuses only the audit topic, so an effect can succeed while
	// the evidence that should record it does not.
	failAudit bool
}

func (server *publishServer) Close() error { return nil }

// SendBinaryRequest answers the way raw Apache Iggy does: it does not know the
// managed commands, which is an answer rather than a failure.
func (server *publishServer) SendBinaryRequest(
	_ context.Context, _ uint32, _ []byte,
) ([]byte, error) {
	return nil, ierror.InvalidCommand{}
}

func (server *publishServer) SendMessages(
	_ context.Context, _ iggcon.Identifier, topic iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	named, err := topic.String()
	if err != nil {
		return nil, err
	}
	if server.failAudit && named == wire.AgentAudit {
		return nil, errors.New("the audit topic is unreachable")
	}
	server.topics = append(server.topics, named)
	server.messages = append(server.messages, messages)
	return &iggcon.SendMessagesResponse{}, nil
}

// sent is every record that reached the log, with the topic it landed on.
func (server *publishServer) sent() ([]string, [][]iggcon.IggyMessage) {
	server.guard.Lock()
	defer server.guard.Unlock()
	return append([]string(nil), server.topics...),
		append([][]iggcon.IggyMessage(nil), server.messages...)
}

// evidence is every policy-evidence record the audit topic received.
func (server *publishServer) evidence(t *testing.T) []laser.PolicyEvidence {
	t.Helper()
	topics, batches := server.sent()
	var records []laser.PolicyEvidence
	for index, topic := range topics {
		if topic != wire.AgentAudit {
			continue
		}
		for _, message := range batches[index] {
			var envelope wire.AgentEnvelope
			if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
				t.Fatalf("decode the audit envelope: %v", err)
			}
			if envelope.Operation == nil ||
				*envelope.Operation != laser.PolicyDecisionOperation {
				t.Fatalf("an audit record must be a policy decision, got %+v",
					envelope.Operation)
			}
			decoded, err := laser.DecodePolicyEvidence(envelope.Body)
			if err != nil {
				t.Fatalf("decode the evidence: %v", err)
			}
			records = append(records, *decoded)
		}
	}
	return records
}

// published is every record that reached a topic other than the audit one.
func (server *publishServer) published(t *testing.T) []iggcon.IggyMessage {
	t.Helper()
	topics, batches := server.sent()
	var records []iggcon.IggyMessage
	for index, topic := range topics {
		if topic == wire.AgentAudit {
			continue
		}
		records = append(records, batches[index]...)
	}
	return records
}

// scriptedGovernor answers with one decision, recording what it was asked.
type scriptedGovernor struct {
	decision *laser.ActionDecision
	err      error
	guard    sync.Mutex
	seen     []laser.GovernedAction
}

func (governor *scriptedGovernor) Decide(
	_ context.Context, action *laser.GovernedAction,
) (*laser.ActionDecision, error) {
	governor.guard.Lock()
	governor.seen = append(governor.seen, *action)
	governor.guard.Unlock()
	if governor.err != nil {
		return nil, governor.err
	}
	return governor.decision, nil
}

func (governor *scriptedGovernor) asked() []laser.GovernedAction {
	governor.guard.Lock()
	defer governor.guard.Unlock()
	return append([]laser.GovernedAction(nil), governor.seen...)
}

func governedHandle(server iggcon.Client) *laser.Laser {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"})
}

// A governor sees the effect before it runs, with everything a policy needs
// to decide: who, where, and what.
func TestGivenAnEffectWhenGovernedThenShouldDescribeItBeforeItRuns(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Allow()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("the body")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	asked := governor.asked()
	if len(asked) != 1 {
		t.Fatalf("want one decision, got %d", len(asked))
	}
	if asked[0].Kind != laser.ActionPublish {
		t.Errorf("want a publish, got %q", asked[0].Kind)
	}
	if asked[0].Stream != "agents" || asked[0].Topic != "events" {
		t.Errorf("want the destination named, got %s/%s",
			asked[0].Stream, asked[0].Topic)
	}
	if string(asked[0].Payload) != "the body" {
		t.Errorf("want the body offered, got %q", asked[0].Payload)
	}
	// An allow records nothing: evidence is for decisions worth keeping.
	if records := server.evidence(t); len(records) != 0 {
		t.Errorf("an allow writes no evidence, got %d records", len(records))
	}
	if len(server.published(t)) != 1 {
		t.Error("an allowed effect must reach the log")
	}
}

// A blocked effect never reaches the log, which is the whole point of a
// pre-effect hook rather than an after-the-fact audit.
func TestGivenABlockedEffectWhenGovernedThenShouldNotReachTheLog(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Block("exfiltration risk")}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err := handle.Topic("events").Publish().
		Payload([]byte("secrets")).Key("k").Send(context.Background())

	var blocked *laser.PolicyBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("want a PolicyBlockedError, got %T: %v", err, err)
	}
	// The caller is told why rather than only that something refused.
	if !strings.Contains(err.Error(), "exfiltration risk") {
		t.Errorf("want the governor's reason, got %q", err.Error())
	}
	if len(server.published(t)) != 0 {
		t.Error("a blocked effect must not reach the log")
	}
	records := server.evidence(t)
	if len(records) != 1 || records[0].Outcome != laser.OutcomeBlocked {
		t.Fatalf("want one blocked record, got %+v", records)
	}
	if records[0].Decision != string(laser.VerdictBlock) {
		t.Errorf("want the block recorded, got %q", records[0].Decision)
	}
}

// A modified body is what the log carries, because a decision that only
// annotated the original would leave the original on the log.
func TestGivenAModifiedBodyWhenGovernedThenShouldPublishTheReplacement(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Modify([]byte("redacted"))}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("the card number")).Key("k").
		Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	published := server.published(t)
	if len(published) != 1 || string(published[0].Payload) != "redacted" {
		t.Fatalf("want the replacement on the log, got %q", published[0].Payload)
	}
	// The counters measure what went out rather than what was offered.
	counters := handle.GovernedCounters()
	if counters == nil || counters.BytesSent != uint64(len("redacted")) {
		t.Errorf("want the replacement's bytes counted, got %+v", counters)
	}
}

// A step-up names the scope an approval must grant, so a handler knows what
// to ask for rather than only that it was refused.
func TestGivenAStepUpWhenGovernedThenShouldNameTheScopeToGrant(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.StepUp("refund:over-1000")}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err := handle.Topic("events").Publish().
		Payload([]byte("refund")).Key("k").Send(context.Background())

	var stepUp *laser.StepUpRequiredError
	if !errors.As(err, &stepUp) {
		t.Fatalf("want a StepUpRequiredError, got %T: %v", err, err)
	}
	if stepUp.Scope != "refund:over-1000" {
		t.Errorf("want the scope named, got %q", stepUp.Scope)
	}
	records := server.evidence(t)
	if len(records) != 1 || records[0].ApprovedScope == nil ||
		*records[0].ApprovedScope != "refund:over-1000" {
		t.Fatalf("want the scope in evidence, got %+v", records)
	}
}

// Deferred work may run later, so the denial says so rather than reading as a
// permanent refusal.
func TestGivenDeferredWorkWhenGovernedThenShouldReportItRetryable(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Defer("the budget resets at midnight")}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background())

	var deferred *laser.PolicyDeferredError
	if !errors.As(err, &deferred) {
		t.Fatalf("want a PolicyDeferredError, got %T: %v", err, err)
	}
	if !deferred.Retryable() {
		t.Error("deferred work is work that may run later")
	}
	if records := server.evidence(t); len(records) != 1 ||
		records[0].Outcome != laser.OutcomeDeferred {
		t.Fatalf("want one deferred record, got %+v", records)
	}
}

// Observing never impacts production: every decision is recorded with what
// enforcement would have done, and the effect runs unmodified regardless.
func TestGivenObserveModeWhenAVerdictWouldDenyThenShouldStillRunTheEffect(t *testing.T) {
	for name, decision := range map[string]*laser.ActionDecision{
		"a block":    laser.Block("would have blocked"),
		"a step up":  laser.StepUp("would have stepped up"),
		"a deferral": laser.Defer("would have deferred"),
		"a modify":   laser.Modify([]byte("would have replaced")),
	} {
		t.Run(name, func(t *testing.T) {
			governor := &scriptedGovernor{decision: decision}
			server := &publishServer{}
			handle := governedHandle(server).
				WithGovernor(governor, laser.GovernorObserve)

			if _, err := handle.Topic("events").Publish().
				Payload([]byte("the original")).Key("k").
				Send(context.Background()); err != nil {
				t.Fatalf("publish: %v", err)
			}

			published := server.published(t)
			if len(published) != 1 || string(published[0].Payload) != "the original" {
				t.Fatalf("want the original on the log, got %q", published[0].Payload)
			}
			records := server.evidence(t)
			if len(records) != 1 {
				t.Fatalf("want one record, got %d", len(records))
			}
			// What it would have done is recorded, and what it did is not.
			if records[0].Decision != decision.Verdict.String() {
				t.Errorf("want the verdict recorded, got %q", records[0].Decision)
			}
			if records[0].Outcome != laser.OutcomeEffected {
				t.Errorf("want the effect recorded as run, got %q", records[0].Outcome)
			}
			if records[0].Mode != string(laser.GovernorObserve) {
				t.Errorf("want the mode recorded, got %q", records[0].Mode)
			}
		})
	}
}

// A broken governor must never fail open: enforcing fails the effect.
func TestGivenAFailingGovernorWhenEnforcingThenShouldFailTheEffect(t *testing.T) {
	governor := &scriptedGovernor{err: errors.New("the policy engine is down")}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background())

	if err == nil {
		t.Fatal("a governor that cannot decide must not let the effect run")
	}
	if len(server.published(t)) != 0 {
		t.Error("the effect must not reach the log")
	}
}

// A governor that answers nothing has decided nothing, and treating that as
// allow would be failing open through the back door.
func TestGivenAGovernorThatDecidesNothingWhenEnforcingThenShouldRefuseIt(t *testing.T) {
	governor := &scriptedGovernor{}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// Observing never impacts the effect, a failing governor included: the
// failure is recorded and the work goes through.
func TestGivenAFailingGovernorWhenObservingThenShouldRecordItAndProceed(t *testing.T) {
	governor := &scriptedGovernor{err: errors.New("the policy engine is down")}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorObserve)

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("observing must not fail the effect: %v", err)
	}

	records := server.evidence(t)
	if len(records) != 1 || records[0].Decision != laser.GovernorErrorDecision {
		t.Fatalf("want the failure recorded, got %+v", records)
	}
	if records[0].Reason == nil ||
		!strings.Contains(*records[0].Reason, "the policy engine is down") {
		t.Errorf("want the failure in the reason, got %v", records[0].Reason)
	}
	if len(server.published(t)) != 1 {
		t.Error("the effect must still reach the log")
	}
}

// A proceeding decision whose evidence did not land must not proceed: a
// governed effect that nobody recorded is an ungoverned effect.
func TestGivenAFailedEvidenceWriteWhenEnforcingThenShouldFailClosed(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Observe()}
	server := &publishServer{failAudit: true}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background())

	if err == nil {
		t.Fatal("an unrecorded governed effect must not run")
	}
	if len(server.published(t)) != 0 {
		t.Error("the effect must not reach the log either")
	}

	// A denial stands whether or not its evidence landed, because refusing
	// is the safe outcome and the caller still has to be told.
	refusing := &publishServer{failAudit: true}
	blocked := governedHandle(refusing).WithGovernor(
		&scriptedGovernor{decision: laser.Block("no")}, laser.GovernorEnforce)
	_, err = blocked.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background())
	var denial *laser.PolicyBlockedError
	if !errors.As(err, &denial) {
		t.Fatalf("want the denial to stand, got %T: %v", err, err)
	}

	// Observing never impacts production, so a failed evidence write there
	// only warns and the effect goes through.
	observing := &publishServer{failAudit: true}
	shadow := governedHandle(observing).WithGovernor(governor, laser.GovernorObserve)
	if _, err := shadow.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("observing must not fail on a failed evidence write: %v", err)
	}
	if len(observing.published(t)) != 1 {
		t.Error("the observed effect must still reach the log")
	}
}

// Evidence chains each decision to the last in its conversation, so a
// reordered, dropped or edited record is detectable.
func TestGivenSeveralDecisionsWhenRecordedThenShouldChainAndVerify(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Observe()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	topic := handle.Topic("events")

	for range 3 {
		if _, err := topic.Publish().Payload([]byte("work")).Key("k").
			Send(context.Background()); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	records := server.evidence(t)
	if len(records) != 3 {
		t.Fatalf("want three records, got %d", len(records))
	}
	if records[0].PreviousDigest != nil {
		t.Error("the first record of a chain names no predecessor")
	}
	for index := 1; index < len(records); index++ {
		if records[index].PreviousDigest == nil ||
			*records[index].PreviousDigest != records[index-1].ReceiptDigest {
			t.Errorf("record %d does not chain to its predecessor", index)
		}
	}
	if !laser.VerifyEvidenceChain(records) {
		t.Error("an unbroken chain must verify")
	}
}

// Evidence nobody can check is not evidence, so a chain that was tampered
// with must fail to verify.
func TestGivenATamperedChainWhenVerifiedThenShouldRefuseIt(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Observe()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	topic := handle.Topic("events")
	for range 3 {
		if _, err := topic.Publish().Payload([]byte("work")).Key("k").
			Send(context.Background()); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	original := server.evidence(t)

	for name, tamper := range map[string]func([]laser.PolicyEvidence) []laser.PolicyEvidence{
		"an edited record": func(records []laser.PolicyEvidence) []laser.PolicyEvidence {
			records[1].Reason = ptr("a reason nobody recorded")
			return records
		},
		"a dropped record": func(records []laser.PolicyEvidence) []laser.PolicyEvidence {
			return []laser.PolicyEvidence{records[0], records[2]}
		},
		"a reordered chain": func(records []laser.PolicyEvidence) []laser.PolicyEvidence {
			return []laser.PolicyEvidence{records[1], records[0], records[2]}
		},
		"a forged digest": func(records []laser.PolicyEvidence) []laser.PolicyEvidence {
			records[2].ReceiptDigest = records[0].ReceiptDigest
			return records
		},
		"a chain joined in the middle": func(
			records []laser.PolicyEvidence,
		) []laser.PolicyEvidence {
			// This record names a predecessor nobody in the set carries, so
			// the chain it claims cannot be checked.
			return records[1:]
		},
		"a record claiming an empty predecessor": func(
			records []laser.PolicyEvidence,
		) []laser.PolicyEvidence {
			// Naming nothing as the predecessor is how a forged record would
			// splice itself into a chain that has not started, so it is not
			// the same as naming none at all.
			records[0].PreviousDigest = ptr("")
			// Resealed, so only the chain rule can catch it: the digest
			// itself reproduces perfectly.
			resealed, err := records[0].Seal()
			if err != nil {
				t.Fatalf("seal: %v", err)
			}
			return []laser.PolicyEvidence{resealed}
		},
		"a replayed opening record": func(
			records []laser.PolicyEvidence,
		) []laser.PolicyEvidence {
			// A second record naming no predecessor cannot belong to a chain
			// that already started.
			return append(records, records[0])
		},
	} {
		t.Run(name, func(t *testing.T) {
			records := append([]laser.PolicyEvidence(nil), original...)
			if laser.VerifyEvidenceChain(tamper(records)) {
				t.Error("a broken chain must not verify")
			}
		})
	}
}

// An AGDX verb is governed as what it is rather than as a plain publish, so a
// policy can tell a tool command from a status it should ignore.
func TestGivenAnAgdxVerbWhenGovernedThenShouldCarryItsOwnKind(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	source, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	for name, expected := range map[wire.AgentKind]laser.ActionKind{
		wire.KindCommand:  laser.ActionCommand,
		wire.KindResponse: laser.ActionResponse,
		wire.KindEvent:    laser.ActionEvent,
	} {
		t.Run(string(name), func(t *testing.T) {
			governor := &scriptedGovernor{decision: laser.Allow()}
			server := &publishServer{}
			handle := governedHandle(server).
				WithGovernor(governor, laser.GovernorEnforce)
			agdx := handle.Agdx(source, conversation)
			correlation := wire.CorrelationID{
				ID: wire.IDFromBytes(laser.NewOperationID())}

			var send *laser.AgdxSend
			switch name {
			case wire.KindCommand:
				send = agdx.Command(correlation, []byte("do it")).
					WithTool("refund").Broadcast()
			case wire.KindResponse:
				send = agdx.Respond(correlation, []byte("done"))
			default:
				send = agdx.Emit([]byte("something happened")).Broadcast()
			}
			if _, err := send.Send(context.Background()); err != nil {
				t.Fatalf("send: %v", err)
			}

			asked := governor.asked()
			if len(asked) != 1 || asked[0].Kind != expected {
				t.Fatalf("want %q, got %+v", expected, asked)
			}
			if asked[0].Source != "planner" {
				t.Errorf("want the acting agent named, got %q", asked[0].Source)
			}
			if asked[0].Conversation == nil || *asked[0].Conversation != conversation {
				t.Errorf("want the conversation named, got %v", asked[0].Conversation)
			}
			if name == wire.KindCommand && asked[0].Tool != "refund" {
				t.Errorf("want the tool named, got %q", asked[0].Tool)
			}
		})
	}
}

// A memory write is governed as what it is, so a policy can bound what an
// agent may remember without bounding everything it may send.
func TestGivenAMemoryWriteWhenGovernedThenShouldCarryItsOwnKind(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Allow()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	if _, err := handle.Memory("notes").Remember(context.Background(),
		laser.MemoryFact, []byte("the sky is blue")); err != nil {
		t.Fatalf("remember: %v", err)
	}

	asked := governor.asked()
	if len(asked) != 1 || asked[0].Kind != laser.ActionMemoryWrite {
		t.Fatalf("want a memory write, got %+v", asked)
	}
}

// A grant answers a step-up in its own conversation, so a handler that gated
// once can act rather than being asked to gate again.
func TestGivenAGrantedApprovalWhenSteppedUpThenShouldProceedOnTheGrant(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	approver, err := wire.NewAgentID("supervisor")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	source, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
	grant, err := laser.GrantFrom(approver, conversation, &laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{
			Kind: wire.KindResponse, Conversation: conversation,
			Source: approver, Correlation: &correlation, Body: []byte("approved"),
		},
	})
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	governor := &scriptedGovernor{decision: laser.StepUp("refund:over-1000")}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	if _, err := handle.Agdx(source, conversation).
		Emit([]byte("refund")).Broadcast().WithGrant(grant).
		Send(context.Background()); err != nil {
		t.Fatalf("the grant answers the step up: %v", err)
	}

	if len(server.published(t)) != 1 {
		t.Error("an approved effect must reach the log")
	}
	// The step-up is still recorded with its scope: the approval answered it,
	// it did not make it disappear.
	records := server.evidence(t)
	if len(records) != 1 || records[0].Decision != string(laser.VerdictStepUp) {
		t.Fatalf("want the step up recorded, got %+v", records)
	}
	if records[0].Outcome != laser.OutcomeEffected {
		t.Errorf("want the effect recorded as run, got %q", records[0].Outcome)
	}
}

// An effect carrying no approval at all is still stepped up, so a handler
// cannot skip the gate by sending from a conversation.
func TestGivenNoGrantWhenSteppedUpThenShouldStillRefuse(t *testing.T) {
	source, _ := wire.NewAgentID("planner")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	governor := &scriptedGovernor{decision: laser.StepUp("refund:over-1000")}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err := handle.Agdx(source, conversation).
		Emit([]byte("refund")).Broadcast().Send(context.Background())

	var stepUp *laser.StepUpRequiredError
	if !errors.As(err, &stepUp) {
		t.Fatalf("want a StepUpRequiredError, got %T: %v", err, err)
	}
	if len(server.published(t)) != 0 {
		t.Error("an ungranted effect must not reach the log")
	}
}

// A grant from another conversation approves nothing, because an approval
// given for one piece of work is not an approval for another.
func TestGivenAGrantFromAnotherConversationWhenSteppedUpThenShouldStillRefuse(t *testing.T) {
	approver, _ := wire.NewAgentID("supervisor")
	source, _ := wire.NewAgentID("planner")
	elsewhere := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 2})}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
	grant, err := laser.GrantFrom(approver, elsewhere, &laser.AgdxMessage{
		Envelope: wire.AgentEnvelope{
			Kind: wire.KindResponse, Conversation: elsewhere,
			Source: approver, Correlation: &correlation,
		},
	})
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	governor := &scriptedGovernor{decision: laser.StepUp("refund:over-1000")}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err = handle.Agdx(source, wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}).
		Emit([]byte("refund")).Broadcast().WithGrant(grant).
		Send(context.Background())

	var stepUp *laser.StepUpRequiredError
	if !errors.As(err, &stepUp) {
		t.Fatalf("want a StepUpRequiredError, got %T: %v", err, err)
	}
}

// A grant is minted only by an approving reply, so it cannot be built from a
// message the approver never sent.
func TestGivenAReplyFromAnotherAgentWhenGrantedThenShouldRefuseIt(t *testing.T) {
	approver, _ := wire.NewAgentID("supervisor")
	impostor, _ := wire.NewAgentID("planner")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}

	for name, reply := range map[string]*laser.AgdxMessage{
		"from another agent": {Envelope: wire.AgentEnvelope{
			Conversation: conversation, Source: impostor, Correlation: &correlation,
		}},
		"from another conversation": {Envelope: wire.AgentEnvelope{
			Conversation: wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 3})},
			Source:       approver, Correlation: &correlation,
		}},
		"without a correlation": {Envelope: wire.AgentEnvelope{
			Conversation: conversation, Source: approver,
		}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := laser.GrantFrom(approver, conversation, reply)
			var invalid *laser.InvalidError
			if !errors.As(err, &invalid) {
				t.Fatalf("want an InvalidError, got %T: %v", err, err)
			}
		})
	}
}

// Counters bound a whole session rather than one handle, so a policy that
// counts cannot be reset by taking a fresh handle off the same governor.
func TestGivenSeveralHandlesWhenGovernedThenShouldShareTheirCounters(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Allow()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	derived := handle.WithDefaultStream("agents")

	for _, publisher := range []*laser.Laser{handle, derived, handle} {
		if _, err := publisher.Topic("events").Publish().
			Payload([]byte("four")).Key("k").Send(context.Background()); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	counters := handle.GovernedCounters()
	if counters == nil || counters.Sends != 3 {
		t.Fatalf("want three sends counted across handles, got %+v", counters)
	}
	if counters.BytesSent != 12 {
		t.Errorf("want the published bytes counted, got %d", counters.BytesSent)
	}
	// What the governor saw is what the next decision bounds.
	asked := governor.asked()
	if asked[2].Counters.Sends != 2 {
		t.Errorf("want the running count offered, got %+v", asked[2].Counters)
	}
}

// An ungoverned handle costs nothing and decides nothing.
func TestGivenNoGovernorWhenPublishingThenShouldNotGovernAnything(t *testing.T) {
	server := &publishServer{}
	handle := governedHandle(server)

	if mode, governed := handle.Governor(); governed {
		t.Errorf("an ungoverned handle governs nothing, got mode %q", mode)
	}
	if handle.GovernedCounters() != nil {
		t.Error("an ungoverned handle counts nothing")
	}
	if _, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(server.evidence(t)) != 0 {
		t.Error("an ungoverned effect records nothing")
	}
}

// The first denial fails a whole batch before anything is sent, because a
// batch half-published is one whose caller cannot tell what landed.
func TestGivenADeniedRecordInABatchWhenGovernedThenShouldSendNothing(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Block("no")}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	first, err := iggcon.NewIggyMessage([]byte("one"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	second, err := iggcon.NewIggyMessage([]byte("two"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}

	_, err = handle.Topic("events").Batch(
		context.Background(), []iggcon.IggyMessage{first, second}, "k")

	var blocked *laser.PolicyBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("want a PolicyBlockedError, got %T: %v", err, err)
	}
	if len(server.published(t)) != 0 {
		t.Error("a denied batch must send nothing")
	}
	// Only the first record was decided: the batch failed before the rest.
	if len(governor.asked()) != 1 {
		t.Errorf("want one decision, got %d", len(governor.asked()))
	}
}

// Evidence must not recurse into governance, or a blocking policy would stop
// its own decision being recorded.
func TestGivenEvidenceWhenWrittenThenShouldNotBeGovernedItself(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Observe()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// One decision for the effect, none for the evidence that recorded it.
	if len(governor.asked()) != 1 {
		t.Fatalf("want one decision, got %d", len(governor.asked()))
	}
	if len(server.evidence(t)) != 1 {
		t.Error("the decision must still be recorded")
	}
}

// The evidence digest is BLAKE3 over the record's canonical encoding with the
// digest field empty, the same hash every Laser client seals with, so a chain
// written by one is verifiable by another.
func TestGivenTheEvidenceHashWhenComputedThenShouldMatchTheSharedVector(t *testing.T) {
	// The published BLAKE3 vector, so swapping the hash for another fails
	// here rather than quietly producing chains no other client can verify.
	published := blake3.Sum256([]byte("abc"))
	if hex.EncodeToString(published[:]) !=
		"6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85" {
		t.Fatal("the evidence hash is not BLAKE3")
	}

	evidence := laser.PolicyEvidence{
		DecisionID: "d1", Decision: "observe", Mode: "enforce", Kind: "publish",
		Stream: "agents", Topic: "events", ChainID: "c1",
		Outcome: laser.OutcomeEffected, AtMicros: 1,
	}
	sealed, err := evidence.Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}

	// The digest covers the canonical encoding with the digest field empty,
	// which is what makes it reproducible from the record alone.
	canonical, err := evidence.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := blake3.Sum256(canonical)
	if sealed.ReceiptDigest != hex.EncodeToString(want[:]) {
		t.Errorf("want %s, got %s",
			hex.EncodeToString(want[:]), sealed.ReceiptDigest)
	}
	// Sealing one record twice gives one digest.
	again, err := evidence.Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if again.ReceiptDigest != sealed.ReceiptDigest {
		t.Error("one record must seal to one digest")
	}
	// A record that changed in any field seals differently, which is what
	// makes an edited record detectable.
	changed := evidence
	changed.Topic = "elsewhere"
	altered, err := changed.Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if altered.ReceiptDigest == sealed.ReceiptDigest {
		t.Error("a changed record must seal differently")
	}
	// A sealed record stands on its own as a one-record chain.
	if !laser.VerifyEvidenceChain([]laser.PolicyEvidence{sealed}) {
		t.Error("a freshly sealed record must verify")
	}
}

// Chains of several conversations interleave on one topic, so each is
// followed separately rather than read as one broken chain.
func TestGivenTwoConversationsWhenRecordedThenShouldChainSeparately(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Observe()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	source, _ := wire.NewAgentID("planner")
	first := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	second := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 2})}

	for _, conversation := range []wire.ConversationID{first, second, first, second} {
		if _, err := handle.Agdx(source, conversation).
			Emit([]byte("work")).Broadcast().Send(context.Background()); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}

	records := server.evidence(t)
	if len(records) != 4 {
		t.Fatalf("want four records, got %d", len(records))
	}
	chains := map[string]int{}
	for _, record := range records {
		chains[record.ChainID]++
	}
	if len(chains) != 2 {
		t.Errorf("want one chain per conversation, got %d", len(chains))
	}
	// Interleaved on the topic, unbroken within each chain.
	if !laser.VerifyEvidenceChain(records) {
		t.Error("interleaved chains must each verify")
	}
}

// A handle that keeps only one chain head still records every decision: an
// evicted head starts a new local chain rather than breaking the old one.
func TestGivenAFullChainBudgetWhenAnotherConversationDecidesThenShouldStartANewChain(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Observe()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernorRetention(
		governor, laser.GovernorEnforce,
		laser.GovernorRetention{Capacity: 1, IdleTTL: time.Hour})
	source, _ := wire.NewAgentID("planner")

	for index := range 3 {
		conversation := wire.ConversationID{
			ID: wire.IDFromBytes([16]byte{15: byte(index)})}
		if _, err := handle.Agdx(source, conversation).
			Emit([]byte("work")).Broadcast().Send(context.Background()); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}

	records := server.evidence(t)
	if len(records) != 3 {
		t.Fatalf("want every decision recorded, got %d", len(records))
	}
	chains := map[string]struct{}{}
	for _, record := range records {
		if record.PreviousDigest != nil {
			t.Error("an evicted head starts a new chain rather than continuing one")
		}
		chains[record.ChainID] = struct{}{}
	}
	if len(chains) != 3 {
		t.Errorf("want a chain per evicted conversation, got %d", len(chains))
	}
	if !laser.VerifyEvidenceChain(records) {
		t.Error("new chains must each verify")
	}
}

// A quorum composes independent voters, and every voter's own verdict is
// recorded so the audit trail names who said what.
func TestGivenAQuorumWhenEveryVoterAgreesThenShouldProceedAndNameThem(t *testing.T) {
	governor := laser.NewQuorumGovernor(laser.All()).
		Voter("safety", &scriptedGovernor{decision: laser.Allow()}, true).
		Voter("budget", &scriptedGovernor{decision: laser.Observe()}, false)
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	records := server.evidence(t)
	if len(records) != 1 {
		t.Fatalf("want one record, got %d", len(records))
	}
	// The strongest affirmative wins, so one voter wanting evidence recorded
	// outranks one that does not.
	if records[0].Decision != string(laser.VerdictObserve) {
		t.Errorf("want the observe recorded, got %q", records[0].Decision)
	}
	if records[0].Reason == nil ||
		!strings.Contains(*records[0].Reason, "safety=allow") ||
		!strings.Contains(*records[0].Reason, "budget=observe") {
		t.Errorf("want the whole ballot recorded, got %v", records[0].Reason)
	}
}

// A quorum combines what its voters said into the one decision the action
// runs under.
func TestGivenVotersWhenCombinedThenShouldApplyTheQuorumRule(t *testing.T) {
	allow := func() laser.ActionGovernor {
		return &scriptedGovernor{decision: laser.Allow()}
	}
	block := func() laser.ActionGovernor {
		return &scriptedGovernor{decision: laser.Block("no")}
	}
	stepUp := func() laser.ActionGovernor {
		return &scriptedGovernor{decision: laser.StepUp("scope")}
	}

	for name, want := range map[string]struct {
		quorum *laser.QuorumGovernor
		// kind is the verdict the quorum reaches. An affirmative one records
		// nothing when it is a plain allow, so the effect running is what
		// proves it.
		kind laser.VerdictKind
	}{
		"every voter under all": {
			quorum: laser.NewQuorumGovernor(laser.All()).
				Voter("a", allow(), false).Voter("b", allow(), false),
			kind: laser.VerdictAllow,
		},
		"one dissenter under all": {
			quorum: laser.NewQuorumGovernor(laser.All()).
				Voter("a", allow(), false).Voter("b", block(), false),
			kind: laser.VerdictBlock,
		},
		"one supporter under any": {
			quorum: laser.NewQuorumGovernor(laser.Any()).
				Voter("a", allow(), false).Voter("b", block(), false),
			kind: laser.VerdictAllow,
		},
		"the threshold met": {
			quorum: laser.NewQuorumGovernor(laser.AtLeast(2)).
				Voter("a", allow(), false).Voter("b", allow(), false).
				Voter("c", block(), false),
			kind: laser.VerdictAllow,
		},
		"the threshold missed": {
			quorum: laser.NewQuorumGovernor(laser.AtLeast(2)).
				Voter("a", allow(), false).Voter("b", block(), false).
				Voter("c", stepUp(), false),
			// The most actionable denial wins, so a block outranks a step up.
			kind: laser.VerdictBlock,
		},
		"a mandatory voter outvoted": {
			// A safety voter cannot be bypassed by a loose policy.
			quorum: laser.NewQuorumGovernor(laser.Any()).
				Voter("safety", block(), true).Voter("b", allow(), false),
			kind: laser.VerdictBlock,
		},
		"a mandatory voter stepping up": {
			quorum: laser.NewQuorumGovernor(laser.Any()).
				Voter("safety", stepUp(), true).Voter("b", allow(), false),
			kind: laser.VerdictStepUp,
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := &publishServer{}
			handle := governedHandle(server).
				WithGovernor(want.quorum, laser.GovernorObserve)

			if _, err := handle.Topic("events").Publish().
				Payload([]byte("work")).Key("k").
				Send(context.Background()); err != nil {
				t.Fatalf("publish: %v", err)
			}

			records := server.evidence(t)
			if want.kind == laser.VerdictAllow {
				if len(records) != 0 {
					t.Fatalf("an allow records nothing, got %+v", records)
				}
				if len(server.published(t)) != 1 {
					t.Error("an allowed effect must reach the log")
				}
				return
			}
			if len(records) != 1 {
				t.Fatalf("want one record, got %d", len(records))
			}
			if records[0].Decision != string(want.kind) {
				t.Errorf("want %q, got %q", want.kind, records[0].Decision)
			}
		})
	}
}

// A body replacement must survive the quorum, so it outranks a plain allow.
func TestGivenOneVoterModifyingWhenTheQuorumPassesThenShouldKeepTheReplacement(t *testing.T) {
	governor := laser.NewQuorumGovernor(laser.All()).
		Voter("redactor", &scriptedGovernor{
			decision: laser.Modify([]byte("redacted"))}, false).
		Voter("budget", &scriptedGovernor{decision: laser.Allow()}, false)
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("the card number")).Key("k").
		Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	published := server.published(t)
	if len(published) != 1 || string(published[0].Payload) != "redacted" {
		t.Fatalf("want the replacement on the log, got %q", published[0].Payload)
	}
}

// The strongest affirmative wins wherever it was enrolled, so a replacement
// is not lost to whichever voter happened to answer first.
func TestGivenAModifyEnrolledLastWhenTheQuorumPassesThenShouldStillReplace(t *testing.T) {
	governor := laser.NewQuorumGovernor(laser.All()).
		Voter("budget", &scriptedGovernor{decision: laser.Allow()}, false).
		Voter("redactor", &scriptedGovernor{
			decision: laser.Modify([]byte("redacted"))}, false)
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("the card number")).Key("k").
		Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	published := server.published(t)
	if len(published) != 1 || string(published[0].Payload) != "redacted" {
		t.Fatalf("want the replacement on the log, got %q", published[0].Payload)
	}
}

// Two voters replacing the body with different content have not agreed on
// anything, so neither replacement is applied.
func TestGivenConflictingReplacementsWhenCombinedThenShouldBlock(t *testing.T) {
	governor := laser.NewQuorumGovernor(laser.All()).
		Voter("one", &scriptedGovernor{decision: laser.Modify([]byte("a"))}, false).
		Voter("two", &scriptedGovernor{decision: laser.Modify([]byte("b"))}, false)
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background())

	var blocked *laser.PolicyBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("want a PolicyBlockedError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "conflicting body replacements") {
		t.Errorf("want the conflict named, got %q", err.Error())
	}
}

// A quorum that cannot decide anything blocks rather than letting everything
// through, because a misconfiguration must not read as approval.
func TestGivenAMisconfiguredQuorumWhenEvaluatedThenShouldBlock(t *testing.T) {
	allow := &scriptedGovernor{decision: laser.Allow()}

	for name, governor := range map[string]*laser.QuorumGovernor{
		"no voters at all": laser.NewQuorumGovernor(laser.All()),
		"a threshold of zero": laser.NewQuorumGovernor(laser.AtLeast(0)).
			Voter("a", allow, false),
		"a threshold past its voters": laser.NewQuorumGovernor(laser.AtLeast(3)).
			Voter("a", allow, false),
		"one voter enrolled twice": laser.NewQuorumGovernor(laser.All()).
			Voter("a", allow, false).Voter("a", allow, false),
		"a timeout of zero": laser.NewQuorumGovernor(laser.All()).
			Voter("a", allow, false).VoterTimeout(0),
	} {
		t.Run(name, func(t *testing.T) {
			server := &publishServer{}
			handle := governedHandle(server).
				WithGovernor(governor, laser.GovernorEnforce)

			_, err := handle.Topic("events").Publish().
				Payload([]byte("work")).Key("k").Send(context.Background())

			var blocked *laser.PolicyBlockedError
			if !errors.As(err, &blocked) {
				t.Fatalf("want a PolicyBlockedError, got %T: %v", err, err)
			}
		})
	}
}

// One hung voter must never stall every governed effect, so a voter is
// bounded: a mandatory one that times out blocks, any other abstains.
func TestGivenAHungVoterWhenPolledThenShouldBoundItAndDecideWithoutIt(t *testing.T) {
	for name, mandatory := range map[string]bool{
		"a mandatory voter": true,
		"an optional voter": false,
	} {
		t.Run(name, func(t *testing.T) {
			governor := laser.NewQuorumGovernor(laser.Any()).
				Voter("hung", &hangingGovernor{}, mandatory).
				Voter("live", &scriptedGovernor{decision: laser.Allow()}, false).
				VoterTimeout(20 * time.Millisecond)
			server := &publishServer{}
			handle := governedHandle(server).
				WithGovernor(governor, laser.GovernorEnforce)

			_, err := handle.Topic("events").Publish().
				Payload([]byte("work")).Key("k").Send(context.Background())

			var blocked *laser.PolicyBlockedError
			switch {
			case mandatory && !errors.As(err, &blocked):
				t.Fatalf("a mandatory voter that did not answer must block, got %v", err)
			case mandatory && !strings.Contains(err.Error(), "hung"):
				t.Errorf("want the voter named, got %q", err.Error())
			case !mandatory && err != nil:
				t.Fatalf("an optional voter that did not answer abstains, got %v", err)
			}
		})
	}
}

// hangingGovernor never answers until its caller gives up.
type hangingGovernor struct{}

func (governor *hangingGovernor) Decide(
	ctx context.Context, _ *laser.GovernedAction,
) (*laser.ActionDecision, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// A verdict this client does not know cannot be applied, and treating it as
// allow would run an effect the governor may have refused.
func TestGivenAnUnknownVerdictWhenAppliedThenShouldBlock(t *testing.T) {
	governor := &scriptedGovernor{decision: &laser.ActionDecision{
		Verdict: laser.PolicyVerdict{Kind: laser.VerdictKind("teleport")},
	}}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	_, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background())

	var blocked *laser.PolicyBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("want a PolicyBlockedError, got %T: %v", err, err)
	}
	if len(server.published(t)) != 0 {
		t.Error("an undecidable verdict must not run the effect")
	}
}

// The deciding policy pack and the risk estimate are recorded verbatim, so
// an auditor can tell which version of which rule decided.
func TestGivenAPolicyRefWhenRecordedThenShouldCarryItVerbatim(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Observe().
		WithPolicy(laser.PolicyRef{
			PackID: "egress", PackVersion: "2026.11.3",
			RuleIDs: []string{"no-pii", "no-secrets"},
		}).
		WithRiskScore(0.75)}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("work")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	records := server.evidence(t)
	if len(records) != 1 || records[0].Policy == nil {
		t.Fatalf("want the policy recorded, got %+v", records)
	}
	if records[0].Policy.PackVersion != "2026.11.3" ||
		len(records[0].Policy.RuleIDs) != 2 {
		t.Errorf("want the pack and rules verbatim, got %+v", records[0].Policy)
	}
	if records[0].RiskScore == nil || *records[0].RiskScore != 0.75 {
		t.Errorf("want the risk score recorded, got %v", records[0].RiskScore)
	}
}

// A request is governed as a request rather than as the command it is on the
// wire, so a policy can bound outstanding work without bounding every send.
func TestGivenARequestWhenGovernedThenShouldCountAsARequest(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Allow()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	source, _ := wire.NewAgentID("planner")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}

	// Nothing answers, so the request times out. What it sent is the point.
	_, _ = handle.Requester(source, "agent.sessions").Request(
		context.Background(), conversation, []byte("do it"),
		laser.RequestOptions{Timeout: 20 * time.Millisecond})

	asked := governor.asked()
	if len(asked) != 1 || asked[0].Kind != laser.ActionRequest {
		t.Fatalf("want a request, got %+v", asked)
	}
	counters := handle.GovernedCounters()
	if counters == nil || counters.Requests != 1 || counters.Sends != 0 {
		t.Errorf("want one request and no sends, got %+v", counters)
	}
}

// A raw publish is governed with the acting agent and conversation read off
// its own headers, so a policy sees who is publishing rather than only where.
func TestGivenAProvenancedPublishWhenGovernedThenShouldReadItsSpine(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Allow()}
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(governor, laser.GovernorEnforce)
	source, _ := wire.NewAgentID("planner")
	target, _ := wire.NewAgentID("executor")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 9})}
	correlation := "corr-1"

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("work")).
		Provenance(&laser.Provenance{
			Conversation:  conversation,
			Agent:         &source,
			TargetAgent:   &target,
			CorrelationID: &correlation,
		}).Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	asked := governor.asked()
	if len(asked) != 1 {
		t.Fatalf("want one decision, got %d", len(asked))
	}
	if asked[0].Source != "planner" || asked[0].Target != "executor" {
		t.Errorf("want the agents named, got %q and %q",
			asked[0].Source, asked[0].Target)
	}
	if asked[0].Conversation == nil || *asked[0].Conversation != conversation {
		t.Errorf("want the conversation named, got %v", asked[0].Conversation)
	}
	if asked[0].Correlation != correlation {
		t.Errorf("want the correlation named, got %q", asked[0].Correlation)
	}
}

// An approval gate asks a named approver and hands back the grant, which is
// the only way to hold one.
func TestGivenAnApprovalGateWhenTheApproverRepliesThenShouldMintTheGrant(t *testing.T) {
	server := &publishServer{}
	handle := governedHandle(server)
	asker, _ := wire.NewAgentID("planner")
	approver, _ := wire.NewAgentID("supervisor")
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})}
	requester := handle.Requester(asker, "agent.sessions")

	go func() {
		// Answer whatever the gate asked, once it has asked it.
		for {
			_, batches := server.sent()
			if len(batches) == 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			var asked wire.AgentEnvelope
			if wire.Unmarshal(batches[0][0].Payload, &asked) != nil {
				return
			}
			requester.Hub().Deliver(&laser.AgdxMessage{
				Envelope: wire.AgentEnvelope{
					Kind: wire.KindResponse, Conversation: conversation,
					Source: approver, Target: &asker,
					Correlation: asked.Correlation, Cause: asked.Record,
					Body: []byte("approved"),
				},
				Record: &laser.Record{
					Provenance: &laser.Provenance{Conversation: conversation}},
			})
			return
		}
	}()

	grant, err := requester.ApprovalGate(context.Background(), approver,
		conversation, []byte("may I refund 2000"), 2*time.Second)
	if err != nil {
		t.Fatalf("gate: %v", err)
	}

	if grant.Approver() != approver {
		t.Errorf("want the approver named, got %q", grant.Approver())
	}
	if grant.Conversation() != conversation {
		t.Error("a grant belongs to the conversation it was asked in")
	}
	if grant.Correlation().IsZero() {
		t.Error("a grant names the request it answered")
	}
	if string(grant.Decision()) != "approved" {
		t.Errorf("want the approver's own body, got %q", grant.Decision())
	}
	// The decision is copied out, so a caller cannot edit the grant it holds.
	grant.Decision()[0] = 'X'
	if string(grant.Decision()) != "approved" {
		t.Error("a grant must not hand out its own storage")
	}
}

// A gate that names no approver asks nobody, which would wait forever.
func TestGivenNoApproverWhenGatedThenShouldRefuseIt(t *testing.T) {
	handle := governedHandle(&publishServer{})
	asker, _ := wire.NewAgentID("planner")

	_, err := handle.Requester(asker, "agent.sessions").ApprovalGate(
		context.Background(), wire.AgentID{},
		wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 1})},
		[]byte("may I"), time.Second)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// A kind read back off the audit topic from a newer peer is visible as
// unrecognized rather than silently treated as one this client knows.
func TestGivenAnUnknownActionKindWhenReadThenShouldReportItUnrecognized(t *testing.T) {
	for _, known := range []laser.ActionKind{
		laser.ActionSend, laser.ActionPublish, laser.ActionRequest,
		laser.ActionCommand, laser.ActionResponse, laser.ActionEvent,
		laser.ActionStatus, laser.ActionError, laser.ActionMemoryWrite,
	} {
		if !known.IsNamed() {
			t.Errorf("%q is a kind this client writes", known)
		}
	}
	if laser.ActionKind("teleport").IsNamed() {
		t.Error("a kind this client does not know must not read as one it does")
	}
	// An AGDX chunk is part of the event it streams, so it is decided as one.
	if laser.ActionKindOf(wire.KindChunk) != laser.ActionEvent {
		t.Error("a chunk is governed as the event it belongs to")
	}
	if laser.ActionKindOf(wire.AgentKind("teleport")) != laser.ActionSend {
		t.Error("an unknown message kind is governed as a plain send")
	}
}

// A governed handle says so, which is what lets a caller tell a shadow
// rollout from an enforcing one without guessing.
func TestGivenAGovernedHandleWhenAskedThenShouldNameItsMode(t *testing.T) {
	handle := governedHandle(&publishServer{}).WithGovernor(
		&scriptedGovernor{decision: laser.Allow()}, laser.GovernorObserve)

	mode, governed := handle.Governor()
	if !governed || mode != laser.GovernorObserve {
		t.Errorf("want observe mode, got %q and %v", mode, governed)
	}
}
