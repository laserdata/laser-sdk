package bridges

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// lookupReader is the consumer name the task scan reads under. It stores no
// offset: a task lookup is a bounded replay rather than a subscription.
const lookupReader = "laser-a2a-task-lookup"

// The task scan bounds, the same as the Rust and TypeScript clients: 32
// passes of up to 10,000 records per partition, and five minutes of slack
// before the task id's own time to cover clock skew between the minting
// client and the server.
const (
	maxLookupPolls   = 32
	lookupBatch      = 10_000
	lookupClockSlack = 5 * time.Minute
)

// A2ABridge turns A2A requests into durable AGDX records.
//
// It owns no task state. Truth is the log: submit publishes a command, and a
// later lookup replays what the log holds, so a task survives a bridge
// restart and two bridges over one deployment answer alike.
type A2ABridge struct {
	laser        *laser.Laser
	source       wire.AgentID
	requestTopic string
	replyTopic   string

	target       *wire.AgentID
	publicURL    string
	description  string
	version      string
	capabilities []wire.CapabilityDescriptor
	signingKey   *laser.SigningKey
	verifier     *laser.KeyRegistry
	hops         []string
	binding      *taskBinding
	// allowedOrigins and callerFrom belong to the HTTP shell: the browser
	// origins the JSON-RPC endpoint serves, and how a request names its
	// authenticated caller.
	allowedOrigins []string
	callerFrom     func(*http.Request) (string, error)
}

// NewA2ABridge opens a bridge publishing commands on requestTopic and
// reading answers from replyTopic, as source.
//
// The two topics may be the same, which is the shared-lane shape: the scan
// then reads one topic rather than two.
func NewA2ABridge(
	client *laser.Laser, source wire.AgentID, requestTopic, replyTopic string,
) *A2ABridge {
	return &A2ABridge{
		laser: client, source: source,
		requestTopic: requestTopic, replyTopic: replyTopic,
		hops: []string{source.String()},
	}
}

// WithTarget addresses every task this bridge submits to one agent, so only
// that agent handles it on a shared session topic.
func (bridge *A2ABridge) WithTarget(agent wire.AgentID) *A2ABridge {
	derived := *bridge
	derived.target = &agent
	return &derived
}

// WithPublicURL names the endpoint the card advertises. The card is refused
// until one is set, because a discovery document nobody can dial is worse
// than none.
func (bridge *A2ABridge) WithPublicURL(url string) (*A2ABridge, error) {
	if url == "" {
		return nil, &BridgeError{Reason: "the A2A public URL must not be empty"}
	}
	derived := *bridge
	derived.publicURL = url
	return &derived, nil
}

// WithDescription and WithVersion fill the card's own fields.
func (bridge *A2ABridge) WithDescription(description string) *A2ABridge {
	derived := *bridge
	derived.description = description
	return &derived
}

func (bridge *A2ABridge) WithVersion(version string) *A2ABridge {
	derived := *bridge
	derived.version = version
	return &derived
}

// WithCapabilities advertises these capabilities as the card's skills.
func (bridge *A2ABridge) WithCapabilities(
	capabilities []wire.CapabilityDescriptor,
) *A2ABridge {
	derived := *bridge
	derived.capabilities = capabilities
	return &derived
}

// WithSigningKey signs every record this bridge publishes, and narrows what
// the task lookup accepts as its own command.
func (bridge *A2ABridge) WithSigningKey(key *laser.SigningKey) *A2ABridge {
	derived := *bridge
	derived.signingKey = key
	return &derived
}

// WithVerifier checks the signatures on the answers a lookup reads.
//
// Without one every answer on the reply topic counts, which is right for a
// trusted lane and wrong for a shared one: a verifier is what makes "the
// target answered" something the bridge checked rather than something the
// record claimed.
func (bridge *A2ABridge) WithVerifier(registry *laser.KeyRegistry) *A2ABridge {
	derived := *bridge
	derived.verifier = registry
	return &derived
}

// WithBridgeHops continues the hop path of work that already crossed other
// bridges, for the whole bridge. A path that already holds this bridge is a
// loop and is refused here rather than at the first publish.
func (bridge *A2ABridge) WithBridgeHops(previous []string) (*A2ABridge, error) {
	hops, err := CallHops(bridge.hops, previous)
	if err != nil {
		return nil, err
	}
	derived := *bridge
	derived.hops = hops
	return &derived, nil
}

// Submit publishes the params as a new task and answers with it.
//
// The whole params JSON rides byte-identical as the command body, so an
// agent reads exactly what the A2A client sent. The task's identity is its
// conversation, and the correlation is derived from it, which is what keeps
// a later lookup stateless.
func (bridge *A2ABridge) Submit(
	ctx context.Context, paramsJSON []byte,
) (*A2ATask, error) {
	return bridge.submit(ctx, paramsJSON, bridge.target, bridge.hops)
}

// SubmitTo addresses one task to one agent, overriding WithTarget.
func (bridge *A2ABridge) SubmitTo(
	ctx context.Context, target wire.AgentID, paramsJSON []byte,
) (*A2ATask, error) {
	return bridge.submit(ctx, paramsJSON, &target, bridge.hops)
}

// SubmitWithHops submits work that already crossed other bridges, carrying
// its hop path with this bridge appended.
func (bridge *A2ABridge) SubmitWithHops(
	ctx context.Context, previous []string, target *wire.AgentID, paramsJSON []byte,
) (*A2ATask, error) {
	hops, err := CallHops(bridge.hops, previous)
	if err != nil {
		return nil, err
	}
	if target == nil {
		target = bridge.target
	}
	return bridge.submit(ctx, paramsJSON, target, hops)
}

func (bridge *A2ABridge) submit(
	ctx context.Context, paramsJSON []byte, target *wire.AgentID, hops []string,
) (*A2ATask, error) {
	return bridge.submitFor(ctx, paramsJSON, target, hops, "")
}

// submitFor publishes a task owned by caller, empty for an unbound bridge.
//
// The id is minted before the command is published, so a binding that cannot
// be made leaves nothing on the log for an id nobody can open.
func (bridge *A2ABridge) submitFor(
	ctx context.Context, paramsJSON []byte, target *wire.AgentID,
	hops []string, caller string,
) (*A2ATask, error) {
	return bridge.submitAddressed(ctx, paramsJSON, target, hops, caller, nil, nil)
}

// SubmitIn submits a task as a child session of parent, in the tree rooted at
// root: the task's submitted start lands on agent.sessions with the ancestry
// before the command, and the command carries it too. The agent that
// handles the task ends the child. The command is addressed like Submit.
func (bridge *A2ABridge) SubmitIn(
	ctx context.Context, parent, root wire.ConversationID, paramsJSON []byte,
) (*A2ATask, error) {
	return bridge.submitAddressed(
		ctx, paramsJSON, bridge.target, bridge.hops, "", &parent, &root)
}

// SubmitInTo is SubmitIn addressed to target.
func (bridge *A2ABridge) SubmitInTo(
	ctx context.Context, target wire.AgentID, parent, root wire.ConversationID,
	paramsJSON []byte,
) (*A2ATask, error) {
	return bridge.submitAddressed(
		ctx, paramsJSON, &target, bridge.hops, "", &parent, &root)
}

// submitAddressed is Submit's common path. ancestry is nil for a task that
// opens no child session.
func (bridge *A2ABridge) submitAddressed(
	ctx context.Context, paramsJSON []byte, target *wire.AgentID,
	hops []string, caller string, parent, root *wire.ConversationID,
) (*A2ATask, error) {
	if len(paramsJSON) == 0 {
		return nil, &BridgeError{Reason: "the A2A params must not be empty"}
	}
	task := laser.NewConversationID()
	// Minted before anything is published, so an unbound caller on a binding
	// bridge publishes nothing.
	id, err := bridge.mintTaskID(ctx, task, caller)
	if err != nil {
		return nil, err
	}
	if parent != nil && root != nil {
		if _, err := bridge.laser.Sessions().OpenChild(
			ctx, task, bridge.source, bridge.source, *parent, *root,
		); err != nil {
			return nil, err
		}
	}
	if err := bridge.publishCommand(
		ctx, task, paramsJSON, target, hops, parent, root,
	); err != nil {
		return nil, err
	}
	return &A2ATask{
		ID:        id,
		ContextID: ContextIDOf(paramsJSON, task),
		Status:    A2ATaskStatus{State: a2aSubmitted},
	}, nil
}

func (bridge *A2ABridge) publishCommand(
	ctx context.Context, task wire.ConversationID, body []byte,
	target *wire.AgentID, hops []string, parent, root *wire.ConversationID,
) error {
	producer := bridge.laser.
		AgdxOn(bridge.laser.DefaultStream(), bridge.requestTopic, bridge.source, task)
	if parent != nil && root != nil {
		producer = producer.WithAncestry(*parent, *root)
	}
	send := producer.
		Command(correlationOf(task), body).
		WithOperation(wire.OperationChat).
		ContentType(wire.ContentJSON).
		WithMetadata(BridgeHopsMetaKey, HopsMetadata(hops))
	if target != nil {
		send = send.WithTarget(*target)
	}
	if bridge.signingKey != nil {
		send = send.Signed(bridge.signingKey)
	}
	if _, err := send.Send(ctx); err != nil {
		return err
	}
	return nil
}

// Task reads what the log says about one task, read forward from the time
// its id was minted.
//
// A task with no answer yet is working, and one whose bounded scan could not
// reach the end of the topic is unknown rather than working: the answer may
// already be there, and saying "working" would be a claim the scan did not
// check. An id this bridge published no command for is refused.
func (bridge *A2ABridge) Task(ctx context.Context, id string) (*A2ATask, error) {
	return bridge.taskFor(ctx, id, "")
}

func (bridge *A2ABridge) taskFor(
	ctx context.Context, id, caller string,
) (*A2ATask, error) {
	conversation, err := bridge.openTaskID(ctx, id, caller)
	if err != nil {
		return nil, err
	}
	view, err := bridge.view(ctx, conversation, id)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return nil, &BridgeError{Reason: fmt.Sprintf("unknown task %q", id)}
	}
	return view.task, nil
}

// Cancel publishes the canceled terminal for a task still in progress.
//
// A task that already ended is refused rather than overwritten: a terminal
// state is a fact on the log, and a second one after it would make the task
// read differently depending on which record a reader saw last.
func (bridge *A2ABridge) Cancel(ctx context.Context, id string) (*A2ATask, error) {
	return bridge.cancelFor(ctx, id, "")
}

func (bridge *A2ABridge) cancelFor(
	ctx context.Context, id, caller string,
) (*A2ATask, error) {
	// Opened before anything is published, so an id this caller cannot open
	// has no effect at all.
	conversation, err := bridge.openTaskID(ctx, id, caller)
	if err != nil {
		return nil, err
	}
	view, err := bridge.view(ctx, conversation, id)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return nil, &BridgeError{Reason: fmt.Sprintf("unknown task %q", id)}
	}
	if view.terminal {
		return nil, &A2ARejectedError{Reason: "the task already ended"}
	}
	return bridge.publishCancel(ctx, conversation, id, view.task.ContextID)
}

// cancelMessage is why the terminal was written, carried on the record.
var cancelMessage = "canceled by the A2A client"

func (bridge *A2ABridge) publishCancel(
	ctx context.Context, conversation wire.ConversationID, id, contextID string,
) (*A2ATask, error) {
	send := bridge.laser.
		AgdxOn(bridge.laser.DefaultStream(), bridge.replyTopic, bridge.source, conversation).
		Fail(correlationOf(conversation), wire.AgentErrorBody{
			Code:      wire.ErrorCancelled,
			Message:   &cancelMessage,
			Retryable: false,
		}).
		WithTaskState(wire.TaskCanceled).
		WithMetadata(BridgeHopsMetaKey, HopsMetadata(bridge.hops))
	if bridge.signingKey != nil {
		send = send.Signed(bridge.signingKey)
	}
	if _, err := send.Send(ctx); err != nil {
		return nil, err
	}
	return &A2ATask{ID: id, ContextID: contextID,
		Status: A2ATaskStatus{State: a2aCanceled}}, nil
}

// A2ARejectedError reports a request the task's own state refuses, which is
// the A2A "task not cancelable" case.
type A2ARejectedError struct{ Reason string }

func (e *A2ARejectedError) Error() string { return e.Reason }

// correlationOf is the correlation a task's conversation implies.
//
// Deriving it rather than minting one is what makes a lookup stateless: a
// bridge holding no table can still find the answers to a task it is handed
// the id of.
func correlationOf(conversation wire.ConversationID) wire.CorrelationID {
	return wire.CorrelationID{ID: conversation.ID}
}

// openTask reads the conversation a task id names.
func openTask(id string) (wire.ConversationID, error) {
	parsed, err := wire.ParseID(id)
	if err != nil {
		return wire.ConversationID{}, &BridgeError{
			Reason: fmt.Sprintf("invalid task id %q: %v", id, err)}
	}
	return wire.ConversationID{ID: parsed}, nil
}

// taskView is what the log says about one task.
type taskView struct {
	task     *A2ATask
	terminal bool
	command  *wire.AgentEnvelope
}

// taskRecords are the bridge's own command for a task and every answer to it.
type taskRecords struct {
	command *wire.AgentEnvelope
	answers []taskAnswer
	// complete reports that every partition was read to its end. A scan that
	// stopped short cannot call a task unanswered.
	complete bool
}

type taskAnswer struct {
	envelope *wire.AgentEnvelope
	// verified reports a signature this bridge's verifier accepted, and
	// signer names who it accepted.
	verified bool
	refused  bool
	signer   string
	keyID    []byte
}

func (bridge *A2ABridge) view(
	ctx context.Context, conversation wire.ConversationID, id string,
) (*taskView, error) {
	records, err := bridge.scanTask(ctx, conversation)
	if err != nil {
		return nil, err
	}
	if records.command == nil && len(records.answers) == 0 && records.complete {
		return nil, nil
	}
	contextID := conversation.String()
	if records.command != nil {
		contextID = ContextIDOf(records.command.Body, conversation)
	}
	target := bridge.answeringTarget(records.command)
	var settled *taskAnswer
	for index := range records.answers {
		answer := &records.answers[index]
		if !bridge.accepts(answer, target) {
			continue
		}
		if answerTerminal(answer.envelope) {
			settled = answer
			break
		}
		settled = answer
	}
	if settled != nil {
		task := TaskFromEnvelope(id, contextID, settled.envelope)
		return &taskView{
			task: &task, terminal: answerTerminal(settled.envelope),
			command: records.command,
		}, nil
	}
	// A scan that did not reach the end of the topic may have missed the
	// answer, so the caller is asked to look again rather than told the task
	// is still working.
	state := a2aWorking
	if !records.complete {
		state = a2aUnspecified
	}
	return &taskView{
		task:    &A2ATask{ID: id, ContextID: contextID, Status: A2ATaskStatus{State: state}},
		command: records.command,
	}, nil
}

// answeringTarget is the agent whose answer settles the task: the one the
// command addressed, else the bridge's own target.
func (bridge *A2ABridge) answeringTarget(command *wire.AgentEnvelope) string {
	if command != nil && command.Target != nil {
		return command.Target.String()
	}
	if bridge.target != nil {
		return bridge.target.String()
	}
	return ""
}

// accepts reports whether an answer may settle the task.
//
// Without a verifier every answer counts, because nothing was checked and
// pretending otherwise would be theatre. With one, a refused signature never
// counts, and a verified one counts when the task was a broadcast, when it
// came from the task's target, or when it is this bridge's own cancel.
func (bridge *A2ABridge) accepts(answer *taskAnswer, target string) bool {
	if bridge.verifier == nil {
		return true
	}
	if answer.refused {
		return false
	}
	if !answer.verified {
		return false
	}
	if target == "" || answer.signer == target {
		return true
	}
	return bridge.signingKey != nil &&
		string(answer.keyID) == string(bridge.signingKey.KeyID())
}

// answerTerminal reports an answer that ends the task.
//
// Delegates to AnswerState rather than keeping its own, narrower rule: that
// function already says a plain KindResponse with no explicit TaskState
// completes the task (the Status a caller reading Task() sees), so this must
// agree the same answer ends it -- a mismatch here let a finished task's
// cancel through instead of refusing it, since an ordinary response (which
// is what most agents send) was reported complete by one path and still
// cancelable by the other.
func answerTerminal(envelope *wire.AgentEnvelope) bool {
	return AnswerState(envelope).IsTerminal()
}

var errNoStream = errors.New("an A2A bridge requires a default stream")

// scanTask reads the bridge's command for a task and every answer to it,
// forward from the time the task id was minted.
func (bridge *A2ABridge) scanTask(
	ctx context.Context, task wire.ConversationID,
) (*taskRecords, error) {
	records := &taskRecords{complete: true}
	stream := bridge.laser.DefaultStream()
	if stream == "" {
		return nil, &BridgeError{Reason: errNoStream.Error()}
	}
	since := taskSince(task)
	topics := []string{bridge.requestTopic, bridge.replyTopic}
	if bridge.requestTopic == bridge.replyTopic {
		topics = topics[:1]
	}
	for _, topic := range topics {
		if err := bridge.scanTopic(ctx, stream, topic, since,
			correlationOf(task), records); err != nil {
			return nil, err
		}
	}
	return records, nil
}

// taskSince is where the scan starts: the task id's own time less the clock
// slack, so a record stamped slightly before the id was minted is still read.
func taskSince(task wire.ConversationID) time.Time {
	// A ULID's first 48 bits are its millisecond timestamp, big-endian.
	octets := task.ID.Bytes()
	var millis uint64
	for _, octet := range octets[:6] {
		millis = millis<<8 | uint64(octet)
	}
	return time.UnixMilli(int64(millis)).Add(-lookupClockSlack)
}

func (bridge *A2ABridge) scanTopic(
	ctx context.Context, stream, topic string, since time.Time,
	correlation wire.CorrelationID, records *taskRecords,
) error {
	client := bridge.laser.Client()
	if client == nil {
		return &BridgeError{Reason: "the A2A bridge is not connected"}
	}
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return &BridgeError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(topic)
	if err != nil {
		return &BridgeError{Reason: "topic name: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil || details == nil {
		// A topic that is not there holds no answers, which is what an
		// unknown task looks like.
		return nil
	}
	reader, err := iggcon.NewIdentifier(lookupReader)
	if err != nil {
		return &BridgeError{Reason: "reader name: " + err.Error()}
	}
	consumer := iggcon.NewSingleConsumer(reader)
	for partition := uint32(0); partition < details.PartitionsCount; partition++ {
		if err := bridge.scanPartition(ctx, client, streamID, topicID,
			consumer, partition, since, correlation, records); err != nil {
			return err
		}
	}
	return nil
}

func (bridge *A2ABridge) scanPartition(
	ctx context.Context, client iggcon.Client,
	streamID, topicID iggcon.Identifier, consumer iggcon.Consumer,
	partition uint32, since time.Time,
	correlation wire.CorrelationID, records *taskRecords,
) error {
	options := iggcon.DefaultPollOptions()
	options.Count = lookupBatch
	start := iggcon.TimestampPollingStrategy(uint64(since.UnixMicro()))
	caughtUp := false
	for poll := 0; poll < maxLookupPolls; poll++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		polled, err := client.PollMessages(
			ctx, streamID, topicID, partition, consumer, start, options)
		if err != nil {
			return &BridgeError{Reason: "read the task's records: " + err.Error()}
		}
		if polled == nil || len(polled.Messages) == 0 {
			caughtUp = true
			break
		}
		for index := range polled.Messages {
			bridge.collect(partition, polled.Messages[index], correlation, records)
		}
		last := polled.Messages[len(polled.Messages)-1]
		start = iggcon.OffsetPollingStrategy(last.Header.Offset + 1)
	}
	if !caughtUp {
		records.complete = false
	}
	return nil
}

// collect keeps the records of one task: the bridge's own command, and every
// answer carrying its correlation.
func (bridge *A2ABridge) collect(
	partition uint32, message iggcon.IggyMessage,
	correlation wire.CorrelationID, records *taskRecords,
) {
	record, err := laser.DeliveredRecord(partition, message, iggcon.Checkpoint{}, false)
	if err != nil {
		// A record this bridge cannot read is not this task's: a lookup
		// reports what it found rather than failing on someone else's bytes.
		return
	}
	opened, err := laser.OpenAgdxMessage(record)
	if err != nil || opened.Envelope == nil {
		return
	}
	envelope := opened.Envelope
	if envelope.Correlation == nil || envelope.Correlation.ID != correlation.ID {
		return
	}
	switch envelope.Kind {
	case wire.KindCommand:
		if records.command == nil && bridge.authored(envelope) {
			records.command = envelope
		}
	case wire.KindResponse, wire.KindError:
		records.answers = append(records.answers, bridge.answerOf(envelope))
	}
}

// authored reports a command as this bridge's own: its source, and under a
// signing key a signature by that key.
func (bridge *A2ABridge) authored(envelope *wire.AgentEnvelope) bool {
	if envelope.Source != bridge.source {
		return false
	}
	if bridge.signingKey == nil {
		return true
	}
	return envelope.Signature != nil &&
		string(envelope.Signature.KeyID) == string(bridge.signingKey.KeyID())
}

// answerOf classifies one answer's authorship.
func (bridge *A2ABridge) answerOf(envelope *wire.AgentEnvelope) taskAnswer {
	answer := taskAnswer{envelope: envelope, signer: envelope.Source.String()}
	if bridge.verifier == nil || envelope.Signature == nil {
		return answer
	}
	answer.keyID = envelope.Signature.KeyID
	principal, err := bridge.verifier.Verify(*envelope)
	if err != nil {
		answer.refused = true
		return answer
	}
	// The principal the key is enrolled under is who answered, not the
	// source the record claims: an unsigned claim is what the verifier is
	// there to replace.
	answer.verified, answer.signer = true, principal
	return answer
}

// Card is the bridge's A2A Agent Card.
//
// Streaming is never advertised: AGDX chunk channels ride the log and are
// read over Apache Iggy's transport, so a client told to expect SSE would
// wait for something that never comes.
func (bridge *A2ABridge) Card() (*AgentCard, error) {
	if bridge.publicURL == "" {
		return nil, &BridgeError{
			Reason: "the A2A card needs a public URL, set it with WithPublicURL"}
	}
	skills := make([]AgentSkill, 0, len(bridge.capabilities))
	for _, capability := range bridge.capabilities {
		skills = append(skills, AgentSkill{
			ID:          capability.SkillID,
			Name:        capability.SkillID,
			Tags:        []string{},
			InputModes:  contentModes(capability.Input),
			OutputModes: contentModes(capability.Output),
		})
	}
	return &AgentCard{
		Name:        bridge.source.String(),
		Description: bridge.description,
		Version:     bridge.version,
		SupportedInterfaces: []AgentInterface{{
			URL:             bridge.publicURL,
			ProtocolBinding: A2AJSONRPCBinding,
			ProtocolVersion: A2AProtocolVersion,
		}},
		Capabilities:       AgentCardCapabilities{},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		Skills:             skills,
	}, nil
}

func contentModes(reference *wire.ContentRef) []string {
	if reference == nil {
		return nil
	}
	return []string{ContentRefMode(*reference)}
}
