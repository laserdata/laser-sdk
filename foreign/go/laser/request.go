package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultRequestTimeout bounds a request whose caller names none.
const DefaultRequestTimeout = 30 * time.Second

// RequestOptions configure one request.
type RequestOptions struct {
	// Timeout bounds the wait for a reply. Zero takes DefaultRequestTimeout.
	//
	// It bounds a reply that never comes rather than a handler that is slow:
	// a target reporting progress extends it.
	Timeout time.Duration
	// ReplyTopic is where the answer is expected. Empty expects it on the
	// topic the request was sent to, which is the usual arrangement on a
	// shared session topic.
	ReplyTopic string
	// Target addresses one agent. The zero value broadcasts, so whichever
	// member of a consumer group picks the work up answers.
	Target wire.AgentID
	// Correlation pairs the request with its reply. The zero value mints one,
	// which is what a caller that is not resuming an exchange wants.
	Correlation wire.CorrelationID
	// Deadline is the drop-dead time stamped on the request, so a consumer
	// past it dead-letters the work rather than starting it. Zero stamps the
	// request's own timeout as its deadline, since a caller that has stopped
	// waiting does not want the work done either.
	Deadline time.Time
	// Grant is an approval this request carries, so a governor's step-up in
	// the grant's own conversation is answered by it rather than raised
	// again.
	Grant *ApprovalGrant
	// Provenance, when set, is the spine the request is sent under: its
	// conversation and ancestry replace the caller's, so a handler asks in a
	// subconversation it derived.
	Provenance *Provenance
}

// Requester sends a request and waits for its answer.
//
// It holds the reply hub, so one requester reads a reply topic once for every
// request in flight rather than each request scanning the topic. Build one per
// topic a caller requests on and share it.
type Requester struct {
	laser  *Laser
	hub    *ReplyHub
	stream string
	topic  string
	source wire.AgentID
}

// Requester opens a request surface for one agent on one topic.
func (laser *Laser) Requester(source wire.AgentID, topic string) *Requester {
	return &Requester{
		laser:  laser,
		hub:    NewReplyHub(),
		stream: laser.defaultStream,
		topic:  topic,
		source: source,
	}
}

// RequesterOn is Requester on a named stream.
func (laser *Laser) RequesterOn(stream, topic string, source wire.AgentID) *Requester {
	requester := laser.Requester(source, topic)
	requester.stream = stream
	return requester
}

// Verifier arms this requester's replies against forged signers: with
// verifier set, Request never settles on an unsigned, unverifiable, or
// wrong-signer reply, waiting out its timeout instead.
func (requester *Requester) Verifier(verifier *KeyRegistry) *Requester {
	requester.hub.SetVerifier(verifier)
	return requester
}

// Hub is the reply hub this requester waits on.
//
// A caller feeds it from whatever reads the reply topic, which is how one read
// serves every request in flight. Deliver returns false for a message that
// answers nothing, so a reader can pass everything through it and act on what
// is left.
func (requester *Requester) Hub() *ReplyHub { return requester.hub }

// Request sends a command on one conversation and waits for its answer.
//
// The wait is registered before the command is sent, because the reply cannot
// exist until it is: registering afterwards could miss an answer that arrived
// in between.
//
// Nothing drives the hub on its own, so a caller must be reading the reply
// topic and passing messages to Hub().Deliver. Deliver returns false for
// anything that answers no waiting request, so one reader can serve both the
// requests and the rest of the traffic.
func (requester *Requester) Request(
	ctx context.Context, conversation wire.ConversationID, body []byte,
	options RequestOptions,
) (*AgdxMessage, error) {
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	correlation := options.Correlation
	if correlation.IsZero() {
		correlation = wire.CorrelationID{ID: wire.IDFromBytes(NewOperationID())}
	}

	if options.Provenance != nil {
		conversation = options.Provenance.Conversation
	}
	expected := ExpectedReply{Session: conversation, Requester: requester.source}
	if !options.Target.IsZero() {
		// A directed request's answer is expected from the agent it was sent
		// to; a broadcast's is not, since whichever group member picks the
		// work up is the one who may answer it.
		expected.ExpectedSigner = options.Target.String()
	}
	ticket, err := requester.hub.Subscribe(correlation, expected)
	if err != nil {
		return nil, err
	}

	agdx := requester.laser.AgdxOn(
		requester.stream, requester.topic, requester.source, conversation)
	if spine := options.Provenance; spine != nil {
		agdx.parent, agdx.root = spine.ParentConversation, spine.RootConversation
	}
	send := agdx.Command(correlation, body)
	if !options.Target.IsZero() {
		send = send.WithTarget(options.Target)
	} else {
		// A request nobody is addressed by is for whichever member of the
		// group picks it up, which every record on a shared topic must say.
		send = send.Broadcast()
	}
	deadline := options.Deadline
	if deadline.IsZero() {
		// A caller that has stopped waiting does not want the work done
		// either, so the request expires when the wait does.
		deadline = time.Now().Add(timeout)
	}
	send = send.WithDeadline(deadline).governedAs(ActionRequest)
	if options.Grant != nil {
		send = send.WithGrant(options.Grant)
	}

	receipt, err := send.Send(ctx)
	if err != nil {
		// The wait is released here rather than left for the deadline: a
		// request that never left should not hold a correlation.
		ticket.Close()
		return nil, err
	}
	// The answer names the command it answers, so the wait can refuse a reply
	// that belongs to another exchange on the same correlation.
	ticket.Expect(receipt.Record)

	return ticket.Await(ctx, timeout)
}
