package bridges

import (
	"context"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// mcpWaitReader is the consumer name a tool call's reply wait reads under. It
// stores no offset: the wait is a bounded live poll, not a subscription.
const mcpWaitReader = "laser-mcp-tool-wait"

// mcpWaitClockSlack covers clock skew between this bridge and the server: the
// poll starts a little before the call was published rather than exactly at
// it, so a reply the server stamped a moment earlier is still seen.
const mcpWaitClockSlack = 2 * time.Second

// mcpWaitInterval is how long a round with nothing new sleeps before trying
// again.
const mcpWaitInterval = 100 * time.Millisecond

// mcpWaitBatch bounds one poll.
const mcpWaitBatch = 100

// awaitReply polls the reply topic for the command's correlated answer until
// it arrives or deadline passes.
//
// There is no durable subscription behind a tools/call answer: the wait reads
// forward live from just before the command was sent, which is what keeps a
// synchronous call cheap on a topic that otherwise holds unrelated traffic.
func (bridge *MCPBridge) awaitReply(
	ctx context.Context, correlation wire.CorrelationID, deadline time.Time,
) (*wire.AgentEnvelope, error) {
	stream := bridge.laser.DefaultStream()
	if stream == "" {
		return nil, &BridgeError{Reason: "an MCP bridge requires a default stream"}
	}
	client := bridge.laser.Client()
	if client == nil {
		return nil, &BridgeError{Reason: "the MCP bridge is not connected"}
	}
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return nil, &BridgeError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(bridge.replyTopic)
	if err != nil {
		return nil, &BridgeError{Reason: "topic name: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		return nil, &BridgeError{Reason: "read the reply topic: " + err.Error()}
	}
	if details == nil {
		return nil, &BridgeError{Reason: "the MCP reply topic is not there"}
	}
	reader, err := iggcon.NewIdentifier(mcpWaitReader)
	if err != nil {
		return nil, &BridgeError{Reason: "reader name: " + err.Error()}
	}
	consumer := iggcon.NewSingleConsumer(reader)
	since := time.Now().Add(-mcpWaitClockSlack)
	cursors := make([]iggcon.PollingStrategy, details.PartitionsCount)
	for partition := range cursors {
		cursors[partition] = iggcon.TimestampPollingStrategy(uint64(since.UnixMicro()))
	}
	options := iggcon.DefaultPollOptions()
	options.Count = mcpWaitBatch

	for {
		for partition := uint32(0); partition < details.PartitionsCount; partition++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			polled, err := client.PollMessages(
				ctx, streamID, topicID, partition, consumer, cursors[partition], options)
			if err != nil {
				return nil, &BridgeError{Reason: "read the tool's reply: " + err.Error()}
			}
			if polled == nil || len(polled.Messages) == 0 {
				continue
			}
			for index := range polled.Messages {
				envelope := matchReply(partition, polled.Messages[index], correlation)
				if envelope != nil {
					return envelope, nil
				}
			}
			last := polled.Messages[len(polled.Messages)-1]
			cursors[partition] = iggcon.OffsetPollingStrategy(last.Header.Offset + 1)
		}
		if !time.Now().Before(deadline) {
			return nil, &laser.TimeoutError{Correlation: correlation, Waited: bridge.timeout}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(mcpWaitInterval):
		}
	}
}

// matchReply reads one record as the answer to this tool call: an AGDX
// response or error on the call's correlation.
func matchReply(
	partition uint32, message iggcon.IggyMessage, correlation wire.CorrelationID,
) *wire.AgentEnvelope {
	record, err := laser.DeliveredRecord(partition, message, iggcon.Checkpoint{}, false)
	if err != nil {
		return nil
	}
	opened, err := laser.OpenAgdxMessage(record)
	if err != nil || opened.Envelope == nil {
		return nil
	}
	envelope := opened.Envelope
	if envelope.Kind != wire.KindResponse && envelope.Kind != wire.KindError {
		return nil
	}
	if envelope.Correlation == nil || envelope.Correlation.ID != correlation.ID {
		return nil
	}
	return envelope
}
