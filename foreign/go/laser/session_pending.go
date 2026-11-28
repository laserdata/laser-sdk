package laser

import (
	"context"
	"sort"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// PendingControl reads the pause and cancel requests operators sent this session on agent.control, folding the retained records outside a runtime.
func (session *Session) PendingControl(ctx context.Context) (PendingControl, error) {
	if session.control != nil {
		if pending, known := session.control.Pending(session.conversation); known {
			return pending, nil
		}
	}
	records, err := session.controlRecords(ctx)
	if err != nil {
		return PendingControl{}, err
	}
	route, err := session.controlRoute(ctx)
	if err != nil {
		return PendingControl{}, err
	}
	if session.control != nil {
		return session.control.Install(session.conversation, records, route), nil
	}
	pending, _ := FoldControl(records, route)
	return pending, nil
}

// CancelRequested reports whether an operator asked to cancel this session, by a cancel request or a forced cancel.
func (session *Session) CancelRequested(ctx context.Context) (bool, error) {
	pending, err := session.PendingControl(ctx)
	return pending.CancelRequested, err
}

// controlRecords are this session's retained control requests as this handle's agent reads them, in log order.
func (session *Session) controlRecords(ctx context.Context) ([]ControlRecord, error) {
	records, err := session.Scope().read(ctx, wire.AgentControl)
	if err != nil {
		if isMissingResource(err) {
			return nil, nil
		}
		return nil, err
	}
	sort.SliceStable(records, func(left, right int) bool {
		first, second := records[left], records[right]
		if first.Message.Header.Timestamp != second.Message.Header.Timestamp {
			return first.Message.Header.Timestamp < second.Message.Header.Timestamp
		}
		if first.PartitionID != second.PartitionID {
			return first.PartitionID < second.PartitionID
		}
		return first.Offset() < second.Offset()
	})
	var requests []ControlRecord
	for _, record := range records {
		if record == nil || record.Provenance == nil ||
			record.Provenance.Conversation != session.conversation {
			continue
		}
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(record.Payload(), &envelope); err != nil {
			continue
		}
		request, made := ControlRequestOf(&envelope, session.agent)
		if !made {
			continue
		}
		requests = append(requests, ControlRecord{
			Conversation: session.conversation,
			At:           ControlPosition{Partition: record.PartitionID, Offset: record.Offset()},
			Request:      request,
		})
	}
	return requests, nil
}

// controlRoute is the agent.control partition this session's requests belong on, nil while the topic does not exist.
func (session *Session) controlRoute(ctx context.Context) (*uint32, error) {
	client := session.laser.Client()
	if client == nil {
		return nil, &InvalidError{Reason: "reading session control needs a connected client"}
	}
	stream, err := iggcon.NewIdentifier(session.config.Stream)
	if err != nil {
		return nil, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	topic, err := iggcon.NewIdentifier(wire.AgentControl)
	if err != nil {
		return nil, &InvalidError{Reason: "topic name: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, stream, topic)
	if err != nil {
		if isMissingResource(err) {
			return nil, nil
		}
		return nil, err
	}
	if details == nil {
		return nil, nil
	}
	route, known := ControlPartition(session.conversation, details.PartitionsCount)
	if !known {
		return nil, nil
	}
	return &route, nil
}
