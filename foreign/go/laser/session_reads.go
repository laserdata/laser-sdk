package laser

import (
	"context"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// readAtReader is the consumer name ReadAt polls under. It stores no offset:
// a point read is a bounded, single-message fetch, not a subscription.
const readAtReader = "laser-read-at"

// ReadAt reads the one record at names, checking that the topic still has
// the generation the reference recorded both before and after the poll, and
// that the returned record sits at the named offset. Nil, nil when the
// record is gone or the topic was recreated mid-read -- not an error, since
// a stale reference is an ordinary thing for a caller holding one from
// earlier to encounter. Only a message reference names a log record; any
// other SourceRef kind is refused.
func (laser *Laser) ReadAt(ctx context.Context, at wire.SourceRef) (*ContextMessage, error) {
	if at.Kind != wire.SourceMessage || at.Message == nil {
		return nil, &InvalidError{Reason: "only a message reference names a log record"}
	}
	ref := at.Message
	client := laser.Client()
	if client == nil {
		return nil, &InvalidError{Reason: "ReadAt needs a connected client"}
	}
	streamID, err := iggcon.NewIdentifier(ref.Stream)
	if err != nil {
		return nil, &InvalidError{Reason: "stream id: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(ref.Topic)
	if err != nil {
		return nil, &InvalidError{Reason: "topic id: " + err.Error()}
	}

	before, err := readAtMetadata(ctx, client, streamID, topicID)
	if err != nil {
		return nil, err
	}
	if before == nil || !readAtMatches(*before, ref) {
		return nil, nil
	}

	reader, err := iggcon.NewIdentifier(readAtReader)
	if err != nil {
		return nil, &InvalidError{Reason: "reader name: " + err.Error()}
	}
	consumer := iggcon.NewSingleConsumer(reader)
	options := iggcon.DefaultPollOptions()
	options.Count = 1
	polled, err := client.PollMessages(ctx, streamID, topicID, ref.Partition, consumer,
		iggcon.OffsetPollingStrategy(ref.Offset), options)
	if err != nil {
		return nil, &InvalidError{Reason: "read at: " + err.Error()}
	}
	var found *iggcon.IggyMessage
	if polled != nil {
		for index := range polled.Messages {
			if polled.Messages[index].Header.Offset == ref.Offset {
				found = &polled.Messages[index]
				break
			}
		}
	}
	if found == nil {
		return nil, nil
	}

	// Checked again after the poll: a topic recreated between the two reads
	// (same id reused, or genuinely a different one) must not be presented as
	// the one the reference was minted against.
	after, err := readAtMetadata(ctx, client, streamID, topicID)
	if err != nil {
		return nil, err
	}
	if after == nil || *after != *before {
		return nil, nil
	}

	record, err := DeliveredRecord(ref.Partition, *found, iggcon.Checkpoint{}, false)
	if err != nil {
		return nil, err
	}
	message := NewContextMessage(record, before.name)
	return &message, nil
}

// readAtSource is the topic metadata ReadAt snapshots before and after its
// poll, to detect a topic recreated mid-read.
type readAtSource struct {
	id         uint32
	generation uint64
	name       string
}

func readAtMetadata(
	ctx context.Context, client iggcon.Client, streamID, topicID iggcon.Identifier,
) (*readAtSource, error) {
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		return nil, &InvalidError{Reason: "read the topic: " + err.Error()}
	}
	if details == nil {
		return nil, nil
	}
	return &readAtSource{id: details.Id, generation: details.CreatedAt, name: details.Name}, nil
}

// readAtMatches reports whether source is still the topic ref was minted
// against: same numeric id, and the same generation when ref recorded one.
func readAtMatches(source readAtSource, ref *wire.MessageSource) bool {
	if source.id != ref.Topic {
		return false
	}
	if ref.Generation != nil && *ref.Generation != source.generation {
		return false
	}
	return true
}
