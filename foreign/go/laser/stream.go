package laser

import (
	"context"
	"errors"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// Stream is one Iggy stream: a namespace of topics.
type Stream struct {
	laser *Laser
	name  string
}

// Topic is one topic within a stream, the log primitive's data handle.
//
// A handle is free to construct and carries no connection of its own, so it
// can be built per call.
type Topic struct {
	laser  *Laser
	stream string
	name   string
	// encryption seals every record this handle publishes, nil to publish
	// them as they are.
	encryption *Encryption
}

// WithEncryption derives a handle that seals each record's payload and
// user-header block before it is published.
//
// It is the producer-side half of the pair: a consumer opened with the same
// key reads them back, and so does every other Laser client and Apache
// Iggy's own encryptor, which all use this AES-256-GCM format. The server
// stores ciphertext and can neither filter nor index what it cannot read,
// which is the trade the key buys.
//
// A publish failure lists its records as the caller sent them, before
// encryption, so a report names what a caller recognises.
func (topic *Topic) WithEncryption(encryption *Encryption) *Topic {
	derived := *topic
	derived.encryption = encryption
	return &derived
}

// Stream selects a stream by name.
func (laser *Laser) Stream(name string) *Stream {
	return &Stream{laser: laser, name: name}
}

// Topic addresses a topic on the stream pinned at connect.
//
// Without a default stream there is nothing to address, so the call is refused
// with the remedy rather than guessing a stream.
func (laser *Laser) Topic(name string) *Topic {
	return &Topic{laser: laser, stream: laser.defaultStream, name: name}
}

// Name is the stream's name.
func (stream *Stream) Name() string { return stream.name }

// Topic addresses a topic on this stream.
func (stream *Stream) Topic(name string) *Topic {
	return &Topic{laser: stream.laser, stream: stream.name, name: name}
}

// Ensure creates the stream when it is absent and leaves an existing one as it
// is.
//
// Two callers racing to create one stream both succeed: the loser's create
// fails, and the stream it wanted is then there, which is the outcome it asked
// for.
func (stream *Stream) Ensure(ctx context.Context) error {
	client, err := stream.laser.connected()
	if err != nil {
		return err
	}
	identifier, err := iggcon.NewIdentifier(stream.name)
	if err != nil {
		return &InvalidError{Reason: "stream name: " + err.Error()}
	}
	switch _, err := client.GetStream(ctx, identifier); {
	case err == nil:
		return nil
	case !isMissingResource(err):
		return &TransportError{Reason: "read the stream", Cause: err}
	}
	if _, err := client.CreateStream(ctx, stream.name, &iggcon.StreamOptions{}); err != nil {
		// A concurrent create is not a failure: re-reading settles which
		// happened, and a stream that now exists is what the caller wanted.
		if _, probe := client.GetStream(ctx, identifier); probe == nil {
			return nil
		}
		return &TransportError{Reason: "create the stream", Cause: err}
	}
	return nil
}

// Delete removes the stream, reporting whether one was there to remove.
func (stream *Stream) Delete(ctx context.Context) (bool, error) {
	client, err := stream.laser.connected()
	if err != nil {
		return false, err
	}
	identifier, err := iggcon.NewIdentifier(stream.name)
	if err != nil {
		return false, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	if err := client.DeleteStream(ctx, identifier); err != nil {
		if isMissingResource(err) {
			stream.laser.forgetStream(stream.name)
			return false, nil
		}
		return false, &TransportError{Reason: "delete the stream", Cause: err}
	}
	stream.laser.forgetStream(stream.name)
	return true, nil
}

// Name is the topic's name.
func (topic *Topic) Name() string { return topic.name }

// StreamName is the stream the topic sits on.
func (topic *Topic) StreamName() string { return topic.stream }

// Ensure creates the topic with partitions when it is absent, creating the
// stream first if needed, and leaves an existing topic as it is.
//
// replicationFactor is how many nodes store each partition. Nil leaves the
// server's default, which is what a single-node deployment needs.
func (topic *Topic) Ensure(
	ctx context.Context, partitions uint32, replicationFactor *uint32,
) error {
	if topic.stream == "" {
		return errNoStream()
	}
	if partitions == 0 {
		return &InvalidError{Reason: "a topic needs at least one partition"}
	}
	if err := validateTopicReplication(replicationFactor); err != nil {
		return err
	}
	if err := topic.laser.Stream(topic.stream).Ensure(ctx); err != nil {
		return err
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return err
	}
	return topic.ensureOptions(ctx, streamID, topicID, true, &iggcon.TopicOptions{PartitionsCount: &partitions, ReplicationFactor: replicationFactor})
}

func validateTopicReplication(factor *uint32) error {
	if factor != nil && (*factor == 0 || *factor > 32) {
		return &InvalidError{Reason: "replication factor must be between 1 and 32"}
	}
	return nil
}

func (topic *Topic) ensureOptions(ctx context.Context, streamID, topicID iggcon.Identifier, create bool, options *iggcon.TopicOptions) error {
	client, err := topic.laser.connected()
	if err != nil {
		return err
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err == nil && details != nil {
		return nil
	}
	if err != nil && !isMissingResource(err) {
		return &TransportError{Reason: "read the topic", Cause: err}
	}
	if !create {
		if err == nil {
			err = ierror.TopicNameNotFound{TopicName: topic.name, StreamId: topic.stream}
		}
		return &TransportError{Reason: "read the topic", Cause: err}
	}
	if _, err := client.CreateTopic(ctx, streamID, topic.name, options); err != nil {
		// A concurrent caller can create the same topic after the initial read.
		current, readErr := client.GetTopic(ctx, streamID, topicID)
		if readErr != nil || current == nil {
			return &TransportError{Reason: "create the topic", Cause: err}
		}
	}
	return nil
}

// Delete removes the topic, reporting whether one was there to remove.
func (topic *Topic) Delete(ctx context.Context) (bool, error) {
	if topic.stream == "" {
		return false, errNoStream()
	}
	client, err := topic.laser.connected()
	if err != nil {
		return false, err
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return false, err
	}
	if err := client.DeleteTopic(ctx, streamID, topicID); err != nil {
		if isMissingResource(err) {
			topic.laser.forgetTopic(topic.stream, topic.name)
			return false, nil
		}
		return false, &TransportError{Reason: "delete the topic", Cause: err}
	}
	topic.laser.forgetTopic(topic.stream, topic.name)
	return true, nil
}

func (topic *Topic) identifiers() (iggcon.Identifier, iggcon.Identifier, error) {
	streamID, err := iggcon.NewIdentifier(topic.stream)
	if err != nil {
		return iggcon.Identifier{}, iggcon.Identifier{},
			&InvalidError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(topic.name)
	if err != nil {
		return iggcon.Identifier{}, iggcon.Identifier{},
			&InvalidError{Reason: "topic name: " + err.Error()}
	}
	return streamID, topicID, nil
}

// connected hands out the live client, refusing a handle that was closed
// rather than letting a nil connection surface as a panic.
func (laser *Laser) connected() (iggcon.Client, error) {
	laser.state().mutex.RLock()
	defer laser.state().mutex.RUnlock()
	if laser.state().client == nil {
		return nil, &TransportError{Reason: "the client is closed"}
	}
	return laser.state().client, nil
}

func errNoStream() error {
	return &InvalidError{Reason: "no default stream set: work in a scratch stream with " +
		"ScratchStream(prefix), or pick one with WithDefaultStream(name)"}
}

// isMissingResource reports the errors that mean "it is not there", which read
// as absence rather than as failure.
func isMissingResource(err error) bool {
	return errors.Is(err, ierror.ResourceNotFound{}) ||
		errors.Is(err, ierror.StreamIdNotFound{}) ||
		errors.Is(err, ierror.StreamNameNotFound{}) ||
		errors.Is(err, ierror.TopicIdNotFound{}) ||
		errors.Is(err, ierror.TopicNameNotFound{})
}
