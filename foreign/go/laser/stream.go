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
			return false, nil
		}
		return false, &TransportError{Reason: "delete the stream", Cause: err}
	}
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
	if err := topic.laser.Stream(topic.stream).Ensure(ctx); err != nil {
		return err
	}
	client, err := topic.laser.connected()
	if err != nil {
		return err
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return err
	}
	switch _, err := client.GetTopic(ctx, streamID, topicID); {
	case err == nil:
		return nil
	case !isMissingResource(err):
		return &TransportError{Reason: "read the topic", Cause: err}
	}
	options := &iggcon.TopicOptions{
		PartitionsCount:   &partitions,
		ReplicationFactor: replicationFactor,
	}
	if _, err := client.CreateTopic(ctx, streamID, topic.name, options); err != nil {
		if _, probe := client.GetTopic(ctx, streamID, topicID); probe == nil {
			return nil
		}
		return &TransportError{Reason: "create the topic", Cause: err}
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
			return false, nil
		}
		return false, &TransportError{Reason: "delete the topic", Cause: err}
	}
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
	return &InvalidError{Reason: "no stream selected: pin one with " +
		"Options.DefaultStream or address it with Stream(name).Topic(name)"}
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
