package laser

import (
	"context"
	"math"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// StreamInfo is one stream as the server reports it.
type StreamInfo struct {
	// ID is the native numeric stream id.
	ID              uint32
	Name            string
	TopicsCount     uint32
	SizeBytes       uint64
	MessagesCount   uint64
	CreatedAtMicros uint64
}

// TopicInfo is one topic as the server reports it.
type TopicInfo struct {
	// ID is the native numeric topic id.
	ID              uint32
	Name            string
	PartitionsCount uint32
	// MessageExpiry is how long a message is kept, zero when messages never
	// expire or the server reported no expiry of its own.
	MessageExpiry time.Duration
	// MaxSizeBytes is the retained size cap, zero when the topic is
	// unlimited or the server reported no cap of its own.
	MaxSizeBytes    uint64
	SizeBytes       uint64
	MessagesCount   uint64
	CreatedAtMicros uint64
}

// PartitionInfo is one partition of a topic as the server reports it.
type PartitionInfo struct {
	ID            uint32
	SegmentsCount uint32
	// CurrentOffset is the offset of the newest message, zero for an empty
	// partition.
	CurrentOffset   uint64
	SizeBytes       uint64
	MessagesCount   uint64
	CreatedAtMicros uint64
}

// ConsumerGroupSummary is one consumer group of a topic as the topic listing
// reports it.
type ConsumerGroupSummary struct {
	// ID is the native numeric group id.
	ID              uint32
	Name            string
	PartitionsCount uint32
	MembersCount    uint32
}

// UpdateTopicOptions are the changes Topic.Update applies. A field left unset
// keeps its value.
type UpdateTopicOptions struct {
	// Name renames the topic.
	Name string
	// ExpireAfter keeps each message this long.
	ExpireAfter time.Duration
	// NeverExpire keeps messages until the size cap removes them.
	NeverExpire bool
	// MaxTopicBytes caps the retained size of the topic.
	MaxTopicBytes uint64
	// UnlimitedTopicSize removes the size cap.
	UnlimitedTopicSize bool
}

// patch turns the options into the update the server reads.
//
// An expiry and a never-expire, or a cap and an unlimited size, are refused
// together rather than one silently winning: the caller asked for two
// different things and only it knows which it meant.
func (options UpdateTopicOptions) patch() (iggcon.TopicUpdate, error) {
	update := iggcon.TopicUpdate{}
	if options.Name != "" {
		name := options.Name
		update.Name = &name
	}
	switch {
	case options.ExpireAfter > 0 && options.NeverExpire:
		return update, &InvalidError{
			Reason: "set ExpireAfter or NeverExpire, not both"}
	case options.ExpireAfter > 0:
		expiry := iggcon.Duration(options.ExpireAfter.Microseconds())
		update.MessageExpiry = &expiry
	case options.NeverExpire:
		expiry := iggcon.IggyExpiryNeverExpire
		update.MessageExpiry = &expiry
	}
	switch {
	case options.MaxTopicBytes > 0 && options.UnlimitedTopicSize:
		return update, &InvalidError{
			Reason: "set MaxTopicBytes or UnlimitedTopicSize, not both"}
	case options.MaxTopicBytes > 0:
		bound := options.MaxTopicBytes
		update.MaxTopicSize = &bound
	case options.UnlimitedTopicSize:
		unlimited := uint64(math.MaxUint64)
		update.MaxTopicSize = &unlimited
	}
	return update, nil
}

// Streams is every stream this connection's user can read.
func (laser *Laser) Streams(ctx context.Context) ([]StreamInfo, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	streams, err := client.GetStreams(ctx)
	if err != nil {
		return nil, &TransportError{Reason: "list the streams", Cause: err}
	}
	listed := make([]StreamInfo, 0, len(streams))
	for _, stream := range streams {
		listed = append(listed, streamInfoOf(stream))
	}
	return listed, nil
}

// Get is this stream as the server reports it, nil when it does not exist.
func (stream *Stream) Get(ctx context.Context) (*StreamInfo, error) {
	client, err := stream.laser.connected()
	if err != nil {
		return nil, err
	}
	identifier, err := iggcon.NewIdentifier(stream.name)
	if err != nil {
		return nil, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	details, err := client.GetStream(ctx, identifier)
	switch {
	case isMissingResource(err):
		return nil, nil
	case err != nil:
		return nil, &TransportError{Reason: "read the stream", Cause: err}
	case details == nil:
		return nil, nil
	}
	info := streamInfoOf(details.Stream)
	return &info, nil
}

// Exists reports whether this stream exists.
func (stream *Stream) Exists(ctx context.Context) (bool, error) {
	info, err := stream.Get(ctx)
	return info != nil, err
}

// Rename renames this stream. Address it by its new name afterwards.
func (stream *Stream) Rename(ctx context.Context, name string) error {
	if name == "" {
		return &InvalidError{Reason: "a stream needs a name"}
	}
	client, err := stream.laser.connected()
	if err != nil {
		return err
	}
	identifier, err := iggcon.NewIdentifier(stream.name)
	if err != nil {
		return &InvalidError{Reason: "stream name: " + err.Error()}
	}
	renamed := name
	if err := client.UpdateStream(
		ctx, identifier, iggcon.StreamUpdate{Name: &renamed}); err != nil {
		return &TransportError{Reason: "rename the stream", Cause: err}
	}
	return nil
}

// Topics is every topic of this stream.
//
// A stream that does not exist fails rather than reading as empty: the server
// lists a missing stream the same way it lists one with no topics, and those
// are not the same answer.
func (stream *Stream) Topics(ctx context.Context) ([]TopicInfo, error) {
	client, err := stream.laser.connected()
	if err != nil {
		return nil, err
	}
	identifier, err := iggcon.NewIdentifier(stream.name)
	if err != nil {
		return nil, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	topics, err := client.GetTopics(ctx, identifier)
	if err != nil {
		return nil, &TransportError{Reason: "list the topics", Cause: err}
	}
	if len(topics) == 0 {
		switch info, err := stream.Get(ctx); {
		case err != nil:
			return nil, err
		case info == nil:
			return nil, &TransportError{
				Reason: "list the topics", Cause: ierror.StreamNameNotFound{}}
		}
	}
	listed := make([]TopicInfo, 0, len(topics))
	for _, topic := range topics {
		listed = append(listed, topicInfoOf(topic))
	}
	return listed, nil
}

// Get is this topic as the server reports it, nil when it or its stream does
// not exist.
func (topic *Topic) Get(ctx context.Context) (*TopicInfo, error) {
	details, err := topic.details(ctx)
	if err != nil || details == nil {
		return nil, err
	}
	info := topicInfoOf(details.Topic)
	return &info, nil
}

// Exists reports whether this topic exists.
func (topic *Topic) Exists(ctx context.Context) (bool, error) {
	details, err := topic.details(ctx)
	return details != nil, err
}

// Update changes the name, retention or size cap of this topic. A field left
// unset keeps its value. Address a renamed topic by its new name afterwards.
func (topic *Topic) Update(ctx context.Context, options UpdateTopicOptions) error {
	update, err := options.patch()
	if err != nil {
		return err
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return err
	}
	client, err := topic.laser.connected()
	if err != nil {
		return err
	}
	if err := client.UpdateTopic(ctx, streamID, topicID, update); err != nil {
		return &TransportError{Reason: "update the topic", Cause: err}
	}
	return nil
}

// DeleteSegments deletes the oldest count segments of one partition with
// their messages. The open segment stays.
func (topic *Topic) DeleteSegments(
	ctx context.Context, partitionID, count uint32,
) error {
	client, streamID, topicID, err := topic.counted(count)
	if err != nil {
		return err
	}
	if err := client.DeleteSegments(
		ctx, streamID, topicID, partitionID, count); err != nil {
		return &TransportError{Reason: "delete the segments", Cause: err}
	}
	return nil
}

// Partitions is every partition of this topic.
//
// A topic that does not exist fails rather than reading as empty, for the
// same reason a missing stream does when its topics are listed.
func (topic *Topic) Partitions(ctx context.Context) ([]PartitionInfo, error) {
	details, err := topic.details(ctx)
	if err != nil {
		return nil, err
	}
	if details == nil {
		return nil, topic.missing()
	}
	listed := make([]PartitionInfo, 0, len(details.Partitions))
	for _, partition := range details.Partitions {
		listed = append(listed, PartitionInfo{
			ID:              partition.Id,
			SegmentsCount:   partition.SegmentsCount,
			CurrentOffset:   partition.CurrentOffset,
			SizeBytes:       partition.SizeBytes,
			MessagesCount:   partition.MessagesCount,
			CreatedAtMicros: partition.CreatedAt,
		})
	}
	return listed, nil
}

// AddPartitions adds count partitions after the existing ones. A topic
// created with a fixed partition layout refuses the change.
func (topic *Topic) AddPartitions(ctx context.Context, count uint32) error {
	client, streamID, topicID, err := topic.counted(count)
	if err != nil {
		return err
	}
	if err := client.CreatePartitions(ctx, streamID, topicID, count); err != nil {
		return &TransportError{Reason: "add the partitions", Cause: err}
	}
	return nil
}

// DeletePartitions deletes the last count partitions with their messages. A
// topic created with a fixed partition layout refuses the change.
func (topic *Topic) DeletePartitions(ctx context.Context, count uint32) error {
	client, streamID, topicID, err := topic.counted(count)
	if err != nil {
		return err
	}
	if err := client.DeletePartitions(ctx, streamID, topicID, count); err != nil {
		return &TransportError{Reason: "delete the partitions", Cause: err}
	}
	return nil
}

// ConsumerGroups is every consumer group of this topic.
//
// A topic that does not exist fails rather than reading as empty.
func (topic *Topic) ConsumerGroups(ctx context.Context) ([]ConsumerGroupSummary, error) {
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return nil, err
	}
	client, err := topic.laser.connected()
	if err != nil {
		return nil, err
	}
	groups, err := client.GetConsumerGroups(ctx, streamID, topicID)
	if err != nil {
		return nil, &TransportError{Reason: "list the consumer groups", Cause: err}
	}
	if len(groups) == 0 {
		switch details, err := topic.details(ctx); {
		case err != nil:
			return nil, err
		case details == nil:
			return nil, topic.missing()
		}
	}
	listed := make([]ConsumerGroupSummary, 0, len(groups))
	for _, group := range groups {
		listed = append(listed, ConsumerGroupSummary{
			ID:              group.Id,
			Name:            group.Name,
			PartitionsCount: group.PartitionsCount,
			MembersCount:    group.MembersCount,
		})
	}
	return listed, nil
}

// details reads this topic, nil when it or its stream is not there.
func (topic *Topic) details(ctx context.Context) (*iggcon.TopicDetails, error) {
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return nil, err
	}
	client, err := topic.laser.connected()
	if err != nil {
		return nil, err
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	switch {
	case isMissingResource(err):
		return nil, nil
	case err != nil:
		return nil, &TransportError{Reason: "read the topic", Cause: err}
	}
	return details, nil
}

// counted resolves the identifiers of a call that acts on a number of
// partitions or segments.
//
// A count of zero is refused rather than sent: the server would either do
// nothing or refuse it, and neither tells the caller it asked for nothing.
func (topic *Topic) counted(
	count uint32,
) (iggcon.Client, iggcon.Identifier, iggcon.Identifier, error) {
	if count == 0 {
		return nil, iggcon.Identifier{}, iggcon.Identifier{},
			&InvalidError{Reason: "count must be greater than zero"}
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return nil, iggcon.Identifier{}, iggcon.Identifier{}, err
	}
	client, err := topic.laser.connected()
	if err != nil {
		return nil, iggcon.Identifier{}, iggcon.Identifier{}, err
	}
	return client, streamID, topicID, nil
}

// missing is the error a listing of a topic that is not there reports.
func (topic *Topic) missing() error {
	return &TransportError{
		Reason: "read the topic", Cause: ierror.TopicNameNotFound{}}
}

func streamInfoOf(stream iggcon.Stream) StreamInfo {
	return StreamInfo{
		ID:              stream.Id,
		Name:            stream.Name,
		TopicsCount:     stream.TopicsCount,
		SizeBytes:       stream.SizeBytes,
		MessagesCount:   stream.MessagesCount,
		CreatedAtMicros: stream.CreatedAt,
	}
}

func topicInfoOf(topic iggcon.Topic) TopicInfo {
	info := TopicInfo{
		ID:              topic.Id,
		Name:            topic.Name,
		PartitionsCount: topic.PartitionsCount,
		SizeBytes:       topic.Size,
		MessagesCount:   topic.MessagesCount,
		CreatedAtMicros: topic.CreatedAt,
	}
	// Unlimited is not a cap, so it reads as none rather than as the largest
	// number this build can hold.
	if topic.MaxTopicSize != math.MaxUint64 {
		info.MaxSizeBytes = topic.MaxTopicSize
	}
	// Never-expire is not a duration, so it reads as no expiry rather than
	// as the longest one this build can hold. A server default is zero
	// micros, which already reads as none.
	if topic.MessageExpiry != iggcon.IggyExpiryNeverExpire {
		info.MessageExpiry = time.Duration(topic.MessageExpiry) * time.Microsecond
	}
	return info
}
