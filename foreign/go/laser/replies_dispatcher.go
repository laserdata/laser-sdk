package laser

import (
	"context"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// replyHubKey names one shared reply dispatcher: a topic on a stream.
type replyHubKey struct{ stream, topic string }

// replyDispatcher is one shared ReplyHub and the goroutine feeding it.
//
// One reader per reply topic, rather than one per waiter, so ten concurrent
// contracts on a topic read its records once between them.
type replyDispatcher struct {
	hub    *ReplyHub
	cancel context.CancelFunc
	done   chan struct{}
}

// replyHubFor is the shared dispatcher for topic on stream, opened on first
// use and reused by every later waiter.
//
// The poller starts from slightly before now, so a reply to a request sent
// the instant after this returns is still read. It runs until the handle is
// closed: Go has no destructor to stop it the way Rust's last-clone drop
// does, so Laser.Close is what ends it, the same as the change-feed
// watchers.
func (laser *Laser) replyHubFor(ctx context.Context, stream, topic string) (*ReplyHub, error) {
	key := replyHubKey{stream: stream, topic: topic}
	shared := laser.state()

	shared.mutex.RLock()
	existing, open := shared.replyHubs[key]
	shared.mutex.RUnlock()
	if open {
		return existing.hub, nil
	}

	// Resolve the topic before taking the write lock: a GetTopic round trip
	// under it would stall every unrelated reply topic's first open.
	client, partitions, err := replyTopicPartitions(ctx, laser, stream, topic)
	if err != nil {
		return nil, err
	}

	shared.mutex.Lock()
	if existing, open := shared.replyHubs[key]; open {
		shared.mutex.Unlock()
		return existing.hub, nil
	}
	hub := NewReplyHub()
	pollCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	dispatcher := &replyDispatcher{hub: hub, cancel: cancel, done: make(chan struct{})}
	if shared.replyHubs == nil {
		shared.replyHubs = map[replyHubKey]*replyDispatcher{}
	}
	shared.replyHubs[key] = dispatcher
	shared.replyHubsOpened++
	shared.mutex.Unlock()

	go dispatcher.run(pollCtx, client, stream, topic, partitions)
	return hub, nil
}

// replyTopicPartitions names the client and partition count of a reply
// topic, refusing one that is not there rather than polling nothing forever.
func replyTopicPartitions(
	ctx context.Context, laser *Laser, stream, topic string,
) (iggcon.Client, uint32, error) {
	client := laser.Client()
	if client == nil {
		return nil, 0, &InvalidError{Reason: "a reply dispatcher needs a connected client"}
	}
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return nil, 0, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(topic)
	if err != nil {
		return nil, 0, &InvalidError{Reason: "topic name: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		return nil, 0, &InvalidError{Reason: "read the reply topic: " + err.Error()}
	}
	if details == nil {
		return nil, 0, &InvalidError{Reason: "the reply topic is not there"}
	}
	return client, details.PartitionsCount, nil
}

// run polls the reply topic and offers everything it reads to the hub.
//
// A read failure is not fatal. The dispatcher outlives any one waiter, so a
// transient refusal must not tear it down and strand the waiters that are
// still holding tickets. The next tick tries again, and a waiter whose reply
// never arrives is ended by its own deadline.
func (dispatcher *replyDispatcher) run(
	ctx context.Context, client iggcon.Client, stream, topic string, partitions uint32,
) {
	defer close(dispatcher.done)
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return
	}
	topicID, err := iggcon.NewIdentifier(topic)
	if err != nil {
		return
	}
	reader, err := iggcon.NewIdentifier(contractWaitReader)
	if err != nil {
		return
	}
	consumer := iggcon.NewSingleConsumer(reader)
	options := iggcon.DefaultPollOptions()
	options.Count = contractWaitBatch

	since := time.Now().Add(-contractClockSlack)
	cursors := make([]iggcon.PollingStrategy, partitions)
	for partition := range cursors {
		cursors[partition] = iggcon.TimestampPollingStrategy(uint64(since.UnixMicro()))
	}

	for {
		for partition := uint32(0); partition < partitions; partition++ {
			if ctx.Err() != nil {
				return
			}
			polled, err := client.PollMessages(
				ctx, streamID, topicID, partition, consumer, cursors[partition], options)
			if err != nil || polled == nil || len(polled.Messages) == 0 {
				continue
			}
			for index := range polled.Messages {
				record, err := DeliveredRecord(
					partition, polled.Messages[index], iggcon.Checkpoint{}, false)
				if err != nil {
					continue
				}
				message, err := OpenAgdx(record)
				if err != nil {
					continue
				}
				// Offered to the hub, which decides: a message nobody is
				// waiting for is left alone rather than dropped, since
				// another waiter may subscribe before it is read again.
				dispatcher.hub.Deliver(message)
			}
			last := polled.Messages[len(polled.Messages)-1]
			cursors[partition] = iggcon.OffsetPollingStrategy(last.Header.Offset + 1)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(contractWaitInterval):
		}
	}
}

// forgetStream drops what this connection cached about a stream that was deleted or renamed.
func (laser *Laser) forgetStream(stream string) {
	shared := laser.state()
	shared.mutex.Lock()
	delete(shared.registries, stream)
	var stopped []*replyDispatcher
	for key, dispatcher := range shared.replyHubs {
		if key.stream == stream {
			stopped = append(stopped, dispatcher)
			delete(shared.replyHubs, key)
		}
	}
	shared.mutex.Unlock()
	for _, dispatcher := range stopped {
		dispatcher.cancel()
	}
}

// forgetTopic drops what this connection cached about a topic that was deleted or changed.
func (laser *Laser) forgetTopic(stream, topic string) {
	key := replyHubKey{stream: stream, topic: topic}
	shared := laser.state()
	shared.mutex.Lock()
	if topic == wire.AgentRegistry {
		delete(shared.registries, stream)
	}
	dispatcher := shared.replyHubs[key]
	delete(shared.replyHubs, key)
	shared.mutex.Unlock()
	if dispatcher != nil {
		dispatcher.cancel()
	}
}

// closeReplyHubs stops every shared dispatcher this connection opened, and
// forgets the agent registries it shared.
func (laser *Laser) closeReplyHubs() {
	shared := laser.state()
	shared.mutex.Lock()
	dispatchers := shared.replyHubs
	shared.replyHubs = nil
	shared.registries = nil
	shared.mutex.Unlock()
	for _, dispatcher := range dispatchers {
		dispatcher.cancel()
	}
}
