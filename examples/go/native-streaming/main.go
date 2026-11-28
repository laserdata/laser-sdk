package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

const (
	example      = "native-streaming"
	topicName    = "events"
	readTimeout  = 5 * time.Second
	retryBackoff = time.Second
)

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) (err error) {
	count := examples.Messages(1_000)
	batch := min(examples.BatchSize(100), count)
	topic := client.Topic(topicName)
	if err := topic.Ensure(ctx, examples.Partitions, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", topicName, err)
	}
	producer, err := topic.Producer().Retries(3).RetryBackoff(retryBackoff).Build(ctx)
	if err != nil {
		return fmt.Errorf("build the producer: %w", err)
	}
	defer func() { err = errors.Join(err, producer.Close(ctx)) }()

	examples.Phase("producer: exact-width header, keyed routing, and batched messages")
	sent, err := publish(ctx, producer, count, batch)
	if err != nil {
		return err
	}

	examples.Phase("consumer: production automatic offset commits")
	automatic, err := receive(ctx, topic, "auto-workers", count, false)
	if err != nil {
		return err
	}
	examples.Phase("consumer: commit after successful handling")
	manual, err := receive(ctx, topic, "manual-workers", count, true)
	if err != nil {
		return err
	}
	if sent != count || automatic != count || manual != count {
		return errors.New("consumer count mismatch")
	}
	fmt.Printf("\nverified %d records through both commit strategies\n", count)
	return nil
}

func publish(ctx context.Context, producer *laser.Producer, count, batch int) (int, error) {
	// A uint16 header keeps its two-byte width on the log.
	kind, err := laser.ProducerHeader("type", uint16(7))
	if err != nil {
		return 0, fmt.Errorf("build the type header: %w", err)
	}
	first, err := laser.NewProducerMessage([]byte("message-0"), kind)
	if err != nil {
		return 0, err
	}
	if _, err := producer.SendKeyed(ctx, first, []byte("account-42")); err != nil {
		return 0, fmt.Errorf("send the keyed message: %w", err)
	}
	sent := 1
	for sent < count {
		size := min(batch, count-sent)
		if sent == 1 {
			size = min(max(1, batch-1), count-sent)
		}
		messages := make([]laser.ProducerMessage, size)
		for offset := range messages {
			message, err := laser.NewProducerMessage(fmt.Appendf(nil, "message-%d", sent+offset))
			if err != nil {
				return sent, err
			}
			messages[offset] = message
		}
		if _, err := producer.SendBatch(ctx, messages); err != nil {
			return sent, fmt.Errorf("send a batch: %w", err)
		}
		sent += size
		fmt.Printf("published %d/%d\n", sent, count)
	}
	return sent, nil
}

func receive(
	ctx context.Context, topic *laser.Topic, group string, expected int, manualCommit bool,
) (seen int, err error) {
	first := iggcon.FirstPollingStrategy()
	consumer, err := topic.Group(group).Consumer(ctx, laser.ConsumerOptions{
		Count:        uint32(min(100, expected)),
		ManualCommit: manualCommit,
		Start:        &first,
	})
	if err != nil {
		return 0, fmt.Errorf("join %s: %w", group, err)
	}
	defer func() { err = errors.Join(err, consumer.Close(ctx)) }()
	for seen < expected {
		read, cancel := context.WithTimeout(ctx, readTimeout)
		record, err := consumer.Next(read)
		cancel()
		if err != nil {
			return seen, fmt.Errorf("%s read after %d/%d: %w", group, seen, expected, err)
		}
		if manualCommit {
			if err := consumer.Commit(ctx, record.Checkpoint); err != nil {
				return seen, fmt.Errorf("%s commit: %w", group, err)
			}
		}
		seen++
		if seen == 1 || seen == expected || seen%100 == 0 {
			fmt.Printf("%s: %d/%d partition=%d offset=%d payload=%s\n",
				group, seen, expected, record.PartitionID, record.Offset(), record.Payload())
		}
	}
	return seen, nil
}
