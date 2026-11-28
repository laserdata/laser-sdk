package laser_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenATypedValueWhenAProducerHeaderIsBuiltThenShouldKeepItsKindAndWidth(t *testing.T) {
	for name, built := range map[string]struct {
		entry func() (iggcon.HeaderEntry, error)
		kind  iggcon.HeaderKind
		value []byte
	}{
		"text":   {func() (iggcon.HeaderEntry, error) { return laser.ProducerHeader("type", "order") }, iggcon.String, []byte("order")},
		"bytes":  {func() (iggcon.HeaderEntry, error) { return laser.ProducerHeader("type", []byte{1, 2}) }, iggcon.Raw, []byte{1, 2}},
		"bool":   {func() (iggcon.HeaderEntry, error) { return laser.ProducerHeader("type", true) }, iggcon.Bool, []byte{1}},
		"uint8":  {func() (iggcon.HeaderEntry, error) { return laser.ProducerHeader("type", uint8(7)) }, iggcon.Uint8, []byte{7}},
		"uint16": {func() (iggcon.HeaderEntry, error) { return laser.ProducerHeader("type", uint16(7)) }, iggcon.Uint16, []byte{7, 0}},
		"uint32": {func() (iggcon.HeaderEntry, error) { return laser.ProducerHeader("type", uint32(7)) }, iggcon.Uint32, []byte{7, 0, 0, 0}},
		"uint64": {func() (iggcon.HeaderEntry, error) { return laser.ProducerHeader("type", uint64(7)) }, iggcon.Uint64, []byte{7, 0, 0, 0, 0, 0, 0, 0}},
		"int32":  {func() (iggcon.HeaderEntry, error) { return laser.ProducerHeader("type", int32(-2)) }, iggcon.Int32, []byte{0xfe, 0xff, 0xff, 0xff}},
	} {
		entry, err := built.entry()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if string(entry.Key.Value) != "type" || entry.Value.Kind != built.kind || !bytes.Equal(entry.Value.Value, built.value) {
			t.Errorf("%s: got key %q kind %v value %v", name, entry.Key.Value, entry.Value.Kind, entry.Value.Value)
		}
		if _, err := laser.NewProducerMessage([]byte("body"), entry); err != nil {
			t.Errorf("%s: the header must be accepted on a message: %v", name, err)
		}
	}
}

func TestGivenAnEmptyKeyWhenAProducerHeaderIsBuiltThenShouldRefuseIt(t *testing.T) {
	var invalid *laser.InvalidError
	if _, err := laser.ProducerHeader("", uint16(7)); !errors.As(err, &invalid) {
		t.Errorf("want an empty header key refused, got %v", err)
	}
}

func TestGivenADirectProducerWhenSendingThenShouldKeepRoutingHeadersAndCommitPositions(t *testing.T) {
	ctx := context.Background()
	s := &directServer{}
	s.streamExists = true
	key := []byte("node-7")
	p, err := directTopic(s).Producer().Partitions(4).BatchLength(2).Routing(laser.KeyRouting(key)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(ctx)
	key[0] = 'x'
	header, _ := iggcon.NewHeaderKeyString("sensor")
	headerValue := iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("cpu")}
	message, err := laser.NewProducerMessage([]byte("ab"), iggcon.HeaderEntry{Key: header, Value: headerValue})
	if err != nil {
		t.Fatal(err)
	}
	messages := []laser.ProducerMessage{message, message, message}
	reply, err := p.SendBatch(ctx, messages)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Confirmations) != 2 || reply.Confirmations[0].BaseOffset != 1 || reply.Confirmations[1].BaseOffset != 2 {
		t.Fatalf("confirmations: %+v", reply)
	}
	if len(s.batches[0]) != 2 || len(s.batches[1]) != 1 || s.topicOptions == nil || *s.topicOptions.PartitionsCount != 4 {
		t.Fatal("unexpected creation or batching")
	}
	for _, route := range s.routes {
		if route.Kind != iggcon.MessageKey || string(route.Value) != "node-7" {
			t.Fatalf("changed route %+v", route)
		}
	}
	got := s.batches[0][0]
	if got.Header.Id == (iggcon.MessageID{}) || messages[0].Header.Id != (iggcon.MessageID{}) || !bytes.Equal(got.UserHeaders, message.UserHeaders) {
		t.Fatal("consumer headers or caller's ID changed")
	}
	if _, err := p.SendToPartition(ctx, message, 1); err != nil {
		t.Fatal(err)
	}
	if s.routes[2].Kind != iggcon.PartitionIdKind {
		t.Fatal("ignored per-send routing")
	}
	stats := p.Statistics()
	if stats.SubmittedRecords != 4 || stats.SubmittedPayloadBytes != 8 || *stats.ConfirmedRecords != 4 || *stats.ConfirmedPayloadBytes != 8 || stats.Latency.Samples != 2 || stats.LastSuccessMillis == nil || stats.Retries != nil {
		t.Fatalf("statistics %+v", stats)
	}
	*stats.ConfirmedRecords = 999
	if *p.Statistics().ConfirmedRecords != 4 {
		t.Fatal("caller mutated live counters")
	}
}

func TestGivenAPartialDirectSendWhenLaterChunkFailsThenShouldReportItsPlaintextRemainder(t *testing.T) {
	ctx := context.Background()
	s := &directServer{failOn: 2}
	s.topicExists = true
	encryption, err := laser.AES256GCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	p, err := directTopic(s).Producer().Encryption(encryption).Retries(0).BatchLength(2).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(ctx)
	var messages []laser.ProducerMessage
	for _, value := range []string{"one", "two", "three", "four", "five"} {
		messages = append(messages, directMessage(t, value))
	}
	_, err = p.SendBatch(ctx, messages)
	var failure *laser.PublishFailure
	if !errors.As(err, &failure) || len(failure.Committed) != 1 || len(failure.Committed[0].Confirmations) != 1 || len(failure.Unconfirmed) != 3 {
		t.Fatalf("partial send: %+v", err)
	}
	if string(failure.Unconfirmed[0].Payload) != "three" || bytes.Equal(s.batches[1][0].Payload, []byte("three")) || failure.Unconfirmed[0].Header.Id != s.batches[1][0].Header.Id {
		t.Fatal("retry lost plaintext or stable ID")
	}
	stats := p.Statistics()
	if stats.FailedCalls != 1 || stats.SubmittedRecords != 5 || *stats.ConfirmedRecords != 0 || stats.LastSuccessMillis != nil || stats.Latency.P50Micros == nil || stats.Latency.P99Micros != nil {
		t.Fatalf("partial call stats: %+v", stats)
	}
}

func TestGivenProducerBoundsWhenBuildingThenShouldRejectConflictsBeforeAnyCall(t *testing.T) {
	ctx := context.Background()
	for _, setup := range []func(*laser.ProducerBuilder) *laser.ProducerBuilder{
		func(b *laser.ProducerBuilder) *laser.ProducerBuilder { return b.BatchLength(-1) },
		func(b *laser.ProducerBuilder) *laser.ProducerBuilder { return b.Partitions(0) },
		func(b *laser.ProducerBuilder) *laser.ProducerBuilder { return b.ReplicationFactor(33) },
		func(b *laser.ProducerBuilder) *laser.ProducerBuilder { return b.ExpireAfter(time.Hour).NeverExpire() },
		func(b *laser.ProducerBuilder) *laser.ProducerBuilder { return b.MaxTopicBytes(12).UnlimitedTopicSize() },
		func(b *laser.ProducerBuilder) *laser.ProducerBuilder { return b.Routing(laser.KeyRouting(nil)) },
	} {
		s := &directServer{}
		_, err := setup(directTopic(s).Producer()).Build(ctx)
		if err == nil || len(s.calls) != 0 {
			t.Fatalf("configuration sent calls %v, error %v", s.calls, err)
		}
	}
	s := &directServer{}
	s.streamExists = true
	if _, err := directTopic(s).Producer().CreateTopic(false).Build(ctx); err == nil || s.topicExists {
		t.Fatal("disabled topic creation must refuse a missing topic")
	}
}

func TestGivenAZeroBatchLengthWhenSendingABatchThenShouldSplitByTheDefault(t *testing.T) {
	ctx := context.Background()
	s := &directServer{}
	s.topicExists = true
	p, err := directTopic(s).Producer().BatchLength(0).Build(ctx)
	if err != nil {
		t.Fatalf("a zero batch length must fall back to the default: %v", err)
	}
	defer p.Close(ctx)
	messages := make([]laser.ProducerMessage, 1001)
	for i := range messages {
		messages[i] = directMessage(t, "record")
	}

	if _, err := p.SendBatch(ctx, messages); err != nil {
		t.Fatal(err)
	}

	if len(s.batches) != 2 || len(s.batches[0]) != 1000 || len(s.batches[1]) != 1 {
		t.Fatalf("want sends of 1000 and 1 records, got %d sends", len(s.batches))
	}
}

func TestGivenAnActiveDirectSendWhenClosingThenShouldWaitOrCancelWithoutRevivingTheProducer(t *testing.T) {
	ctx := context.Background()
	s := &directServer{entered: make(chan struct{}), release: make(chan struct{})}
	s.topicExists = true
	p, err := directTopic(s).Producer().Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() { _, err := p.Send(ctx, []byte("active")); sent <- err }()
	<-s.entered
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := p.Flush(cancelCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush: %v", err)
	}
	if err := p.Close(cancelCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("close: %v", err)
	}
	if _, err := p.Send(ctx, []byte("late")); err == nil {
		t.Fatal("send admitted after close")
	}
	close(s.release)
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var invalid *laser.InvalidError
	if err := p.Flush(ctx); !errors.As(err, &invalid) {
		t.Fatalf("flush after close must be refused, got %v", err)
	}
	before := p.Statistics()
	if _, err := p.SendBatchWithRouting(ctx, nil, laser.KeyRouting(nil)); err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	if after := p.Statistics(); after.SubmittedRecords != 2 || after.FailedCalls != 1 || after.Latency.Samples != before.Latency.Samples {
		t.Fatalf("stats: %+v", after)
	}
}

func TestGivenDirectProducerStatisticsWhenEnoughCallsCompleteThenShouldExposeTailPercentiles(t *testing.T) {
	ctx := context.Background()
	server := &directServer{}
	server.topicExists = true
	producer, err := directTopic(server).Producer().Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close(ctx)
	message := directMessage(t, "sample")
	for i := 1; i <= 1000; i++ {
		if _, err := producer.SendMessage(ctx, message); err != nil {
			t.Fatal(err)
		}
		if i != 99 && i != 100 && i != 999 && i != 1000 {
			continue
		}
		latency := producer.Statistics().Latency
		if latency.Samples != uint64(i) || latency.P50Micros == nil || (latency.P99Micros != nil) != (i >= 100) || (latency.P999Micros != nil) != (i >= 1000) {
			t.Fatalf("after %d calls: %+v", i, latency)
		}
		for _, bound := range []*uint64{latency.P50Micros, latency.P99Micros, latency.P999Micros} {
			if bound != nil && (*bound == 0 || *bound&(*bound-1) != 0) {
				t.Fatalf("invalid histogram upper bound %d", *bound)
			}
		}
	}
}

type directServer struct {
	streamServer
	failOn  int
	batches [][]iggcon.IggyMessage
	routes  []iggcon.Partitioning
	entered chan struct{}
	release chan struct{}
}

func (s *directServer) SendMessages(ctx context.Context, _, _ iggcon.Identifier, route iggcon.Partitioning, messages []iggcon.IggyMessage) (*iggcon.SendMessagesResponse, error) {
	if s.entered != nil {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.batches = append(s.batches, messages)
	s.routes = append(s.routes, route)
	if len(s.batches) == s.failOn {
		return nil, errors.New("publish refused")
	}
	return &iggcon.SendMessagesResponse{Confirmations: []iggcon.SendMessagesConfirmation{{BaseOffset: uint64(len(s.batches))}}}, nil
}

func directTopic(s *directServer) *laser.Topic {
	return laser.FromClient(s, laser.Options{DefaultStream: "direct"}).Topic("events")
}

func directMessage(t *testing.T, body string) laser.ProducerMessage {
	t.Helper()
	m, err := laser.NewProducerMessage([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return m
}
