package laser

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	nativereader "github.com/apache/iggy/foreign/go/client/reader"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestGivenAPartitionStoppedAfterLastingRefusalsWhenReadThenShouldReportTheRefusalAndResumeAfterTheStoredOffset(t *testing.T) {
	for name, refusal := range map[string]error{
		"missing permission":  ierror.ErrUnauthorized,
		"partition not owned": ierror.ErrConsumerGroupPartitionNotOwned,
		"missing partition":   ierror.ErrPartitionNotFound,
		"oversized message":   ierror.ErrInvalidSizeBytes,
	} {
		inner := &scriptedReader{answers: []scriptedAnswer{
			{err: partitionError(3, refusal, nil)},
			{message: receivedOn(t, 3, "after")},
		}}
		consumer := &Consumer{inner: inner}

		_, err := consumer.Next(context.Background())

		var transport *TransportError
		if !errors.As(err, &transport) || !errors.Is(err, refusal) {
			t.Fatalf("%s: want the refusal reported, got %v", name, err)
		}
		if want := []scriptedSeek{{3, iggcon.NextPollingStrategy()}}; !reflect.DeepEqual(inner.seeks, want) {
			t.Fatalf("%s: want the partition resumed after its stored offset, got %+v", name, inner.seeks)
		}
		record, err := consumer.Next(context.Background())
		if err != nil || string(record.Payload()) != "after" {
			t.Fatalf("%s: want the next record handed over, got %v, %v", name, record, err)
		}
	}
}

func TestGivenARecordThatDoesNotDecodeWhenReadThenShouldReportItOnceAndReadPastIt(t *testing.T) {
	skip := iggcon.OffsetPollingStrategy(8)
	inner := &scriptedReader{answers: []scriptedAnswer{
		{err: partitionError(1, errors.New("message 7 does not decode"), &skip)},
		{message: receivedOn(t, 1, "after")},
	}}
	consumer := &Consumer{inner: inner}

	_, err := consumer.Next(context.Background())

	var transport *TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("want the record reported, got %v", err)
	}
	if want := []scriptedSeek{{1, skip}}; !reflect.DeepEqual(inner.seeks, want) {
		t.Fatalf("want the partition resumed past the record, got %+v", inner.seeks)
	}
	if record, err := consumer.Next(context.Background()); err != nil || string(record.Payload()) != "after" {
		t.Fatalf("want the record after it handed over, got %v, %v", record, err)
	}
}

func TestGivenAPartitionWithANewHistoryWhenReadThenShouldResumeItWithoutReporting(t *testing.T) {
	inner := &scriptedReader{answers: []scriptedAnswer{
		{err: partitionError(2, ierror.ErrHistoryUnavailable, nil)},
		{message: receivedOn(t, 2, "after")},
	}}
	consumer := &Consumer{inner: inner}

	record, err := consumer.Next(context.Background())

	if err != nil || string(record.Payload()) != "after" {
		t.Fatalf("want the read to go on with the next record, got %v, %v", record, err)
	}
	if want := []scriptedSeek{{2, iggcon.NextPollingStrategy()}}; !reflect.DeepEqual(inner.seeks, want) {
		t.Fatalf("want the partition resumed after its stored offset, got %+v", inner.seeks)
	}
}

func TestGivenAReadWithAnUnknownOutcomeWhenReadThenShouldAbandonItResumeThePartitionAndReadOn(t *testing.T) {
	for name, failure := range map[string]error{
		"unknown outcome": ierror.FetchProgressUncertain{OperationID: 41},
		"blocked lane":    ierror.PartitionLaneBlocked{PartitionID: 4, OperationID: 41},
	} {
		for _, abandonFailure := range []error{nil, ierror.ErrOperationNotRetained, errors.New("link lost")} {
			client := &abandoningClient{failure: abandonFailure}
			inner := &scriptedReader{answers: []scriptedAnswer{
				{err: partitionError(4, failure, nil)},
				{message: receivedOn(t, 4, "after")},
			}}
			consumer := &Consumer{inner: inner, topic: FromClient(client, Options{DefaultStream: "fleet"}).Topic("readings")}

			record, err := consumer.Next(context.Background())

			if err != nil || string(record.Payload()) != "after" {
				t.Fatalf("%s, abandon %v: want the read to go on, got %v, %v", name, abandonFailure, record, err)
			}
			if !reflect.DeepEqual(client.abandoned, []uint64{41}) {
				t.Fatalf("%s, abandon %v: want operation 41 abandoned, got %v", name, abandonFailure, client.abandoned)
			}
			if want := []scriptedSeek{{4, iggcon.NextPollingStrategy()}}; !reflect.DeepEqual(inner.seeks, want) {
				t.Fatalf("%s, abandon %v: want the partition resumed, got %+v", name, abandonFailure, inner.seeks)
			}
		}
	}
}

func TestGivenALostReadTheReaderAlreadyAbandonedWhenReadThenShouldResumeThePartitionWithoutAbandoningAgain(t *testing.T) {
	lost := fmt.Errorf("%w: offsets 5 to 9 were stored by a read whose reply was lost", ierror.FetchProgressUncertain{})
	inner := &scriptedReader{answers: []scriptedAnswer{
		{err: partitionError(0, lost, nil)},
		{message: receivedOn(t, 0, "after")},
	}}
	consumer := &Consumer{inner: inner}

	record, err := consumer.Next(context.Background())

	if err != nil || string(record.Payload()) != "after" {
		t.Fatalf("want the read to go on, got %v, %v", record, err)
	}
	if want := []scriptedSeek{{0, iggcon.NextPollingStrategy()}}; !reflect.DeepEqual(inner.seeks, want) {
		t.Fatalf("want the partition resumed, got %+v", inner.seeks)
	}
}

func TestGivenAnErrorOfTheWholeReaderWhenReadThenShouldReportItAndSeekNothing(t *testing.T) {
	inner := &scriptedReader{answers: []scriptedAnswer{
		{err: &nativereader.ReadError{Err: ierror.ErrUnauthorized}},
		{err: partitionError(6, errors.New("the link dropped"), nil)},
	}}
	consumer := &Consumer{inner: inner}

	for range 2 {
		_, err := consumer.Next(context.Background())

		var transport *TransportError
		if !errors.As(err, &transport) {
			t.Fatalf("want the error reported, got %v", err)
		}
	}
	if len(inner.seeks) != 0 {
		t.Fatalf("want an error the reader retries by itself left alone, got %+v", inner.seeks)
	}
}

func TestGivenAResumeThatFailsWhenReadThenShouldReportTheRefusalOrTheFailedResume(t *testing.T) {
	failed := errors.New("the reader is closed")
	for name, stopped := range map[string]struct {
		cause error
		want  error
	}{
		"a refusal keeps its own error":    {ierror.ErrUnauthorized, ierror.ErrUnauthorized},
		"a new history reports the resume": {ierror.ErrHistoryUnavailable, failed},
	} {
		inner := &scriptedReader{seekFailure: failed, answers: []scriptedAnswer{{err: partitionError(5, stopped.cause, nil)}}}
		consumer := &Consumer{inner: inner}

		_, err := consumer.Next(context.Background())

		var transport *TransportError
		if !errors.As(err, &transport) || !errors.Is(err, stopped.want) {
			t.Fatalf("%s: want %v reported, got %v", name, stopped.want, err)
		}
	}
}

func TestGivenAStoppedPartitionWhenConsumingWithAHandlerThenShouldReadOnOrReportAsARead(t *testing.T) {
	inner := &scriptedReader{answers: []scriptedAnswer{
		{message: receivedOn(t, 2, "first")},
		{err: partitionError(2, ierror.ErrHistoryUnavailable, nil)},
		{message: receivedOn(t, 2, "second")},
		{err: partitionError(2, ierror.ErrUnauthorized, nil)},
		{message: receivedOn(t, 2, "never handed over by this call")},
	}}
	consumer := &Consumer{inner: inner}
	var handled []string

	err := consumer.Each(context.Background(), func(_ context.Context, record *Record) error {
		handled = append(handled, string(record.Payload()))
		return nil
	})

	if !errors.Is(err, ierror.ErrUnauthorized) {
		t.Fatalf("want the lasting refusal reported, got %v", err)
	}
	if !reflect.DeepEqual(handled, []string{"first", "second"}) {
		t.Fatalf("want the read to go on over the new history, got %v", handled)
	}
	want := []scriptedSeek{{2, iggcon.NextPollingStrategy()}, {2, iggcon.NextPollingStrategy()}}
	if !reflect.DeepEqual(inner.seeks, want) {
		t.Fatalf("want the partition resumed both times, got %+v", inner.seeks)
	}
}

// scriptedAnswer is one message or one failure a scripted reader hands over.
type scriptedAnswer struct {
	message nativereader.ReceivedMessage
	err     error
}

// scriptedSeek is one seek a scripted reader was asked for.
type scriptedSeek struct {
	partition uint32
	start     iggcon.PollingStrategy
}

// scriptedReader hands over a fixed run of messages and failures and records its seeks.
type scriptedReader struct {
	consumerReader
	answers     []scriptedAnswer
	seeks       []scriptedSeek
	seekFailure error
}

func (scripted *scriptedReader) Next(context.Context) (nativereader.ReceivedMessage, error) {
	if len(scripted.answers) == 0 {
		return nativereader.ReceivedMessage{}, context.Canceled
	}
	answer := scripted.answers[0]
	scripted.answers = scripted.answers[1:]
	return answer.message, answer.err
}

func (scripted *scriptedReader) Consume(
	ctx context.Context, handler func(context.Context, nativereader.ReceivedMessage) error,
) error {
	for {
		message, err := scripted.Next(ctx)
		if err != nil {
			return err
		}
		if err := handler(ctx, message); err != nil {
			return err
		}
	}
}

func (scripted *scriptedReader) Seek(_ context.Context, partition uint32, start iggcon.PollingStrategy) error {
	scripted.seeks = append(scripted.seeks, scriptedSeek{partition, start})
	return scripted.seekFailure
}

// abandoningClient records the operations it is asked to abandon.
type abandoningClient struct {
	iggcon.Client
	abandoned []uint64
	failure   error
}

func (client *abandoningClient) AbandonOperation(_ context.Context, operation uint64) error {
	client.abandoned = append(client.abandoned, operation)
	return client.failure
}

func partitionError(partition uint32, cause error, skip *iggcon.PollingStrategy) error {
	return &nativereader.ReadError{PartitionID: &partition, Err: cause, Skip: skip}
}

func receivedOn(t *testing.T, partition uint32, body string) nativereader.ReceivedMessage {
	t.Helper()
	message, err := iggcon.NewIggyMessage([]byte(body))
	if err != nil {
		t.Fatalf("build the message: %v", err)
	}
	return nativereader.ReceivedMessage{PartitionID: partition, Message: message}
}
