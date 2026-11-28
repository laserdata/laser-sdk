package laser_test

import (
	"context"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

type inference struct {
	Model  string `json:"model" cbor:"model"`
	Tokens int    `json:"tokens" cbor:"tokens"`
}

// A typed publish stamps the codec's content type, so a reader knows how to
// decode the body without being told out of band.
func TestGivenATypedPublishWhenSentThenShouldStampTheCodecsContentType(t *testing.T) {
	for name, want := range map[string]struct {
		view func(*laser.Topic) interface {
			Publish(inference) *laser.PublishRequest
		}
		kind wire.ContentType
	}{
		"json": {func(topic *laser.Topic) interface {
			Publish(inference) *laser.PublishRequest
		} {
			return laser.TypedJSON[inference](topic)
		}, wire.ContentJSON},
		"cbor": {func(topic *laser.Topic) interface {
			Publish(inference) *laser.PublishRequest
		} {
			return laser.TypedCBOR[inference](topic)
		}, wire.ContentCBOR},
	} {
		t.Run(name, func(t *testing.T) {
			server := &streamServer{}
			topic := streamHandle(server, laser.Options{DefaultStream: "agents"}).
				Topic("inferences")

			if _, err := want.view(topic).Publish(inference{Model: "m", Tokens: 3}).
				Send(context.Background()); err != nil {
				t.Fatalf("publish: %v", err)
			}

			headers := sentHeaders(t, server)
			code, _ := want.kind.Code()
			if headers[wire.HeaderContentType] != string([]byte{code}) {
				t.Errorf("want %q, got %q", want.kind, headers[wire.HeaderContentType])
			}
			if len(server.sentMessages[0].Payload) == 0 {
				t.Error("want an encoded body")
			}
		})
	}
}

// A body has to survive its own codec, or a typed topic writes something it
// cannot read back.
func TestGivenATypedBodyWhenRoundTrippedThenShouldSurviveItsCodec(t *testing.T) {
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "agents"}).Topic("inferences")
	typed := laser.TypedJSON[inference](topic)
	sent := inference{Model: "opus", Tokens: 120}

	if _, err := typed.Publish(sent).Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	record, err := laser.OpenRecord(server.sentMessages[0])
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	decoded, err := typed.Decode(record)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Body != sent {
		t.Errorf("want %+v, got %+v", sent, decoded.Body)
	}
	if decoded.Record != record {
		t.Error("want the record kept alongside its body")
	}
}

// One unreadable record is usually one bad writer, not a broken topic, so a
// reader is told where it sat and carries on.
func TestGivenAnUndecodableBodyWhenReadThenShouldNameWhereItSat(t *testing.T) {
	message, err := iggcon.NewIggyMessage([]byte("not json"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	message.Header.Offset = 17
	record, err := laser.OpenRecord(message)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	record.PartitionID = 2

	_, err = laser.TypedJSON[inference](nil).Decode(record)

	var failure *laser.TypedDecodeError
	if !errors.As(err, &failure) {
		t.Fatalf("want a TypedDecodeError, got %T: %v", err, err)
	}
	if failure.PartitionID != 2 || failure.Offset != 17 {
		t.Errorf("want partition 2 offset 17, got %d / %d",
			failure.PartitionID, failure.Offset)
	}
	if failure.Unwrap() == nil {
		t.Error("want the cause kept")
	}
}

// A reader must see the good records even when one in the middle is bad.
func TestGivenOneBadRecordWhenReplayedThenShouldKeepReadingTheRest(t *testing.T) {
	good, err := iggcon.NewIggyMessage([]byte(`{"model":"a","tokens":1}`))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	good.Header.Offset, good.Header.Timestamp = 0, 10
	bad, err := iggcon.NewIggyMessage([]byte("not json"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	bad.Header.Offset, bad.Header.Timestamp = 1, 20
	alsoGood, err := iggcon.NewIggyMessage([]byte(`{"model":"b","tokens":2}`))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	alsoGood.Header.Offset, alsoGood.Header.Timestamp = 2, 30

	server := &cursorServer{
		partitions: 1,
		byPartition: map[uint32][]iggcon.IggyMessage{
			0: {good, bad, alsoGood},
		},
	}
	topic := cursorHandle(server).Topic("inferences")
	typed := laser.TypedJSON[inference](topic)
	cursor, err := typed.Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	var models []string
	var failures []*laser.TypedDecodeError
	if err := typed.Records(context.Background(), cursor,
		func(record *laser.TypedRecord[inference]) bool {
			models = append(models, record.Body.Model)
			return true
		},
		func(failure *laser.TypedDecodeError) { failures = append(failures, failure) },
	); err != nil {
		t.Fatalf("records: %v", err)
	}

	if len(models) != 2 || models[0] != "a" || models[1] != "b" {
		t.Errorf("want both good records, got %v", models)
	}
	if len(failures) != 1 || failures[0].Offset != 1 {
		t.Errorf("want the bad record reported at offset 1, got %+v", failures)
	}
}

// Passing no error handler skips the unreadable records silently, which is
// what a reader that only wants what it can parse asks for.
func TestGivenNoErrorHandlerWhenReplayedThenShouldSkipQuietly(t *testing.T) {
	bad, err := iggcon.NewIggyMessage([]byte("not json"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	server := &cursorServer{
		partitions:  1,
		byPartition: map[uint32][]iggcon.IggyMessage{0: {bad}},
	}
	topic := cursorHandle(server).Topic("inferences")
	typed := laser.TypedJSON[inference](topic)
	cursor, err := typed.Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	seen := 0
	if err := typed.Records(context.Background(), cursor,
		func(*laser.TypedRecord[inference]) bool { seen++; return true }, nil); err != nil {
		t.Fatalf("records: %v", err)
	}
	if seen != 0 {
		t.Errorf("want nothing delivered, got %d", seen)
	}
}

// A caller's own codec has to be used for both halves, or the view writes with
// one encoding and reads with another.
func TestGivenACallerSuppliedCodecWhenUsedThenShouldDriveBothHalves(t *testing.T) {
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "agents"}).Topic("inferences")
	typed := laser.TypedWith[inference](topic, reversingCodec{})

	if _, err := typed.Publish(inference{Model: "m", Tokens: 1}).
		Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	headers := sentHeaders(t, server)
	if headers[wire.HeaderContentType] != "application/reversed" {
		t.Errorf("want the codec's content type, got %q", headers[wire.HeaderContentType])
	}
	record, err := laser.OpenRecord(server.sentMessages[0])
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	decoded, err := typed.Decode(record)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Body.Model != "m" {
		t.Errorf("want the body back through the same codec, got %+v", decoded.Body)
	}
}

// reversingCodec is JSON written backwards: enough to tell that both halves
// went through the caller's codec rather than the built-in one.
type reversingCodec struct{}

func (reversingCodec) ContentType() wire.ContentType { return "application/reversed" }

func (reversingCodec) Encode(value any) ([]byte, error) {
	encoded, err := laser.JSONCodec{}.Encode(value)
	if err != nil {
		return nil, err
	}
	return reverse(encoded), nil
}

func (reversingCodec) Decode(payload []byte, into any) error {
	return laser.JSONCodec{}.Decode(reverse(payload), into)
}

func reverse(payload []byte) []byte {
	out := make([]byte, len(payload))
	for index, value := range payload {
		out[len(payload)-1-index] = value
	}
	return out
}

func TestGivenTypedBodiesWhenPublishedAsABatchThenShouldSendEveryEncodedBody(t *testing.T) {
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "agents"}).Topic("inferences")
	typed := laser.TypedJSON[inference](topic)

	if _, err := typed.PublishBatch([]inference{{Model: "a", Tokens: 1}, {Model: "b", Tokens: 2}}).
		Send(context.Background()); err != nil {
		t.Fatalf("publish the batch: %v", err)
	}

	sent := server.sent()
	if len(sent) != 2 {
		t.Fatalf("want two records in one send, got %d", len(sent))
	}
	for index, want := range []string{"a", "b"} {
		decoded, err := typed.Decode(&laser.Record{Message: sent[index]})
		if err != nil || decoded.Body.Model != want {
			t.Fatalf("record %d: want model %q, got %+v and %v", index, want, decoded, err)
		}
	}
	code, _ := wire.ContentJSON.Code()
	if headers := sentHeaders(t, server); headers[wire.HeaderContentType] != string([]byte{code}) {
		t.Errorf("want the JSON content type stamped, got %q", headers[wire.HeaderContentType])
	}
}

// unencodable is a codec whose bodies never encode.
type unencodable struct{ laser.JSONCodec }

func (unencodable) Encode(any) ([]byte, error) { return nil, errors.New("body does not encode") }

func TestGivenABodyThatDoesNotEncodeWhenPublishedAsABatchThenShouldSendNothing(t *testing.T) {
	server := &streamServer{}
	topic := streamHandle(server, laser.Options{DefaultStream: "agents"}).Topic("inferences")

	_, err := laser.TypedWith[inference](topic, unencodable{}).
		PublishBatch([]inference{{Model: "a"}}).Send(context.Background())

	if err == nil || len(server.sent()) != 0 {
		t.Fatalf("a batch with an unencodable body must not be sent, got %v and %d records", err, len(server.sent()))
	}
}

func TestGivenAnIndexKeyWhenEmptyOrReservedThenShouldRefuseThePublish(t *testing.T) {
	for _, field := range []string{"", wire.HeaderIdxPrefix + "host"} {
		server := &streamServer{}
		topic := streamHandle(server, laser.Options{DefaultStream: "agents"}).Topic("inferences")

		_, err := topic.Publish().Payload([]byte("body")).Index(field, "node-7").Send(context.Background())

		var invalid *laser.InvalidError
		if !errors.As(err, &invalid) || len(server.sent()) != 0 {
			t.Fatalf("index key %q must be refused before sending, got %v", field, err)
		}
	}
}
