package laser_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenMultipleKeysWhenReadThenShouldBatchOnceAndPreserveAbsentAndEmpty(t *testing.T) {
	slot := func(entry *wire.KvEntry) []byte {
		return batchBytes(t, wire.OkReply(wire.KvOutcome{Kind: wire.KvValue, Entry: entry}))
	}
	client := &managedClient{reply: batchBytes(t, wire.BatchReply{Results: [][]byte{
		slot(nil), slot(&wire.KvEntry{Value: []byte{}}), slot(&wire.KvEntry{Value: []byte("value")}),
	}})}
	handle := managedHandle(client, laser.Options{DefaultStream: "agents", Capabilities: kvCapabilities()})
	keys := [][]byte{[]byte("missing"), []byte("empty"), []byte("full")}
	values, err := handle.Kv("ns").GetMany(context.Background(), keys)
	if err != nil {
		t.Fatal(err)
	}
	if client.calls != 1 || client.lastCode != wire.AgdxBatchCode || len(values) != len(keys) ||
		values[0] != nil || values[1] == nil || len(values[1]) != 0 || string(values[2]) != "value" {
		t.Fatalf("batch returned %+v in %d calls to %d", values, client.calls, client.lastCode)
	}
	var frame wire.ManagedRequestEnvelope
	if err := wire.Unmarshal(client.lastBody, &frame); err != nil {
		t.Fatal(err)
	}
	var batch wire.BatchRequest
	if err := wire.Unmarshal(frame.Payload, &batch); err != nil {
		t.Fatal(err)
	}
	for i, item := range batch.Ops {
		var get wire.KvGet
		if err := wire.Unmarshal(item.Payload, &get); err != nil {
			t.Fatal(err)
		}
		if item.Code != wire.AgdxKvGetCode || get.Namespace != "stream:agents/ns" || string(get.Key) != string(keys[i]) {
			t.Fatalf("unexpected batch item %d: %+v", i, get)
		}
	}
}

func TestGivenNoLeaseCapabilityWhenBarrieredThenShouldRejectSingleAndBatchedReadsBeforeSend(t *testing.T) {
	client := &managedClient{}
	handle := managedHandle(client, laser.Options{Capabilities: kvCapabilities()})
	position := wire.MutationPosition{Offset: 2}
	body := batchBytes(t, wire.KvGet{V: wire.KvOpVersion, Key: []byte("a"), Namespace: "ns", MinPosition: &position})
	_, single := handle.Kv("ns").GetEntryAtLeast(context.Background(), []byte("a"), position)
	_, batched := handle.ExecuteBatch(context.Background(), []wire.BatchItem{{Code: wire.AgdxKvGetCode, Payload: body}})
	var unsupported *laser.UnsupportedError
	if !errors.As(single, &unsupported) || !errors.As(batched, &unsupported) || client.calls != 0 {
		t.Fatalf("single=%v batched=%v calls=%d", single, batched, client.calls)
	}
}

func TestGivenAFrameLevelRefusalWhenBatchingThenShouldReportTheCommandFailure(t *testing.T) {
	client := &managedClient{reply: batchBytes(t, wire.NewCommandError(wire.ResultForbidden, "no grant"))}
	handle := managedHandle(client, laser.Options{Capabilities: kvCapabilities()})

	_, err := handle.Kv("ns").GetMany(context.Background(), [][]byte{[]byte("a")})

	var failure *laser.CommandFailure
	if !errors.As(err, &failure) || failure.ResultCode() != wire.ResultForbidden || client.calls != 1 {
		t.Fatalf("want the forbidden command failure after one call, got %T %v after %d", err, err, client.calls)
	}
}

func TestGivenAnUnavailableFrameWhenBatchingThenShouldRepeatUnderOneIdentity(t *testing.T) {
	client := &sequencedClient{replies: [][]byte{
		batchBytes(t, wire.NewCommandError(wire.ResultUnavailable, "busy")),
		batchBytes(t, wire.BatchReply{Results: [][]byte{kvValueSlot(t, "value")}}),
	}}
	handle := managedHandle(client, laser.Options{Capabilities: kvCapabilities()})

	values, err := handle.Kv("ns").GetMany(context.Background(), [][]byte{[]byte("a")})
	if err != nil {
		t.Fatal(err)
	}

	if len(client.bodies) != 2 || len(values) != 1 || string(values[0]) != "value" {
		t.Fatalf("want the value on the second attempt, got %q after %d attempts", values, len(client.bodies))
	}
	var first, second wire.ManagedRequestEnvelope
	if err := wire.Unmarshal(client.bodies[0], &first); err != nil {
		t.Fatal(err)
	}
	if err := wire.Unmarshal(client.bodies[1], &second); err != nil {
		t.Fatal(err)
	}
	if first.OperationID.IsZero() || first.OperationID != second.OperationID {
		t.Fatalf("attempts carried identities %s and %s", first.OperationID, second.OperationID)
	}
}

func TestGivenAFailedSlotWhenReadingManyKeysThenShouldReportThatSlotsFailure(t *testing.T) {
	client := &managedClient{reply: batchBytes(t, wire.BatchReply{Results: [][]byte{
		kvValueSlot(t, "value"), batchBytes(t, wire.NewCommandError(wire.ResultForbidden, "no grant")),
	}})}
	handle := managedHandle(client, laser.Options{Capabilities: kvCapabilities()})

	_, err := handle.Kv("ns").GetMany(context.Background(), [][]byte{[]byte("a"), []byte("b")})

	var failure *laser.CommandFailure
	if !errors.As(err, &failure) || failure.ResultCode() != wire.ResultForbidden {
		t.Fatalf("want the slot's forbidden failure, got %T %v", err, err)
	}
}

func TestGivenAResultCountMismatchWhenBatchingThenShouldRefuseTheReply(t *testing.T) {
	client := &managedClient{reply: batchBytes(t, wire.BatchReply{Results: [][]byte{kvValueSlot(t, "value")}})}
	handle := managedHandle(client, laser.Options{Capabilities: kvCapabilities()})

	_, err := handle.Kv("ns").GetMany(context.Background(), [][]byte{[]byte("a"), []byte("b")})

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a codec error for one result to two commands, got %T %v", err, err)
	}
}

func TestGivenTooManyKeysWhenReadingManyThenShouldRefuseBeforeSending(t *testing.T) {
	client := &managedClient{}
	handle := managedHandle(client, laser.Options{Capabilities: kvCapabilities()})
	keys := make([][]byte, wire.MaxBatchOps+1)
	for i := range keys {
		keys[i] = []byte{byte(i)}
	}

	_, err := handle.Kv("ns").GetMany(context.Background(), keys)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) || client.calls != 0 {
		t.Fatalf("want an invalid error before any send, got %T %v after %d calls", err, err, client.calls)
	}
}

func batchBytes(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := wire.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// sequencedClient answers each managed send with the next scripted reply.
type sequencedClient struct {
	clusterOfOne
	replies [][]byte
	bodies  [][]byte
}

func (client *sequencedClient) Close() error { return nil }

func (client *sequencedClient) SendBinaryRequest(_ context.Context, _ uint32, payload []byte) ([]byte, error) {
	client.bodies = append(client.bodies, payload)
	return client.replies[min(len(client.bodies), len(client.replies))-1], nil
}

func kvValueSlot(t *testing.T, value string) []byte {
	t.Helper()
	return batchBytes(t, wire.OkReply(wire.KvOutcome{Kind: wire.KvValue, Entry: &wire.KvEntry{Value: []byte(value)}}))
}
