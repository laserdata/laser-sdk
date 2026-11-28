package laser_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenARawCommandWhenSentThenShouldReturnACopyOfTheReply(t *testing.T) {
	server := &commandServer{reply: []byte{9, 8, 7}}
	handle := laser.FromClient(server, laser.Options{})

	reply, err := handle.SendCommand(context.Background(), 4242, []byte{1, 2})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	want := []sentCommand{{code: 4242, payload: []byte{1, 2}}}
	if !reflect.DeepEqual(server.sent, want) {
		t.Fatalf("got %+v, want %+v", server.sent, want)
	}
	server.reply[0] = 0
	if !reflect.DeepEqual(reply, []byte{9, 8, 7}) {
		t.Fatalf("the reply must not alias the connection's buffer, got %v", reply)
	}
}

func TestGivenARawCommandForOneReplicaWhenSentThenShouldNameThatReplica(t *testing.T) {
	server := &commandServer{reply: []byte{5}}
	handle := laser.FromClient(server, laser.Options{})

	reply, err := handle.SendCommandToReplica(context.Background(), 10, []byte{3}, 2)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	replica := uint8(2)
	want := []sentCommand{{code: 10, payload: []byte{3}, replica: &replica}}
	if !reflect.DeepEqual(server.sent, want) {
		t.Fatalf("got %+v, want %+v", server.sent, want)
	}
	server.reply[0] = 0
	if !reflect.DeepEqual(reply, []byte{5}) {
		t.Fatalf("the reply must not alias the connection's buffer, got %v", reply)
	}
}

func TestGivenARefusedRawCommandWhenSentThenShouldReportTheServersRefusal(t *testing.T) {
	server := &commandServer{failure: ierror.Unauthorized{}}
	handle := laser.FromClient(server, laser.Options{})

	for name, send := range map[string]func() ([]byte, error){
		"the picked node": func() ([]byte, error) { return handle.SendCommand(context.Background(), 10, nil) },
		"one replica": func() ([]byte, error) {
			return handle.SendCommandToReplica(context.Background(), 10, nil, 1)
		},
	} {
		reply, err := send()
		var transport *laser.TransportError
		if reply != nil || !errors.As(err, &transport) || !errors.Is(err, ierror.Unauthorized{}) {
			t.Fatalf("%s: got %v and %v", name, reply, err)
		}
		if !laser.IsPermissionDenied(err) {
			t.Fatalf("%s: a refused grant must classify as permission denied", name)
		}
	}
}

func TestGivenAClosedClientWhenARawCommandIsSentThenShouldRefuseIt(t *testing.T) {
	server := &commandServer{reply: []byte{1}}
	handle := laser.FromClient(server, laser.Options{})
	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	_, err := handle.SendCommand(context.Background(), 10, nil)

	var transport *laser.TransportError
	if !errors.As(err, &transport) || len(server.sent) != 0 {
		t.Fatalf("got %v with %d requests sent", err, len(server.sent))
	}
}

type sentCommand struct {
	code    uint32
	payload []byte
	replica *uint8
}

// commandServer records raw commands and answers each with one scripted reply.
type commandServer struct {
	clusterOfOne
	reply   []byte
	failure error
	sent    []sentCommand
}

func (server *commandServer) Close() error { return nil }

func (server *commandServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	if server.failure != nil {
		return nil, server.failure
	}
	server.sent = append(server.sent, sentCommand{code: code, payload: append([]byte(nil), payload...)})
	return server.reply, nil
}

func (server *commandServer) SendBinaryRequestToReplica(
	_ context.Context, code uint32, payload []byte, replica uint8,
) ([]byte, error) {
	if server.failure != nil {
		return nil, server.failure
	}
	server.sent = append(server.sent,
		sentCommand{code: code, payload: append([]byte(nil), payload...), replica: &replica})
	return server.reply, nil
}
