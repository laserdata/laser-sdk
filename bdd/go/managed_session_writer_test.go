package bdd_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func managedWriterStep(ctx context.Context, state *world, flow *managedSessionFlow, text string, args []string) error {
	switch {
	case text == "a native lane writer without session read grants writes lifecycle and state":
		stream, err := state.client.Stream(state.stream).Get(ctx)
		if err != nil {
			return err
		}
		lane, err := state.topic(wire.AgentSessions).Get(ctx)
		if err != nil {
			return err
		}
		if stream == nil || lane == nil {
			return fmt.Errorf("the native session lane does not exist")
		}
		username := "writer-" + state.stream
		password := rand.Text()
		permissions := laser.Permissions{Global: laser.GlobalPermissions{ReadServers: true}, Streams: map[uint32]laser.StreamPermissions{
			stream.ID: {ReadStream: true, ReadTopics: true, Topics: map[uint32]laser.TopicPermissions{lane.ID: {ReadTopic: true, PollMessages: true, SendMessages: true}}},
		}}
		user := state.client.User(username)
		if _, err := user.Create(ctx, password, laser.CreateUserOptions{Status: laser.UserActive, Permissions: &permissions}); err != nil {
			return err
		}
		state.t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := user.Delete(cleanup); err != nil {
				state.t.Errorf("remove scenario user: %v", err)
			}
		})
		credentials, endpoint, ok := strings.Cut(state.address, "@")
		if !ok {
			return fmt.Errorf("test deployment address has no credential separator")
		}
		prefix := ""
		if scheme, _, ok := strings.Cut(credentials, "://"); ok {
			prefix = scheme + "://"
		}
		client, err := laser.ConnectWith(ctx, prefix+username+":"+password+"@"+endpoint, laser.Options{DefaultStream: state.stream})
		if err != nil {
			return err
		}
		state.t.Cleanup(func() {
			if err := client.Close(); err != nil {
				state.t.Errorf("close scenario user: %v", err)
			}
		})
		flow.writer = client
		id, err := bridgeAgent("writer")
		if err != nil {
			return err
		}
		session := client.Sessions().Create("writer").As(id)
		_, lease, err := session.Begin(ctx)
		if err != nil {
			return err
		}
		defer lease.Release()
		value, err := dynOf(1)
		if err != nil {
			return err
		}
		if _, err = session.State().Set(ctx, "step", value); err != nil {
			return err
		}
		if _, err = session.Complete(ctx, "done"); err != nil {
			return err
		}
		flow.byLabel["writer"] = session
		return nil
	case text == "aggregate session reads remain refused for that writer":
		sessions := flow.writer.Sessions()
		id := flow.byLabel["writer"].Conversation()
		_, getErr := sessions.Get(ctx, id)
		_, sourcesErr := sessions.Sources(ctx, id)
		for _, err := range []error{getErr, sourcesErr} {
			var coded interface{ ResultCode() wire.ResultCode }
			if !errors.Is(err, ierror.Unauthorized{}) && (!errors.As(err, &coded) || !coded.ResultCode().IsPermissionDenied()) {
				return fmt.Errorf("want a permission refusal, got %v", err)
			}
		}
		return nil
	case strings.HasSuffix(text, " has a bound addressee filter"):
		binding, err := state.topic(wire.AgentSessions).Group(args[0]).Filter().Get(ctx)
		if err != nil {
			return err
		}
		if binding == nil {
			return fmt.Errorf("worker group has no filter binding")
		}
		return nil
	case strings.Contains(text, " sends untargeted low-altitude work with send_agent and request"):
		source, err := bridgeAgent(args[0])
		if err != nil {
			return err
		}
		conversation := flow.session.Conversation()
		if _, err := state.client.SendAgent(ctx, wire.AgentSessions, []byte("send"), &laser.Provenance{Conversation: conversation, Agent: &source}); err != nil {
			return err
		}
		partitions, err := allPartitions(ctx, state, wire.AgentSessions)
		if err != nil {
			return err
		}
		bound, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		consumer, err := state.topic(wire.AgentSessions).Consumer(bound, "request-replies", laser.ConsumerOptions{Partitions: partitions})
		if err != nil {
			return err
		}
		defer consumer.Close(ctx)
		requester := state.client.Requester(source, wire.AgentSessions)
		done := make(chan error, 1)
		go func() {
			for {
				r, err := consumer.Next(bound)
				if err != nil {
					done <- err
					return
				}
				message, err := laser.OpenAgdx(r)
				if err == nil && message != nil && requester.Hub().Deliver(message) {
					done <- nil
					return
				}
			}
		}()
		reply, err := requester.Request(bound, conversation, []byte("request"), laser.RequestOptions{Timeout: 20 * time.Second})
		cancel()
		readErr := <-done
		if err != nil {
			return err
		}
		if readErr != nil {
			return readErr
		}
		if reply == nil || string(reply.Envelope.Body) != "answered" {
			return fmt.Errorf("untargeted request returned an unexpected reply")
		}
		return nil
	case strings.Contains(text, " handles both untargeted records for the session "):
		id := flow.byLabel[args[1]].Conversation()
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			flow.worker.mu.Lock()
			defer flow.worker.mu.Unlock()
			if len(flow.worker.messages) < 2 {
				return false, nil
			}
			if len(flow.worker.messages) != 2 || flow.worker.messages[0] != id || flow.worker.messages[1] != id {
				return false, fmt.Errorf("unexpected handled records: %v", flow.worker.messages)
			}
			return true, nil
		})
	}
	return fmt.Errorf("this runner does not implement %q", text)
}
