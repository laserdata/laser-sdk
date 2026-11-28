package laser

import (
	"context"
	"sort"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenCachedStreamStateWhenAStreamOrTopicChangesThenShouldDropWhatItCached(t *testing.T) {
	changes := map[string]struct {
		change func(*Laser) error
		hubs   []string
		// registry is whether the stream's shared registry is still cached afterwards.
		registry bool
	}{
		"stream deleted": {func(handle *Laser) error {
			_, err := handle.Stream("agents").Delete(context.Background())
			return err
		}, []string{"other/agent.sessions"}, false},
		"stream renamed": {func(handle *Laser) error {
			return handle.Stream("agents").Rename(context.Background(), "fleet")
		}, []string{"other/agent.sessions"}, false},
		"topic deleted": {func(handle *Laser) error {
			_, err := handle.Stream("agents").Topic("agent.sessions").Delete(context.Background())
			return err
		}, []string{"agents/replies", "other/agent.sessions"}, true},
		"topic updated": {func(handle *Laser) error {
			return handle.Stream("agents").Topic("agent.sessions").Update(context.Background(), UpdateTopicOptions{Name: "sessions"})
		}, []string{"agents/replies", "other/agent.sessions"}, true},
		"partitions added": {func(handle *Laser) error {
			return handle.Stream("agents").Topic("agent.sessions").AddPartitions(context.Background(), 1)
		}, []string{"agents/replies", "other/agent.sessions"}, true},
		"partitions deleted": {func(handle *Laser) error {
			return handle.Stream("agents").Topic("agent.sessions").DeletePartitions(context.Background(), 1)
		}, []string{"agents/replies", "other/agent.sessions"}, true},
		"registry topic deleted": {func(handle *Laser) error {
			_, err := handle.Stream("agents").Topic(wire.AgentRegistry).Delete(context.Background())
			return err
		}, []string{"agents/agent.sessions", "agents/replies", "other/agent.sessions"}, false},
	}
	for name, changed := range changes {
		handle := FromClient(&adminRecorder{}, Options{DefaultStream: "agents"})
		stopped := map[string]bool{}
		shared := handle.state()
		shared.registries = map[string]*sharedRegistry{"agents": {registry: NewAgentRegistry()}, "other": {registry: NewAgentRegistry()}}
		shared.replyHubs = map[replyHubKey]*replyDispatcher{}
		for _, key := range []replyHubKey{{"agents", "agent.sessions"}, {"agents", "replies"}, {"other", "agent.sessions"}} {
			shared.replyHubs[key] = &replyDispatcher{hub: NewReplyHub(), cancel: func() { stopped[key.stream+"/"+key.topic] = true }}
		}

		if err := changed.change(handle); err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		var kept []string
		for key := range shared.replyHubs {
			kept = append(kept, key.stream+"/"+key.topic)
			if stopped[key.stream+"/"+key.topic] {
				t.Errorf("%s: a kept reply reader was stopped: %v", name, key)
			}
		}
		sort.Strings(kept)
		if len(kept) != len(changed.hubs) {
			t.Errorf("%s: want the reply readers %v kept, got %v", name, changed.hubs, kept)
		}
		for index := range kept {
			if index < len(changed.hubs) && kept[index] != changed.hubs[index] {
				t.Errorf("%s: want the reply readers %v kept, got %v", name, changed.hubs, kept)
			}
		}
		if len(stopped) != 3-len(kept) {
			t.Errorf("%s: want every dropped reply reader stopped, %d stopped of %d dropped", name, len(stopped), 3-len(kept))
		}
		if _, cached := shared.registries["agents"]; cached != changed.registry {
			t.Errorf("%s: registry cached = %v, want %v", name, cached, changed.registry)
		}
		if _, cached := shared.registries["other"]; !cached {
			t.Errorf("%s: another stream's registry must stay cached", name)
		}
	}
}

// adminRecorder accepts every stream and topic change.
type adminRecorder struct{ iggcon.Client }

func (*adminRecorder) Close() error { return nil }

func (*adminRecorder) DeleteStream(context.Context, iggcon.Identifier) error { return nil }

func (*adminRecorder) UpdateStream(context.Context, iggcon.Identifier, iggcon.StreamUpdate) error {
	return nil
}

func (*adminRecorder) DeleteTopic(context.Context, iggcon.Identifier, iggcon.Identifier) error {
	return nil
}

func (*adminRecorder) UpdateTopic(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.TopicUpdate) error {
	return nil
}

func (*adminRecorder) CreatePartitions(context.Context, iggcon.Identifier, iggcon.Identifier, uint32) error {
	return nil
}

func (*adminRecorder) DeletePartitions(context.Context, iggcon.Identifier, iggcon.Identifier, uint32) error {
	return nil
}
