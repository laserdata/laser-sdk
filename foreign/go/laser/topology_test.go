package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenNoOverridesWhenAskedThenShouldNameTheDefaultTopology(t *testing.T) {
	handle := laser.FromClient(&announcingClient{}, laser.Options{})

	got := topologyOf(handle)
	want := topology{wire.OpsStream, wire.ControlTopic, wire.DLQTopic, wire.ChangesTopic}
	if got != want {
		t.Errorf("want %+v, got %+v", want, got)
	}
}

func TestGivenTopologyOptionsWhenWrappingAClientThenShouldNameThem(t *testing.T) {
	handle := laser.FromClient(&announcingClient{}, laser.Options{
		OpsStream: "acme-ops", ControlTopic: "acme-control",
		DLQTopic: "acme-dead", ChangesTopic: "acme-changes",
	})

	got := topologyOf(handle)
	want := topology{"acme-ops", "acme-control", "acme-dead", "acme-changes"}
	if got != want {
		t.Errorf("want %+v, got %+v", want, got)
	}
}

func TestGivenAHandleWhenScopedWithEveryTopologyMethodThenShouldLeaveItUntouched(t *testing.T) {
	original := laser.FromClient(&announcingClient{}, laser.Options{})

	scoped := original.
		WithOpsStream("acme-ops").
		WithControlTopic("acme-control").
		WithDLQTopic("acme-dead").
		WithChangesTopic("acme-changes").
		WithDefaultStream("acme")

	want := topology{"acme-ops", "acme-control", "acme-dead", "acme-changes"}
	if got := topologyOf(scoped); got != want {
		t.Errorf("want the scoped handle on %+v, got %+v", want, got)
	}
	untouched := topologyOf(laser.FromClient(&announcingClient{}, laser.Options{}))
	if got := topologyOf(original); got != untouched {
		t.Errorf("want the original on %+v, got %+v", untouched, got)
	}
}

func TestGivenAnAnnouncedTopologyWhenRefreshedThenShouldAdoptItExceptWhereOverridden(t *testing.T) {
	announced := wire.DefaultWireTopology()
	announced.OpsStream = "fleet-ops"
	announced.ControlTopic = "fleet-control"
	announced.DLQTopic = "fleet-dead"
	announced.ChangesTopic = "fleet-changes"
	announce := wire.NewBackendAnnounce(wire.NewOpVersions(1, 1, 1, 1))
	announce.Topology = &announced
	client := &announcingClient{reply: marshalled(t, announce)}
	handle := laser.FromClient(client, laser.Options{ChangesTopic: "pinned-changes"})
	derived := handle.WithOpsStream("pinned-ops")

	if got := handle.RefreshCapabilities(context.Background()).Hello; got != laser.HelloAnswered {
		t.Fatalf("want an answered probe, got %s", got)
	}

	want := topology{"fleet-ops", "fleet-control", "fleet-dead", "pinned-changes"}
	if got := topologyOf(handle); got != want {
		t.Errorf("want %+v, got %+v", want, got)
	}
	wantDerived := topology{"pinned-ops", "fleet-control", "fleet-dead", "pinned-changes"}
	if got := topologyOf(derived); got != wantDerived {
		t.Errorf("want the derived handle on %+v, got %+v", wantDerived, got)
	}
}

func TestGivenAProbeWithoutATopologyWhenRefreshedThenShouldKeepTheOneAlreadyAnnounced(t *testing.T) {
	announced := wire.DefaultWireTopology()
	announced.OpsStream = "fleet-ops"
	announce := wire.NewBackendAnnounce(wire.NewOpVersions(1, 1, 1, 1))
	announce.Topology = &announced
	client := &announcingClient{reply: marshalled(t, announce)}
	handle := laser.FromClient(client, laser.Options{})
	handle.RefreshCapabilities(context.Background())

	client.reply = marshalled(t, wire.NewBackendAnnounce(wire.NewOpVersions(1, 1, 1, 1)))
	handle.RefreshCapabilities(context.Background())

	if got := handle.OpsStream(); got != "fleet-ops" {
		t.Errorf("want the announced ops stream kept, got %q", got)
	}
}

func TestGivenAnEmptyNameWhenScopedThenShouldFallBackToTheAnnouncedName(t *testing.T) {
	handle := laser.FromClient(&announcingClient{}, laser.Options{OpsStream: "acme-ops"})

	if got := handle.WithOpsStream("").OpsStream(); got != wire.OpsStream {
		t.Errorf("want the default ops stream, got %q", got)
	}
}

func TestGivenAHalfGivenAddressWhenConnectingThenShouldRefuseBeforeDialing(t *testing.T) {
	for name, scenario := range map[string]struct {
		given   string
		options laser.Options
	}{
		"address without credentials":           {options: laser.Options{Address: "127.0.0.1:8090"}},
		"credentials without an address":        {options: laser.Options{Username: "iggy", Password: "secret"}},
		"an address beside a connection string": {given: "iggy:secret@127.0.0.1:8090", options: laser.Options{Address: "127.0.0.1:8090", Username: "iggy", Password: "secret"}},
		"nothing given":                         {},
	} {
		t.Run(name, func(t *testing.T) {
			scenario.options.Timeout = 30 * time.Second
			scenario.options.Environment = noEnv()
			started := time.Now()
			_, err := laser.ConnectWith(context.Background(), scenario.given, scenario.options)
			var config *laser.ConfigError
			if !errors.As(err, &config) {
				t.Fatalf("want a ConfigError, got %T %v", err, err)
			}
			if elapsed := time.Since(started); elapsed > 5*time.Second {
				t.Errorf("a configuration refusal must not wait on the deadline, took %v", elapsed)
			}
		})
	}
}

type topology struct {
	opsStream, controlTopic, dlqTopic, changesTopic string
}

func topologyOf(handle *laser.Laser) topology {
	return topology{handle.OpsStream(), handle.ControlTopic(), handle.DLQTopic(), handle.ChangesTopic()}
}

// announcingClient answers the capability probe with reply, or as Apache Iggy when it has none.
type announcingClient struct {
	clusterOfOne
	reply []byte
}

func (client *announcingClient) Close() error { return nil }

func (client *announcingClient) SendBinaryRequest(
	ctx context.Context, code uint32, payload []byte,
) ([]byte, error) {
	if client.reply == nil {
		return client.clusterOfOne.SendBinaryRequest(ctx, code, payload)
	}
	return client.reply, nil
}

func marshalled(t *testing.T, announce wire.BackendAnnounce) []byte {
	t.Helper()
	encoded, err := wire.Marshal(announce)
	if err != nil {
		t.Fatalf("encode the announcement: %v", err)
	}
	return encoded
}
