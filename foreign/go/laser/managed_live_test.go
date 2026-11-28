//go:build integration

package laser_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALiveStoreWhenBatchingAndExpiringThenShouldAnswerEverySlotAndVersion(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	if caps := client.Capabilities(); !caps.Kv.Available || !caps.Kv.CAS {
		t.Skip("the deployment serves no managed key-value store with compare-and-swap")
	}
	kv := liveScratch(t, client, ctx, "laser-live-kv").Laser().Kv("live")
	for key, value := range map[string]string{"a": "first", "b": ""} {
		if err := kv.Set([]byte(key)).Bytes([]byte(value)).Send(ctx); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}

	values, err := kv.GetMany(ctx, [][]byte{[]byte("a"), []byte("missing"), []byte("b")})
	if err != nil {
		t.Fatalf("get many: %v", err)
	}
	if want := [][]byte{[]byte("first"), nil, {}}; !reflect.DeepEqual(values, want) {
		t.Fatalf("get many = %q, want %q", values, want)
	}

	version, err := kv.Expire(ctx, []byte("a"), time.Hour)
	if err != nil {
		t.Fatalf("expire: %v", err)
	}
	stored, err := kv.GetEntry(ctx, []byte("a"))
	if err != nil || stored == nil || stored.Version != version || stored.ExpiresAtMicros == nil {
		t.Fatalf("expire answered version %d, the entry reads %+v, %v", version, stored, err)
	}

	committed, err := kv.Set([]byte("c")).Bytes([]byte("claimed")).ExpectAbsent().TTL(time.Minute).Commit(ctx)
	if err != nil || committed == 0 {
		t.Fatalf("conditional write with a ttl: version %d, %v", committed, err)
	}
	claimed, err := kv.GetEntry(ctx, []byte("c"))
	if err != nil || claimed == nil || claimed.ExpiresAtMicros == nil {
		t.Fatalf("the claimed entry must carry its expiry, got %+v, %v", claimed, err)
	}
	if client.SeenPositions().Kv == nil {
		t.Fatal("the replies carried no key-value frontier")
	}
}

func TestGivenALiveGraphWhenRelinkedAndReadThroughALensThenShouldSupersedeAndAnswer(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	if !client.Capabilities().Graph {
		t.Skip("the deployment serves no managed graph")
	}
	scoped := liveScratch(t, client, ctx, "laser-live-graph").Laser()
	host := wire.EntityNode("host", "node-7")
	opened := func() []wire.NodeID {
		t.Helper()
		live, err := scoped.Graph("live").Neighbors(ctx, host.ID, wire.EdgeOut, "opened", 1)
		if err != nil {
			t.Fatalf("neighbors: %v", err)
		}
		var targets []wire.NodeID
		for _, edge := range live.Edges {
			if edge.ValidTo == nil {
				targets = append(targets, edge.To)
			}
		}
		return targets
	}

	if err := scoped.Graph("live").Link(ctx, "host:node-7", "opened", "ticket:7"); err != nil {
		t.Fatalf("link: %v", err)
	}
	if want := []wire.NodeID{wire.EntityNode("ticket", "7").ID}; !reflect.DeepEqual(opened(), want) {
		t.Fatalf("live targets after the link = %v, want %v", opened(), want)
	}

	closed, err := scoped.Graph("live").Relink(ctx, "host:node-7", "opened", "ticket:9")
	if err != nil || closed != 1 {
		t.Fatalf("relink closed %d edges, %v, want 1", closed, err)
	}
	if want := []wire.NodeID{wire.EntityNode("ticket", "9").ID}; !reflect.DeepEqual(opened(), want) {
		t.Fatalf("live targets after the relink = %v, want %v", opened(), want)
	}

	if err := scoped.Graph("live").Unlink(ctx, "host:node-7", "opened", "ticket:9"); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if remaining := opened(); len(remaining) != 0 {
		t.Fatalf("live targets after the unlink = %v, want none", remaining)
	}

	lensed, err := scoped.Graph("live").Conversation(laser.NewConversationID()).StartIDs(host.ID).Fetch(ctx)
	if err != nil {
		t.Fatalf("a lens read must be served: %v", err)
	}
	if len(lensed.Nodes) != 0 {
		t.Fatalf("a conversation that asserted nothing saw %d nodes", len(lensed.Nodes))
	}
}

func TestGivenALiveIndexWhenOneBuilderRunsAgainThenShouldBindEveryReplyToItsOwnExecution(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	if caps := client.Capabilities(); !caps.Query.Available || !caps.Materializations {
		t.Skip("the deployment serves no managed queries and materializations")
	}
	scoped := liveScratch(t, client, ctx, "laser-live-query").Laser()
	topic := scoped.Topic("readings")
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		t.Fatalf("create the topic: %v", err)
	}
	for _, cpu := range []int{41, 42, 43} {
		if _, err := topic.Publish().JSON(map[string]int{"cpu": cpu}).Send(ctx); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	projection := wire.Projection{ID: "cpu_readings.v1", Name: "CPU readings", Version: 1, ContentType: wire.ContentJSON,
		Extraction: wire.IndexSchema{Fields: []wire.IndexField{wire.TypedIndexField("cpu", "/cpu", wire.FieldInt)}}}
	view, err := topic.Materialize().Typed(projection).Index("cpu_readings").Apply(ctx)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if wait, err := view.Wait(ctx, wire.ReadinessCaughtUp, 25*time.Second); err != nil || !wait.Ready() {
		t.Fatalf("the view did not catch up: %+v, %v", wait, err)
	}

	request := scoped.Query("cpu_readings").SelectFields("cpu").OrderAsc("cpu").Limit(2)
	first := request.ExecutionID()
	for call := range 2 {
		page, err := request.Fetch(ctx)
		if err != nil || len(page.Rows) != 2 {
			t.Fatalf("call %d: %d rows, %v", call, pageRows(page), err)
		}
	}
	if request.ExecutionID() == first {
		t.Fatal("the builder kept its execution identity across calls")
	}

	set, err := request.FetchAll(ctx)
	if err != nil {
		t.Fatalf("fetch all: %v", err)
	}
	collected := make([]int64, len(set.Rows))
	for index, row := range set.Rows {
		collected[index], _ = row.ValueI64("cpu")
	}
	if want := []int64{41, 42, 43}; !reflect.DeepEqual(collected, want) {
		t.Fatalf("fetch all read %v over pages of two, want %v", collected, want)
	}
}

func pageRows(page *wire.QueryResult) int {
	if page == nil {
		return 0
	}
	return len(page.Rows)
}
