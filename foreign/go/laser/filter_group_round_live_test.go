//go:build integration

package laser_test

import (
	"context"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALiveGroupPolicyWhenReadingRoundsUnderTheLocalGuardThenShouldDeliverOnlyWhatTheFilterSelects(t *testing.T) {
	client := dialLive(t)
	if !client.Capabilities().Filters.Catalog || !client.Capabilities().Filters.GroupPolicyReads {
		t.Skip("requires the filter catalog and group-aware reads")
	}
	ctx := liveContext(t)
	scratch := liveScratch(t, client, ctx, "laser-round-guard")
	topic := scratch.Laser().Topic("changes")
	if err := topic.Ensure(ctx, 3, nil); err != nil {
		t.Fatal(err)
	}
	selected := 0
	for index := range 30 {
		table := "ground_stations"
		if index%5 == 0 {
			table = "satellites"
			selected++
		}
		row := map[string]any{"table": table, "row": index}
		if _, err := topic.PublishBatch().Key(liveName("key")).AddJSON(row).Send(ctx); err != nil {
			t.Fatal(err)
		}
	}
	group := topic.Group("orbit-desk")
	filter := wire.NewConsumerFilter(wire.ExprPredicate("table", wire.CmpEq, wire.StringTyped("satellites")), wire.CodecJSON)
	if _, err := group.Create().Filter("satellites", filter).Build(ctx); err != nil {
		t.Fatal(err)
	}
	reader, err := group.OpenReaderWith(ctx, laser.GroupReaderOptions{LocalGuard: true, Count: 4, MaxExamined: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(ctx)

	var examined uint64
	for delivered := 0; delivered < selected; delivered++ {
		record, err := reader.NextRecord(ctx)
		if err != nil {
			t.Fatalf("read record %d of %d: %v", delivered, selected, err)
		}
		examined += reader.ExaminedInRound()
		var row struct {
			Table string `json:"table"`
		}
		if err := record.JSON(&row); err != nil || row.Table != "satellites" {
			t.Fatalf("want a satellites row, got %q and %v", record.Payload(), err)
		}
		if !record.Evaluation.Selected() || record.HeadersMalformed() || !reader.Owns(record.Page()) {
			t.Fatalf("want a selected record of this reader, got %+v", record.Evaluation)
		}
		if err := reader.AckRecord(ctx, record.Page(), record.Offset()); err != nil {
			t.Fatalf("acknowledge offset %d: %v", record.Offset(), err)
		}
	}
	if examined == 0 {
		t.Fatal("the rounds that found records examined none")
	}
	// The reader drains what the filter rejects, so nothing is left to read.
	for {
		page, more, err := reader.ReadRound(ctx)
		if err != nil || page != nil {
			t.Fatalf("want no further match, got %+v and %v", page, err)
		}
		if !more {
			break
		}
	}

	type answer struct {
		page *laser.MatchedPage
		err  error
	}
	answered := make(chan answer, 1)
	go func() {
		page, err := reader.NextPage(ctx)
		answered <- answer{page, err}
	}()
	if _, err := topic.Publish().JSON(map[string]any{"table": "satellites", "row": 99}).Send(ctx); err != nil {
		t.Fatal(err)
	}
	got := <-answered
	if got.err != nil || len(got.page.Records) != 1 {
		t.Fatalf("want the record published while waiting, got %+v and %v", got.page, got.err)
	}
	if err := reader.Ack(ctx, got.page); err != nil {
		t.Fatal(err)
	}
}

func TestGivenALiveGroupWhenTheReaderNamesOnePartitionThenShouldReadOnlyThatPartition(t *testing.T) {
	client := dialLive(t)
	if !client.Capabilities().Filters.GroupPolicyReads {
		t.Skip("requires group-aware reads")
	}
	ctx := liveContext(t)
	scratch := liveScratch(t, client, ctx, "laser-round-partition")
	topic := scratch.Laser().Topic("events")
	producer, err := topic.Producer().Partitions(2).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for partition := range uint32(2) {
		if _, err := producer.SendToPartition(ctx, directMessage(t, "record"), partition); err != nil {
			t.Fatal(err)
		}
	}
	if err := producer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	group := topic.Group("second-half")
	if err := group.EnsureNative(ctx); err != nil {
		t.Fatal(err)
	}
	first := wire.StartAtFirst()
	reader, err := group.OpenReaderWith(ctx, laser.GroupReaderOptions{Partitions: []uint32{1}, Start: &first, MaxUnackedPages: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(ctx)

	page, err := reader.NextPage(ctx)
	if err != nil || page.PartitionID != 1 || len(page.Records) != 1 {
		t.Fatalf("want the record of partition 1, got %+v and %v", page, err)
	}
	if page.Records[0].Evaluation.Kind != laser.EvaluationUnfiltered {
		t.Fatalf("an unbound group reads unfiltered, got %+v", page.Records[0].Evaluation)
	}
	if err := reader.Ack(ctx, page); err != nil {
		t.Fatal(err)
	}
	brief, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if again, err := reader.TryNextPage(brief); err != nil || again != nil {
		t.Fatalf("the partition left out must stay unread, got %+v and %v", again, err)
	}
}
