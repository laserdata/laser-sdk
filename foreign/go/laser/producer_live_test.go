//go:build integration

package laser_test

import (
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenADirectProducerOnIggyWhenPublishingThenShouldReadTheRecordsFromTheirPartition(t *testing.T) {
	ctx := liveContext(t)
	client := dialLive(t)
	scratch := liveScratch(t, client, ctx, "direct")
	topic := scratch.Laser().Topic("readings")
	producer, err := topic.Producer().Partitions(2).BatchLength(1).Routing(laser.PartitionRouting(1)).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close(ctx)
	if _, err = producer.SendBatch(ctx, []laser.ProducerMessage{directMessage(t, "one"), directMessage(t, "two")}); err != nil {
		t.Fatal(err)
	}
	cursor, err := topic.Replay()
	if err != nil {
		t.Fatal(err)
	}
	records, err := cursor.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].PartitionID != 1 || records[1].PartitionID != 1 || string(records[0].Payload()) != "one" || string(records[1].Payload()) != "two" {
		t.Fatalf("unexpected direct delivery %+v", records)
	}
}

func TestGivenManagedProducerReportingWhenTwoHandlesPublishThenShouldUseOneIsolatedObserver(t *testing.T) {
	t.Setenv("LASER_PRODUCER_TELEMETRY_INTERVAL_MS", "1000")
	ctx := liveContext(t)
	client := dialLive(t)
	if client.Capabilities().Hello != laser.HelloAnswered {
		t.Skip("requires the managed client metadata extension")
	}
	me, err := client.Me(ctx)
	if err != nil {
		t.Fatal(err)
	}
	agent := agentID(t, liveName("observer"))
	if err := client.AdvertisePresence(ctx, wire.AgentPresence{V: 1, Agent: agent}); err != nil {
		t.Fatal(err)
	}
	scratch := liveScratch(t, client, ctx, "report")
	topic := scratch.Laser().Topic("stats")
	p, err := topic.Producer().Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close(ctx)
	other, err := topic.Producer().Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(ctx)
	if _, err := p.Send(ctx, []byte("sample")); err != nil {
		t.Fatal(err)
	}
	firstID, otherID := p.Statistics().InstanceID, other.Statistics().InstanceID
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		entries, err := client.ClientMetadata().WithMetadataOnly(true).All(ctx)
		if err != nil {
			t.Fatal(err)
		}
		original, reported := false, false
		for _, entry := range entries {
			if entry.ClientID == me.ClientID {
				var presence wire.AgentPresence
				if err := wire.Unmarshal(entry.Metadata, &presence); err != nil || presence.Agent != agent {
					t.Fatal("producer observation overwrote the application's presence")
				}
				original = true
				continue
			}
			var report wire.ProducerPresence
			if wire.Unmarshal(entry.Metadata, &report) != nil || report.ProducerPresenceVersion != 1 {
				continue
			}
			foundFirst, foundOther := false, false
			for _, s := range report.Producers {
				if s.InstanceID == firstID {
					foundFirst = s.Stream == scratch.Name() && s.Topic == "stats" && s.SubmittedRecords == 1 && s.ConfirmedRecords != nil && *s.ConfirmedRecords == 1 && s.SubmittedPayloadBytes == 6 && s.Latency.Samples == 1
				}
				if s.InstanceID == otherID {
					foundOther = true
				}
			}
			if foundFirst && foundOther && report.ExpiresAfterMillis == 3000 {
				reported = true
			}
		}
		if original && reported {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("managed producer statistics failed to appear before the deadline")
		case <-ticker.C:
		}
	}
}
