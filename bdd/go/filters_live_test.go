package bdd_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedLiveFilterScenariosWhenRunThenShouldHold runs consumer
// filters against a streaming server.
//
// The scenarios that need a managed backend are tagged and skipped. What is
// left proves the two things open Apache Iggy must get right: an unbound
// group is delivered every record, and the saved-filter catalog is refused
// rather than half-served.
func TestGivenTheSharedLiveFilterScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "filters_live.feature", 2*time.Minute, liveFilterStep)
}

// feedTopic is the change feed the anomaly desk reads.
const feedTopic = "fleet_changes"

// loadFeed is the order the shared corpus publishes its records in.
func loadFeed() ([]string, error) {
	raw, err := os.ReadFile(filepath.Join(scenarios, "filter_records.json"))
	if err != nil {
		return nil, err
	}
	var fixtures recordFixtures
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		return nil, err
	}
	if len(fixtures.Feed) == 0 {
		return nil, fmt.Errorf("the corpus names no feed")
	}
	return fixtures.Feed, nil
}

func liveFilterStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text

	switch {
	case text == "a fresh fleet change feed":
		return openFeed(ctx, state)

	case text == "the anomaly desk lists its saved filters":
		return listSavedFilters(ctx, state)

	case text == "the catalog is refused as unsupported":
		var unsupported *laser.UnsupportedError
		if !errors.As(state.failure, &unsupported) {
			return fmt.Errorf("want the catalog refused as unsupported, got %v",
				state.failure)
		}
		return nil

	case text == "the anomaly desk reads every record through an unbound consumer group":
		return readUnboundGroup(ctx, state)

	case text == "it receives every original feed record":
		return receivedWholeFeed(state)

	case text == "the anomaly desk reads every record through an unbound group reader":
		return readUnboundReader(ctx, state)

	case text == "the advanced reader follows the group-aware read capability":
		return followedGroupCapability(state)

	case text == "the group reads back its saved binding after three refusals":
		return bindingAfterCatalogRefusals(ctx, state)

	default:
		return managedFilterStep(ctx, state, step)
	}
}

// openFeed publishes the shared corpus in its feed order.
func openFeed(ctx context.Context, state *world) error {
	if err := state.freshStream(ctx); err != nil {
		return err
	}
	if err := state.topic(feedTopic).Ensure(ctx, 1, nil); err != nil {
		return err
	}
	records, err := loadRecords()
	if err != nil {
		return err
	}
	feed, err := loadFeed()
	if err != nil {
		return err
	}
	var published [][]byte
	for _, name := range feed {
		payload, found := records[name]
		if !found {
			return fmt.Errorf("the feed names the record %q, which the corpus "+
				"does not define", name)
		}
		// One record per call, because the feed's order is what the readers
		// are checked against and a batch could be split across a flush.
		if _, err := state.topic(feedTopic).
			Publish().Payload(payload).Send(ctx); err != nil {
			return err
		}
		published = append(published, payload)
	}
	state.put("feed", published)
	return nil
}

func listSavedFilters(ctx context.Context, state *world) error {
	_, state.failure = state.topic(feedTopic).Group("anomaly-desk").Filter().Revisions(ctx, 1, 10)
	if state.failure == nil {
		return fmt.Errorf("a server with no managed backend served the catalog")
	}
	return nil
}

// readUnboundGroup reads the feed through a group that carries no policy.
//
// Nothing filters it, so every record must arrive: a group with no policy
// that quietly delivered a subset would be worse than one that refused.
func readUnboundGroup(ctx context.Context, state *world) error {
	first := iggcon.FirstPollingStrategy()
	consumer, err := state.topic(feedTopic).Group("unbound-consumers").Consumer(
		ctx, laser.ConsumerOptions{Start: &first, ManualCommit: true})
	if err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = consumer.Close(shutdown)
	}()

	feed, found := state.get("feed")
	if !found {
		return fmt.Errorf("the feed was never published")
	}
	wanted := feed.([][]byte)
	var delivered [][]byte
	for range wanted {
		read, cancel := context.WithTimeout(ctx, 15*time.Second)
		record, err := consumer.Next(read)
		cancel()
		if err != nil {
			return err
		}
		delivered = append(delivered, record.Payload())
		if err := consumer.Commit(ctx, record.Checkpoint); err != nil {
			return err
		}
	}
	state.put("delivered", delivered)
	return nil
}

func receivedWholeFeed(state *world) error {
	feed, found := state.get("feed")
	if !found {
		return fmt.Errorf("the feed was never published")
	}
	delivered, found := state.get("delivered")
	if !found {
		return fmt.Errorf("nothing was read")
	}
	wanted := feed.([][]byte)
	got := delivered.([][]byte)
	if len(got) != len(wanted) {
		return fmt.Errorf("want %d records, got %d", len(wanted), len(got))
	}
	for index, payload := range wanted {
		if string(got[index]) != string(payload) {
			return fmt.Errorf("record %d differs from what was published", index)
		}
	}
	return nil
}

// readUnboundReader reads through the advanced group reader, which asks the
// server to resolve the group's own policy.
//
// An unbound group has none, so a server that serves group-aware reads must
// deliver the feed unfiltered, and one that does not must refuse rather than
// answer as though it had applied a policy it never read.
func readUnboundReader(ctx context.Context, state *world) error {
	group := state.topic(feedTopic).Group("unbound-readers")
	if _, err := group.Create().Build(ctx); err != nil {
		return err
	}
	options := firstStart()
	options.Count = 100
	options.MaxExamined = 100
	reader, err := group.OpenReaderWith(ctx, options)
	if err != nil {
		state.failure = err
		return nil
	}
	defer reader.Close(ctx)
	feed, found := state.get("feed")
	if !found {
		return fmt.Errorf("the feed was never published")
	}
	var delivered [][]byte
	for len(delivered) < len(feed.([][]byte)) {
		page, err := nextPage(ctx, reader)
		if err != nil {
			return err
		}
		if page.Page.Policy.Mode != wire.ModeUnfiltered {
			return fmt.Errorf("an unbound group must read unfiltered, got mode %q", page.Page.Policy.Mode)
		}
		for _, record := range page.Records {
			if record.Evaluation.Kind != laser.EvaluationUnfiltered {
				return fmt.Errorf("record %d of an unbound group was evaluated", record.Offset())
			}
			delivered = append(delivered, record.Payload())
		}
		if err := reader.Ack(ctx, page); err != nil {
			return err
		}
	}
	state.put("delivered", delivered)
	return nil
}

// followedGroupCapability checks the reader did what the server advertised.
func followedGroupCapability(state *world) error {
	served := state.client.Capabilities().Filters.GroupPolicyReads
	if !served {
		var unsupported *laser.UnsupportedError
		if !errors.As(state.failure, &unsupported) {
			return fmt.Errorf("the server advertises no group-aware reads, so "+
				"the reader must be refused as unsupported, got %v", state.failure)
		}
		if !strings.Contains(unsupported.Surface, "filter") {
			return fmt.Errorf("want the filters surface named, got %q",
				unsupported.Surface)
		}
		return nil
	}
	if state.failure != nil {
		return fmt.Errorf("the server advertises group-aware reads, so the "+
			"reader must succeed, got %v", state.failure)
	}
	return receivedWholeFeed(state)
}

// bindingAfterCatalogRefusals configures a group policy and reads it back
// through a catalog that refuses the first three reads.
//
// The read carries no operation identity to repeat under, so before the
// managed band walked the roster again a refusal the catalog served a moment
// later reached the caller as a final answer.
func bindingAfterCatalogRefusals(ctx context.Context, state *world) error {
	filters, err := namedFilters()
	if err != nil {
		return err
	}
	safeMode, named := filters["safe mode"]
	if !named {
		return fmt.Errorf("the corpus names no \"safe mode\" filter")
	}
	group := state.topic(feedTopic).Group("anomaly-desk")
	info, err := group.Create().Filter("safe-mode", safeMode).Build(ctx)
	if err != nil {
		return fmt.Errorf("configure the group policy: %w", err)
	}
	if info.Filter == nil {
		return fmt.Errorf("the group was created without a saved binding")
	}
	binding, err := group.Filter().Get(ctx)
	if err != nil {
		return fmt.Errorf("the catalog must serve the binding read: %w", err)
	}
	if binding == nil {
		return fmt.Errorf("the binding read came back empty")
	}
	if binding.Identity != info.Filter.Identity {
		return fmt.Errorf("want the saved binding %v, got %v",
			info.Filter.Identity, binding.Identity)
	}
	return nil
}
