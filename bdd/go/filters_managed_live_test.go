package bdd_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func managedFilterStep(ctx context.Context, state *world, step bdd.Step) error {
	text := step.Text
	args := argumentsOf(step)
	switch {
	case strings.HasPrefix(text, "the anomaly desk reads the feed with the "):
		if len(args) != 1 {
			return fmt.Errorf("want one filter name, got %v", args)
		}
		group, _, err := createPolicy(ctx, state.topic(feedTopic), "anomaly-desk", args[0])
		if err != nil {
			return err
		}
		reader, err := openReader(ctx, state, group, firstStart())
		if err != nil {
			return err
		}
		page, err := nextPage(ctx, reader)
		if err != nil {
			return err
		}
		savePage(state, page)
		if err := reader.Ack(ctx, page); err != nil {
			return err
		}
		return reader.Close(ctx)
	case text == "the anomaly desk manages a saved policy by numeric group id":
		return manageNumericPolicy(ctx, state)
	case text == "the anomaly desk scans a hundred non-matches before its first match":
		return sparseScan(ctx, state, false)
	case text == "the anomaly desk consumes a hundred non-matches before its first match":
		return sparseScan(ctx, state, true)
	case text == "its first match follows an empty scan of one hundred records":
		return receivedNamed(state, []string{"mode update"})
	case text == "the anomaly desk rejects a page after releasing its policy":
		return stalePolicyPage(ctx, state)
	case strings.HasPrefix(text, "a \"") && strings.HasSuffix(text, "record follow the feed"):
		records, err := loadRecords()
		if err != nil {
			return err
		}
		if len(args) != 2 {
			return fmt.Errorf("want two record names, got %v", args)
		}
		for _, name := range args {
			body, ok := records[name]
			if !ok {
				return fmt.Errorf("unknown fixture %q", name)
			}
			if _, err := state.topic(feedTopic).Publish().Payload(body).Send(ctx); err != nil {
				return err
			}
		}
		return nil
	case strings.HasPrefix(text, "the anomaly desk skips the record its "):
		if len(args) != 1 {
			return fmt.Errorf("want one filter name, got %v", args)
		}
		return skipStoppedRecord(ctx, state, args[0])
	case strings.HasPrefix(text, "it receives the \""):
		return receivedNamed(state, args)
	default:
		return fmt.Errorf("unknown managed filter step %q", text)
	}
}

func receivedNamed(state *world, names []string) error {
	records, err := loadRecords()
	if err != nil {
		return err
	}
	var want [][]byte
	for _, name := range names {
		body, ok := records[name]
		if !ok {
			return fmt.Errorf("unknown record %q", name)
		}
		want = append(want, body)
	}
	delivered, ok := state.get("delivered")
	if !ok || !slices.EqualFunc(delivered.([][]byte), want, bytes.Equal) {
		return fmt.Errorf("want %q, received %q", want, delivered)
	}
	return nil
}

func createPolicy(ctx context.Context, topic *laser.Topic, groupName, filterName string) (*laser.ConsumerGroup, *laser.ConsumerGroupInfo, error) {
	filters, err := namedFilters()
	if err != nil {
		return nil, nil, err
	}
	filter, ok := filters[filterName]
	if !ok {
		return nil, nil, fmt.Errorf("unknown filter %q", filterName)
	}
	group := topic.Group(groupName)
	request := group.Create().Filter(strings.ReplaceAll(filterName, " ", "-"), filter)
	info, err := request.Build(ctx)
	if err != nil {
		return nil, nil, err
	}
	if info.Filter == nil {
		return nil, nil, fmt.Errorf("group %s has no saved filter", groupName)
	}
	return group, info, nil
}

// readTimeout bounds one wait for a page, as the peers' bindings bound theirs.
const readTimeout = 15 * time.Second

func firstStart() laser.GroupReaderOptions {
	first := wire.StartAtFirst()
	return laser.GroupReaderOptions{Start: &first}
}

// openReader joins the group as a reader that leaves it with the scenario.
func openReader(
	ctx context.Context, state *world, group *laser.ConsumerGroup, options laser.GroupReaderOptions,
) (*laser.GroupReader, error) {
	reader, err := group.OpenReaderWith(ctx, options)
	if err != nil {
		return nil, err
	}
	state.t.Cleanup(func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := reader.Close(closeCtx); err != nil {
			state.t.Errorf("leave filtered group: %v", err)
		}
	})
	return reader, nil
}

func nextPage(ctx context.Context, reader *laser.GroupReader) (*laser.MatchedPage, error) {
	bounded, stop := context.WithTimeout(ctx, readTimeout)
	defer stop()
	return reader.NextPage(bounded)
}

func savePage(state *world, page *laser.MatchedPage) {
	var bodies [][]byte
	for _, record := range page.Records {
		bodies = append(bodies, record.Payload())
	}
	state.put("delivered", bodies)
}

func manageNumericPolicy(ctx context.Context, state *world) error {
	filters, err := namedFilters()
	if err != nil {
		return err
	}
	topic := state.topic(feedTopic)
	group := topic.Group("managed-desk")
	operationID := laser.NewOperationID()
	request := group.Create().Filter("safe-mode", filters["safe mode"]).OperationID(operationID)
	first, err := request.Build(ctx)
	if err != nil {
		return err
	}
	again, err := request.Build(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(first, again) || first.Filter == nil {
		return fmt.Errorf("retried group creation returned different bindings")
	}
	numeric := topic.GroupByID(uint64(first.ID))
	same, err := numeric.Filter().ConfigureAs(ctx, &operationID, wire.DefinitionSpec("safe-mode", filters["safe mode"]))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(same, first.Filter) {
		return fmt.Errorf("the repeated saved operation returned a different binding")
	}
	consumer, err := numeric.Consumer(ctx, laser.ConsumerOptions{ManualCommit: true})
	if err != nil {
		return err
	}
	var delivered [][]byte
	for range 3 {
		record, err := consumer.Next(ctx)
		if err != nil {
			_ = consumer.Close(ctx)
			return err
		}
		delivered = append(delivered, record.Payload())
		if err := consumer.Commit(ctx, record.Checkpoint); err != nil {
			_ = consumer.Close(ctx)
			return err
		}
	}
	if err := consumer.Close(ctx); err != nil {
		return err
	}
	reader, err := openReader(ctx, state, numeric, firstStart())
	if err != nil {
		return err
	}
	page, err := nextPage(ctx, reader)
	if err != nil {
		return err
	}
	if err := group.Filter().SetRevisionEnabled(ctx, first.Filter.Revision, false); err != nil {
		return err
	}
	if err := reader.Ack(ctx, page); err != nil {
		return err
	}
	_, err = reader.TryNextPage(ctx)
	if err := wantFilterReason(err, wire.ReasonRevisionDisabled); err != nil {
		return err
	}
	if err := group.Filter().SetRevisionEnabled(ctx, first.Filter.Revision, true); err != nil {
		return err
	}
	if err := reader.Close(ctx); err != nil {
		return err
	}
	savePage(state, page)
	if !slices.EqualFunc(delivered, state.values["delivered"].([][]byte), bytes.Equal) {
		return fmt.Errorf("the reader and consumer returned different records")
	}
	_, err = group.Filter().Release(ctx)
	return err
}

func wantFilterReason(err error, reason wire.FilterErrorReason) error {
	var refusal *laser.FilterError
	if !errors.As(err, &refusal) || refusal.Reason() != reason {
		return fmt.Errorf("expected %s, got %v", reason, err)
	}
	return nil
}

func sparseScan(ctx context.Context, state *world, normal bool) error {
	records, err := loadRecords()
	if err != nil {
		return err
	}
	topic := state.topic("sparse_changes")
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		return err
	}
	for range 100 {
		if _, err := topic.Publish().Payload(records["ground station update"]).Send(ctx); err != nil {
			return err
		}
	}
	if _, err := topic.Publish().Payload(records["mode update"]).Send(ctx); err != nil {
		return err
	}
	group, _, err := createPolicy(ctx, topic, "bounded-readers", "safe mode")
	if err != nil {
		return err
	}
	if normal {
		consumer, err := group.Consumer(ctx, laser.ConsumerOptions{Count: 100, ManualCommit: true})
		if err != nil {
			return err
		}
		defer consumer.Close(ctx)
		record, err := consumer.Next(ctx)
		if err != nil {
			return err
		}
		stored, err := consumer.StoredOffset(ctx, 0)
		if err != nil {
			return err
		}
		if record.Offset() != 100 || stored == nil || stored.Offset != 99 {
			return fmt.Errorf("first match at %d, stored %v", record.Offset(), stored)
		}
		state.put("delivered", [][]byte{record.Payload()})
		return consumer.Commit(ctx, record.Checkpoint)
	}
	options := firstStart()
	options.Count = 1
	options.MaxExamined = 100
	reader, err := openReader(ctx, state, group, options)
	if err != nil {
		return err
	}
	bounded, stop := context.WithTimeout(ctx, readTimeout)
	defer stop()
	for {
		empty, more, err := reader.ReadRound(bounded)
		if err != nil {
			return err
		}
		if reader.ExaminedInRound() == 0 {
			select {
			case <-bounded.Done():
				return fmt.Errorf("the first assignment never became readable: %w", bounded.Err())
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		if empty != nil || !more || reader.ExaminedInRound() != 100 {
			return fmt.Errorf("expected a round of 100 examined non-matches with work left, got page %v, more %t, examined %d",
				empty, more, reader.ExaminedInRound())
		}
		break
	}
	page, err := nextPage(ctx, reader)
	if err != nil {
		return err
	}
	if page.Examined() != 1 || len(page.Records) != 1 || page.Records[0].Offset() != 100 {
		return fmt.Errorf("unexpected page after sparse scan: %+v", page.Page)
	}
	savePage(state, page)
	if err := reader.Ack(ctx, page); err != nil {
		return err
	}
	return reader.Close(ctx)
}

func stalePolicyPage(ctx context.Context, state *world) error {
	group, _, err := createPolicy(ctx, state.topic(feedTopic), "policy-ack", "safe mode")
	if err != nil {
		return err
	}
	reader, err := openReader(ctx, state, group, firstStart())
	if err != nil {
		return err
	}
	page, err := nextPage(ctx, reader)
	if err != nil {
		return err
	}
	if _, err := group.Filter().Release(ctx); err != nil {
		return err
	}
	if err := wantFilterReason(reader.Ack(ctx, page), wire.ReasonConflict); err != nil {
		return err
	}
	current, err := nextPage(ctx, reader)
	if err != nil {
		return err
	}
	if len(current.Records) == 0 || current.Records[0].Offset() != 0 {
		return fmt.Errorf("released policy moved the group's offset")
	}
	savePage(state, current)
	if err := reader.Ack(ctx, current); err != nil {
		return err
	}
	return reader.Close(ctx)
}

func skipStoppedRecord(ctx context.Context, state *world, name string) error {
	group, _, err := createPolicy(ctx, state.topic(feedTopic), "fault-skip", name)
	if err != nil {
		return err
	}
	reader, err := openReader(ctx, state, group, firstStart())
	if err != nil {
		return err
	}
	page, err := nextPage(ctx, reader)
	if err != nil {
		return err
	}
	if err := reader.Ack(ctx, page); err != nil {
		return err
	}
	_, err = nextPage(ctx, reader)
	var stopped *laser.FilterFaultError
	if !errors.As(err, &stopped) {
		return fmt.Errorf("expected the malformed record to stop the read as a filter fault, got %v", err)
	}
	feed, err := loadFeed()
	if err != nil {
		return err
	}
	if stopped.Offset != uint64(len(feed)) {
		return fmt.Errorf("expected the fault at offset %d, after the feed, got %d", len(feed), stopped.Offset)
	}
	if err := reader.Skip(ctx, stopped.Page, stopped.Offset); err != nil {
		return err
	}
	after, err := nextPage(ctx, reader)
	if err != nil {
		return err
	}
	savePage(state, after)
	if err := reader.Ack(ctx, after); err != nil {
		return err
	}
	return reader.Close(ctx)
}
