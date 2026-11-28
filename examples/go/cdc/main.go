// A satellite fleet streams the change feed of its mission-ops database: every
// battery reading, orbit maneuver, and ground-station status flip. The anomaly
// desk only wants satellites entering safe mode or leaving the fleet. The
// desk's consumer group owns that filter: the server evaluates it next to the
// data, so the desk receives a handful of records out of hundreds, and
// everything else never leaves the server.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example     = "cdc"
	topicName   = "fleet_changes"
	alertsTopic = "fleet_alerts"
	partitions  = 3
	groupName   = "anomaly-desk"
	satellites  = 8
	feedSize    = 240
	routine     = 1
	critical    = 2
	readTimeout = 15 * time.Second
)

// fleetChange is one record of the change feed: a captured row change tagged
// by its table, or a telemetry event a satellite reports.
type fleetChange struct {
	Table       string           `json:"table,omitempty"`
	Op          string           `json:"op,omitempty"`
	Changed     []string         `json:"changed,omitempty"`
	After       *entity          `json:"after,omitempty"`
	Before      *entity          `json:"before,omitempty"`
	Event       string           `json:"event,omitempty"`
	SatelliteID string           `json:"satellite_id,omitempty"`
	Fields      *telemetryFields `json:"fields,omitempty"`
}

// entity is a satellite or a ground station row.
type entity struct {
	ID         string `json:"id"`
	Name       string `json:"name,omitempty"`
	Mode       string `json:"mode,omitempty"`
	Orbit      string `json:"orbit,omitempty"`
	BatteryPct *int   `json:"battery_pct,omitempty"`
	Status     string `json:"status,omitempty"`
}

type telemetryFields struct {
	Mode       string `json:"mode,omitempty"`
	BatteryPct *int   `json:"battery_pct,omitempty"`
}

type feed struct {
	records []fleetChange
	// strictMatches counts two safe-mode transitions, one telemetry report of
	// safe mode and one decommission.
	strictMatches int
}

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	if !examples.ManagedGate(client.Capabilities(), examples.FeatureFilterCatalog, example, "consumer group filters") {
		return nil
	}
	topic := client.Topic(topicName)

	examples.Phase("publish a busy fleet change feed, keyed by satellite")
	changes := fleetFeed()
	publishedBytes, err := publishFeed(ctx, topic, changes)
	if err != nil {
		return err
	}

	examples.Phase("create the anomaly desk group with its filter")
	desk := topic.Group(groupName + "-" + examples.RunToken())
	created, err := desk.Create().Filter("safe-mode", safeModeFilter()).Build(ctx)
	if err != nil {
		return fmt.Errorf("create the desk group: %w", err)
	}
	binding := created.Filter
	if binding == nil {
		return errors.New("the group was created unbound")
	}
	fmt.Printf("  group %s (%d) runs revision %d of its own filter from now on\n",
		created.Name, created.ID, binding.Revision)

	examples.Phase("consume as the group: the application names the group, the server runs its filter")
	deliveredBytes, err := consumeAsGroup(ctx, desk, changes.strictMatches)
	if err != nil {
		return err
	}
	fmt.Printf("  delivered %d of %d records, %d of %d payload bytes: %.1f%% stayed on the server\n",
		changes.strictMatches, len(changes.records), deliveredBytes, publishedBytes,
		100*float64(publishedBytes-deliveredBytes)/float64(publishedBytes))

	examples.Phase("page the matches again with the group reader and its own scan budget")
	paged, err := pageMatches(ctx, desk, changes.strictMatches)
	if err != nil {
		return err
	}
	fmt.Printf("  the reader handed out %d matches in pages, each acknowledged after handling\n", paged)

	examples.Phase("test the group's filter against a battery update of a satellite already in safe mode")
	stillSafe, err := json.Marshal(satelliteUpdate(2, "safe", 58, "battery_pct"))
	if err != nil {
		return err
	}
	tested, err := desk.Filter().Test(ctx, stillSafe, nil)
	if err != nil {
		return fmt.Errorf("test the filter: %w", err)
	}
	fmt.Printf("  strict, transitions only: %s\n", tested.Explanation.Verdict)

	examples.Phase("preview every partition, nothing is stored")
	for partition := uint32(0); partition < partitions; partition++ {
		request, err := desk.Filter().Preview(ctx, partition)
		if err != nil {
			return fmt.Errorf("preview partition %d: %w", partition, err)
		}
		preview, err := request.MaxRecords(10).Send(ctx)
		if err != nil {
			return fmt.Errorf("preview partition %d: %w", partition, err)
		}
		fmt.Printf("  partition %d: examined %d, matched %d, stopped at %s\n",
			partition, preview.Examined, preview.Matched, preview.Stop)
	}

	examples.Phase("route binary alerts on a header, their payload is never decoded")
	if err := routeAlerts(ctx, client); err != nil {
		return err
	}
	if err := runCodecs(ctx, client); err != nil {
		return err
	}
	return manageRevisions(ctx, topic, desk, binding, changes.strictMatches)
}

// publishFeed sends one batch per partition key, so every change of one
// satellite or station stays in order on one partition.
func publishFeed(ctx context.Context, topic *laser.Topic, changes feed) (int, error) {
	if err := topic.Ensure(ctx, partitions, nil); err != nil {
		return 0, fmt.Errorf("ensure %s: %w", topicName, err)
	}
	publishedBytes := 0
	byKey := map[string][]fleetChange{}
	for _, change := range changes.records {
		payload, err := json.Marshal(change)
		if err != nil {
			return 0, err
		}
		publishedBytes += len(payload)
		byKey[keyOf(change)] = append(byKey[keyOf(change)], change)
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		started := time.Now()
		batch := topic.PublishBatch().Key(key)
		for _, change := range byKey[key] {
			batch.AddJSON(change)
		}
		if _, err := batch.Send(ctx); err != nil {
			return 0, fmt.Errorf("publish the changes of %s: %w", key, err)
		}
		fmt.Printf("  %s: %d records in %d ms\n", key, len(byKey[key]), time.Since(started).Milliseconds())
	}
	fmt.Printf("  %d records, %d bytes: battery readings, maneuvers, station flips, and %d safe-mode or decommission events\n",
		len(changes.records), publishedBytes, changes.strictMatches)
	return publishedBytes, nil
}

func consumeAsGroup(ctx context.Context, desk *laser.ConsumerGroup, expected int) (delivered int, err error) {
	first := iggcon.FirstPollingStrategy()
	consumer, err := desk.Consumer(ctx, laser.ConsumerOptions{Start: &first, ManualCommit: true})
	if err != nil {
		return 0, fmt.Errorf("join the desk group: %w", err)
	}
	defer func() { err = errors.Join(err, consumer.Close(ctx)) }()
	for range expected {
		read, cancel := context.WithTimeout(ctx, readTimeout)
		record, err := consumer.Next(read)
		cancel()
		if err != nil {
			return delivered, fmt.Errorf("read as the desk group: %w", err)
		}
		var change fleetChange
		if err := json.Unmarshal(record.Payload(), &change); err != nil {
			return delivered, fmt.Errorf("decode offset %d: %w", record.Offset(), err)
		}
		fmt.Printf("  partition %d offset %d: %s\n", record.PartitionID, record.Offset(), describe(change))
		delivered += len(record.Payload())
		if err := consumer.Commit(ctx, record.Checkpoint); err != nil {
			return delivered, fmt.Errorf("commit offset %d: %w", record.Offset(), err)
		}
	}
	return delivered, nil
}

func pageMatches(ctx context.Context, desk *laser.ConsumerGroup, expected int) (paged int, err error) {
	pager, err := openPager(ctx, desk)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, pager.Close(ctx)) }()
	return readMatches(ctx, pager, expected)
}

// openPager opens the reader once it holds every partition. Its start applies
// to the partitions held at the open, and the group hands them over one by one.
func openPager(ctx context.Context, desk *laser.ConsumerGroup) (*laser.GroupReader, error) {
	first := wire.StartAtFirst()
	deadline := time.Now().Add(readTimeout)
	for {
		pager, err := desk.OpenReaderWith(ctx, laser.GroupReaderOptions{
			Start: &first, Count: 10, MaxExamined: 100, LocalGuard: true,
		})
		if err != nil {
			return nil, fmt.Errorf("open the group reader: %w", err)
		}
		held, err := pager.Partitions(ctx)
		if err == nil && len(held) == partitions {
			return pager, nil
		}
		if err := errors.Join(err, pager.Close(ctx)); err != nil {
			return nil, fmt.Errorf("open the group reader: %w", err)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("the group reader held %d of %d partitions", len(held), partitions)
		}
	}
}

// routeAlerts publishes binary alert frames that carry their priority as a
// header. A pager group with a headers-only filter selects the critical ones
// without decoding a payload, so the alert topic can hold any format.
func routeAlerts(ctx context.Context, client *laser.Laser) (err error) {
	alerts := client.Topic(alertsTopic)
	if err := alerts.Ensure(ctx, 1, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", alertsTopic, err)
	}
	producer, err := alerts.Producer().Build(ctx)
	if err != nil {
		return fmt.Errorf("build the alert producer: %w", err)
	}
	defer func() { err = errors.Join(err, producer.Close(ctx)) }()
	for _, alert := range []struct {
		priority    uint8
		satelliteID string
	}{{routine, "sat-001"}, {critical, "sat-003"}, {routine, "sat-004"}, {critical, "sat-007"}} {
		frame := append([]byte{0x0a, 0x07}, alert.satelliteID...)
		priority, err := laser.ProducerHeader("priority", alert.priority)
		if err != nil {
			return fmt.Errorf("build the priority header: %w", err)
		}
		message, err := laser.NewProducerMessage(frame, priority)
		if err != nil {
			return err
		}
		if _, err := producer.SendKeyed(ctx, message, []byte(alert.satelliteID)); err != nil {
			return fmt.Errorf("send the alert of %s: %w", alert.satelliteID, err)
		}
	}
	pagerGroup := alerts.Group(groupName + "-pager-" + examples.RunToken())
	filter := wire.HeadersOnlyFilter(wire.ExprHeaderPredicate("priority", wire.CmpEq, wire.IntTyped(critical)))
	if _, err := pagerGroup.Create().Filter("critical-alerts", filter).Build(ctx); err != nil {
		return fmt.Errorf("create the pager group: %w", err)
	}
	defer func() {
		_, deleteErr := pagerGroup.Filter().Delete(ctx)
		err = errors.Join(err, deleteErr)
	}()
	first := wire.StartAtFirst()
	pager, err := pagerGroup.OpenReaderWith(ctx, laser.GroupReaderOptions{Start: &first})
	if err != nil {
		return fmt.Errorf("open the pager reader: %w", err)
	}
	defer func() { err = errors.Join(err, pager.Close(ctx)) }()
	for range 2 {
		record, err := nextRecord(ctx, pager)
		if err != nil {
			return err
		}
		fmt.Printf("  critical alert at offset %d: %d opaque bytes\n", record.Offset(), len(record.Payload()))
		if err := pager.AckRecord(ctx, record.Page(), record.Offset()); err != nil {
			return fmt.Errorf("acknowledge offset %d: %w", record.Offset(), err)
		}
	}
	return nil
}

// manageRevisions lists the desk's own revision, runs a stricter variant in its
// own group, pauses and resumes it, then releases and deletes both policies.
func manageRevisions(
	ctx context.Context, topic *laser.Topic, desk *laser.ConsumerGroup, binding *wire.FilterBinding, expected int,
) error {
	examples.Phase("a group runs one policy: its own filter lists the revision it runs")
	revisions, err := desk.Filter().Revisions(ctx, 1, 10)
	if err != nil {
		return fmt.Errorf("list the revisions: %w", err)
	}
	fmt.Printf("  the group lists %d revision(s) and runs revision %d\n", revisions.TotalResults, binding.Revision)

	examples.Phase("A/B: the transitions-only variant runs in its own group")
	variantName := groupName + "-transitions-" + examples.RunToken()
	variant := topic.Group(variantName)
	transitions := wire.NewConsumerFilter(safeModeTransition(), wire.CodecJSON)
	created, err := variant.Create().Filter("safe-mode-transitions", transitions).Build(ctx)
	if err != nil {
		return fmt.Errorf("create the variant group: %w", err)
	}
	if created.Filter == nil {
		return errors.New("the variant was created unbound")
	}
	if err := pauseAndResume(ctx, variant, variantName, created.Filter.Revision, expected); err != nil {
		return err
	}

	examples.Phase("a group that runs a policy cannot be switched to another one")
	_, err = desk.Filter().Configure(ctx, "safe-mode-transitions", transitions)
	if err == nil {
		return errors.New("a running policy was replaced")
	}
	if !refusedWith(err, wire.ReasonConflict) {
		return fmt.Errorf("replace a running policy: %w", err)
	}
	fmt.Println("  refused with conflict: create a new group for another policy")

	examples.Phase("release both policies")
	released, err := desk.Filter().Release(ctx)
	if err != nil {
		return fmt.Errorf("release the desk policy: %w", err)
	}
	if _, err := variant.Filter().Release(ctx); err != nil {
		return fmt.Errorf("release the variant policy: %w", err)
	}
	fmt.Printf("  %s is unbound again and receives every record, its filter stays saved as revision %d\n",
		released.Group.Group, released.Revision)

	examples.Phase("delete both filters: nothing of them stays in the catalog")
	if _, err := desk.Filter().Delete(ctx); err != nil {
		return fmt.Errorf("delete the desk filter: %w", err)
	}
	if _, err := variant.Filter().Delete(ctx); err != nil {
		return fmt.Errorf("delete the variant filter: %w", err)
	}
	fmt.Println("  deleted with every revision, the groups keep reading everything")
	return nil
}

func pauseAndResume(
	ctx context.Context, variant *laser.ConsumerGroup, variantName string, revision uint32, expected int,
) (err error) {
	first := wire.StartAtFirst()
	reader, err := variant.OpenReaderWith(ctx, laser.GroupReaderOptions{Start: &first, Count: 1, LocalGuard: true})
	if err != nil {
		return fmt.Errorf("open the variant reader: %w", err)
	}
	defer func() { err = errors.Join(err, reader.Close(ctx)) }()
	record, err := nextRecord(ctx, reader)
	if err != nil {
		return err
	}
	var change fleetChange
	if err := record.JSON(&change); err != nil {
		return fmt.Errorf("decode offset %d: %w", record.Offset(), err)
	}
	fmt.Printf("  %s: %s\n", variantName, describe(change))

	examples.Phase("pause the variant: new reads stop, in-flight work still acknowledges")
	if err := variant.Filter().SetRevisionEnabled(ctx, revision, false); err != nil {
		return fmt.Errorf("pause the variant: %w", err)
	}
	if err := reader.AckRecord(ctx, record.Page(), record.Offset()); err != nil {
		return fmt.Errorf("acknowledge while paused: %w", err)
	}
	_, err = reader.TryNextPage(ctx)
	if err == nil {
		return errors.New("a disabled revision kept reading")
	}
	if !refusedWith(err, wire.ReasonRevisionDisabled) {
		return fmt.Errorf("read through a paused revision: %w", err)
	}
	fmt.Println("  paused: the server refuses new reads with revision_disabled")
	if err := variant.Filter().SetRevisionEnabled(ctx, revision, true); err != nil {
		return fmt.Errorf("resume the variant: %w", err)
	}
	if _, err := readMatches(ctx, reader, 1); err != nil {
		return err
	}
	fmt.Printf("  resumed: the desk handled %d broad events, the variant 2 transitions\n", expected)
	return nil
}

// readMatches reads and acknowledges until expected safe-mode or decommission
// events arrived, decoding each one into the typed change it is.
func readMatches(ctx context.Context, reader *laser.GroupReader, expected int) (int, error) {
	delivered := 0
	for delivered < expected {
		record, err := nextRecord(ctx, reader)
		if err != nil {
			return delivered, err
		}
		var change fleetChange
		if err := record.JSON(&change); err != nil {
			return delivered, fmt.Errorf("decode offset %d: %w", record.Offset(), err)
		}
		fmt.Printf("  partition %d offset %d: %s\n", record.PartitionID, record.Offset(), describe(change))
		if err := reader.AckRecord(ctx, record.Page(), record.Offset()); err != nil {
			return delivered, fmt.Errorf("acknowledge offset %d: %w", record.Offset(), err)
		}
		delivered++
	}
	return delivered, nil
}

func nextRecord(ctx context.Context, reader *laser.GroupReader) (*laser.MatchedRecord, error) {
	read, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	record, err := reader.NextRecord(read)
	if err != nil {
		return nil, fmt.Errorf("read the next match: %w", err)
	}
	return record, nil
}

func refusedWith(err error, reason wire.FilterErrorReason) bool {
	var refusal *laser.FilterError
	return errors.As(err, &refusal) && refusal.Reason() == reason
}

// describe tells what the desk sees from the typed record.
func describe(change fleetChange) string {
	switch {
	case change.Event != "":
		mode := "no"
		if change.Fields != nil && change.Fields.Mode != "" {
			mode = change.Fields.Mode
		}
		return fmt.Sprintf("%s reported %s mode", change.SatelliteID, mode)
	case change.Op == "d":
		return change.Before.ID + " left the fleet"
	case change.Table == "satellites":
		return fmt.Sprintf("%s entered %s mode at %d%% battery", change.After.Name, change.After.Mode, *change.After.BatteryPct)
	default:
		return fmt.Sprintf("%s is %s", change.After.ID, change.After.Status)
	}
}

func keyOf(change fleetChange) string {
	switch {
	case change.Event != "":
		return change.SatelliteID
	case change.Op == "d":
		return change.Before.ID
	default:
		return change.After.ID
	}
}

// safeModeFilter is strict: a satellite update that marks its mode as changed
// to safe, a telemetry report of safe mode, or a satellite leaving the fleet.
func safeModeFilter() wire.ConsumerFilter {
	return wire.NewConsumerFilter(wire.ExprAnyOf(
		safeModeTransition(),
		wire.ExprAllOf(
			wire.ExprPredicate("table", wire.CmpEq, wire.StringTyped("satellites")),
			wire.ExprPredicate("op", wire.CmpEq, wire.StringTyped("d")),
		),
		wire.ExprAllOf(
			wire.ExprPredicate("event", wire.CmpEq, wire.StringTyped("satellite.telemetry_changed")),
			wire.ExprPredicate("fields.mode", wire.CmpEq, wire.StringTyped("safe")),
		),
	), wire.CodecJSON)
}

// safeModeTransition must change the mode and set its new value to safe.
func safeModeTransition() wire.FilterExpr {
	return wire.ExprAllOf(
		wire.ExprPredicate("table", wire.CmpEq, wire.StringTyped("satellites")),
		wire.ExprPredicate("op", wire.CmpEq, wire.StringTyped("u")),
		wire.ExprPredicate("changed", wire.CmpContains, wire.StringTyped("mode")),
		wire.ExprPredicate("after.mode", wire.CmpEq, wire.StringTyped("safe")),
	)
}

// fleetFeed is deterministic: mostly battery telemetry, battery updates, orbit
// maneuvers and ground-station status flips, with the four events the anomaly
// desk cares about spread through it.
func fleetFeed() feed {
	modes := slices.Repeat([]string{"nominal"}, satellites)
	stations := []string{"svalbard", "kiruna", "punta-arenas"}
	var changes feed
	for tick := range feedSize {
		index := tick % satellites
		battery := 90 - (tick*7)%40
		mode := modes[index]
		switch {
		case tick == feedSize/4 || tick == 3*feedSize/4:
			entering := 6
			if tick == feedSize/4 {
				entering = 2
			}
			modes[entering] = "safe"
			changes.strictMatches++
			changes.records = append(changes.records, satelliteUpdate(entering, "safe", battery, "mode"))
		case tick == feedSize/2:
			changes.strictMatches++
			changes.records = append(changes.records, telemetry(4, telemetryFields{Mode: "safe"}))
		case tick == feedSize-1:
			changes.strictMatches++
			changes.records = append(changes.records,
				fleetChange{Table: "satellites", Op: "d", Before: &entity{ID: satelliteID(7)}})
		case tick%10 <= 4:
			changes.records = append(changes.records, telemetry(index, telemetryFields{BatteryPct: &battery}))
		case tick%10 <= 7 || mode == "safe":
			changes.records = append(changes.records, satelliteUpdate(index, mode, battery, "battery_pct"))
		case tick%10 == 8:
			status := "online"
			if tick%20 == 8 {
				status = "maintenance"
			}
			changes.records = append(changes.records, fleetChange{
				Table: "ground_stations", Op: "u", Changed: []string{"status"},
				After: &entity{ID: stations[tick%3], Status: status},
			})
		default:
			changes.records = append(changes.records, satelliteUpdate(index, "maneuver", battery, "mode"))
		}
	}
	return changes
}

func satelliteUpdate(index int, mode string, battery int, changed string) fleetChange {
	orbit := "meo"
	if index%2 == 0 {
		orbit = "leo"
	}
	return fleetChange{
		Table: "satellites", Op: "u", Changed: []string{changed},
		After: &entity{
			ID: satelliteID(index), Name: fmt.Sprintf("Kestrel-%d", index+1),
			Mode: mode, Orbit: orbit, BatteryPct: &battery,
		},
	}
}

func telemetry(index int, fields telemetryFields) fleetChange {
	return fleetChange{Event: "satellite.telemetry_changed", SatelliteID: satelliteID(index), Fields: &fields}
}

func satelliteID(index int) string { return fmt.Sprintf("sat-%03d", index+1) }
