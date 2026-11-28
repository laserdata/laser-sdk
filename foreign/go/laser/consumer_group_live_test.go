//go:build integration

// Live coverage for consumer groups.
//
// A consumer group is the surface least worth believing without a server. Its
// whole behaviour is something the server decides: it hands out the numeric
// id, it stamps the identity from its own metadata, and above all it assigns
// partitions across the members. A stub can be told to return any of that, so
// a hermetic test proves only that the client asked.
//
// These tests therefore do the one thing a fixture cannot: they put two
// readers in one group against a real deployment and check that the server
// split the partitions between them, with no partition given to both.
package laser_test

import (
	"context"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// liveGroupTopic is a topic with partitions enough to split across members.
//
// Four, because the assignment is only observable when there is more than one
// partition to hand out: a single-partition topic would give one member
// everything and tell us nothing about the split.
func liveGroupTopic(t *testing.T, client *laser.Laser, ctx context.Context) *laser.Topic {
	t.Helper()
	scratch := liveScratch(t, client, ctx, "laser-live-group")
	topic := scratch.Laser().Topic("records")
	if err := topic.Ensure(ctx, 4, nil); err != nil {
		t.Fatalf("create a topic on %s: %v", scratch.Name(), err)
	}
	return topic
}

// TestGivenALiveServerWhenCreatingAConsumerGroupThenShouldStampItsIdentity
// walks the lifecycle against the server: create, read back, delete.
//
// The identity is the part worth asserting live. Every component of it comes
// from the server's own metadata, so a client that assembled it from the
// names it already held would pass a stubbed test and produce a binding no
// server would honour.
func TestGivenALiveServerWhenCreatingAConsumerGroupThenShouldStampItsIdentity(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	topic := liveGroupTopic(t, client, ctx)

	group := topic.Group("live-workers")
	info, err := group.Create().Build(ctx)
	if err != nil {
		t.Fatalf("create the consumer group: %v", err)
	}
	if info.Name != "live-workers" {
		t.Fatalf("the group was created as %q", info.Name)
	}
	if info.PartitionsCount != 4 {
		t.Errorf("the group reports %d partitions on a four-partition topic",
			info.PartitionsCount)
	}

	// No id is asserted non-zero. This server numbers consumer groups and the
	// topics within a stream from zero, the same way it numbers partitions, so
	// a first group is genuinely group 0 and a check for a non-zero id would
	// fail on a correct client.
	identity := info.Identity
	if identity.GroupID != uint64(info.ID) {
		t.Errorf("the identity names group %d but the server assigned %d",
			identity.GroupID, info.ID)
	}
	// The creation times are the half that must be present whatever the
	// numbering base. They are what tells a recreated stream or topic from the
	// one it replaced, so a zero here would mean the client never read them,
	// which is exactly the bug a stub cannot catch.
	if identity.StreamCreatedAtMicros == 0 || identity.TopicCreatedAtMicros == 0 {
		t.Errorf("the identity carries no incarnation times, so a recreated "+
			"stream or topic would look like the same one: %+v", identity)
	}

	// Creating again must reach the same group rather than failing or making
	// a second one, which is what lets a deployment create its groups on every
	// start and converge.
	again, err := group.Create().Build(ctx)
	if err != nil {
		t.Fatalf("recreating an existing group must succeed: %v", err)
	}
	if again.ID != info.ID {
		t.Errorf("recreating reached group %d rather than %d", again.ID, info.ID)
	}

	// The group shows up in the topic's own listing, which is the other way an
	// operator finds it.
	groups, err := topic.ConsumerGroups(ctx)
	if err != nil {
		t.Fatalf("list the topic's consumer groups: %v", err)
	}
	found := false
	for _, summary := range groups {
		if summary.Name == "live-workers" {
			found = true
		}
	}
	if !found {
		t.Errorf("the group is missing from the listing of %d groups", len(groups))
	}

	removed, err := group.Delete(ctx)
	if err != nil {
		t.Fatalf("delete the consumer group: %v", err)
	}
	if !removed {
		t.Error("deleting an existing group must report that one was removed")
	}
	removed, err = group.Delete(ctx)
	if err != nil {
		t.Fatalf("deleting an absent group must not fail: %v", err)
	}
	if removed {
		t.Error("deleting an absent group must report that nothing was removed")
	}
}

// TestGivenAGroupAddressedByIDWhenReadLiveThenShouldReachTheSameGroup proves
// the numeric path against a real id, which only the server can mint.
func TestGivenAGroupAddressedByIDWhenReadLiveThenShouldReachTheSameGroup(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	topic := liveGroupTopic(t, client, ctx)

	created, err := topic.Group("live-by-id").Create().Build(ctx)
	if err != nil {
		t.Fatalf("create the consumer group: %v", err)
	}

	byID := topic.GroupByID(uint64(created.ID))
	info, err := byID.Info(ctx)
	if err != nil {
		t.Fatalf("read the group by its numeric id: %v", err)
	}
	if info.Name != "live-by-id" {
		t.Errorf("addressing by id reached %q rather than the group created", info.Name)
	}
	if info.Identity != created.Identity {
		t.Errorf("the two handles disagree about the identity: %+v and %+v",
			info.Identity, created.Identity)
	}

	// The catalog keys a binding by name, so a numeric handle has to be able
	// to resolve one off the server.
	ref, err := byID.GroupRef(ctx)
	if err != nil {
		t.Fatalf("resolve the group reference from a numeric handle: %v", err)
	}
	if ref.Group != "live-by-id" || ref.Topic != topic.Name() {
		t.Errorf("the group reference resolved to %+v", ref)
	}
}

// TestGivenTwoReadersInOneGroupWhenConsumingThenShouldSplitThePartitions is
// the test this whole file exists for.
//
// Two members join one group on a four-partition topic. The server assigns
// partitions across them, and what has to hold is that every partition went
// to exactly one member: a partition given to both would have the group
// processing the same record twice, and a partition given to neither would
// have records nobody reads.
//
// Nothing here asserts WHICH member gets which partition. That is the
// server's decision and it rebalances as members come and go, so pinning it
// would be testing the server's current mood rather than the contract.
func TestGivenTwoReadersInOneGroupWhenConsumingThenShouldSplitThePartitions(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	topic := liveGroupTopic(t, client, ctx)

	group := topic.Group("live-split")
	info, err := group.Create().Build(ctx)
	if err != nil {
		t.Fatalf("create the consumer group: %v", err)
	}

	// Records across several keys, so the partitions actually have something
	// on them whichever way the keys hash.
	published := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	for _, key := range published {
		if _, err := topic.Publish().
			Payload([]byte(key)).Key(key).Send(ctx); err != nil {
			t.Fatalf("publish under key %s: %v", key, err)
		}
	}

	// The second member dials its OWN connection, and that is not a detail.
	// Apache Iggy identifies a group member by the client connection, so two
	// readers opened on one connection join as a single member: each is handed
	// every partition and each reads every record, which is the opposite of
	// what a group is for. A second process gets this for free. A second
	// reader inside one process does not.
	other := dialLive(t)
	otherTopic := other.Stream(topic.StreamName()).Topic(topic.Name())

	first := joinLiveGroup(t, topic, ctx, "live-split")
	second := joinLiveGroup(t, otherTopic, ctx, "live-split")

	// What is asserted is the behaviour, not the assignment table. A member's
	// partition list is empty until it has polled, so reading it at join time
	// would prove nothing, and which member holds which partition is the
	// server's to decide and to change as members come and go.
	//
	// The contract that must hold whatever it decides: between them the
	// members see every record exactly once.
	byFirst := drainLiveGroupMember(t, first)
	bySecond := drainLiveGroupMember(t, second)
	t.Logf("the first member read %v holding partitions %v",
		byFirst, first.Partitions())
	t.Logf("the second member read %v holding partitions %v",
		bySecond, second.Partitions())

	seen := map[string]int{}
	for _, body := range append(append([]string{}, byFirst...), bySecond...) {
		seen[body]++
	}
	for _, body := range published {
		switch seen[body] {
		case 1:
		case 0:
			t.Errorf("no member read %q, so the group leaves records unread", body)
		default:
			t.Errorf("%d members read %q, so the group processes a record twice",
				seen[body], body)
		}
	}
	if len(seen) != len(published) {
		t.Errorf("the group read %d distinct records, %d were published",
			len(seen), len(published))
	}

	// Every partition the two members hold between them must be held by one of
	// them alone, which is the assignment rule behind the behaviour above.
	held := map[uint32]int{}
	for _, partition := range append(append([]uint32{}, first.Partitions()...),
		second.Partitions()...) {
		held[partition]++
	}
	for partition, count := range held {
		if count > 1 {
			t.Errorf("partition %d is held by %d members at once", partition, count)
		}
	}
	if len(held) > int(info.PartitionsCount) {
		t.Errorf("the group holds %d partitions on a %d-partition topic",
			len(held), info.PartitionsCount)
	}

	// With two members the server reports the membership it is balancing.
	after, err := group.Info(ctx)
	if err != nil {
		t.Fatalf("read the group after its members joined: %v", err)
	}
	t.Logf("the group reports %d members holding %d partitions",
		after.MembersCount, after.PartitionsCount)
	if after.MembersCount == 0 {
		t.Error("the group reports no members while two readers are joined")
	}
}

// joinLiveGroup joins the group as one member and closes it with the test.
//
// A group member names no partitions of its own, because the server assigns
// them. That is also why a group reader cannot hit the silent timeout a
// standalone consumer hits when it names a partition that does not exist.
func joinLiveGroup(
	t *testing.T, topic *laser.Topic, ctx context.Context, group string,
) *laser.Consumer {
	t.Helper()
	start := iggcon.FirstPollingStrategy()
	consumer, err := topic.ConsumerGroup(ctx, group, laser.ConsumerOptions{
		Start:    &start,
		PollWait: livePollWait,
		Count:    10,
	})
	if err != nil {
		t.Fatalf("join the consumer group %s: %v", group, err)
	}
	t.Cleanup(func() {
		closing, cancel := context.WithTimeout(context.Background(), liveOperationTimeout)
		defer cancel()
		if err := consumer.Close(closing); err != nil {
			t.Errorf("close the group member: %v", err)
		}
	})
	return consumer
}

// drainLiveGroupMember reads what this member has been assigned, until a read
// comes back empty.
//
// A read that finds nothing is how a member says it is caught up, or that the
// server gave it no partition at all. Both are ordinary, so an empty read ends
// the drain rather than failing it: which member gets what is the server's
// decision, and a member holding nothing is a correct outcome for a group with
// more members than it needs.
func drainLiveGroupMember(t *testing.T, consumer *laser.Consumer) []string {
	t.Helper()
	var bodies []string
	for {
		// Each read gets its own short deadline, because the end of the drain
		// is a read that times out and the test must not wait the whole
		// operation budget to learn that.
		read, cancel := context.WithTimeout(context.Background(), livePollWait)
		record, err := consumer.Next(read)
		cancel()
		if err != nil {
			return bodies
		}
		if record == nil {
			return bodies
		}
		bodies = append(bodies, string(record.Payload()))
	}
}

// TestGivenARecreatedGroupWhenReadLiveThenShouldCarryAnotherIdentity proves
// the rule the identity exists to enforce.
//
// A group deleted and recreated under the same name is a different group, and
// its identity has to say so. If it did not, the new group would inherit the
// old one's filter policy, and a reader would silently run a filter nobody
// configured for it.
func TestGivenARecreatedGroupWhenReadLiveThenShouldCarryAnotherIdentity(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	topic := liveGroupTopic(t, client, ctx)

	group := topic.Group("live-recreated")
	before, err := group.Create().Build(ctx)
	if err != nil {
		t.Fatalf("create the consumer group: %v", err)
	}
	if _, err := group.Delete(ctx); err != nil {
		t.Fatalf("delete the consumer group: %v", err)
	}
	after, err := group.Create().Build(ctx)
	if err != nil {
		t.Fatalf("recreate the consumer group: %v", err)
	}

	if after.Identity == before.Identity {
		t.Errorf("a recreated group kept the identity %+v, so it would inherit "+
			"the policy of the group that was deleted", after.Identity)
	}
	// The stream and topic did not change, so those halves must still match.
	// Only the group number is free to differ, and showing that is what makes
	// the check above meaningful rather than incidental.
	if after.Identity.StreamID != before.Identity.StreamID ||
		after.Identity.TopicID != before.Identity.TopicID {
		t.Errorf("the stream and topic changed under a recreated group: %+v then %+v",
			before.Identity, after.Identity)
	}
}

func TestGivenAManagedGroupWhenHandlersAndMarksFinishThenShouldCommitOnlyProcessedRecords(t *testing.T) {
	client := dialLive(t)
	if !client.Capabilities().Filters.GroupPolicyReads {
		t.Skip("requires group-aware reads")
	}
	ctx := liveContext(t)
	scratch := liveScratch(t, client, ctx, "laser-policy-commit")
	topic := scratch.Laser().Topic("events")
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"first", "second"} {
		if _, err := topic.Publish().Payload([]byte(body)).Send(ctx); err != nil {
			t.Fatal(err)
		}
	}

	failed, err := topic.Group("failed").Consumer(ctx, laser.ConsumerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("handler failed")
	if err := failed.Each(ctx, func(context.Context, *laser.Record) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("handler error = %v", err)
	}
	if err := failed.Close(ctx); err != nil {
		t.Fatal(err)
	}
	stored, err := failed.StoredOffset(ctx, 0)
	if err != nil || stored != nil {
		t.Fatalf("failed handler stored offset %v, error %v", stored, err)
	}

	each := iggcon.AutoCommit{After: iggcon.CommitAfterMarked, Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessage}}
	marked, err := topic.Group("marked").Consumer(ctx, laser.ConsumerOptions{AutoCommit: &each})
	if err != nil {
		t.Fatal(err)
	}
	first, err := marked.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := marked.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := marked.Mark(second.Checkpoint); err != nil {
		t.Fatal(err)
	}
	stored, err = marked.StoredOffset(ctx, 0)
	if err != nil || stored != nil {
		t.Fatalf("out-of-order mark advanced to %v, error %v", stored, err)
	}
	if err := marked.Mark(first.Checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := marked.Close(ctx); err != nil {
		t.Fatal(err)
	}
	stored, err = marked.StoredOffset(ctx, 0)
	if err != nil || stored == nil || stored.Offset != second.Offset() {
		t.Fatalf("completed prefix offset %v, error %v", stored, err)
	}
}

func TestGivenAManagedConsumerWhenSkippingAStoppedRecordThenShouldResumeAfterIt(t *testing.T) {
	client := dialLive(t)
	if !client.Capabilities().Filters.Catalog {
		t.Skip("requires the filter catalog")
	}
	ctx := liveContext(t)
	scratch := liveScratch(t, client, ctx, "laser-consumer-skip")
	topic := scratch.Laser().Topic("events")
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := topic.PublishBatch().AddRawBytes([]byte("{"), wire.ContentJSON).AddJSON(map[string]bool{"alert": true}).Send(ctx); err != nil {
		t.Fatal(err)
	}
	group := topic.Group("alerts")
	filter := wire.NewConsumerFilter(wire.ExprPredicate("alert", wire.CmpEq, wire.BooleanTyped(true)), wire.CodecJSON)
	if _, err := group.Create().Filter("alert", filter).Build(ctx); err != nil {
		t.Fatal(err)
	}
	consumer, err := group.Consumer(ctx, laser.ConsumerOptions{ManualCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close(ctx)
	_, err = consumer.Next(ctx)
	var fault *laser.FilterFaultError
	if !errors.As(err, &fault) || fault.Offset != 0 || fault.PartitionID != 0 {
		t.Fatalf("expected a fault at offset zero, got %v", err)
	}
	if err := consumer.Skip(ctx, 0, 1); err == nil {
		t.Fatal("skipped another offset")
	}
	if err := consumer.Skip(ctx, fault.PartitionID, fault.Offset); err != nil {
		t.Fatal(err)
	}
	record, err := consumer.Next(ctx)
	if err != nil || record.Offset() != 1 {
		t.Fatalf("next = %v, %v", record, err)
	}
	if err := consumer.Commit(ctx, record.Checkpoint); err != nil {
		t.Fatal(err)
	}
}
