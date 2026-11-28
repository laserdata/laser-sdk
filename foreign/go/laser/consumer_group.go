package laser

import (
	"context"
	"errors"
	"fmt"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// groupTarget addresses a consumer group either by name or by the native
// numeric id the server assigned it.
//
// A group is created by name, but the server answers with a number, and a
// binding is scoped to that number. Keeping both forms in one target is what
// lets a handle built either way reach the same group.
type groupTarget struct {
	name   string
	id     uint64
	byName bool
}

// ConsumerGroup is one consumer group of a topic.
//
// The group owns its filter policy: readers built from this handle run
// whatever the group is configured with, bound or unbound, and never name a
// filter themselves. That is the whole point of a group policy, because two
// members that each chose their own filter would split one group's work by
// something other than the partition assignment.
//
// Handles are free to construct and safe to copy.
type ConsumerGroup struct {
	topic  *Topic
	target groupTarget
	// configuredAt is the control-log position of the last configuration this
	// handle performed. Every read built from the handle carries it, so an
	// application never observes its own configuration as absent through
	// another node that has not caught up yet.
	configuredAt *wire.CatalogPosition
}

// Group addresses a consumer group of this topic by name.
//
// It is named Group rather than ConsumerGroup because Topic.ConsumerGroup
// already joins a group and hands back a reader. This is the handle that owns
// the group itself: its lifecycle and its policy.
func (topic *Topic) Group(name string) *ConsumerGroup {
	return &ConsumerGroup{
		topic:  topic,
		target: groupTarget{name: name, byName: true},
	}
}

// GroupByID addresses a consumer group of this topic by its native numeric id.
//
// A numeric handle reads the group's name from the server when it needs one,
// because the catalog addresses a group by name and only the server knows
// which name a number belongs to.
func (topic *Topic) GroupByID(id uint64) *ConsumerGroup {
	return &ConsumerGroup{topic: topic, target: groupTarget{id: id}}
}

// Name is the group name, and false for a handle addressed by numeric id.
func (group *ConsumerGroup) Name() (string, bool) {
	return group.target.name, group.target.byName
}

// ID is the native numeric id, and false for a handle addressed by name.
func (group *ConsumerGroup) ID() (uint64, bool) {
	return group.target.id, !group.target.byName
}

// Topic is the topic this group reads.
func (group *ConsumerGroup) Topic() *Topic { return group.topic }

// ConsumerGroupInfo is a consumer group as the server knows it.
type ConsumerGroupInfo struct {
	// ID is the native numeric group id.
	ID uint32
	// Name is the group name.
	Name string
	// PartitionsCount and MembersCount are what the server reports for the
	// group right now. Members come and go, so both are a reading rather than
	// a property of the group.
	PartitionsCount uint32
	MembersCount    uint32
	// Identity is the exact group incarnation inside its stream and topic
	// incarnations. A recreated group is another identity and inherits no
	// policy, which is what stops a new group silently running an old one's
	// filter.
	Identity wire.FilterGroupIdentity
	// Filter is the active policy, nil for an unbound group whose readers
	// receive every record. It stays nil on a deployment that serves no
	// filter catalog, because nothing there could have configured one.
	Filter *wire.FilterBinding
}

// Create prepares the group, with an optional filter policy configured in the
// same call.
func (group *ConsumerGroup) Create() *CreateConsumerGroup {
	return &CreateConsumerGroup{group: group}
}

// Filter is the group's filter policy: configure it, read it, and release it.
func (group *ConsumerGroup) Filter() *GroupFilter {
	return &GroupFilter{group: group}
}

// Selector names this group the way a filtered read addresses its consumer.
//
// A named handle selects by name and a numeric one by id, because the server
// resolves either, and resolving a number to a name here would cost a round
// trip on every read.
func (group *ConsumerGroup) Selector() wire.FilterConsumer {
	if group.target.byName {
		return wire.NamedGroup(group.target.name)
	}
	return wire.GroupByID(group.target.id)
}

// Filters is the match-oriented reader of this group's policy: pages of
// records with their original offsets, an independent scan budget, and
// explicit acknowledgments.
//
// An unbound group receives every record, which is a supported way to run one
// rather than a misconfiguration.
func (group *ConsumerGroup) Filters() *Filters {
	filters := group.topic.Filters(group.Selector())
	filters.minCatalogPosition = group.configuredAt
	return filters
}

// Consumer joins this group as one member, load balanced with the others.
//
// A member names no partitions of its own: the server assigns them and
// rebalances as members come and go. Apache Iggy identifies a member by the
// client connection. On a server that resolves group policies each consumer
// opened from a connection string joins on its own connection. On Apache Iggy,
// or with FromClient, consumers share the client's connection and join as one
// member, so a second member needs its own client.
func (group *ConsumerGroup) Consumer(
	ctx context.Context, options ConsumerOptions,
) (*Consumer, error) {
	if len(options.Partitions) > 0 {
		return nil, &InvalidError{Reason: "a consumer group member is assigned its partitions by the server, so it must not name its own"}
	}
	if group.topic.stream == "" {
		return nil, errNoStream()
	}
	id, err := group.identifier()
	if err != nil {
		return nil, err
	}
	identity := iggcon.NewGroupConsumer(id)
	caps := group.laser().Capabilities()
	// A caller-supplied client was never probed, so the probe runs once here.
	if !caps.Filters.GroupPolicyReads && caps.Hello == HelloUnknown {
		caps = group.laser().RefreshCapabilities(ctx)
	}
	if !caps.Filters.GroupPolicyReads {
		if caps.Filters.Native {
			return nil, &UnsupportedError{Surface: "consumer group policy reads"}
		}
		if caps.Hello == HelloUnknown || caps.Hello == HelloFailed {
			return nil, &TransportError{Reason: "could not establish consumer group policy capabilities", Cause: context.DeadlineExceeded}
		}
	}
	if group.target.byName && !options.SkipGroupCreate {
		if err := group.EnsureNative(ctx); err != nil {
			return nil, err
		}
	}
	if !caps.Filters.GroupPolicyReads {
		return group.topic.openConsumer(ctx, identity, options)
	}
	return openPolicyConsumer(ctx, group, identity, options)
}

// EnsureNative creates the native group when it is absent and leaves an
// existing one alone, without touching its policy.
//
// A group that already exists is the outcome the caller asked for, so a
// concurrent create is not a failure. A numeric handle is refused: the server
// assigns the number, so there is nothing to create under one.
func (group *ConsumerGroup) EnsureNative(ctx context.Context) error {
	if !group.target.byName {
		return &InvalidError{Reason: "a consumer group is created by name, " +
			"address it with Topic.Group(name)"}
	}
	client, err := group.laser().connected()
	if err != nil {
		return err
	}
	streamID, topicID, err := group.topic.identifiers()
	if err != nil {
		return err
	}
	if _, err := client.CreateConsumerGroup(
		ctx, streamID, topicID, group.target.name,
	); err != nil {
		if isExistingGroup(err) {
			return nil
		}
		return &TransportError{Reason: "create the consumer group", Cause: err}
	}
	return nil
}

// Info is the group as the server knows it: its id, its membership, its exact
// identity and its active policy.
//
// The policy is read only where a filter catalog serves one. Reading the
// identity after the policy would leave a window where a recreated group's
// binding looked current, so the identity the policy carries is checked
// against the one read here and a mismatch is reported rather than returned.
func (group *ConsumerGroup) Info(ctx context.Context) (*ConsumerGroupInfo, error) {
	details, err := group.native(ctx)
	if err != nil {
		return nil, err
	}
	identity, err := group.identityOf(ctx, uint64(details.Id))
	if err != nil {
		return nil, err
	}
	info := &ConsumerGroupInfo{
		ID:              details.Id,
		Name:            details.Name,
		PartitionsCount: details.PartitionsCount,
		MembersCount:    details.MembersCount,
		Identity:        identity,
	}
	if !group.laser().Capabilities().Filters.Catalog {
		return info, nil
	}
	binding, err := group.Filter().Get(ctx)
	if err != nil {
		return nil, err
	}
	if binding != nil && binding.Identity != identity {
		return nil, &FilterError{Failure: wire.NewFilterError(wire.ReasonSourceChanged,
			"the group incarnation changed while its policy was inspected")}
	}
	info.Filter = binding
	return info, nil
}

// Delete removes the group with its stored offsets, reporting whether one was
// there to remove.
//
// A group that was already gone reads as absence rather than failure, which is
// what lets a deferred cleanup run after an explicit delete. A recreated group
// is another identity and inherits no policy.
func (group *ConsumerGroup) Delete(ctx context.Context) (bool, error) {
	client, err := group.laser().connected()
	if err != nil {
		return false, err
	}
	streamID, topicID, err := group.topic.identifiers()
	if err != nil {
		return false, err
	}
	groupID, err := group.identifier()
	if err != nil {
		return false, err
	}
	if err := client.DeleteConsumerGroup(ctx, streamID, topicID, groupID); err != nil {
		if isMissingResource(err) || isMissingGroup(err) {
			return false, nil
		}
		return false, &TransportError{Reason: "delete the consumer group", Cause: err}
	}
	return true, nil
}

// GroupRef names the group the way the filter catalog addresses it.
//
// A numeric handle has to ask the server for the name, because the catalog
// keys a binding by name and a number means nothing to it.
func (group *ConsumerGroup) GroupRef(ctx context.Context) (wire.FilterGroupRef, error) {
	stream := group.topic.stream
	if stream == "" {
		return wire.FilterGroupRef{}, errNoStream()
	}
	name, err := group.NativeName(ctx)
	if err != nil {
		return wire.FilterGroupRef{}, err
	}
	return wire.FilterGroupRef{
		Stream: stream, Topic: group.topic.name, Group: name,
	}, nil
}

// NativeName is the group's name, read from the server for a numeric handle.
func (group *ConsumerGroup) NativeName(ctx context.Context) (string, error) {
	if group.target.byName {
		return group.target.name, nil
	}
	details, err := group.native(ctx)
	if err != nil {
		return "", err
	}
	return details.Name, nil
}

// CatalogPosition is the control-log position of the last configuration this
// handle performed, nil when it has configured nothing.
func (group *ConsumerGroup) CatalogPosition() *wire.CatalogPosition {
	return group.configuredAt
}

// rememberPosition records a configuration's position, keeping the later of
// the two so an out-of-order reply cannot move the handle backwards.
func (group *ConsumerGroup) rememberPosition(position *wire.CatalogPosition) {
	if position == nil {
		return
	}
	if group.configuredAt == nil || supersedes(*group.configuredAt, *position) {
		group.configuredAt = position
	}
}

// laser is the client this group reads through.
func (group *ConsumerGroup) laser() *Laser { return group.topic.laser }

// identifier is the group as Apache Iggy addresses it.
func (group *ConsumerGroup) identifier() (iggcon.Identifier, error) {
	if group.target.byName {
		identifier, err := iggcon.NewIdentifier(group.target.name)
		if err != nil {
			return iggcon.Identifier{},
				&InvalidError{Reason: "consumer group name: " + err.Error()}
		}
		return identifier, nil
	}
	if group.target.id > uint64(^uint32(0)) {
		return iggcon.Identifier{}, &InvalidError{
			Reason: "consumer group id exceeds 32 bits"}
	}
	identifier, err := iggcon.NewIdentifier(uint32(group.target.id))
	if err != nil {
		return iggcon.Identifier{},
			&InvalidError{Reason: "consumer group id: " + err.Error()}
	}
	return identifier, nil
}

// native is the group as Apache Iggy reports it.
func (group *ConsumerGroup) native(
	ctx context.Context,
) (*iggcon.ConsumerGroupDetails, error) {
	client, err := group.laser().connected()
	if err != nil {
		return nil, err
	}
	streamID, topicID, err := group.topic.identifiers()
	if err != nil {
		return nil, err
	}
	groupID, err := group.identifier()
	if err != nil {
		return nil, err
	}
	details, err := client.GetConsumerGroup(ctx, streamID, topicID, groupID)
	if err != nil {
		if isMissingResource(err) || isMissingGroup(err) {
			return nil, group.notFound()
		}
		return nil, &TransportError{Reason: "read the consumer group", Cause: err}
	}
	if details == nil {
		return nil, group.notFound()
	}
	return details, nil
}

// notFound is the typed absence every group verb reports, so a caller matches
// one error whether it addressed the group by name or by number.
func (group *ConsumerGroup) notFound() error {
	return &FilterError{Failure: wire.NewFilterError(wire.ReasonNotFound,
		fmt.Sprintf("consumer group %s does not exist", group.describe()))}
}

// describe names the group for an error message.
func (group *ConsumerGroup) describe() string {
	if group.target.byName {
		return group.target.name
	}
	return fmt.Sprintf("#%d", group.target.id)
}

// identityOf is the exact identity of this group inside the current stream and
// topic incarnations.
func (group *ConsumerGroup) identityOf(
	ctx context.Context, groupID uint64,
) (wire.FilterGroupIdentity, error) {
	incarnation, err := group.topic.sourceIncarnation(ctx)
	if err != nil {
		return wire.FilterGroupIdentity{}, err
	}
	return incarnation.identity(groupID), nil
}

// sourceIncarnation is the stream and topic incarnation a numeric group id is
// scoped to.
//
// It is read rather than remembered because a stream or topic that was deleted
// and recreated under the same name is a different incarnation, and a binding
// scoped to the old one must not look current against the new one.
type sourceIncarnation struct {
	streamID              uint32
	streamCreatedAtMicros uint64
	topicID               uint32
	topicCreatedAtMicros  uint64
}

// identity is the exact identity of group groupID inside this incarnation.
func (incarnation sourceIncarnation) identity(groupID uint64) wire.FilterGroupIdentity {
	return wire.FilterGroupIdentity{
		StreamID:              incarnation.streamID,
		StreamCreatedAtMicros: incarnation.streamCreatedAtMicros,
		TopicID:               incarnation.topicID,
		TopicCreatedAtMicros:  incarnation.topicCreatedAtMicros,
		GroupID:               groupID,
	}
}

// sourceIncarnation reads the stream and topic incarnation this topic sits in.
func (topic *Topic) sourceIncarnation(ctx context.Context) (sourceIncarnation, error) {
	if topic.stream == "" {
		return sourceIncarnation{}, errNoStream()
	}
	stream, err := topic.laser.Stream(topic.stream).Get(ctx)
	if err != nil {
		return sourceIncarnation{}, err
	}
	if stream == nil {
		return sourceIncarnation{}, &FilterError{Failure: wire.NewFilterError(
			wire.ReasonNotFound, "source stream does not exist")}
	}
	details, err := topic.Get(ctx)
	if err != nil {
		return sourceIncarnation{}, err
	}
	if details == nil {
		return sourceIncarnation{}, &FilterError{Failure: wire.NewFilterError(
			wire.ReasonNotFound, "source topic does not exist")}
	}
	return sourceIncarnation{
		streamID:              stream.ID,
		streamCreatedAtMicros: stream.CreatedAtMicros,
		topicID:               details.ID,
		topicCreatedAtMicros:  details.CreatedAtMicros,
	}, nil
}

// CreateConsumerGroup creates a consumer group. Build it with
// ConsumerGroup.Create.
type CreateConsumerGroup struct {
	group       *ConsumerGroup
	policy      *wire.GroupFilterSpec
	operationID *wire.U128
}

// Filter configures the group with filter once it exists. The definition is
// saved as the group's own filter under name, which follows the registered
// filter name rule and need not be unique.
func (request *CreateConsumerGroup) Filter(
	name string, filter wire.ConsumerFilter,
) *CreateConsumerGroup {
	policy := wire.DefinitionSpec(name, filter)
	request.policy = &policy
	return request
}

// Policy configures the group with this policy once it exists.
func (request *CreateConsumerGroup) Policy(
	policy wire.GroupFilterSpec,
) *CreateConsumerGroup {
	request.policy = &policy
	return request
}

// OperationID pins the idempotency key of the policy configuration.
//
// A caller that records the id before it asks resumes the same configuration
// after a crash and reads its first outcome, rather than configuring twice and
// having to work out which answer was real.
func (request *CreateConsumerGroup) OperationID(id wire.U128) *CreateConsumerGroup {
	request.operationID = &id
	return request
}

// Build creates the group, then configures its policy when one was given.
//
// A policy is checked against the deployment's capabilities before the native
// group is created, so a server that serves no catalog leaves nothing behind:
// creating the group first and failing afterwards would strand a group with no
// policy under a name the caller then has to clean up.
func (request *CreateConsumerGroup) Build(
	ctx context.Context,
) (*ConsumerGroupInfo, error) {
	group := request.group
	if request.policy != nil && !group.laser().Capabilities().Filters.Catalog {
		return nil, &InvalidError{Reason: "this deployment serves no filter " +
			"catalog, so a consumer group cannot be given a policy here"}
	}
	if err := group.EnsureNative(ctx); err != nil {
		return nil, err
	}
	details, err := group.native(ctx)
	if err != nil {
		return nil, err
	}
	identity, err := group.identityOf(ctx, uint64(details.Id))
	if err != nil {
		return nil, err
	}
	info := &ConsumerGroupInfo{
		ID:              details.Id,
		Name:            details.Name,
		PartitionsCount: details.PartitionsCount,
		MembersCount:    details.MembersCount,
		Identity:        identity,
	}
	switch {
	case request.policy != nil:
		binding, err := group.Filter().configureScopedAs(
			ctx, request.operationID, *request.policy, identity)
		if err != nil {
			// The group is left as it is rather than removed: it may already
			// have members, and deleting it to tidy up a failed policy would
			// take their stored offsets with it.
			return nil, &ConsumerGroupSetupError{
				GroupID: details.Id, Name: details.Name,
				Identity: identity, Cause: err,
			}
		}
		info.Filter = binding
	case group.laser().Capabilities().Filters.Catalog:
		if info.Filter, err = group.Filter().Get(ctx); err != nil {
			return nil, err
		}
	}
	return info, nil
}

// ConsumerGroupSetupError reports a group that exists but whose policy could
// not be configured. The group is left as it is.
type ConsumerGroupSetupError struct {
	GroupID  uint32
	Name     string
	Identity wire.FilterGroupIdentity
	Cause    error
}

func (e *ConsumerGroupSetupError) Error() string {
	return fmt.Sprintf(
		"consumer group %s (#%d) exists but its policy could not be configured: %v",
		e.Name, e.GroupID, e.Cause)
}

func (e *ConsumerGroupSetupError) Unwrap() error { return e.Cause }

// GroupFilter is a consumer group's filter policy. Build it with
// ConsumerGroup.Filter. Every verb needs a deployment that serves the filter
// catalog.
type GroupFilter struct {
	group *ConsumerGroup
}

// Configure gives the group filter as its policy.
//
// The definition is saved as the group's own filter under name and the group
// is bound to it in one catalog transaction, so a reader never sees a group
// that is half configured. The same content again keeps the binding rather
// than adding a revision, which is what lets a deployment configure its groups
// on every start and converge instead of accumulating.
func (filter *GroupFilter) Configure(
	ctx context.Context, name string, consumerFilter wire.ConsumerFilter,
) (*wire.FilterBinding, error) {
	return filter.ConfigureWith(ctx, wire.DefinitionSpec(name, consumerFilter))
}

// ConfigureWith is Configure with a policy spec.
func (filter *GroupFilter) ConfigureWith(
	ctx context.Context, policy wire.GroupFilterSpec,
) (*wire.FilterBinding, error) {
	return filter.ConfigureAs(ctx, nil, policy)
}

// ConfigureAs is ConfigureWith under a caller-chosen operation id, so a caller
// that records the id first resumes the same configuration after a crash and
// reads its first outcome.
func (filter *GroupFilter) ConfigureAs(
	ctx context.Context, operationID *wire.U128, policy wire.GroupFilterSpec,
) (*wire.FilterBinding, error) {
	details, err := filter.group.native(ctx)
	if err != nil {
		return nil, err
	}
	identity, err := filter.group.identityOf(ctx, uint64(details.Id))
	if err != nil {
		return nil, err
	}
	return filter.configureScopedAs(ctx, operationID, policy, identity)
}

// configureScopedAs configures the policy against one exact group incarnation.
func (filter *GroupFilter) configureScopedAs(
	ctx context.Context, operationID *wire.U128,
	policy wire.GroupFilterSpec, identity wire.FilterGroupIdentity,
) (*wire.FilterBinding, error) {
	ref, err := filter.group.GroupRef(ctx)
	if err != nil {
		return nil, err
	}
	catalog := filter.group.laser().FilterCatalog()

	var binding *wire.FilterBinding
	var position *wire.CatalogPosition
	if operationID != nil {
		binding, position, err = catalog.ConfigureGroupAs(
			ctx, *operationID, ref, policy, identity)
	} else {
		binding, position, err = catalog.ConfigureGroup(ctx, ref, policy)
	}
	if err != nil {
		return nil, err
	}
	// The binding is checked against the incarnation it was meant for, because
	// a group deleted and recreated under the same name between the read and
	// the write would otherwise have been configured by mistake.
	if binding != nil && binding.Identity != identity {
		return nil, &FilterError{Failure: wire.NewFilterError(wire.ReasonSourceChanged,
			"configured binding identifies another group incarnation")}
	}
	filter.group.rememberPosition(position)
	return binding, nil
}

// Get is the active policy, nil for an unbound group.
func (filter *GroupFilter) Get(ctx context.Context) (*wire.FilterBinding, error) {
	if err := filter.group.laser().FilterCatalog().requireServed(); err != nil {
		return nil, err
	}
	details, err := filter.group.native(ctx)
	if err != nil {
		return nil, err
	}
	identity, err := filter.group.identityOf(ctx, uint64(details.Id))
	if err != nil {
		return nil, err
	}
	ref, err := filter.group.GroupRef(ctx)
	if err != nil {
		return nil, err
	}
	binding, err := filter.group.laser().FilterCatalog().Binding(ctx, ref)
	if err != nil {
		// An unbound group is an absence, not a failure: it reads every
		// record, which is a supported way to run a group.
		var failure *FilterError
		if errors.As(err, &failure) && failure.NotFound() {
			return nil, nil
		}
		return nil, err
	}
	if binding != nil && binding.Identity != identity {
		return nil, &FilterError{Failure: wire.NewFilterError(wire.ReasonSourceChanged,
			"the saved binding identifies another group incarnation")}
	}
	return binding, nil
}

// active is the policy the group runs, refusing an unbound group.
//
// The verbs below all address the group's own filter through its binding, so
// a group with no policy has nothing for them to act on and says so rather
// than acting on whichever filter happens to be first in the catalog.
func (filter *GroupFilter) active(ctx context.Context) (*wire.FilterBinding, error) {
	binding, err := filter.Get(ctx)
	if err != nil {
		return nil, err
	}
	if binding == nil {
		return nil, &FilterError{Failure: wire.NewFilterError(wire.ReasonNotFound,
			"the consumer group has no filter policy")}
	}
	return binding, nil
}

// Summary is the group's own filter as a catalog row, bound or released: its
// name, state, latest revision and the owning group. It is nil when the group
// has no filter of its own.
//
// The row is looked up by the group's exact identity rather than by its name,
// so a group recreated under the same name does not find the filter the
// previous group owned.
func (filter *GroupFilter) Summary(ctx context.Context) (*wire.FilterSummary, error) {
	if err := filter.group.laser().FilterCatalog().requireServed(); err != nil {
		return nil, err
	}
	details, err := filter.group.native(ctx)
	if err != nil {
		return nil, err
	}
	identity, err := filter.group.identityOf(ctx, uint64(details.Id))
	if err != nil {
		return nil, err
	}
	page, err := filter.group.laser().FilterCatalog().Filters(
		ctx, FilterQuery{Owner: &identity}, 1, 1)
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, nil
	}
	// The owner is checked again on the way out rather than trusted from the
	// query, because a row whose owner is another incarnation would otherwise
	// be returned as this group's own filter.
	for index := range page.Items {
		summary := &page.Items[index]
		if summary.Owner != nil && summary.Owner.Identity == identity {
			return summary, nil
		}
	}
	return nil, nil
}

// Revisions pages through the revisions of the group's own filter, newest
// first. Page counts from 1 and results is the page length.
func (filter *GroupFilter) Revisions(
	ctx context.Context, page, results uint32,
) (*wire.FilterRevisionPage, error) {
	active, err := filter.active(ctx)
	if err != nil {
		return nil, err
	}
	return filter.group.laser().FilterCatalog().Revisions(
		ctx, active.FilterID, page, results)
}

// SetRevisionEnabled pauses or resumes one revision of the group's own filter.
//
// A paused active revision refuses new reads while records already delivered
// can still be acknowledged, which is what lets an operator stop a filter
// without stranding the work its readers are already holding.
func (filter *GroupFilter) SetRevisionEnabled(
	ctx context.Context, revision uint32, enabled bool,
) error {
	active, err := filter.active(ctx)
	if err != nil {
		return err
	}
	position, err := filter.group.laser().FilterCatalog().SetRevisionEnabled(
		ctx, active.FilterID, revision, enabled)
	if err != nil {
		return err
	}
	filter.group.rememberPosition(position)
	return nil
}

// Delete removes the group's own filter with every revision, reporting whether
// one was there to remove.
//
// A bound group is released first, so its consumers receive every record from
// their next poll rather than reading through a filter that no longer exists.
func (filter *GroupFilter) Delete(ctx context.Context) (bool, error) {
	own, err := filter.Summary(ctx)
	if err != nil {
		return false, err
	}
	if own == nil {
		return false, nil
	}
	position, err := filter.group.laser().FilterCatalog().Drop(ctx, own.ID)
	if err != nil {
		return false, err
	}
	filter.group.rememberPosition(position)
	return true, nil
}

// Preview runs the active policy over the stored records of one partition.
//
// A preview joins no group, stores no offset and changes no consumer state, so
// it is safe to run against a group that is being read right now. It is pinned
// to the exact revision the group runs rather than to the group, so what it
// shows is what the group's readers currently see.
func (filter *GroupFilter) Preview(
	ctx context.Context, partitionID uint32,
) (*PreviewRequest, error) {
	active, err := filter.active(ctx)
	if err != nil {
		return nil, err
	}
	return filter.group.topic.Filters(filter.group.Selector()).Preview(
		partitionID, wire.SavedRevision(active.FilterID, active.Revision)), nil
}

// Test evaluates the active policy against one supplied record and explains
// the verdict. Nothing is read from a stream and nothing is stored.
func (filter *GroupFilter) Test(
	ctx context.Context, payload []byte, headers []wire.FilterHeader,
) (*wire.FilterTestResult, error) {
	active, err := filter.active(ctx)
	if err != nil {
		return nil, err
	}
	return filter.group.topic.Filters(filter.group.Selector()).Test(
		ctx, wire.SavedRevision(active.FilterID, active.Revision), payload, headers)
}

// Release releases this group's binding, leaving the group unbound and reading
// every record.
//
// It names the exact incarnation the binding was saved for, so a release
// cannot take out a binding that belongs to a group recreated under the same
// name since.
func (filter *GroupFilter) Release(ctx context.Context) (*wire.FilterBinding, error) {
	binding, err := filter.Get(ctx)
	if err != nil {
		return nil, err
	}
	if binding == nil {
		return nil, &FilterError{Failure: wire.NewFilterError(wire.ReasonNotFound,
			"the consumer group runs no policy, so there is none to release")}
	}
	released, position, err := filter.group.laser().
		FilterCatalog().ReleaseBinding(ctx, *binding)
	if err != nil {
		return nil, err
	}
	filter.group.rememberPosition(position)
	return released, nil
}

// supersedes reports whether position is later on the control log than known.
//
// A revision orders outcomes on its own, because it keeps counting across a
// recreated control log. Without one the comparison falls back to the offset,
// and a changed operation or partition also counts as later: a control log
// that was recreated restarts its offsets, so a smaller offset on a different
// partition or operation is newer rather than older.
func supersedes(known, position wire.CatalogPosition) bool {
	if known.Revision != nil && position.Revision != nil {
		return *position.Revision > *known.Revision
	}
	if position.OperationID != nil &&
		(known.OperationID == nil || *known.OperationID != *position.OperationID) {
		return true
	}
	return known.PartitionID != position.PartitionID || known.Offset < position.Offset
}

// isExistingGroup reports the error that means the group is already there,
// which is the outcome an ensure asked for rather than a failure.
func isExistingGroup(err error) bool {
	return errors.Is(err, ierror.ConsumerGroupNameAlreadyExists{})
}

// isMissingGroup reports the errors that mean the group is not there.
func isMissingGroup(err error) bool {
	return errors.Is(err, ierror.ConsumerGroupIdNotFound{}) ||
		errors.Is(err, ierror.ConsumerGroupNameNotFound{})
}
