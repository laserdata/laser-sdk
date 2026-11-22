package laser

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// supersedes decides whether a handle moves its read-your-writes position
// forward. It is unexported and only reachable through a catalog configure, so
// it is tested here directly: a deployment serving no filter catalog cannot
// reach it at all, and a mutation sweep showed every branch of it surviving
// through the public surface.
//
// Getting it wrong is quiet and costly in both directions. Too eager and a
// handle claims to have seen a configuration it has not, so a later read can
// be answered by a node that is behind. Too reluctant and the handle keeps
// sending a stale position for ever, so every read waits for a frontier that
// has already passed.

func position(partition uint32, offset uint64) wire.CatalogPosition {
	return wire.CatalogPosition{PartitionID: partition, Offset: offset}
}

func withRevision(base wire.CatalogPosition, revision uint64) wire.CatalogPosition {
	base.Revision = &revision
	return base
}

func withOperation(base wire.CatalogPosition, operation wire.U128) wire.CatalogPosition {
	base.OperationID = &operation
	return base
}

func TestGivenTwoRevisionsWhenComparedThenShouldPreferTheHigherOne(t *testing.T) {
	t.Parallel()
	known := withRevision(position(0, 100), 4)

	if !supersedes(known, withRevision(position(0, 1), 5)) {
		t.Error("a higher revision must supersede, even on a lower offset: a " +
			"recreated control log restarts its offsets")
	}
	if supersedes(known, withRevision(position(0, 900), 3)) {
		t.Error("a lower revision must not supersede, however high its offset")
	}
	if supersedes(known, withRevision(position(0, 900), 4)) {
		t.Error("the same revision is not later than itself")
	}
}

func TestGivenNoRevisionWhenComparedThenShouldFallBackToTheOffset(t *testing.T) {
	t.Parallel()
	known := position(2, 100)

	if !supersedes(known, position(2, 101)) {
		t.Error("a later offset on the same partition must supersede")
	}
	if supersedes(known, position(2, 99)) {
		t.Error("an earlier offset on the same partition must not supersede")
	}
	if supersedes(known, position(2, 100)) {
		t.Error("the same position is not later than itself")
	}
}

func TestGivenAnotherPartitionWhenComparedThenShouldSupersede(t *testing.T) {
	t.Parallel()
	// A control log that was recreated restarts its offsets, so a smaller
	// offset on a different partition is newer rather than older. Comparing
	// offsets alone would strand the handle on a position that no longer
	// exists.
	if !supersedes(position(2, 500), position(3, 1)) {
		t.Error("a position on another partition must supersede")
	}
}

func TestGivenAnotherOperationWhenComparedThenShouldSupersede(t *testing.T) {
	t.Parallel()
	first := wire.U128FromUint64(1)
	second := wire.U128FromUint64(2)

	if !supersedes(withOperation(position(0, 100), first),
		withOperation(position(0, 1), second)) {
		t.Error("another proven operation must supersede, whatever the offset")
	}
	if supersedes(withOperation(position(0, 100), first),
		withOperation(position(0, 1), first)) {
		t.Error("the same operation at an earlier offset must not supersede")
	}
	// A position carrying no operation falls through to the offset rule rather
	// than counting as a new operation.
	if supersedes(withOperation(position(0, 100), first), position(0, 50)) {
		t.Error("a position with no operation and a lower offset must not supersede")
	}
}

func TestGivenOneSideHasNoRevisionWhenComparedThenShouldUseTheOffsetRule(t *testing.T) {
	t.Parallel()
	// The revision rule needs both sides to carry one. With only one, the
	// comparison falls back rather than treating a missing revision as zero,
	// which would make every revisioned position supersede every plain one.
	if supersedes(withRevision(position(0, 100), 9), position(0, 50)) {
		t.Error("a lower offset must not supersede when only one side is revisioned")
	}
	if !supersedes(withRevision(position(0, 100), 9), position(0, 150)) {
		t.Error("a higher offset must supersede when only one side is revisioned")
	}
}
