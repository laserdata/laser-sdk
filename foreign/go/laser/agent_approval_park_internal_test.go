package laser

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A decision minted by one client has to be recognized by the other, and the
// correlation is derived rather than carried, so the two derivations have to
// agree byte for byte. These vectors were printed by the Rust client's own
// approval_correlation (sdk/src/agent/ctx.rs) rather than copied from this
// implementation, so they fail if either side drifts.
func TestGivenRustsOwnVectorsWhenDerivingApprovalCorrelationsThenShouldMatch(t *testing.T) {
	for _, vector := range []struct {
		at   wire.LogPosition
		want [16]byte
	}{
		{
			wire.LogPosition{StreamID: 1, TopicID: 2, PartitionID: 3, Offset: 40},
			[16]byte{166, 166, 151, 195, 63, 210, 188, 228,
				218, 200, 152, 6, 157, 195, 134, 158},
		},
		{
			wire.LogPosition{},
			[16]byte{166, 166, 194, 233, 236, 127, 86, 59,
				153, 93, 205, 105, 125, 167, 114, 249},
		},
		{
			wire.LogPosition{StreamID: 7, TopicID: 11, PartitionID: 2, Offset: 99},
			[16]byte{166, 166, 44, 40, 182, 3, 252, 96,
				163, 102, 230, 15, 8, 244, 133, 98},
		},
	} {
		got := approvalCorrelation(vector.at).Bytes()
		if got != vector.want {
			t.Errorf("for %+v want %v, got %v", vector.at, vector.want, got)
		}
		if !isApprovalCorrelation(approvalCorrelation(vector.at)) {
			t.Errorf("for %+v the derived correlation reads as not an approval", vector.at)
		}
	}
}

// Two different records must not share a correlation, or one decision would
// resume the wrong parked record.
func TestGivenTwoRecordsWhenDerivingApprovalCorrelationsThenShouldDiffer(t *testing.T) {
	first := approvalCorrelation(wire.LogPosition{
		StreamID: 1, TopicID: 2, PartitionID: 3, Offset: 40})
	second := approvalCorrelation(wire.LogPosition{
		StreamID: 1, TopicID: 2, PartitionID: 3, Offset: 41})
	if first == second {
		t.Fatal("two positions derived the same approval correlation")
	}
}

// An ordinary minted correlation must not read as an approval, or every
// reply on the topic would be treated as an approval decision.
func TestGivenAMintedCorrelationWhenCheckedThenShouldNotReadAsAnApproval(t *testing.T) {
	// Minted the way every other correlation in this package is.
	minted := wire.CorrelationID{ID: wire.IDFromBytes(NewOperationID())}
	if isApprovalCorrelation(minted) {
		t.Fatal("a minted correlation read as an approval correlation")
	}
}
