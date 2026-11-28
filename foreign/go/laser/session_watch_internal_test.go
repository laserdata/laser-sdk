package laser

import (
	"slices"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAWatchWhenThePageOverlapsThenShouldReportEachSessionOnce(t *testing.T) {
	first, second := NewConversationID(), NewConversationID()
	watch := SessionWatch{next: 5, series: 2}
	page := wire.SessionChanges{Series: 2, Floor: 4, Results: 3, Items: []wire.SessionChangeRow{
		{Seq: 4, Sessions: []wire.ConversationID{second}},
		{Seq: 5, Sessions: []wire.ConversationID{first, second}},
		{Seq: 6, Sessions: []wire.ConversationID{first}},
	}}
	change, ready := watch.observe(&page)
	if !ready || change.Resync || !slices.Equal(change.Sessions, []wire.ConversationID{first, second}) || watch.next != 7 {
		t.Fatalf("first read returned %+v with next %d", change, watch.next)
	}
	if _, ready := watch.observe(&page); ready {
		t.Fatal("overlapping page repeated a change")
	}
}

func TestGivenAWatchWhenItsIndexLosesContinuityThenShouldRequestResync(t *testing.T) {
	for _, test := range []struct {
		name string
		page wire.SessionChanges
		next uint64
	}{
		{"different series", wire.SessionChanges{Series: 2, TotalResults: 9}, 10},
		{"pruned changes", wire.SessionChanges{Series: 1, Floor: 6, TotalResults: 9}, 10},
		{"truncated row", wire.SessionChanges{Series: 1, Items: []wire.SessionChangeRow{{Seq: 5, Truncated: true}, {Seq: 6}}}, 7},
	} {
		t.Run(test.name, func(t *testing.T) {
			watch := SessionWatch{next: 5, series: 1}
			change, ready := watch.observe(&test.page)
			if !ready || !change.Resync || len(change.Sessions) != 0 || watch.next != test.next || watch.series != test.page.Series {
				t.Fatalf("resync returned %+v with next %d and series %d", change, watch.next, watch.series)
			}
		})
	}
}
