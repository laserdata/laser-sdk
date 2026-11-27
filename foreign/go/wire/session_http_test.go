package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The managed session reads as the HTTP surface serves them.
//
// These are what a console renders a session from, so a field that decoded
// wrongly shows an operator the wrong history, and a frontier that decoded
// wrongly makes a paged walk skip or repeat records.

func TestGivenSessionReadFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("session_http_info.json", func(t *testing.T) {
		info := roundTripJSONFixture[wire.SessionInfo](t, "session_http_info.json")
		if info.Stream == "" || info.ID.IsZero() {
			t.Error("a session names the stream and conversation it belongs to")
		}
		// A child session keeps both its parent and its root, so a tree can be
		// rebuilt without walking every ancestor.
		if info.Parent == nil || info.Root == nil {
			t.Error("the fixture session is a child")
		}
	})

	t.Run("session_http_page.json", func(t *testing.T) {
		page := roundTripJSONFixture[wire.SessionPage](t, "session_http_page.json")
		if len(page.Items) == 0 {
			t.Error("the page lists sessions")
		}
	})

	t.Run("session_http_page_rebuilding.json", func(t *testing.T) {
		page := roundTripJSONFixture[wire.SessionPage](t,
			"session_http_page_rebuilding.json")
		// A rebuilding index says so, rather than letting an incomplete
		// answer look like an empty one.
		if !page.Rebuilding {
			t.Error("the fixture page is rebuilding")
		}
	})

	t.Run("session_http_events.json", func(t *testing.T) {
		page := roundTripJSONFixture[wire.SessionEventsPage](t,
			"session_http_events.json")
		if len(page.Items) == 0 {
			t.Fatal("the page lists events")
		}
		// Every event points back at the record it was folded from, which is
		// what lets a reader fetch the original.
		if page.Items[0].At.Kind != wire.SourceMessage {
			t.Error("an event names its source record")
		}
	})

	t.Run("session_http_state_head.json", func(t *testing.T) {
		view := roundTripJSONFixture[wire.SessionStateView](t,
			"session_http_state_head.json")
		// Naming the operation that produced the revision lets a caller match
		// its own write against the state it is reading back.
		if view.HeadOpID == nil {
			t.Error("this fixture names the operation its revision came from")
		}
	})

	t.Run("session_http_state.json", func(t *testing.T) {
		view := roundTripJSONFixture[wire.SessionStateView](t, "session_http_state.json")
		if view.Revision == 0 {
			t.Error("a state view names the revision it folded to")
		}
		if _, present := view.Document.Field("tasks"); !present {
			t.Error("the fixture document carries its fields")
		}
		// The history is what tells an operator which patch produced the
		// current document and which were refused.
		if len(view.History) == 0 {
			t.Error("a state view carries its history")
		}
	})

	t.Run("session_http_links.json", func(t *testing.T) {
		view := roundTripJSONFixture[wire.SessionLinksView](t, "session_http_links.json")
		if len(view.Links) == 0 {
			t.Fatal("the view lists links")
		}
		link := view.Links[0]
		if link.Surface == "" || link.Resource == "" {
			t.Error("a link names the surface and resource it touched")
		}
		if link.Relation != wire.RelationWrote {
			t.Errorf("want a write relation, got %q", link.Relation)
		}
	})

	t.Run("session_http_sources.json", func(t *testing.T) {
		sources := roundTripJSONFixture[wire.SessionSources](t,
			"session_http_sources.json")
		if len(sources.Sources) == 0 {
			t.Fatal("the view lists source partitions")
		}
		source := sources.Sources[0]
		// Folded is how far the index read and head is where the log is, so
		// the distance between them is what the index has yet to see. Both
		// are absent before the first record rather than zero, which would
		// read as a partition that has been fully folded.
		if source.Folded == nil || source.Head == nil || source.RetainedFrom == nil {
			t.Fatal("the fixture source has been read")
		}
		if *source.Folded >= *source.Head {
			t.Error("the fixture index is behind the log head")
		}
		// Retained marks where the source still holds records, so a reader
		// below it has lost data rather than merely lagging.
		if *source.RetainedFrom > *source.Folded {
			t.Error("the fixture has not lost records")
		}
	})

	// A session failure reaches HTTP as the surface's own classified body,
	// not as the binary variant, so a browser client branches on the same
	// code it would get from any other route.
	t.Run("session_http_error.json", func(t *testing.T) {
		body := roundTripJSONFixture[wire.ErrorBody](t, "session_http_error.json")
		if body.Code != wire.ResultNotFound {
			t.Errorf("want a not-found code, got %q", body.Code)
		}
		converted := wire.ErrorBodyOf(wire.SessionError{
			Kind:    wire.SessionNotFound,
			Message: "00000000000000000000000004",
		})
		if converted.Code != body.Code || converted.Message != body.Message {
			t.Errorf("converting the failure gives %+v, the fixture has %+v",
				converted, body)
		}
	})
}

// The session change feed, which a reader follows to learn what moved.
//
// Each fixture is a different shape the feed can take, and telling them apart
// is what keeps a follower from silently missing rows.
func TestGivenSessionChangeFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	for _, name := range []string{
		"session_http_changes_page.json",
		"session_http_changes_empty.json",
		"session_http_changes_start.json",
		"session_http_changes_start_new.json",
		"session_http_changes_gap.json",
		"session_http_changes_truncated.json",
		"session_http_changes_origins.json",
		"session_http_changes_no_retained_rows.json",
	} {
		t.Run(name, func(t *testing.T) {
			roundTripJSONFixture[wire.SessionChanges](t, name)
		})
	}

	// A page without its series is refused rather than read as series zero,
	// which would let a follower continue across an index rebuild it should
	// have noticed.
	t.Run("session_http_changes_legacy.json", func(t *testing.T) {
		var page wire.SessionChanges
		if err := wire.UnmarshalJSON(
			readFixture(t, "session_http_changes_legacy.json"), &page); err == nil {
			t.Error("a change page without its series must be refused")
		}
	})

	// A page of rows reports what moved. Paging is positional now: page p of
	// size r holds the sequences (p-1)*r+1 through p*r, so an append or a
	// prune never shifts a page out from under a reader.
	page := roundTripJSONFixture[wire.SessionChanges](t,
		"session_http_changes_page.json")
	if len(page.Items) == 0 {
		t.Fatal("the page carries rows")
	}
	if len(page.Items[0].Sessions) == 0 {
		t.Error("a change row names the sessions it moved")
	}
	if page.Results == 0 {
		t.Error("a page reports the size the server used, which fixes its range")
	}

	// Reading past the last page is empty rather than an error, so a follower
	// that caught up simply sees nothing new.
	empty := roundTripJSONFixture[wire.SessionChanges](t,
		"session_http_changes_empty.json")
	if len(empty.Items) != 0 {
		t.Error("the empty fixture carries no rows")
	}
	if empty.Page <= empty.TotalPages {
		t.Errorf("the empty fixture reads past the last page, got %d of %d",
			empty.Page, empty.TotalPages)
	}

	// The floor is the oldest sequence the feed still holds. A reader below it
	// lost rows and restarts there. A rebuilt feed starts from the beginning,
	// where one that merely pruned starts from where it still holds rows.
	started := roundTripJSONFixture[wire.SessionChanges](t,
		"session_http_changes_start.json")
	fresh := roundTripJSONFixture[wire.SessionChanges](t,
		"session_http_changes_start_new.json")
	if started.Floor <= 1 {
		t.Error("a pruned feed holds rows only from where it still has them")
	}
	if fresh.Floor != 1 || fresh.TotalResults != 0 {
		t.Errorf("a rebuilt feed starts from the beginning, got %+v", fresh)
	}

	// Every page of one registration names the same change series, so a
	// reader can tell a rebuilt index from a continuing one.
	if page.Series == 0 || page.Series != started.Series {
		t.Error("a registration's pages share one change series")
	}
}
