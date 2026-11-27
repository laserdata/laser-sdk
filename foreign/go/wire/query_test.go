package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Every query request shape in the corpus, including the three additive
// variants a server must be able to read unchanged.
func TestGivenQueryRequestFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("query_envelope.bin", func(t *testing.T) {
		envelope := roundTripFixture[wire.QueryEnvelope](t, "query_envelope.bin")
		if err := envelope.Validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		query := envelope.Query
		if query.Target.Kind != wire.TargetOperational || query.Target.Index != "readings" {
			t.Errorf("want the readings index, got %+v", query.Target)
		}
		if query.TimeRange == nil || query.TimeRange.Start != 1000 || query.TimeRange.End != 2000 {
			t.Errorf("want the inclusive range 1000..2000, got %+v", query.TimeRange)
		}
		if query.Vector == nil || query.Vector.TopK != 5 {
			t.Errorf("want a top-5 vector search, got %+v", query.Vector)
		}
		if query.Filter == nil || query.Filter.Kind != wire.FilterAll {
			t.Error("want the conjunction filter")
		}
	})
	t.Run("query_envelope_aggregate.bin", func(t *testing.T) {
		envelope := roundTripFixture[wire.QueryEnvelope](t, "query_envelope_aggregate.bin")
		if envelope.Query.Aggregate == nil {
			t.Fatal("the aggregate fixture carries an aggregation")
		}
		if len(envelope.Query.Aggregate.Funcs) == 0 {
			t.Error("an aggregation carries at least one call")
		}
	})
	t.Run("query_envelope_raw_sql.bin", func(t *testing.T) {
		envelope := roundTripFixture[wire.QueryEnvelope](t, "query_envelope_raw_sql.bin")
		if envelope.Query.RawSQL == nil {
			t.Fatal("the raw-SQL fixture carries a statement")
		}
	})
	t.Run("query_envelope_text.bin", func(t *testing.T) {
		envelope := roundTripFixture[wire.QueryEnvelope](t, "query_envelope_text.bin")
		if envelope.Query.Text == nil {
			t.Fatal("the text fixture carries a lexical search")
		}
	})
	t.Run("query_envelope_read_your_writes.bin", func(t *testing.T) {
		envelope := roundTripFixture[wire.QueryEnvelope](t, "query_envelope_read_your_writes.bin")
		if envelope.Query.Consistency != wire.ConsistencyReadYourWrites {
			t.Errorf("want read-your-writes, got %q", envelope.Query.Consistency)
		}
	})
	t.Run("query_target_discriminants.bin", func(t *testing.T) {
		roundTripFixture[[]wire.QueryTarget](t, "query_target_discriminants.bin")
	})
	t.Run("query_error_discriminants.bin", func(t *testing.T) {
		errors := roundTripFixture[[]wire.QueryError](t, "query_error_discriminants.bin")
		if len(errors) == 0 {
			t.Fatal("the discriminant fixture lists every error variant")
		}
		// Every listed variant must classify and project cleanly, since a
		// client dispatches retry behaviour on the projection.
		for i, queryError := range errors {
			if queryError.Code() == "" {
				t.Errorf("error %d has no classification", i)
			}
			if queryError.ResultCode().String() == "" {
				t.Errorf("error %d does not project onto a result code", i)
			}
		}
	})
}

// A query target is internally tagged, so its kind rides alongside the
// variant's own fields rather than nesting them.
func TestGivenAQueryTargetWhenEncodedThenShouldCarryItsKindInline(t *testing.T) {
	encoded, err := wire.Marshal(wire.OperationalTarget("readings"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Two entries: the kind and the index, at one level.
	if encoded[0] != 0xa2 {
		t.Fatalf("want a two-entry map, got head 0x%02x", encoded[0])
	}

	var back wire.QueryTarget
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Kind != wire.TargetOperational || back.Index != "readings" {
		t.Fatalf("round trip changed the target: %+v", back)
	}

}

// The page contract is load-bearing: has_more is exact and free, while a total
// exists only when asked for, so a caller cannot misread a page-bounded number
// as a count.
func TestGivenAResultPageWhenValidatedThenShouldKeepItsContract(t *testing.T) {
	valid := wire.Page{Limit: 50, HasMore: true}
	if err := valid.Validate(); err != nil {
		t.Fatalf("a consistent page must validate: %v", err)
	}

	if err := (wire.Page{Limit: 0}).Validate(); err == nil {
		t.Error("a zero limit must be refused")
	}
	if err := (wire.Page{Limit: wire.MaxPageSize + 1}).Validate(); err == nil {
		t.Error("a limit past the cap must be refused")
	}

	// Without want_total there is no number at all, so page math reports none.
	if _, ok := valid.TotalPages(); ok {
		t.Error("page math is undefined without an exact total")
	}
	total := uint64(101)
	withTotal := wire.Page{Limit: 50, Total: &total, HasMore: true}
	pages, ok := withTotal.TotalPages()
	if !ok || pages != 3 {
		t.Errorf("want 3 pages for 101 rows at 50, got %d (ok=%v)", pages, ok)
	}

	offset := uint64(40)
	withOffset := wire.Page{Limit: 50, Offset: &offset}
	atLeast, ok := withOffset.AtLeast(20)
	if !ok || atLeast != 60 {
		t.Errorf("want a lower bound of 60, got %d (ok=%v)", atLeast, ok)
	}
}

// An unauthorized query is forbidden, not unauthenticated: the caller
// authenticated but lacks the grant for the named resource.
func TestGivenQueryErrorsWhenProjectedThenShouldClassifyForRetry(t *testing.T) {
	for name, pair := range map[string]struct {
		err  wire.QueryError
		want wire.ResultCode
	}{
		"unauthorized is forbidden": {
			wire.QueryError{Kind: wire.QueryUnauthorized}, wire.ResultForbidden,
		},
		"stale is stale": {
			wire.QueryError{Kind: wire.QueryStale}, wire.ResultStale,
		},
		"unavailable is unavailable": {
			wire.QueryError{Kind: wire.QueryUnavailable}, wire.ResultUnavailable,
		},
		"a malformed request is an invalid argument": {
			wire.QueryError{Kind: wire.QueryInvalid}, wire.ResultInvalidArgument,
		},
		"an unregistered schema is not found": {
			wire.QueryError{Kind: wire.QuerySchemaNotFound}, wire.ResultNotFound,
		},
	} {
		if got := pair.err.ResultCode(); got != pair.want {
			t.Errorf("%s: want %s, got %s", name, pair.want, got)
		}
	}

	// Only the transient classes invite a retry.
	if !(wire.QueryError{Kind: wire.QueryStale}).ResultCode().IsRetryable() {
		t.Error("a stale read is retryable while the projector catches up")
	}
	if (wire.QueryError{Kind: wire.QueryUnauthorized}).ResultCode().IsRetryable() {
		t.Error("a missing grant is not retryable")
	}
}

// The reply side, which the schema port unblocked. Each row is positionally
// aligned with the result fields, so a width mismatch would misalign values
// against names.
func TestGivenQueryReplyFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("query_reply_ok.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.QueryReply](t, "query_reply_ok.bin")
		if !reply.IsOk() {
			t.Fatalf("want a successful reply, got %+v", reply.Err)
		}
		if err := reply.Result.Validate(); err != nil {
			t.Fatalf("the corpus result must validate: %v", err)
		}
		for i, row := range reply.Result.Rows {
			if len(row.Values) != len(reply.Result.Fields) {
				t.Errorf("row %d has %d values for %d fields",
					i, len(row.Values), len(reply.Result.Fields))
			}
		}
		if _, ok := reply.Result.Context.ResolvedTarget.Index(); !ok {
			t.Error("the fixture resolves to an operational index")
		}
	})
	t.Run("query_reply_err_stale.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.QueryReply](t, "query_reply_err_stale.bin")
		if reply.IsOk() || reply.Err.Kind != wire.QueryStale {
			t.Fatalf("want a stale failure, got %+v", reply)
		}
		// Fail-not-downgrade: the caller is told how far behind the projector
		// is rather than silently served older rows.
		if reply.Err.Required <= reply.Err.Applied {
			t.Errorf("a stale read reports required past applied, got %d <= %d",
				reply.Err.Required, reply.Err.Applied)
		}
		if !reply.Err.ResultCode().IsRetryable() {
			t.Error("a stale read is retryable while the projector catches up")
		}
	})
	t.Run("query_reply_err_too_large.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.QueryReply](t, "query_reply_err_too_large.bin")
		if reply.IsOk() || reply.Err.Kind != wire.QueryTooLarge {
			t.Fatalf("want a too-large failure, got %+v", reply)
		}
		// Page with a smaller limit instead of retrying unchanged.
		if reply.Err.ResultCode().IsRetryable() {
			t.Error("an oversized result is not fixed by retrying unchanged")
		}
	})
}

// A result whose rows do not match its schema width must be refused: the values
// are positional, so a short row would silently shift every later value onto
// the wrong field name.
func TestGivenAMisalignedResultWhenValidatedThenShouldBeRefused(t *testing.T) {
	reply := roundTripFixture[wire.QueryReply](t, "query_reply_ok.bin")

	misaligned := *reply.Result
	misaligned.Rows = []wire.Row{{Values: nil}}
	if err := misaligned.Validate(); err == nil {
		t.Fatal("a row of the wrong width must be refused")
	}

	empty := *reply.Result
	empty.Fields = nil
	if err := empty.Validate(); err == nil {
		t.Fatal("a result with no schema must be refused")
	}
}
