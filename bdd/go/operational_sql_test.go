package bdd_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedOperationalSqlScenariosWhenRunThenShouldHold runs the
// authorized operational SQL feature: SQL reads materialized indexes through
// the same managed query authority as a structured query, and refuses the
// relations and dialects it is not meant to serve.
func TestGivenTheSharedOperationalSqlScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "operational_sql.feature", 3*time.Minute, operationalSQLStep)
}

// sqlIndex is the topic the feature materializes whole records into.
const sqlIndex = "whole_records"

// sqlFlow is the materialization the scenario set up.
type sqlFlow struct {
	materialization *laser.Materialization
	binding         wire.BackendBinding
	dialect         wire.SqlDialect
}

func sqlFlowOf(state *world) *sqlFlow {
	if existing, found := state.get("operational-sql"); found {
		return existing.(*sqlFlow)
	}
	flow := &sqlFlow{}
	state.put("operational-sql", flow)
	return flow
}

func operationalSQLStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	flow := sqlFlowOf(state)
	text := step.Text

	switch {
	case text == "I materialize whole records twice under one operation identity":
		return materializeWholeTwice(ctx, state, flow)

	case text == "I publish two arbitrary records to the materialized topic":
		return publishArbitrary(ctx, state, sqlIndex)

	case text == "materialization reaches the captured source head":
		return materializationCaughtUp(ctx, flow)

	case text == "operational SQL counts the two materialized records":
		return sqlCountsTwo(ctx, state, sqlIndex, wire.DialectSQLite)

	case text == "operational SQL refuses an internal relation and an unserved dialect":
		return sqlRefusals(ctx, state)

	case text == "a declared SQL backend":
		kind := os.Getenv("LASER_BDD_SQL_BACKEND")
		switch kind {
		case "postgres":
			flow.dialect = wire.DialectPostgres
		case "velodb":
			flow.dialect = wire.DialectMySQL
		default:
			return fmt.Errorf("unsupported test backend %q", kind)
		}
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for caps := state.client.Capabilities(); ; caps = state.client.RefreshCapabilities(ctx) {
			for _, backend := range caps.EnabledBackends() {
				if backend.Implementation.Kind == kind && backend.Mode == wire.BackendOperational && backend.Query != nil && backend.Query.RawSQL && slices.Contains(backend.Query.Dialects, flow.dialect) {
					flow.binding = wire.BackendBinding{ResourceID: backend.ResourceID, Generation: backend.ObservedBackendGeneration}
					return nil
				}
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("waiting for backend %s to advertise %s: %w", kind, flow.dialect, ctx.Err())
			case <-ticker.C:
			}
		}
	case text == "I materialize whole records on the declared SQL backend":
		topic := state.topic("remote_records")
		if err := topic.Ensure(ctx, 1, nil); err != nil {
			return err
		}
		flow.materialization, err = topic.Materialize().Backend(flow.binding).Apply(ctx)
		return err
	case text == "I publish two arbitrary records to the remote materialized topic":
		return publishArbitrary(ctx, state, "remote_records")
	case text == "the remote materialized query returns both original bodies exactly once":
		return originalBodies(ctx, state, "remote_records", []string{"not-json", string([]byte{0, 255, 1, 2})})
	case text == "operational SQL in the backend dialect counts the two remote records":
		return sqlCountsTwo(ctx, state, "remote_records", flow.dialect)
	case text == "operational SQL refuses internal and catalog relations of the declared backend":
		for _, relation := range []string{"_ld_watermarks", "ld_watermarks", "information_schema.tables", sqlIndex} {
			if _, err := state.client.Query("remote_records").RawSQL(flow.dialect, "SELECT * FROM "+relation).Fetch(ctx); err == nil {
				return fmt.Errorf("remote SQL served relation %q", relation)
			}
		}
		if _, err := state.client.Query("remote_records").RawSQL(wire.DialectSQLite, "SELECT count(*) FROM remote_records").Fetch(ctx); err == nil {
			return fmt.Errorf("remote SQL accepted SQLite dialect")
		}
		return nil
	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// materializeWholeTwice applies the same setup twice under one operation
// identity, which is what makes the second apply a repeat rather than a
// second materialization.
func materializeWholeTwice(ctx context.Context, state *world, flow *sqlFlow) error {
	topic := state.client.Stream(state.stream).Topic(sqlIndex)
	if err := topic.Ensure(ctx, 1, nil); err != nil {
		return err
	}
	identity := laser.NewOperationID()
	if _, err := topic.Materialize().OperationID(identity).Apply(ctx); err != nil {
		return err
	}
	materialization, err := topic.Materialize().OperationID(identity).Apply(ctx)
	if err != nil {
		return err
	}
	flow.materialization = materialization
	return nil
}

// publishArbitrary publishes bodies that are not JSON, so the whole-record
// materialization is what carries them rather than a typed extraction.
func publishArbitrary(ctx context.Context, state *world, topic string) error {
	for _, body := range [][]byte{[]byte("not-json"), {0, 255, 1, 2}} {
		if _, err := state.client.Stream(state.stream).Topic(topic).
			Publish().Payload(body).Send(ctx); err != nil {
			return err
		}
	}
	return nil
}

func materializationCaughtUp(ctx context.Context, flow *sqlFlow) error {
	if flow.materialization == nil {
		return fmt.Errorf("the scenario set up no materialization")
	}
	wait, err := flow.materialization.Wait(ctx, wire.ReadinessCaughtUp, 20*time.Second)
	if err != nil {
		return err
	}
	if wait == nil || wait.Kind != laser.WaitReady {
		return fmt.Errorf("want the materialization caught up, got %+v", wait)
	}
	return nil
}

// sqlCountsTwo reads the materialized rows back through SQL, which is the
// point: the same authority serves both query shapes.
func sqlCountsTwo(ctx context.Context, state *world, index string, dialect wire.SqlDialect) error {
	result, err := state.client.Query(index).
		RawSQL(dialect, "SELECT count(*) AS n FROM "+index).
		Fetch(ctx)
	if err != nil {
		return err
	}
	if len(result.Rows) != 1 {
		return fmt.Errorf("want one counted row, got %d", len(result.Rows))
	}
	count, err := rowInt(result, &result.Rows[0], "n")
	if err != nil {
		return err
	}
	if count != 2 {
		return fmt.Errorf("want two materialized records counted, got %d", count)
	}
	return nil
}

// sqlRefusals asserts both refusals: an internal relation is not readable at
// all, and a dialect the backend does not serve is refused before it is sent.
func sqlRefusals(ctx context.Context, state *world) error {
	refused := []struct {
		dialect   wire.SqlDialect
		statement string
		why       string
	}{
		{wire.DialectSQLite, "SELECT * FROM _ld_watermarks", "an internal relation"},
		{wire.DialectMySQL, "SELECT * FROM " + sqlIndex, "an unserved dialect"},
	}
	for _, attempt := range refused {
		_, err := state.client.Query(sqlIndex).
			RawSQL(attempt.dialect, attempt.statement).
			Fetch(ctx)
		if err == nil {
			return fmt.Errorf("want %s refused, it answered", attempt.why)
		}
	}
	return nil
}

func originalBodies(ctx context.Context, state *world, index string, want []string) error {
	result, err := state.client.Query(index).WithPayload().Fetch(ctx)
	if err != nil {
		return err
	}
	got := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		found := false
		for i, field := range result.Fields {
			if field.Name == "__laser_original_payload" && i < len(row.Values) && row.Values[i].Kind == wire.TypedBinary {
				got = append(got, string(row.Values[i].Bytes))
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("row missing original binary payload")
		}
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		return fmt.Errorf("expected bodies %q, got %q", want, got)
	}
	return nil
}
