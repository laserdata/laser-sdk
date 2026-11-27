package bdd_test

import (
	"context"
	"fmt"
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
//
// The remote-backend scenario is planned, not implemented: it needs a
// deployment that declares a SQL backend, which this runner has no way to
// stand up. It reports as a skip naming that, rather than passing on a
// deployment that has none.
func TestGivenTheSharedOperationalSqlScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeaturePlanned(t, "operational_sql.feature", 3*time.Minute, operationalSQLStep,
		map[string]string{
			"Materialize onto a declared SQL backend and read it in its dialect": "" +
				"needs a deployment that declares a SQL backend, which this " +
				"runner cannot stand up",
		})
}

// sqlIndex is the topic the feature materializes whole records into.
const sqlIndex = "whole_records"

// sqlFlow is the materialization the scenario set up.
type sqlFlow struct {
	materialization *laser.Materialization
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
		return sqlCountsTwo(ctx, state)

	case text == "operational SQL refuses an internal relation and an unserved dialect":
		return sqlRefusals(ctx, state)

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
func sqlCountsTwo(ctx context.Context, state *world) error {
	result, err := state.client.Query(sqlIndex).
		RawSQL(wire.DialectSQLite, "SELECT count(*) AS n FROM "+sqlIndex).
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
