package laser_test

import (
	"reflect"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAPageWhenItsRowsAreBoundThenShouldReadEveryRowByFieldName(t *testing.T) {
	fields := []wire.LogicalField{
		{ID: 1, Name: "host", Required: true, FieldType: wire.StringType()},
		{ID: 2, Name: "cpu", Required: true, FieldType: wire.LongType()},
	}
	distance := float32(0.25)
	page := []wire.Row{
		{Values: []wire.TypedValue{wire.StringTyped("node-1"), wire.LongTyped(42)}, Score: &distance},
		{Values: []wire.TypedValue{wire.StringTyped("node-2"), wire.LongTyped(-1)}},
	}

	rows := laser.QueryRowsFromPage(fields, page)

	want := []laser.QueryRow{{Fields: fields, Row: page[0]}, {Fields: fields, Row: page[1]}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("got %+v, want %+v", rows, want)
	}
	if cpu, found := rows[0].ValueU64("cpu"); !found || cpu != 42 {
		t.Fatalf("got %d found %v", cpu, found)
	}
	if cpu, found := rows[1].ValueU64("cpu"); found || cpu != 0 {
		t.Fatalf("a negative column read as unsigned: %d found %v", cpu, found)
	}
	if host, found := rows[1].ValueU64("host"); found || host != 0 {
		t.Fatalf("a text column read as unsigned: %d found %v", host, found)
	}
	if rows[0].Score == nil || *rows[0].Score != distance || rows[1].Score != nil {
		t.Fatalf("got scores %v and %v", rows[0].Score, rows[1].Score)
	}
}

func TestGivenAPageWithoutRowsWhenBoundThenShouldAnswerNoRows(t *testing.T) {
	rows := laser.QueryRowsFromPage([]wire.LogicalField{{ID: 1, Name: "host", FieldType: wire.StringType()}}, nil)
	if len(rows) != 0 {
		t.Fatalf("got %+v", rows)
	}
}
