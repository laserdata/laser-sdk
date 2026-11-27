package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Every graph fixture in the corpus, covering the node and edge shapes with and
// without provenance, both query forms, the upsert, and the reply.
func TestGivenGraphFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("graph_node.bin", func(t *testing.T) {
		node := roundTripFixture[wire.GraphNode](t, "graph_node.bin")
		if len(node.Embedding) != 3 {
			t.Errorf("want a three-element embedding, got %d", len(node.Embedding))
		}
		if len(node.Attrs) != 1 || node.Attrs[0].Key != "value" {
			t.Errorf("want a value attribute, got %+v", node.Attrs)
		}
	})
	t.Run("graph_node_sourced.bin", func(t *testing.T) {
		node := roundTripFixture[wire.GraphNode](t, "graph_node_sourced.bin")
		if node.Source == nil {
			t.Error("the sourced fixture points back at its record")
		}
	})
	t.Run("graph_node_conversation.bin", func(t *testing.T) {
		roundTripFixture[wire.GraphNode](t, "graph_node_conversation.bin")
	})
	t.Run("graph_node_producer.bin", func(t *testing.T) {
		node := roundTripFixture[wire.GraphNode](t, "graph_node_producer.bin")
		if node.Producer == nil {
			t.Error("the producer fixture names its producer")
		}
	})

	t.Run("graph_edge.bin", func(t *testing.T) {
		edge := roundTripFixture[wire.GraphEdge](t, "graph_edge.bin")
		if edge.Weight != 1 {
			t.Errorf("want weight 1, got %v", edge.Weight)
		}
		if edge.ValidFrom == nil || edge.ValidTo == nil {
			t.Fatal("the fixture carries a valid-time window")
		}
		// The window is half-open: inclusive start, exclusive end.
		if !edge.ValidAt(*edge.ValidFrom) {
			t.Error("the window includes its start")
		}
		if edge.ValidAt(*edge.ValidTo) {
			t.Error("the window excludes its end")
		}
		if edge.ValidAt(*edge.ValidFrom - 1) {
			t.Error("an instant before the window is outside it")
		}
	})
	t.Run("graph_edge_sourced.bin", func(t *testing.T) {
		roundTripFixture[wire.GraphEdge](t, "graph_edge_sourced.bin")
	})
	t.Run("graph_edge_conversation.bin", func(t *testing.T) {
		roundTripFixture[wire.GraphEdge](t, "graph_edge_conversation.bin")
	})
	t.Run("graph_edge_producer.bin", func(t *testing.T) {
		roundTripFixture[wire.GraphEdge](t, "graph_edge_producer.bin")
	})

	t.Run("graph_query.bin", func(t *testing.T) {
		query := roundTripFixture[wire.GraphQuery](t, "graph_query.bin")
		if query.Start.Kind != wire.StartMatch {
			t.Fatalf("want a match start, got kind %d", query.Start.Kind)
		}
		if query.Return != wire.ReturnPaths {
			t.Errorf("want a paths return, got %q", query.Return)
		}
		if len(query.Traverse) != 1 || query.Traverse[0].Max != 2 {
			t.Errorf("want one hop of max 2, got %+v", query.Traverse)
		}
	})
	t.Run("graph_query_conversation.bin", func(t *testing.T) {
		query := roundTripFixture[wire.GraphQuery](t, "graph_query_conversation.bin")
		if query.Conversation == nil {
			t.Error("the lens fixture names its conversation")
		}
	})
	t.Run("graph_neighbors.bin", func(t *testing.T) {
		roundTripFixture[wire.GraphNeighbors](t, "graph_neighbors.bin")
	})
	t.Run("graph_neighbors_conversation.bin", func(t *testing.T) {
		roundTripFixture[wire.GraphNeighbors](t, "graph_neighbors_conversation.bin")
	})
	t.Run("graph_upsert.bin", func(t *testing.T) {
		roundTripFixture[wire.GraphUpsert](t, "graph_upsert.bin")
	})
	t.Run("graph_upsert_session.bin", func(t *testing.T) {
		upsert := roundTripFixture[wire.GraphUpsert](t, "graph_upsert_session.bin")
		if upsert.Session == nil {
			t.Error("the session fixture carries a session reference")
		}
	})
	t.Run("graph_reply.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.GraphReply](t, "graph_reply.bin")
		if !reply.IsOk() {
			t.Fatalf("want a successful reply, got %+v", reply)
		}
	})
}

// The ergonomic builders must mint the same content-addressed ids the other
// clients mint, or the same entity would become two nodes.
func TestGivenTheGraphBuildersWhenUsedThenShouldMintContentAddressedIDs(t *testing.T) {
	ada := wire.EntityNode("Person", "ada")
	if ada.ID != wire.ContentNodeID("Person", []byte("ada")) {
		t.Error("an entity node must carry its content-addressed id")
	}
	if len(ada.Labels) != 1 || ada.Labels[0] != "Person" {
		t.Errorf("want the label kept, got %+v", ada.Labels)
	}
	if len(ada.Attrs) != 1 || ada.Attrs[0].Key != "value" || ada.Attrs[0].Value.Str != "ada" {
		t.Errorf("want the value kept as an attribute, got %+v", ada.Attrs)
	}

	acme := wire.EntityNode("Company", "acme")
	edge := wire.RelateNodes(ada, "works_at", acme)
	if edge.ID != wire.ContentEdgeID(ada.ID, "works_at", acme.ID) {
		t.Error("a related edge must carry its content-addressed id")
	}
	if edge.Weight != 1 {
		t.Errorf("want the default weight 1, got %v", edge.Weight)
	}

	// Re-building the same extraction must be a no-op for an idempotent upsert.
	if wire.RelateNodes(ada, "works_at", acme).ID != edge.ID {
		t.Error("re-relating the same nodes must be one edge")
	}
}

// The defaults are omitted on the wire, so a query that asks for them stays
// byte-identical to one written before they existed.
func TestGivenGraphDefaultsWhenEncodedThenShouldStayOffTheWire(t *testing.T) {
	if !wire.EdgeOut.IsOut() || !wire.EdgeDir("").IsOut() {
		t.Error("outgoing is the default direction")
	}
	if wire.EdgeIn.IsOut() || wire.EdgeBoth.IsOut() {
		t.Error("only outgoing is the default")
	}
	if !wire.ReturnNodes.IsNodes() || !wire.GraphReturn("").IsNodes() {
		t.Error("nodes is the default return")
	}
	if !wire.ConsistencyEventual.IsEventual() || !wire.Consistency("").IsEventual() {
		t.Error("eventual is the default consistency")
	}

	// A hop with the default direction encodes only its max.
	encoded, err := wire.Marshal(wire.Hop{Max: 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if encoded[0] != 0xa1 {
		t.Fatalf("want a one-entry map, got head 0x%02x", encoded[0])
	}
}

// Consistency is ordered so a capability check is "wanted is at most served".
func TestGivenConsistencyLevelsWhenComparedThenShouldOrderByStrength(t *testing.T) {
	if wire.ConsistencyEventual.Rank() >= wire.ConsistencyReadYourWrites.Rank() {
		t.Fatal("a stronger level must compare greater")
	}
}

// The filter grammar is shared with the query surface, so the tree must survive
// nesting in both directions.
func TestGivenANestedFilterWhenRoundTrippedThenShouldKeepItsTree(t *testing.T) {
	original := wire.AllOf(
		wire.Pred("label", wire.CmpEq, wire.StringTyped("Person")),
		wire.Negate(wire.AnyOf(
			wire.Pred("age", wire.CmpLt, wire.IntTyped(18)),
			wire.Pred("name", wire.CmpPrefix, wire.StringTyped("tmp-")),
		)),
	)

	encoded, err := wire.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.Filter
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	again, err := wire.Marshal(back)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if string(again) != string(encoded) {
		t.Fatal("a nested filter must re-encode identically")
	}
	if back.Kind != wire.FilterAll || len(back.Children) != 2 {
		t.Fatalf("want a two-child conjunction, got %+v", back)
	}
	if back.Children[1].Kind != wire.FilterNot {
		t.Error("the second child is a negation")
	}
	if back.Children[1].Negated.Children[0].Pred.Op != wire.CmpLt {
		t.Error("the nested predicate operator must survive")
	}
}

// A graph name one tier accepts, every tier accepts.
func TestGivenAGraphNameWhenValidatedThenShouldApplyTheSharedRule(t *testing.T) {
	if err := wire.ValidateGraphName("knowledge"); err != nil {
		t.Errorf("a plain name must be accepted: %v", err)
	}
	if err := wire.ValidateGraphName(""); err == nil {
		t.Error("an empty name must be refused")
	}
	if err := wire.ValidateGraphName("bad\nname"); err == nil {
		t.Error("a control character must be refused")
	}
}
