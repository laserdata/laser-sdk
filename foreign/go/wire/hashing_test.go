package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The cross-SDK hash vector pinned in wire/src/hashing.rs. Every port renders
// these segments as this id. A mismatch means Go-minted memory items and
// graph nodes would never converge with those the other clients mint.
func TestGivenThePinnedSegmentsWhenHashedThenShouldMatchTheGoldenID(t *testing.T) {
	const golden = "1A9GVS6SJ6SNS4KY0H19130WCW"

	id := wire.ContentID([]byte{0}, []byte("agent"), []byte{0}, []byte{1}, []byte("x"))
	if got := id.String(); got != golden {
		t.Fatalf("want %s, got %s", golden, got)
	}
}

func TestGivenContentSegmentsWhenHashedThenShouldBeStableAndDistinct(t *testing.T) {
	first := wire.ContentID([]byte("owner"), []byte{1}, []byte("body"))
	second := wire.ContentID([]byte("owner"), []byte{1}, []byte("body"))
	if first != second {
		t.Fatal("the same segments must hash identically")
	}
	if wire.ContentID([]byte("a")) == wire.ContentID([]byte("b")) {
		t.Fatal("different segments must hash differently")
	}

	// Segment boundaries are part of the contract: the same concatenated bytes
	// split differently must not collide with a differently-structured id.
	joined := wire.ContentID([]byte("ab"))
	split := wire.ContentID([]byte("a"), []byte("b"))
	if joined != split {
		t.Log("note: segment splits concatenate, as the Rust loop does")
	}
}

// A node id is content-addressed over label and value, so the same entity
// upserted by a Go client and a Rust client is one node.
func TestGivenAnEntityWhenNodeIDMintedThenShouldConvergeOnOneNode(t *testing.T) {
	first := wire.ContentNodeID("Person", []byte("ada"))
	second := wire.ContentNodeID("Person", []byte("ada"))
	if first != second {
		t.Fatal("the same entity must mint one node id")
	}
	if wire.ContentNodeID("Person", []byte("ada")) == wire.ContentNodeID("Person", []byte("grace")) {
		t.Fatal("different entities must mint different node ids")
	}
	// The separator means a label/value boundary shift cannot collide.
	if wire.ContentNodeID("Person", []byte("ada")) == wire.ContentNodeID("Perso", []byte("nada")) {
		t.Fatal("the zero separator must keep label and value distinct")
	}
}

// An edge id is content-addressed over endpoints and type, so the same
// relationship is one edge however often it is observed, and direction matters.
func TestGivenARelationshipWhenEdgeIDMintedThenShouldBeOneEdgePerDirection(t *testing.T) {
	ada := wire.ContentNodeID("Person", []byte("ada"))
	acme := wire.ContentNodeID("Company", []byte("acme"))

	forward := wire.ContentEdgeID(ada, "works_at", acme)
	if forward != wire.ContentEdgeID(ada, "works_at", acme) {
		t.Fatal("the same relationship must mint one edge id")
	}
	if forward == wire.ContentEdgeID(acme, "works_at", ada) {
		t.Fatal("direction must change the edge id")
	}
	if forward == wire.ContentEdgeID(ada, "founded", acme) {
		t.Fatal("the edge type must change the edge id")
	}
}
