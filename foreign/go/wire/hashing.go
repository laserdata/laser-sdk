package wire

// The one content hash every SDK shares, so a content-addressed id is
// identical across languages, whether it names a deduped memory item or a
// converged graph node.
//
// Pure, dependency-free FNV-1a with no clock or entropy: id generation lives
// SDK-side, while this deterministic hashing belongs to the contract. A port
// reproduces it from the same byte segments, and the golden vector pins the
// result.
const (
	fnvOffset uint64 = 0xcbf2_9ce4_8422_2325
	fnvPrime  uint64 = 0x0000_0100_0000_01b3
)

// fnv1a hashes the concatenated segments under a salt.
func fnv1a(salt uint8, segments [][]byte) uint64 {
	hash := fnvOffset
	hash ^= uint64(salt)
	hash *= fnvPrime
	for _, segment := range segments {
		for _, b := range segment {
			hash ^= uint64(b)
			hash *= fnvPrime
		}
	}
	return hash
}

// ContentID is a deterministic 128-bit content id over segments, whose
// concatenation is the content being addressed.
//
// Two independently salted FNV-1a passes fill the high and low halves, so the
// same segments always yield the same id and different input yields a different
// one. The caller assembles the segments, typically an owner, a discriminator
// byte and a body, and their order is part of the contract.
func ContentID(segments ...[]byte) ID {
	high := fnv1a(0x4d, segments)
	low := fnv1a(0xc7, segments)

	var out ID
	for i := 7; i >= 0; i-- {
		out[i] = byte(high)
		high >>= 8
	}
	for i := 15; i >= 8; i-- {
		out[i] = byte(low)
		low >>= 8
	}
	return out
}

// ContentNodeID is a content-addressed node id: the stable hash of the entity's
// label and canonical value, so the same entity extracted from different
// records, or upserted by different callers in any SDK, converges on one node.
// That convergence is what makes a graph rather than disconnected pairs.
func ContentNodeID(label string, value []byte) NodeID {
	return NodeID{ID: ContentID([]byte(label), []byte{0}, value)}
}

// ContentEdgeID is a content-addressed edge id over its endpoints and type, so
// the same relationship observed any number of times is one edge. Idempotent
// upsert keys off this id.
func ContentEdgeID(from NodeID, edgeType string, to NodeID) EdgeID {
	fromBytes := from.Bytes()
	toBytes := to.Bytes()
	return EdgeID{ID: ContentID(
		fromBytes[:], []byte{0}, []byte(edgeType), []byte{0}, toBytes[:],
	)}
}

// MaterializationOperationID is the operation id of one step of a
// materialization setup, derived from the setup's own identity.
//
// A repeated setup repeats every step under the same identity, so a step is
// never applied twice. The id is never zero, which the contract reserves for
// "no operation".
func MaterializationOperationID(setup U128, step uint8) U128 {
	id := ContentID([]byte("laser.materialization"), littleEndianU128(setup), []byte{step})
	operation := U128(id)
	if operation.IsZero() {
		return U128FromUint64(1)
	}
	return operation
}

// littleEndianU128 renders the setup identity the way the derivation hashes
// it, which is little-endian because that is how the reference implementation
// writes a u128 into the digest.
func littleEndianU128(value U128) []byte {
	out := make([]byte, len(value))
	for i := range value {
		out[i] = value[len(value)-1-i]
	}
	return out
}
