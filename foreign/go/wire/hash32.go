package wire

import "encoding/binary"

// The XXH32 constants, from the algorithm's own specification.
const (
	xxh32Prime1 uint32 = 2654435761
	xxh32Prime2 uint32 = 2246822519
	xxh32Prime3 uint32 = 3266489917
	xxh32Prime4 uint32 = 668265263
	xxh32Prime5 uint32 = 374761393
)

// XXHash32 is the 32-bit xxHash of some bytes under a seed.
//
// It is written here rather than taken as a dependency for the same reason
// the Avro decoder and the regular expression engine are: the streaming
// server routes a message key to a partition by this exact hash, so a client
// that computed a different number would write a key to one partition and
// look for it on another. The Go ecosystem's xxHash packages are the 64-bit
// and XXH3 variants, neither of which is this one.
func XXHash32(data []byte, seed uint32) uint32 {
	var hash uint32
	remaining := data

	if len(data) >= 16 {
		// Four accumulators consume sixteen bytes at a time, then fold
		// together. This is what makes the hash fast on long inputs, and the
		// fold order is part of the algorithm rather than an implementation
		// choice.
		first := seed + xxh32Prime1 + xxh32Prime2
		second := seed + xxh32Prime2
		third := seed
		fourth := seed - xxh32Prime1
		for len(remaining) >= 16 {
			first = xxh32Round(first, binary.LittleEndian.Uint32(remaining[0:4]))
			second = xxh32Round(second, binary.LittleEndian.Uint32(remaining[4:8]))
			third = xxh32Round(third, binary.LittleEndian.Uint32(remaining[8:12]))
			fourth = xxh32Round(fourth, binary.LittleEndian.Uint32(remaining[12:16]))
			remaining = remaining[16:]
		}
		hash = rotateLeft32(first, 1) + rotateLeft32(second, 7) +
			rotateLeft32(third, 12) + rotateLeft32(fourth, 18)
	} else {
		hash = seed + xxh32Prime5
	}

	// The total length is mixed in, so two inputs that differ only in length
	// cannot collide through the tail alone.
	hash += uint32(len(data))

	for len(remaining) >= 4 {
		hash += binary.LittleEndian.Uint32(remaining[0:4]) * xxh32Prime3
		hash = rotateLeft32(hash, 17) * xxh32Prime4
		remaining = remaining[4:]
	}
	for _, byteValue := range remaining {
		hash += uint32(byteValue) * xxh32Prime5
		hash = rotateLeft32(hash, 11) * xxh32Prime1
	}

	// The avalanche, which spreads every input bit across the whole output.
	hash ^= hash >> 15
	hash *= xxh32Prime2
	hash ^= hash >> 13
	hash *= xxh32Prime3
	hash ^= hash >> 16
	return hash
}

func xxh32Round(accumulator, input uint32) uint32 {
	accumulator += input * xxh32Prime2
	accumulator = rotateLeft32(accumulator, 13)
	return accumulator * xxh32Prime1
}

func rotateLeft32(value uint32, bits uint) uint32 {
	return value<<bits | value>>(32-bits)
}

// PartitionForKey is the partition the streaming server routes a message key
// to.
//
// A client that computed this differently would write a keyed record to one
// partition and look for it on another, which reads as data that silently
// vanished rather than as an error anybody sees.
func PartitionForKey(key []byte, partitions uint32) uint32 {
	if partitions == 0 {
		return 0
	}
	return XXHash32(key, 0) % partitions
}
