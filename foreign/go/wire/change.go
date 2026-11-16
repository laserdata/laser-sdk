package wire

// ChangeRecord is one materialized-view advancement on the change feed.
//
// After committing a projector batch for a binding that opted into notify, the
// managed backend publishes one record naming the index, the partition, the
// offset window the batch covered and how many rows landed. A consumer awaits
// the record then queries, instead of sleeping and retrying: change
// notification as records on a topic, never a server-push watch, because the
// substrate is a log.
type ChangeRecord struct {
	V uint32 `cbor:"v"`
	// Index is the materialized index the batch advanced.
	Index string `cbor:"index"`
	// PartitionID is the source partition the batch covered.
	PartitionID uint32 `cbor:"partition_id"`
	// FromOffset is the first source offset in the committed batch.
	FromOffset uint64 `cbor:"from_offset"`
	// ToOffset is the last source offset in the batch, the new watermark.
	ToOffset uint64 `cbor:"to_offset"`
	// Rows is how many rows the batch landed in the view.
	Rows uint32 `cbor:"rows"`
	// Stream is the stream whose source the batch read, when the deployment
	// publishes change records per stream.
	Stream *string `cbor:"stream,omitempty"`
}
