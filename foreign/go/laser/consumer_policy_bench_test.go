package laser

import (
	"context"
	"testing"

	"github.com/apache/iggy/foreign/go/contracts"
)

// BenchmarkBufferedGroupDelivery measures handing an already decoded page to
// the caller. Network polling and durable acknowledgment are outside this loop.
func BenchmarkBufferedGroupDelivery(b *testing.B) {
	const count = 1000
	ctx := context.Background()
	page := &MatchedPage{Records: make([]MatchedRecord, count)}
	for i := range page.Records {
		page.Records[i].Message.Header.Offset = uint64(i)
	}
	partition := &policyPartition{byCheckpoint: make(map[iggcon.Checkpoint]*policyDelivery, count)}
	consumer := policyConsumer{lifetime: ctx, partitions: map[uint32]*policyPartition{0: partition}}
	b.ReportAllocs()
	for b.Loop() {
		if consumer.batch == nil {
			consumer.batch = page
			consumer.index = 0
			clear(partition.byCheckpoint)
			partition.delivered = partition.delivered[:0]
		}
		if _, err := consumer.Next(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
