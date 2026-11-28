package laser

import (
	"context"
	"math"
	"math/bits"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
	"weak"

	iggy "github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Each recorder belongs to one producer. The shared observer keeps weak references.
type producerRecorder struct {
	mu       sync.Mutex
	stats    wire.ProducerStatistics
	buckets  [64]uint64
	registry *producerReporter
}

func newProducerRecorder(client *Laser, stream, topic string) *producerRecorder {
	now := uint64(time.Now().UnixMilli())
	record := &producerRecorder{stats: wire.ProducerStatistics{InstanceID: NewConversationID().String(), Stream: stream, Topic: topic,
		FirstActivityMillis: now, LastActivityMillis: now, ConfirmedRecords: new(uint64), ConfirmedPayloadBytes: new(uint64)}}
	config := client.Connection()
	if config == nil || client.Capabilities().Hello != HelloAnswered {
		return record
	}
	interval := telemetrySetting("LASER_PRODUCER_TELEMETRY_INTERVAL_MS", 10000, 60000)
	capacity := telemetrySetting("LASER_PRODUCER_TELEMETRY_MAX_PRODUCERS", 32, 32)
	if interval == 0 || capacity == 0 {
		return record
	}
	shared := client.state()
	shared.mutex.Lock()
	if shared.client == nil {
		shared.mutex.Unlock()
		return record
	}
	if shared.producers == nil {
		shared.producers = &producerReporter{dial: config.TCPOptions, interval: time.Duration(max(interval, 1000)) * time.Millisecond}
	}
	reporter := shared.producers
	shared.mutex.Unlock()
	reporter.add(record, int(capacity))
	return record
}
func telemetrySetting(name string, fallback, limit uint64) uint64 {
	n, err := strconv.ParseUint(os.Getenv(name), 10, 64)
	if err != nil {
		return fallback
	}
	return min(n, limit)
}
func saturatingCounter(n, delta uint64) uint64 { return n + min(delta, math.MaxUint64-n) }
func (r *producerRecorder) begin(records, bytes uint64) time.Time {
	now := time.Now()
	r.mu.Lock()
	r.stats.LastActivityMillis = uint64(now.UnixMilli())
	r.stats.SubmittedRecords = saturatingCounter(r.stats.SubmittedRecords, records)
	r.stats.SubmittedPayloadBytes = saturatingCounter(r.stats.SubmittedPayloadBytes, bytes)
	r.mu.Unlock()
	return now
}
func (r *producerRecorder) finish(start time.Time, records, bytes uint64, success bool) {
	now := time.Now()
	micros := uint64(max(1, now.Sub(start).Microseconds()))
	bucket := min(63, bits.Len64(micros-1))
	r.mu.Lock()
	defer r.mu.Unlock()
	millis := uint64(now.UnixMilli())
	r.stats.LastActivityMillis = millis
	if success {
		if r.stats.LastSuccessMillis == nil {
			r.stats.LastSuccessMillis = new(uint64)
		}
		*r.stats.LastSuccessMillis = millis
		*r.stats.ConfirmedRecords = saturatingCounter(*r.stats.ConfirmedRecords, records)
		*r.stats.ConfirmedPayloadBytes = saturatingCounter(*r.stats.ConfirmedPayloadBytes, bytes)
	} else {
		r.stats.FailedCalls = saturatingCounter(r.stats.FailedCalls, 1)
	}
	r.stats.Latency.Samples = saturatingCounter(r.stats.Latency.Samples, 1)
	r.buckets[bucket] = saturatingCounter(r.buckets[bucket], 1)
}
func (r *producerRecorder) snapshot() wire.ProducerStatistics {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.stats
	out.ConfirmedRecords = ptr(*out.ConfirmedRecords)
	out.ConfirmedPayloadBytes = ptr(*out.ConfirmedPayloadBytes)
	if out.LastSuccessMillis != nil {
		out.LastSuccessMillis = ptr(*out.LastSuccessMillis)
	}
	samples := out.Latency.Samples
	percentile := func(perMille, minimum uint64) *uint64 {
		if samples < minimum {
			return nil
		}
		rank := samples/1000*perMille + (samples%1000*perMille+999)/1000
		for i, count := range r.buckets {
			if count >= rank {
				return ptr(uint64(1) << i)
			}
			rank -= count
		}
		return nil
	}
	out.Latency.P50Micros = percentile(500, 1)
	out.Latency.P99Micros = percentile(990, 100)
	out.Latency.P999Micros = percentile(999, 1000)
	return out
}

func (r *producerRecorder) unregister() {
	if reg := r.registry; reg != nil {
		reg.mu.Lock()
		reg.records = slices.DeleteFunc(reg.records, func(p weak.Pointer[producerRecorder]) bool { v := p.Value(); return v == nil || v == r })
		reg.mu.Unlock()
	}
}

type producerReporter struct {
	mu sync.Mutex
	// dial lowers the connection string on demand, so the reporter holds no credentials a fmt verb can reach.
	dial     func() ([]tcp.Option, error)
	interval time.Duration
	records  []weak.Pointer[producerRecorder]
	closed   bool
	cancel   context.CancelFunc
	done     chan struct{}
}

func (reg *producerReporter) add(record *producerRecorder, capacity int) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.records = slices.DeleteFunc(reg.records, func(p weak.Pointer[producerRecorder]) bool { return p.Value() == nil })
	if reg.closed || len(reg.records) >= capacity {
		return
	}
	record.registry = reg
	reg.records = append(reg.records, weak.Make(record))
	if reg.done != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	reg.cancel = cancel
	reg.done = make(chan struct{})
	go reg.run(ctx, reg.done)
}
func (reg *producerReporter) stop() {
	reg.mu.Lock()
	reg.closed = true
	done := reg.done
	if reg.cancel != nil {
		reg.cancel()
	}
	reg.mu.Unlock()
	if done != nil {
		<-done
	}
}
func (reg *producerReporter) snapshot() []wire.ProducerStatistics {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	var stats []wire.ProducerStatistics
	reg.records = slices.DeleteFunc(reg.records, func(p weak.Pointer[producerRecorder]) bool {
		record := p.Value()
		if record == nil {
			return true
		}
		stats = append(stats, record.snapshot())
		return false
	})
	return stats
}
func (reg *producerReporter) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(reg.interval)
	defer ticker.Stop()
	var observer *Laser
	closeObserver := func() {
		if observer != nil {
			_ = observer.Close()
			observer = nil
		}
	}
	defer closeObserver()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		records := reg.snapshot()
		if len(records) == 0 {
			closeObserver()
			continue
		}
		report := wire.ProducerPresence{ProducerPresenceVersion: 1, ObservedAtMillis: uint64(time.Now().UnixMilli()), ExpiresAfterMillis: uint64(3 * reg.interval.Milliseconds()), Producers: records}
		payload, err := wire.Marshal(report)
		if err != nil || len(payload) > wire.MaxClientMetadata {
			continue
		}
		if observer == nil {
			connect, cancel := context.WithTimeout(ctx, 3*time.Second)
			observer = reg.openObserver(connect)
			cancel()
		}
		if observer != nil {
			send, cancel := context.WithTimeout(ctx, 3*time.Second)
			_, err := observer.sendPresence(send, payload, nil)
			cancel()
			if err != nil {
				closeObserver()
			}
		}
	}
}
func (reg *producerReporter) openObserver(ctx context.Context) *Laser {
	options, err := reg.dial()
	if err != nil {
		return nil
	}
	client, err := iggy.NewIggyClient(iggy.WithTcp(options...))
	if err != nil {
		return nil
	}
	if err := client.Connect(ctx); err != nil {
		_ = client.Close()
		return nil
	}
	caps := NegotiateCapabilities(ctx, client, OpenCapabilities())
	if caps.Hello != HelloAnswered {
		_ = client.Close()
		return nil
	}
	return FromClient(client, Options{Capabilities: &caps})
}
