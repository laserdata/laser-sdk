// A source of the current time in epoch microseconds. The seam an SLA timer
// or deadline check reads, so a test can drive time deterministically
// instead of sleeping. Mirrors the `StateStore`/`Deduplicator` seams: one
// interface, a real implementation, and a test double.
export interface Clock {
  nowMicros(): bigint
}

// The real clock, reading the same epoch-microsecond time the substrate
// stamps.
export class SystemClock implements Clock {
  nowMicros(): bigint {
    return BigInt(Date.now()) * 1000n
  }
}

// A test clock whose time is set and advanced explicitly, so a deadline or
// SLA test fires on demand without sleeping.
export class TestClock implements Clock {
  private current: bigint

  // A test clock starting at `startMicros`.
  constructor(startMicros: bigint) {
    this.current = startMicros
  }

  // Move time forward by `byMicros`.
  advance(byMicros: bigint): void {
    this.current += byMicros
  }

  // Set the absolute time.
  set(nowMicros: bigint): void {
    this.current = nowMicros
  }

  nowMicros(): bigint {
    return this.current
  }
}
