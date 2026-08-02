export interface Clock {
  nowMicros(): bigint
}

export class SystemClock implements Clock {
  nowMicros(): bigint {
    return BigInt(Date.now()) * 1000n
  }
}

export class TestClock implements Clock {
  private current: bigint

  constructor(startMicros: bigint) {
    this.current = startMicros
  }

  advance(byMicros: bigint): void {
    this.current += byMicros
  }

  set(nowMicros: bigint): void {
    this.current = nowMicros
  }

  nowMicros(): bigint {
    return this.current
  }
}
