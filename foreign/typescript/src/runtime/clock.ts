import { InvalidError } from "../client/errors.js"

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
    this.current = microseconds(startMicros)
  }

  advance(byMicros: bigint): void {
    this.current = BigInt.asUintN(64, this.current + microseconds(byMicros))
  }

  set(nowMicros: bigint): void {
    this.current = microseconds(nowMicros)
  }

  nowMicros(): bigint {
    return this.current
  }
}

const U64_MAX = 0xffff_ffff_ffff_ffffn

function microseconds(value: bigint): bigint {
  if (value < 0n || value > U64_MAX) throw new InvalidError("clock microseconds must fit u64")
  return value
}

/** Adds two microsecond counts and stops at the unsigned 64-bit ceiling, so a
 * very long TTL or deadline stays encodable. */
export function saturatingAdd(left: bigint, right: bigint): bigint {
  const sum = left + right
  return sum > U64_MAX ? U64_MAX : sum
}
