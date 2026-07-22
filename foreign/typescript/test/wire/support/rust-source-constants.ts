import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"

function resolveFactor(factor: string, values: ReadonlyMap<string, number>): number | undefined {
  const trimmed = factor.trim()
  if (/^\d[\d_]*$/.test(trimmed)) {
    return Number(trimmed.replaceAll("_", ""))
  }
  return values.get(trimmed)
}

function evaluateNumericExpr(
  expr: string,
  values: ReadonlyMap<string, number>
): number | undefined {
  const additiveTerms = expr.split("+")
  let sum = 0
  for (const term of additiveTerms) {
    const factors = term.split("*")
    let product = 1
    for (const factor of factors) {
      const resolved = resolveFactor(factor, values)
      if (resolved === undefined) return undefined
      product *= resolved
    }
    sum += product
  }
  return sum
}

// Parses `pub const NAME: u8/u32/u64/usize = EXPR;` declarations from a Rust
// source file, evaluating literal integers, `+`, and `*`. Rust constants may
// reference a sibling defined later in the same file, so this resolves by
// fixpoint: repeat passes over the unresolved set until a pass makes no
// progress, rather than assuming source order is also dependency order.
export async function parseRustNumericConstants(
  path: string
): Promise<ReadonlyMap<string, number>> {
  const source = await readFile(path, "utf8")
  const pending = new Map<string, string>()
  for (const match of source.matchAll(
    /pub const ([A-Z][A-Z0-9_]*): (?:u8|u16|u32|u64|usize) = ([^;]+);/g
  )) {
    const name = match[1]
    const expr = match[2]?.trim()
    if (name === undefined || expr === undefined) continue
    pending.set(name, expr)
  }

  const values = new Map<string, number>()
  let progressed = true
  while (pending.size > 0 && progressed) {
    progressed = false
    for (const [name, expr] of pending) {
      const resolved = evaluateNumericExpr(expr, values)
      if (resolved === undefined) continue
      values.set(name, resolved)
      pending.delete(name)
      progressed = true
    }
  }
  assert.equal(
    pending.size,
    0,
    `unresolved constants in ${path}: ${[...pending.keys()].join(", ")}`
  )
  return values
}

export async function parseRustStringConstants(path: string): Promise<ReadonlyMap<string, string>> {
  const source = await readFile(path, "utf8")
  const values = new Map<string, string>()
  for (const match of source.matchAll(/pub const ([A-Z][A-Z0-9_]*): &str = "([^"]*)";/g)) {
    const name = match[1]
    const value = match[2]
    if (name === undefined || value === undefined) continue
    values.set(name, value)
  }
  return values
}
