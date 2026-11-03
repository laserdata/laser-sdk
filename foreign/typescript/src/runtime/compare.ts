/** Orders two strings by code point, which is the byte order of their UTF-8
 * forms and so the order Rust `str::cmp` gives. `localeCompare` and the
 * default UTF-16 order differ from it.
 * @internal */
export function compareCodePoints(left: string, right: string): number {
  const a = left[Symbol.iterator]()
  const b = right[Symbol.iterator]()
  for (;;) {
    const x = a.next()
    const y = b.next()
    if (x.done === true || y.done === true) return Number(x.done !== true) - Number(y.done !== true)
    const delta = (x.value.codePointAt(0) ?? 0) - (y.value.codePointAt(0) ?? 0)
    if (delta !== 0) return delta
  }
}
