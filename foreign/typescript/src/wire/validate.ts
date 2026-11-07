import { InvalidError } from "../client/errors.js"

const UTF8 = new TextEncoder()
const CONTROL_CHARACTER = /\p{Cc}/u

/**
 * Rust `validate_bounded_text`: 1 to `cap` UTF-8 bytes without a control
 * character.
 * @internal
 */
export function validateBoundedText(label: string, value: string, cap: number): void {
  if (value.length === 0) throw new InvalidError(`${label} must not be empty`)
  const bytes = UTF8.encode(value).byteLength
  if (bytes > cap) {
    throw new InvalidError(`${label} is ${String(bytes)}B, exceeds cap ${String(cap)}B`)
  }
  if (hasControlCharacter(value)) throw new InvalidError(`${label} contains a control character`)
}

/**
 * Rust `validate_stored_text`: no NUL, which no relational text column holds.
 * @internal
 */
export function validateStoredText(label: string, value: string): void {
  if (value.includes("\0")) throw new InvalidError(`${label} contains a NUL character`)
}

/** Whether `value` holds a control character, as Rust `char::is_control`.
 * @internal */
export function hasControlCharacter(value: string): boolean {
  return CONTROL_CHARACTER.test(value)
}

/** Rust `json_depth_within`: whether JSON text nests no deeper than `maxDepth`.
 * @internal */
export function jsonDepthWithin(text: string, maxDepth: number): boolean {
  let depth = 0
  for (let index = 0; index < text.length; index += 1) {
    const character = text[index]
    if (character === '"') {
      for (index += 1; index < text.length && text[index] !== '"'; index += 1)
        if (text[index] === "\\") index += 1
      if (index >= text.length) return true
    } else if (character === "[" || character === "{") {
      depth += 1
      if (depth > maxDepth) return false
    } else if (character === "]" || character === "}") {
      depth = Math.max(0, depth - 1)
    }
  }
  return true
}
