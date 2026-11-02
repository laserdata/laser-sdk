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

/** Whether `value` holds a control character, as Rust `char::is_control`.
 * @internal */
export function hasControlCharacter(value: string): boolean {
  return CONTROL_CHARACTER.test(value)
}
