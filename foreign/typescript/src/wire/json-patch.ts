import { InvalidError } from "../client/errors.js"
import { MAX_STATE_DOCUMENT_BYTES, MAX_STATE_PATCH_OPS } from "./limits.js"

export function validatePortableJson(value: unknown, seen = new WeakSet<object>()): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new InvalidError("state value must be JSON")
    if (Number.isInteger(value) && !Number.isSafeInteger(value))
      throw new InvalidError("state JSON integer exceeds the exact number range")
    return
  }
  if (typeof value !== "object") throw new InvalidError("state value must be JSON")
  if (seen.has(value)) throw new InvalidError("state value must be JSON")
  seen.add(value)
  if (Array.isArray(value)) {
    for (const item of value) validatePortableJson(item, seen)
  } else {
    const prototype: unknown = Object.getPrototypeOf(value)
    if (prototype !== Object.prototype && prototype !== null)
      throw new InvalidError("state value must be JSON")
    for (const item of Object.values(value)) validatePortableJson(item, seen)
  }
  seen.delete(value)
}

function jsonBytes(value: unknown): number {
  validatePortableJson(value)
  const json: unknown = JSON.stringify(value)
  if (typeof json !== "string") throw new InvalidError("state value must be JSON")
  return new TextEncoder().encode(json).length
}

// Tokens that address an object's internals rather than its data. A patch is
// peer-supplied, so `/__proto__/...` must never reach a property write: `in`
// resolves it on every object and the assignment would land on
// `Object.prototype`, polluting every object in the process.
const FORBIDDEN_TOKENS = new Set(["__proto__", "constructor", "prototype"])

function decodePointer(path: string): readonly string[] {
  if (path === "") return []
  if (!path.startsWith("/")) throw new InvalidError(`invalid JSON Pointer \`${path}\``)
  return path
    .slice(1)
    .split("/")
    .map((part) => {
      if (/~(?:[^01]|$)/.test(part)) throw new InvalidError(`invalid JSON Pointer \`${path}\``)
      const token = part.replaceAll("~1", "/").replaceAll("~0", "~")
      if (FORBIDDEN_TOKENS.has(token)) {
        throw new InvalidError(`JSON Patch path \`${path}\` addresses a reserved property`)
      }
      return token
    })
}

function arrayIndex(token: string, length: number, allowEnd: boolean): number {
  if (token === "-" && allowEnd) return length
  if (!/^(?:0|[1-9][0-9]*)$/.test(token)) {
    throw new InvalidError(`invalid JSON Patch array index \`${token}\``)
  }
  const index = Number(token)
  const maximum = allowEnd ? length : length - 1
  if (!Number.isSafeInteger(index) || index < 0 || index > maximum) {
    throw new InvalidError(`JSON Patch array index \`${token}\` is out of bounds`)
  }
  return index
}

function objectOf(value: unknown, path: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new InvalidError(`JSON Patch path \`${path}\` does not address a container`)
  }
  return value as Record<string, unknown>
}

function parentAt(document: unknown, tokens: readonly string[], path: string): [unknown, string] {
  if (tokens.length === 0) throw new InvalidError("the document root has no parent")
  let current = document
  for (const token of tokens.slice(0, -1)) {
    if (Array.isArray(current)) {
      current = current[arrayIndex(token, current.length, false)]
    } else {
      const object = objectOf(current, path)
      if (!Object.hasOwn(object, token)) {
        throw new InvalidError(`JSON Patch path \`${path}\` does not exist`)
      }
      current = object[token]
    }
  }
  return [current, tokens.at(-1) ?? ""]
}

function valueAt(document: unknown, path: string): unknown {
  const tokens = decodePointer(path)
  let current = document
  for (const token of tokens) {
    if (Array.isArray(current)) current = current[arrayIndex(token, current.length, false)]
    else {
      const object = objectOf(current, path)
      if (!Object.hasOwn(object, token)) {
        throw new InvalidError(`JSON Patch path \`${path}\` does not exist`)
      }
      current = object[token]
    }
  }
  return current
}

function addValue(document: unknown, path: string, value: unknown): unknown {
  const tokens = decodePointer(path)
  if (tokens.length === 0) return value
  const [parent, token] = parentAt(document, tokens, path)
  if (Array.isArray(parent)) parent.splice(arrayIndex(token, parent.length, true), 0, value)
  else objectOf(parent, path)[token] = value
  return document
}

function removeValue(
  document: unknown,
  path: string
): { readonly document: unknown; readonly value: unknown } {
  const tokens = decodePointer(path)
  if (tokens.length === 0) return { document: null, value: document }
  const [parent, token] = parentAt(document, tokens, path)
  if (Array.isArray(parent)) {
    const index = arrayIndex(token, parent.length, false)
    const value: unknown = parent[index]
    parent.splice(index, 1)
    return { document, value }
  }
  const object = objectOf(parent, path)
  if (!Object.hasOwn(object, token)) {
    throw new InvalidError(`JSON Patch path \`${path}\` does not exist`)
  }
  const value = object[token]
  Reflect.deleteProperty(object, token)
  return { document, value }
}

function equalJson(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true
  if (Array.isArray(left) && Array.isArray(right)) {
    return (
      left.length === right.length && left.every((value, index) => equalJson(value, right[index]))
    )
  }
  if (
    typeof left === "object" &&
    left !== null &&
    !Array.isArray(left) &&
    typeof right === "object" &&
    right !== null &&
    !Array.isArray(right)
  ) {
    const a = left as Readonly<Record<string, unknown>>
    const b = right as Readonly<Record<string, unknown>>
    const keys = Object.keys(a)
    return (
      keys.length === Object.keys(b).length &&
      keys.every((key) => Object.hasOwn(b, key) && equalJson(a[key], b[key]))
    )
  }
  return false
}

function patchObject(value: unknown): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new InvalidError("each JSON Patch operation must be an object")
  }
  return value as Readonly<Record<string, unknown>>
}

export function applyJsonPatch(document: unknown, patch: unknown): unknown {
  if (!Array.isArray(patch)) throw new InvalidError("a JSON Patch document must be an array")
  if (patch.length > MAX_STATE_PATCH_OPS)
    throw new InvalidError("state patch has too many operations")
  if (jsonBytes(patch) > MAX_STATE_DOCUMENT_BYTES)
    throw new InvalidError("state patch exceeds the document byte cap")
  jsonBytes(document)
  let result = structuredClone(document)
  for (const raw of patch) {
    const operation = patchObject(raw)
    const op = operation["op"]
    const path = operation["path"]
    if (typeof op !== "string" || typeof path !== "string") {
      throw new InvalidError("a JSON Patch operation requires string `op` and `path`")
    }
    switch (op) {
      case "add":
        if (!("value" in operation)) throw new InvalidError("JSON Patch add requires `value`")
        result = addValue(result, path, structuredClone(operation["value"]))
        break
      case "remove":
        result = removeValue(result, path).document
        break
      case "replace":
        if (!("value" in operation)) throw new InvalidError("JSON Patch replace requires `value`")
        valueAt(result, path)
        result = removeValue(result, path).document
        result = addValue(result, path, structuredClone(operation["value"]))
        break
      case "move": {
        const from = operation["from"]
        if (typeof from !== "string") throw new InvalidError("JSON Patch move requires `from`")
        if (path.startsWith(`${from}/`))
          throw new InvalidError("JSON Patch cannot move a value into its child")
        const removed = removeValue(result, from)
        result = addValue(removed.document, path, removed.value)
        break
      }
      case "copy": {
        const from = operation["from"]
        if (typeof from !== "string") throw new InvalidError("JSON Patch copy requires `from`")
        result = addValue(result, path, structuredClone(valueAt(result, from)))
        break
      }
      case "test":
        if (!("value" in operation)) throw new InvalidError("JSON Patch test requires `value`")
        if (!equalJson(valueAt(result, path), operation["value"])) {
          throw new InvalidError(`JSON Patch test failed at \`${path}\``)
        }
        break
      default:
        throw new InvalidError(`unknown JSON Patch operation \`${op}\``)
    }
  }
  if (jsonBytes(result) > MAX_STATE_DOCUMENT_BYTES)
    throw new InvalidError("state document exceeds its byte cap")
  return result
}
