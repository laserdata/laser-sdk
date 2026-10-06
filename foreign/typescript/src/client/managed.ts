import type { LaserTransport } from "../iggy/apache-iggy.js"
import { decodeOne } from "../wire/cbor.js"
import type { ManagedCommand } from "../wire/commands.js"
import { ResultCodeName } from "../wire/result.js"
import {
  CheckpointExecutionError,
  FilterExecutionError,
  ForkExecutionError,
  GraphExecutionError,
  InvalidError,
  KvExecutionError,
  type LaserError,
  ProtocolError,
  QueryExecutionError,
  UnsupportedError
} from "./errors.js"
import { type Capabilities, requireCapability } from "./capabilities.js"

export type ManagedTransport = Pick<LaserTransport, "sendManaged">

function requireVersion<Request, Reply>(
  capabilities: Capabilities,
  command: ManagedCommand<Request, Reply>
): void {
  if (command.version === undefined || capabilities.versions === undefined) return
  const got = capabilities.versions[command.version.surface]
  if (got !== command.version.expected && command.version.surface === "filter") {
    // A catalog of another filter op version is a typed version gap, as the
    // Rust SDK and the server report it.
    const message = `filter catalog version ${String(got)} is not supported, expected ${String(command.version.expected)}`
    throw new FilterExecutionError(`version_skew: ${message}`, {
      code: { kind: "known", name: "VersionSkew" },
      reason: "version_skew",
      message
    })
  }
  if (got !== command.version.expected) {
    const message = `${command.version.surface} wire version mismatch: expected ${String(got)}, got ${String(command.version.expected)}`
    const detail = { kind: "version" as const, expected: got ?? 0, got: command.version.expected }
    switch (command.version.surface) {
      case "query":
        throw new QueryExecutionError(message, detail)
      case "kv":
        throw new KvExecutionError(message, detail)
      case "fork":
        throw new ForkExecutionError(message, detail)
      case "graph":
        throw new GraphExecutionError(message, detail)
      case "checkpoint":
        throw new CheckpointExecutionError(message, detail)
      case "filter":
      case "control":
        throw new ProtocolError(message, { commandCode: command.code })
    }
  }
}

export function requireManagedCommand<Request, Reply>(
  capabilities: Capabilities,
  command: ManagedCommand<Request, Reply>
): void {
  requireCapability(capabilities, command.surface)
  requireVersion(capabilities, command)
}

export async function executeManaged<Request, Reply>(
  transport: ManagedTransport,
  capabilities: Capabilities,
  command: ManagedCommand<Request, Reply>,
  request: Request,
  options?: { readonly retryAfterReconnect?: boolean }
): Promise<Reply> {
  requireManagedCommand(capabilities, command)
  command.validate?.(request)
  const payload = command.encode(request)
  const reply = await transport.sendManaged(command.code, payload, options)
  return decodeManagedReply((bytes) => command.decode(bytes), reply)
}

const RESULT_CODE_BY_WIRE_NAME: ReadonlyMap<string, keyof typeof ResultCodeName> = new Map(
  (Object.keys(ResultCodeName) as (keyof typeof ResultCodeName)[]).map((name) => [
    name.replace(/(?<!^)([A-Z])/g, "_$1").toLowerCase(),
    name
  ])
)

/** Decodes a typed managed reply, or the surface-agnostic command error a
 * server answers for a code it does not handle. */
export function decodeManagedReply<Reply>(
  decode: (reply: Uint8Array) => Reply,
  reply: Uint8Array
): Reply {
  try {
    return decode(reply)
  } catch (error) {
    throw commandErrorOf(reply) ?? error
  }
}

function commandErrorOf(reply: Uint8Array): LaserError | undefined {
  let value: unknown
  try {
    value = decodeOne(reply, "CommandError")
  } catch {
    return undefined
  }
  if (!(value instanceof Map)) return undefined
  const wireCode: unknown = value.get("code")
  const message: unknown = value.get("message")
  if (typeof message !== "string") return undefined
  // A code from a newer peer arrives as `{ unrecognized: n }`.
  const unrecognized: unknown =
    wireCode instanceof Map && wireCode.size === 1 ? wireCode.get("unrecognized") : undefined
  if (
    typeof unrecognized === "number" &&
    Number.isInteger(unrecognized) &&
    unrecognized >= 0 &&
    unrecognized <= 0xffff
  ) {
    return new ProtocolError(`Unrecognized(${String(unrecognized)}): ${message}`, {
      resultCode: unrecognized
    })
  }
  if (typeof wireCode !== "string") return undefined
  const name = RESULT_CODE_BY_WIRE_NAME.get(wireCode)
  if (name === undefined) return undefined
  if (name === "Unsupported") return new UnsupportedError(message, { surface: "managed" })
  if (name === "InvalidArgument" || name === "VersionSkew") return new InvalidError(message)
  return new ProtocolError(`${name}: ${message}`, { resultCode: ResultCodeName[name] })
}
