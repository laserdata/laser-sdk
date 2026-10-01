import type { LaserTransport } from "../iggy/apache-iggy.js"
import type { ManagedCommand } from "../wire/commands.js"
import { FilterExecutionError, ProtocolError } from "./errors.js"
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
    throw new ProtocolError(
      `${command.version.surface} wire version mismatch: expected ${String(command.version.expected)}, got ${String(got)}`,
      { commandCode: command.code }
    )
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
  return command.decode(reply)
}
