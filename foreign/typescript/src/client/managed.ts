import type { LaserTransport } from "../iggy/apache-iggy.js"
import type { ManagedCommand } from "../wire/commands.js"
import { ProtocolError } from "./errors.js"
import { type Capabilities, requireCapability } from "./capabilities.js"

export type ManagedTransport = Pick<LaserTransport, "sendManaged">

function requireVersion<Request, Reply>(
  capabilities: Capabilities,
  command: ManagedCommand<Request, Reply>
): void {
  if (command.version === undefined || capabilities.versions === undefined) return
  const got = capabilities.versions[command.version.surface]
  if (got !== command.version.expected) {
    throw new ProtocolError(
      `${command.version.surface} wire version mismatch: expected ${String(command.version.expected)}, got ${String(got)}`,
      { commandCode: command.code }
    )
  }
}

export async function executeManaged<Request, Reply>(
  transport: ManagedTransport,
  capabilities: Capabilities,
  command: ManagedCommand<Request, Reply>,
  request: Request
): Promise<Reply> {
  requireCapability(capabilities, command.surface)
  requireVersion(capabilities, command)
  command.validate?.(request)
  const payload = command.encode(request)
  const reply = await transport.sendManaged(command.code, payload)
  return command.decode(reply)
}
