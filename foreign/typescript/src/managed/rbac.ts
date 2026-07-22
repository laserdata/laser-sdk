import type { Capabilities } from "../client/capabilities.js"
import { AuthzExecutionError, ProtocolError, UnsupportedError } from "../client/errors.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import {
  AuthzHistoryCommand,
  BindRolesCommand,
  DefineRoleCommand,
  DeleteRoleCommand,
  GetBindingsCommand,
  GetRoleCommand,
  ListRolesCommand,
  WhoamiCommand,
  type ManagedCommand
} from "../wire/commands.js"
import {
  type AuthzHistoryReply,
  type AuthzReply,
  type AuthzSubject,
  type Role,
  type WhoamiReply,
  validateRoleName
} from "../wire/authz.js"

export type RbacBackend = ManagedTransport

function unexpected(op: string, reply: AuthzReply): Error {
  if (reply.kind === "err") {
    if (reply.error.kind === "unsupported") return new UnsupportedError(reply.error.message)
    return new AuthzExecutionError(`authz ${op} failed: ${reply.error.kind}`, reply.error)
  }
  if (reply.kind === "unrecognized") {
    return new ProtocolError(`authz ${op}: unrecognized reply variant \`${reply.tag}\``)
  }
  return new ProtocolError(`authz ${op}: unexpected reply variant \`${reply.kind}\``)
}

async function executeAuthzOk<Request>(
  backend: RbacBackend,
  capabilities: Capabilities,
  command: ManagedCommand<Request, AuthzReply>,
  request: Request
): Promise<void> {
  const reply = await executeManaged(backend, capabilities, command, request)
  if (reply.kind === "ok") return
  throw unexpected("write", reply)
}

// The caller's own effective capabilities (bound roles and their flattened
// grants). Always answered to the authenticated caller.
export async function whoami(
  backend: RbacBackend,
  capabilities: Capabilities
): Promise<WhoamiReply> {
  const reply = await executeManaged(backend, capabilities, WhoamiCommand, undefined)
  if (reply.kind === "whoami") return reply.reply
  throw unexpected("whoami", reply)
}

// List defined roles, optionally filtered by name prefix or a free-text
// search.
export async function listRoles(
  backend: RbacBackend,
  capabilities: Capabilities,
  options: { readonly namePrefix?: string; readonly search?: string } = {}
): Promise<readonly Role[]> {
  const reply = await executeManaged(backend, capabilities, ListRolesCommand, options)
  if (reply.kind === "roles") return reply.reply.roles
  throw unexpected("listRoles", reply)
}

// One role by name, or `undefined`. The name must pass `validateRoleName`.
export async function getRole(
  backend: RbacBackend,
  capabilities: Capabilities,
  name: string
): Promise<Role | undefined> {
  validateRoleName(name)
  const reply = await executeManaged(backend, capabilities, GetRoleCommand, { name })
  if (reply.kind === "role") return reply.role
  throw unexpected("getRole", reply)
}

// One user's bound role names.
export async function getBindings(
  backend: RbacBackend,
  capabilities: Capabilities,
  userId: number
): Promise<readonly string[]> {
  const reply = await executeManaged(backend, capabilities, GetBindingsCommand, { userId })
  if (reply.kind === "bindings") return reply.reply.roles
  throw unexpected("getBindings", reply)
}

// Define or replace a role (upsert). Requires `authz:admin`. The role name
// must pass `validateRoleName`.
export async function defineRole(
  backend: RbacBackend,
  capabilities: Capabilities,
  role: Role
): Promise<void> {
  validateRoleName(role.name)
  await executeAuthzOk(backend, capabilities, DefineRoleCommand, { role })
}

// Delete a role by name. Requires `authz:admin`. The name must pass
// `validateRoleName`.
export async function deleteRole(
  backend: RbacBackend,
  capabilities: Capabilities,
  name: string
): Promise<void> {
  validateRoleName(name)
  await executeAuthzOk(backend, capabilities, DeleteRoleCommand, { name })
}

// Bind a user's whole role set (replace). Requires `authz:admin`. Every
// name must pass `validateRoleName`. With `expectRevision` set, applies
// only if the binding's current revision matches (a compare-and-swap over
// the role set).
export async function bindRoles(
  backend: RbacBackend,
  capabilities: Capabilities,
  userId: number,
  roles: readonly string[],
  expectRevision?: bigint
): Promise<void> {
  for (const role of roles) validateRoleName(role)
  await executeAuthzOk(backend, capabilities, BindRolesCommand, {
    userId,
    roles,
    ...(expectRevision !== undefined ? { expectRevision } : {})
  })
}

// Read a page of the authorization change history for `subject`. Requires
// `authz:read`.
export async function authzHistory(
  backend: RbacBackend,
  capabilities: Capabilities,
  subject: AuthzSubject,
  limit: number,
  afterRevision?: bigint
): Promise<AuthzHistoryReply> {
  const reply = await executeManaged(backend, capabilities, AuthzHistoryCommand, {
    subject,
    limit,
    ...(afterRevision !== undefined ? { afterRevision } : {})
  })
  if (reply.kind === "history") return reply.reply
  throw unexpected("authzHistory", reply)
}
