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

export async function whoami(
  backend: RbacBackend,
  capabilities: Capabilities
): Promise<WhoamiReply> {
  const reply = await executeManaged(backend, capabilities, WhoamiCommand, undefined)
  if (reply.kind === "whoami") return reply.reply
  throw unexpected("whoami", reply)
}

export async function listRoles(
  backend: RbacBackend,
  capabilities: Capabilities,
  options: { readonly namePrefix?: string; readonly search?: string } = {}
): Promise<readonly Role[]> {
  const reply = await executeManaged(backend, capabilities, ListRolesCommand, options)
  if (reply.kind === "roles") return reply.reply.roles
  throw unexpected("listRoles", reply)
}

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

export async function getBindings(
  backend: RbacBackend,
  capabilities: Capabilities,
  userId: number
): Promise<readonly string[]> {
  const reply = await executeManaged(backend, capabilities, GetBindingsCommand, { userId })
  if (reply.kind === "bindings") return reply.reply.roles
  throw unexpected("getBindings", reply)
}

export async function defineRole(
  backend: RbacBackend,
  capabilities: Capabilities,
  role: Role
): Promise<void> {
  validateRoleName(role.name)
  await executeAuthzOk(backend, capabilities, DefineRoleCommand, { role })
}

export async function deleteRole(
  backend: RbacBackend,
  capabilities: Capabilities,
  name: string
): Promise<void> {
  validateRoleName(name)
  await executeAuthzOk(backend, capabilities, DeleteRoleCommand, { name })
}

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
