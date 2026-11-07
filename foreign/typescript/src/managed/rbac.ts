import { randomUUID } from "node:crypto"
import type { CallOptions } from "../client/call-options.js"
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
  type BindingsReply,
  type AuthzSubject,
  type Role,
  type WhoamiReply,
  validateMutationId,
  validateRole,
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
  request: Request,
  options: CallOptions
): Promise<void> {
  const reply = await executeManaged(backend, capabilities, command, request, options)
  if (reply.kind === "ok") return
  throw unexpected("write", reply)
}

export async function whoami(
  backend: RbacBackend,
  capabilities: Capabilities,
  options: CallOptions = {}
): Promise<WhoamiReply> {
  const reply = await executeManaged(backend, capabilities, WhoamiCommand, undefined, options)
  if (reply.kind === "whoami") return reply.reply
  throw unexpected("whoami", reply)
}

export async function listRoles(
  backend: RbacBackend,
  capabilities: Capabilities,
  options: { readonly namePrefix?: string; readonly search?: string } & CallOptions = {}
): Promise<readonly Role[]> {
  const { signal, ...filter } = options
  const reply = await executeManaged(
    backend,
    capabilities,
    ListRolesCommand,
    filter,
    signal === undefined ? {} : { signal }
  )
  if (reply.kind === "roles") return reply.reply.roles
  throw unexpected("listRoles", reply)
}

export async function getRole(
  backend: RbacBackend,
  capabilities: Capabilities,
  name: string,
  options: CallOptions = {}
): Promise<Role | undefined> {
  validateRoleName(name)
  const reply = await executeManaged(backend, capabilities, GetRoleCommand, { name }, options)
  if (reply.kind === "role") return reply.role
  throw unexpected("getRole", reply)
}

export async function getBindings(
  backend: RbacBackend,
  capabilities: Capabilities,
  userId: number,
  options: CallOptions = {}
): Promise<BindingsReply> {
  const reply = await executeManaged(backend, capabilities, GetBindingsCommand, { userId }, options)
  if (reply.kind === "bindings") return reply.reply
  throw unexpected("getBindings", reply)
}

export async function defineRole(
  backend: RbacBackend,
  capabilities: Capabilities,
  role: Role,
  mutationId: string = randomUUID(),
  options: CallOptions = {}
): Promise<void> {
  validateRole(role)
  validateMutationId(mutationId)
  await executeAuthzOk(backend, capabilities, DefineRoleCommand, { role, mutationId }, options)
}

export async function deleteRole(
  backend: RbacBackend,
  capabilities: Capabilities,
  name: string,
  mutationId: string = randomUUID(),
  options: CallOptions = {}
): Promise<void> {
  validateRoleName(name)
  validateMutationId(mutationId)
  await executeAuthzOk(backend, capabilities, DeleteRoleCommand, { name, mutationId }, options)
}

export async function bindRoles(
  backend: RbacBackend,
  capabilities: Capabilities,
  userId: number,
  roles: readonly string[],
  expectRevision?: bigint,
  mutationId: string = randomUUID(),
  options: CallOptions = {}
): Promise<BindingsReply> {
  for (const role of roles) validateRoleName(role)
  validateMutationId(mutationId)
  const reply = await executeManaged(
    backend,
    capabilities,
    BindRolesCommand,
    {
      userId,
      roles,
      ...(expectRevision !== undefined ? { expectRevision } : {}),
      mutationId
    },
    options
  )
  if (reply.kind === "bindings") return reply.reply
  throw unexpected("bindRoles", reply)
}

export async function authzHistory(
  backend: RbacBackend,
  capabilities: Capabilities,
  subject: AuthzSubject,
  limit: number,
  afterRevision?: bigint,
  options: CallOptions = {}
): Promise<AuthzHistoryReply> {
  const reply = await executeManaged(
    backend,
    capabilities,
    AuthzHistoryCommand,
    {
      subject,
      limit,
      ...(afterRevision !== undefined ? { afterRevision } : {})
    },
    options
  )
  if (reply.kind === "history") return reply.reply
  throw unexpected("authzHistory", reply)
}
