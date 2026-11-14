import { type CallOptions, abortable, throwIfAborted } from "./call-options.js"
import { InvalidError, UnsupportedError } from "./errors.js"
import { millis, millisToMicros } from "./duration.js"
import type { LaserTransport } from "../iggy/apache-iggy.js"
import {
  type AdminClient,
  abandonOperation,
  changePassword,
  createToken,
  createUser,
  deleteToken,
  deleteUser,
  pingServer,
  readClient,
  readClients,
  readMe,
  readStats,
  readTokens,
  readCluster,
  metadataChangesRequest,
  optionsScopeCode,
  readMetadataChanges,
  readOptions,
  readSnapshot,
  readUser,
  readUsers,
  resolveOperation,
  sendCommand,
  updatePermissions,
  updateUser
} from "../iggy/server-admin.js"

/** Whether a user can sign in. */
export type UserStatus = "active" | "inactive"

/** Grants that apply to the whole server. A flag left out is `false`. */
export interface GlobalPermissions {
  readonly manageServers?: boolean
  readonly readServers?: boolean
  readonly manageUsers?: boolean
  readonly readUsers?: boolean
  readonly manageStreams?: boolean
  readonly readStreams?: boolean
  readonly manageTopics?: boolean
  readonly readTopics?: boolean
  readonly pollMessages?: boolean
  readonly sendMessages?: boolean
}

/** Grants on one topic. A flag left out is `false`. */
export interface TopicPermissions {
  readonly manageTopic?: boolean
  readonly readTopic?: boolean
  readonly pollMessages?: boolean
  readonly sendMessages?: boolean
}

/** Grants on one stream. `topics` adds grants on single topics of the stream, keyed by the native topic id. */
export interface StreamPermissions {
  readonly manageStream?: boolean
  readonly readStream?: boolean
  readonly manageTopics?: boolean
  readonly readTopics?: boolean
  readonly pollMessages?: boolean
  readonly sendMessages?: boolean
  readonly topics?: ReadonlyMap<number, TopicPermissions>
}

/** What a user may do. `global` applies to the whole server, and `streams` adds grants on single streams, keyed by the native stream id. */
export interface Permissions {
  readonly global?: GlobalPermissions
  readonly streams?: ReadonlyMap<number, StreamPermissions>
}

/** One user as the server reports it. `permissions` is absent in a listing and for a user without grants. */
export interface UserInfo {
  /** The native numeric user id. */
  readonly id: number
  readonly username: string
  readonly status: UserStatus
  readonly createdAtMicros: bigint
  readonly permissions?: Permissions
}

/** The settings of a new user. A user starts active without grants. */
export interface CreateUserOptions {
  readonly status?: UserStatus
  readonly permissions?: Permissions
}

/** The changes `User.update` applies. A field left out keeps its value. */
export interface UpdateUserOptions {
  readonly username?: string
  readonly status?: UserStatus
}

/** One personal access token of the signed-in user. */
export interface PersonalAccessTokenInfo {
  readonly name: string
  /** When the token stops working. Absent for a token that never expires. */
  readonly expiresAtMicros?: bigint
}

/** One consumer group a connected client is a member of, by native ids. */
export interface ClientConsumerGroup {
  readonly streamId: number
  readonly topicId: number
  readonly groupId: number
}

/** One connected client as the server reports it. `consumerGroups` is empty in a listing. */
export interface ClientInfo {
  readonly clientId: number
  /** The signed-in user. Absent before the client signs in. */
  readonly userId?: number
  readonly address: string
  readonly transport: string
  readonly consumerGroupsCount: number
  readonly consumerGroups: readonly ClientConsumerGroup[]
}

/** The cache use of one partition, by native ids. */
export interface CacheMetrics {
  readonly streamId: number
  readonly topicId: number
  readonly partitionId: number
  readonly hits: bigint
  readonly misses: bigint
  readonly hitRatio: number
}

/** The server statistics. Sizes are in bytes and times in microseconds. */
export interface ServerStats {
  readonly processId: number
  readonly cpuUsage: number
  readonly totalCpuUsage: number
  readonly memoryUsageBytes: bigint
  readonly totalMemoryBytes: bigint
  readonly availableMemoryBytes: bigint
  readonly runTimeMicros: bigint
  readonly startTimeMicros: bigint
  readonly readBytes: bigint
  readonly writtenBytes: bigint
  readonly messagesSizeBytes: bigint
  readonly streamsCount: number
  readonly topicsCount: number
  readonly partitionsCount: number
  readonly segmentsCount: number
  readonly messagesCount: bigint
  readonly clientsCount: number
  readonly consumerGroupsCount: number
  readonly hostname: string
  readonly osName: string
  readonly osVersion: string
  readonly kernelVersion: string
  readonly serverVersion: string
  readonly threadsCount: number
  readonly freeDiskSpaceBytes: bigint
  readonly totalDiskSpaceBytes: bigint
  readonly openFilesCount: bigint
  readonly openFilesLimit: bigint
  readonly cacheMetrics: readonly CacheMetrics[]
}

/** The role of one cluster node. */
export type ClusterNodeRole = "leader" | "follower"

/** The state of one cluster node. */
export type ClusterNodeStatus =
  "healthy" | "starting" | "stopping" | "unreachable" | "maintenance" | "unknown"

/** One node of the cluster. A port is 0 when the node does not serve that transport. */
export interface ClusterNode {
  readonly replicaId: number
  readonly name: string
  readonly ip: string
  readonly tcpPort: number
  readonly quicPort: number
  readonly httpPort: number
  readonly websocketPort: number
  readonly role: ClusterNodeRole
  readonly status: ClusterNodeStatus
}

/** The cluster as the server reports it. A single server reports one node. */
export interface ClusterInfo {
  readonly name: string
  readonly nodes: readonly ClusterNode[]
}

/** The resource an option catalog describes. */
export type OptionsScope = "topic" | "stream" | "user"

/** One option the server accepts for a scope. `kind` names the value type,
 * such as `uint64`, `bool`, or `string`. `defaultValue` is the default in its
 * text form, absent when the option has no default. Integers are decimal,
 * floats are the `0x` hex of their bits, and raw bytes are base64. */
export interface OptionInfo {
  readonly key: string
  readonly kind: string
  /** An update may set the option. */
  readonly updatable: boolean
  /** An update may reset the option to its default. */
  readonly resettable: boolean
  /** The server accepts the option and never returns it. */
  readonly secret: boolean
  /** The server computes the option and never accepts it. */
  readonly readOnly: boolean
  /** A node that does not know the option must not serve the resource. */
  readonly critical: boolean
  readonly defaultValue?: string
  readonly description: string
}

/** A family of metadata change records, and the unit a query filters on. */
export type MetadataChangeGroup =
  | "streams"
  | "topics"
  | "partitions"
  | "consumer_groups"
  | "consumer_group_members"
  | "users"
  | "personal_access_tokens"
  | "sessions"

/** What one metadata change record says happened. */
export type MetadataChangeKind =
  | "stream_created"
  | "stream_updated"
  | "stream_deleted"
  | "topic_created"
  | "topic_updated"
  | "topic_deleted"
  | "partitions_changed"
  | "consumer_group_created"
  | "consumer_group_deleted"
  | "consumer_group_member_joined"
  | "consumer_group_member_left"
  | "consumer_group_partition_handed_over"
  | "user_created"
  | "user_updated"
  | "user_deleted"
  | "user_password_changed"
  | "user_permissions_updated"
  | "personal_access_token_created"
  | "personal_access_token_deleted"
  | "session_registered"
  | "session_closed"

/** The outcome of a metadata change query. `ok` returns the records or items
 * after the start. `behind` means the answering node has not applied the
 * start yet, so ask again. `expired` means the node no longer holds every
 * record after the start, and `reset` means the start belongs to another
 * history. After `expired` or `reset`, read the current state again. */
export type MetadataChangeStatus = "ok" | "behind" | "expired" | "reset"

/** Whether a page holds change records or current state items. */
export type MetadataChangePhase = "changes" | "current"

/** Why a member left, a token was deleted, a session closed, or a user's permissions changed. */
export type MetadataChangeReason =
  | "left"
  | "session_ended"
  | "deleted"
  | "expired"
  | "logout"
  | "revoked"
  | "updated"
  | "resource_deleted"

/** Where a metadata change query starts. `earliest` is the oldest record the
 * requested groups still hold completely, and `latest` returns no records,
 * only a cursor at the newest record. `after` takes the `next` cursor of an
 * earlier page. `current` pages through the current state, starting a scan
 * without a `pageToken` and continuing it with the `pageToken` of the page
 * before. */
export type MetadataChangeStart =
  | { readonly mode: "earliest" }
  | { readonly mode: "latest" }
  | { readonly mode: "after"; readonly cursor: string }
  | { readonly mode: "current"; readonly pageToken?: string }

/** One request for a page of metadata changes or of the current state. The
 * start defaults to `earliest`. `groups` empty or absent means every group.
 * `sinceMicros` and `untilMicros` keep records stamped inside the inclusive
 * range and apply only to the `earliest` and `after` starts. `limit` is 1 to
 * 1000 records or items, 100 by default. */
export interface MetadataChangesQuery {
  readonly start?: MetadataChangeStart
  readonly groups?: readonly MetadataChangeGroup[]
  readonly sinceMicros?: bigint
  readonly untilMicros?: bigint
  readonly limit?: number
}

/** What one change record or current state item is about. Each kind fills
 * the fields that name its resource and what changed, and leaves the rest
 * absent or empty. Ids are native ids. A `*CreatedOp` or
 * `userCreatedAtMicros` tells a recreated resource apart from an older one
 * with the same id. Resource options are not carried. */
export interface MetadataSubject {
  readonly streamId?: number
  readonly streamCreatedOp?: bigint
  readonly topicId?: number
  readonly topicCreatedOp?: bigint
  readonly groupId?: number
  readonly memberId?: number
  readonly partitionId?: number
  readonly userId?: number
  readonly userCreatedAtMicros?: bigint
  readonly clientId?: bigint
  readonly session?: bigint
  /** The stream, topic, consumer group, or personal access token name. */
  readonly name?: string
  readonly username?: string
  readonly createdAtMicros?: bigint
  readonly partitionsCount?: number
  readonly partitionIds: readonly number[]
  readonly removedPartitionIds: readonly number[]
  readonly consumerGroupsCount?: number
  readonly membersCount?: number
  /** The op of the request a deferred delete answers. */
  readonly requestedOp?: bigint
  readonly status?: UserStatus
  readonly permissions?: Permissions
  readonly expiresAtMicros?: bigint
  readonly reason?: MetadataChangeReason
}

/** One committed metadata change. `kind` is absent for a kind this client
 * does not know, and its subject is then empty. `actorUserId` is absent for a
 * change the server made itself. */
export interface MetadataChangeRecord {
  readonly op: bigint
  /** The position of this record among the records of one op. */
  readonly ordinal: number
  readonly timestampMicros: bigint
  readonly actorUserId?: number
  readonly kind?: MetadataChangeKind
  readonly subject: MetadataSubject
}

/** One resource of the current state. `group` is absent for a group this
 * client does not know, and its subject is then empty. */
export interface MetadataItem {
  readonly group?: MetadataChangeGroup
  readonly subject: MetadataSubject
}

/** One page of metadata changes, or of the current state. Send `next` back
 * with the `after` start to read on, and `pageToken` with the `current` start
 * to continue a current state scan. */
export interface MetadataChanges {
  readonly status: MetadataChangeStatus
  readonly phase: MetadataChangePhase
  /** The newest op the answering node applied. */
  readonly headOp: bigint
  /** The oldest op from which the requested groups are complete. */
  readonly oldestOp: bigint
  /** The requested groups are complete after this time, 0 when the node dropped nothing. */
  readonly retainedSinceMicros: bigint
  /** The cursor after this page, absent on `expired` and `reset`. */
  readonly next?: string
  /** More records follow `next`, or another current state page exists. */
  readonly hasMore: boolean
  readonly records: readonly MetadataChangeRecord[]
  readonly items: readonly MetadataItem[]
  /** The token of the next current state page, absent once the scan ends. */
  readonly pageToken?: string
}

const USER_STATUSES: ReadonlySet<unknown> = new Set(["active", "inactive"])

function knownStatus(status: UserStatus | undefined): void {
  if (status !== undefined && !USER_STATUSES.has(status))
    throw new InvalidError(`unknown user status \`${status}\`, expected active or inactive`)
}

/** @internal The raw transport call behind one server administration verb. */
export async function administer<Value>(
  transport: LaserTransport,
  what: string,
  idempotent: boolean,
  options: CallOptions,
  operation: (client: AdminClient) => Promise<Value>
): Promise<Value> {
  const run = transport.administer?.bind(transport)
  if (run === undefined)
    throw new UnsupportedError(`${what} needs the Apache Iggy transport`, { surface: "streaming" })
  throwIfAborted(options.signal, what)
  return abortable(run(operation, `failed to run ${what}`, idempotent), options.signal, what)
}

/** @internal */
export const serverVerbs = {
  users: (transport: LaserTransport, options: CallOptions) =>
    administer(transport, "user listing", true, options, readUsers),
  tokens: (transport: LaserTransport, options: CallOptions) =>
    administer(transport, "personal access token listing", true, options, readTokens),
  ping: (transport: LaserTransport, options: CallOptions) =>
    administer(transport, "ping", true, options, pingServer),
  stats: (transport: LaserTransport, options: CallOptions) =>
    administer(transport, "server statistics", true, options, readStats),
  me: (transport: LaserTransport, options: CallOptions) =>
    administer(transport, "client lookup", true, options, readMe),
  clients: (transport: LaserTransport, options: CallOptions) =>
    administer(transport, "client listing", true, options, readClients),
  cluster: (transport: LaserTransport, options: CallOptions) =>
    administer(transport, "cluster lookup", true, options, readCluster),
  snapshot: (transport: LaserTransport, options: CallOptions) =>
    administer(transport, "server snapshot", true, options, readSnapshot),
  client: (transport: LaserTransport, clientId: number, options: CallOptions) =>
    administer(transport, "client lookup", true, options, (client) => readClient(client, clientId)),
  resolveOperation: async (
    transport: LaserTransport,
    operation: bigint,
    timeoutMs: number,
    options: CallOptions
  ) => {
    const timeout = millis(timeoutMs, "timeoutMs")
    return administer(transport, "operation resolve", true, options, (client) =>
      resolveOperation(client, operation, timeout)
    )
  },
  abandonOperation: (transport: LaserTransport, operation: bigint, options: CallOptions) =>
    administer(transport, "operation abandon", true, options, (client) =>
      abandonOperation(client, operation)
    ),
  sendCommand: (
    transport: LaserTransport,
    code: number,
    payload: Uint8Array,
    replica: number | undefined,
    options: CallOptions
  ) =>
    administer(transport, `command ${String(code)}`, false, options, (client) =>
      sendCommand(client, code, payload, replica)
    ),
  describeOptions: async (transport: LaserTransport, scope: OptionsScope, options: CallOptions) => {
    const code = optionsScopeCode(scope)
    return administer(transport, "option catalog", true, options, (client) =>
      readOptions(client, code)
    )
  },
  metadataChanges: async (
    transport: LaserTransport,
    query: MetadataChangesQuery,
    options: CallOptions
  ) => {
    const payload = metadataChangesRequest(query)
    return administer(transport, "metadata change read", true, options, (client) =>
      readMetadataChanges(client, payload)
    )
  }
}

/** One user of the server, addressed by name. Build it with `Laser.user`. Each verb needs the matching user permission on this connection. */
export class User {
  private constructor(
    private readonly transport: LaserTransport,
    readonly name: string
  ) {}

  /** @internal */
  static create(transport: LaserTransport, name: string): User {
    return new User(transport, name)
  }

  /** This user with its permissions. Resolves `undefined` when it does not exist. */
  async get(options: CallOptions = {}): Promise<UserInfo | undefined> {
    return administer(this.transport, "user lookup", true, options, (client) =>
      readUser(client, this.name)
    )
  }

  /** Whether this user exists. */
  async exists(options: CallOptions = {}): Promise<boolean> {
    return (await this.get(options)) !== undefined
  }

  /** Creates this user with `password`. An existing user of the same name fails the call. */
  async create(password: string, options: CreateUserOptions & CallOptions = {}): Promise<UserInfo> {
    knownStatus(options.status)
    return administer(this.transport, "user create", false, options, (client) =>
      createUser(client, this.name, password, options.status ?? "active", options.permissions)
    )
  }

  /** Renames this user or changes its status. A deactivated user can no longer sign in. After a rename, address the user by its new name. */
  async update(options: UpdateUserOptions & CallOptions = {}): Promise<void> {
    knownStatus(options.status)
    return administer(this.transport, "user update", false, options, (client) =>
      updateUser(client, this.name, options.username, options.status)
    )
  }

  /** Replaces every permission of this user. `undefined` removes every grant. */
  async setPermissions(permissions?: Permissions, options: CallOptions = {}): Promise<void> {
    return administer(this.transport, "permission update", false, options, (client) =>
      updatePermissions(client, this.name, permissions)
    )
  }

  /** Changes this user's password. The server checks `currentPassword` first. */
  async changePassword(
    currentPassword: string,
    newPassword: string,
    options: CallOptions = {}
  ): Promise<void> {
    return administer(this.transport, "password change", false, options, (client) =>
      changePassword(client, this.name, currentPassword, newPassword)
    )
  }

  /** Deletes this user. Resolves `false` when the user did not exist. */
  async delete(options: CallOptions = {}): Promise<boolean> {
    return administer(this.transport, "user delete", false, options, (client) =>
      deleteUser(client, this.name)
    )
  }
}

/** One personal access token of the signed-in user, addressed by name. Build it with `Laser.personalAccessToken`. A connection string that carries the token value in place of a username and password signs in with it. */
export class PersonalAccessToken {
  private constructor(
    private readonly transport: LaserTransport,
    readonly name: string
  ) {}

  /** @internal */
  static create(transport: LaserTransport, name: string): PersonalAccessToken {
    return new PersonalAccessToken(transport, name)
  }

  /** Creates this token and resolves its value. The server shows the value only once. `expiryMs` sets how long the token works. Without it the token never expires. */
  async create(options: CallOptions & { readonly expiryMs?: number } = {}): Promise<string> {
    const expiry =
      options.expiryMs === undefined ? 0n : millisToMicros(millis(options.expiryMs, "expiryMs"))
    return administer(this.transport, "personal access token create", false, options, (client) =>
      createToken(client, this.name, expiry)
    )
  }

  /** Deletes this token. Resolves `false` when the token did not exist. */
  async delete(options: CallOptions = {}): Promise<boolean> {
    return administer(this.transport, "personal access token delete", false, options, (client) =>
      deleteToken(client, this.name)
    )
  }
}
