import { CREATE_STREAM } from "apache-iggy/dist/wire/stream/create-stream.command.js"
import { UPDATE_STREAM } from "apache-iggy/dist/wire/stream/update-stream.command.js"
import { UPDATE_TOPIC } from "apache-iggy/dist/wire/topic/update-topic.command.js"
import { DELETE_SEGMENTS } from "apache-iggy/dist/wire/segment/delete-segments.command.js"
import { GET_CLUSTER_METADATA } from "apache-iggy/dist/wire/cluster/get-cluster-metadata.command.js"
import { SNAPSHOT } from "apache-iggy/dist/wire/system/snapshot.command.js"
import {
  DESCRIBE_OPTIONS,
  OptionsScope as IggyOptionsScope
} from "apache-iggy/dist/wire/system/describe-options.command.js"
import {
  GET_METADATA_CHANGES,
  MetadataChangeStart as IggyMetadataChangeStart
} from "apache-iggy/dist/wire/system/get-metadata-changes.command.js"
import {
  MetadataChangeCursor,
  MetadataCurrentPageToken
} from "apache-iggy/dist/wire/system/metadata-changes.utils.js"
import { CHANGE_PASSWORD } from "apache-iggy/dist/wire/user/change-password.command.js"
import { CREATE_USER } from "apache-iggy/dist/wire/user/create-user.command.js"
import { DELETE_USER } from "apache-iggy/dist/wire/user/delete-user.command.js"
import { GET_USER } from "apache-iggy/dist/wire/user/get-user.command.js"
import { GET_USERS } from "apache-iggy/dist/wire/user/get-users.command.js"
import { UPDATE_PERMISSIONS } from "apache-iggy/dist/wire/user/update-permissions.command.js"
import { UPDATE_USER } from "apache-iggy/dist/wire/user/update-user.command.js"
import {
  type UserPermissions,
  deserializePermissions
} from "apache-iggy/dist/wire/user/permissions.utils.js"
import { CREATE_TOKEN } from "apache-iggy/dist/wire/token/create-token.command.js"
import { DELETE_TOKEN } from "apache-iggy/dist/wire/token/delete-token.command.js"
import { GET_TOKENS } from "apache-iggy/dist/wire/token/get-tokens.command.js"
import { UserStatus as IggyUserStatus } from "apache-iggy/dist/wire/user/user.utils.js"
import { serializeIdentifier } from "apache-iggy/dist/wire/identifier.utils.js"
import { GET_CLIENT } from "apache-iggy/dist/wire/client/get-client.command.js"
import { GET_CLIENTS } from "apache-iggy/dist/wire/client/get-clients.command.js"
import { GET_ME } from "apache-iggy/dist/wire/client/get-me.command.js"
import { GET_STATS } from "apache-iggy/dist/wire/system/get-stats.command.js"
import { PING } from "apache-iggy/dist/wire/system/ping.command.js"
import { CodecError, InvalidError } from "../client/errors.js"
import type {
  ClusterInfo,
  ClusterNodeRole,
  ClusterNodeStatus,
  ClientConsumerGroup,
  ClientInfo,
  MetadataChangeGroup,
  MetadataChangeReason,
  MetadataChangeStart,
  MetadataChanges,
  MetadataChangesQuery,
  MetadataSubject,
  OptionInfo,
  OptionsScope,
  Permissions,
  PersonalAccessTokenInfo,
  ServerStats,
  StreamPermissions,
  TopicPermissions,
  UserInfo,
  UserStatus
} from "../client/server.js"
import { type LaserTransport, serverErrorCode } from "./apache-iggy.js"

/** The raw client one administration call runs on. */
export type AdminClient = Parameters<Parameters<NonNullable<LaserTransport["administer"]>>[0]>[0]

const RESOURCE_NOT_FOUND = 20
const NO_USER_ID = 0xffff_ffff

// One raw command. A missing resource resolves `undefined`.
async function command(
  client: AdminClient,
  code: number,
  payload: Buffer
): Promise<Buffer | undefined> {
  const raw = await client.clientProvider()
  try {
    const response = await raw.sendCommand(code, payload)
    return response.length === 0 ? Buffer.alloc(0) : response.data
  } catch (error) {
    if (serverErrorCode(error) === RESOURCE_NOT_FOUND) return undefined
    throw error
  }
}

function userStatus(code: number): UserStatus {
  return IggyUserStatus[code] === "Inactive" ? "inactive" : "active"
}

function statusCode(status: UserStatus): IggyUserStatus {
  const code = { active: IggyUserStatus.Active, inactive: IggyUserStatus.Inactive }[status] as
    IggyUserStatus | undefined
  if (code === undefined)
    throw new InvalidError(`unknown user status \`${status}\`, expected active or inactive`)
  return code
}

// A token record: name u8 length and bytes, expiry u64 micros (0 never expires).
function decodeTokens(data: Buffer): PersonalAccessTokenInfo[] {
  const tokens: PersonalAccessTokenInfo[] = []
  for (let pos = 0; pos < data.length;) {
    const nameLength = data.readUInt8(pos)
    const name = data.subarray(pos + 1, pos + 1 + nameLength).toString()
    const expiresAtMicros = data.readBigUInt64LE(pos + 1 + nameLength)
    tokens.push(expiresAtMicros === 0n ? { name } : { name, expiresAtMicros })
    pos += 1 + nameLength + 8
  }
  return tokens
}

function fromWirePermissions(wire: UserPermissions): Permissions {
  const global = wire.global
  return {
    global: {
      manageServers: global.ManageServers,
      readServers: global.ReadServers,
      manageUsers: global.ManageUsers,
      readUsers: global.ReadUsers,
      manageStreams: global.ManageStreams,
      readStreams: global.ReadStreams,
      manageTopics: global.ManageTopics,
      readTopics: global.ReadTopics,
      pollMessages: global.PollMessages,
      sendMessages: global.SendMessages
    },
    streams: new Map(
      wire.streams.map((stream) => [
        stream.streamId,
        {
          manageStream: stream.permissions.manageStream,
          readStream: stream.permissions.readStream,
          manageTopics: stream.permissions.manageTopics,
          readTopics: stream.permissions.readTopics,
          pollMessages: stream.permissions.pollMessages,
          sendMessages: stream.permissions.sendMessages,
          topics: new Map(
            stream.topics.map((topic) => [
              topic.topicId,
              {
                manageTopic: topic.permissions.manage,
                readTopic: topic.permissions.read,
                pollMessages: topic.permissions.pollMessages,
                sendMessages: topic.permissions.sendMessages
              }
            ])
          )
        }
      ])
    )
  }
}

function toWirePermissions(permissions: Permissions): UserPermissions {
  const global = permissions.global ?? {}
  return {
    global: {
      ManageServers: global.manageServers === true,
      ReadServers: global.readServers === true,
      ManageUsers: global.manageUsers === true,
      ReadUsers: global.readUsers === true,
      ManageStreams: global.manageStreams === true,
      ReadStreams: global.readStreams === true,
      ManageTopics: global.manageTopics === true,
      ReadTopics: global.readTopics === true,
      PollMessages: global.pollMessages === true,
      SendMessages: global.sendMessages === true
    },
    streams: [...(permissions.streams ?? new Map<number, StreamPermissions>())].map(
      ([streamId, stream]) => ({
        streamId,
        permissions: {
          manageStream: stream.manageStream === true,
          readStream: stream.readStream === true,
          manageTopics: stream.manageTopics === true,
          readTopics: stream.readTopics === true,
          pollMessages: stream.pollMessages === true,
          sendMessages: stream.sendMessages === true
        },
        topics: [...(stream.topics ?? new Map<number, TopicPermissions>())].map(
          ([topicId, topic]) => ({
            topicId,
            permissions: {
              manage: topic.manageTopic === true,
              read: topic.readTopic === true,
              pollMessages: topic.pollMessages === true,
              sendMessages: topic.sendMessages === true
            }
          })
        )
      })
    )
  }
}

// A user record: id u32, created at u64 micros, status u8, username u8 length
// and bytes, then the options and the derived options, each a u32 length and
// its bytes. A detailed record follows with a permissions flag, a u32 length,
// and the permissions.
function decodeUser(data: Buffer, pos: number): { readonly info: UserInfo; readonly end: number } {
  const nameLength = data.readUInt8(pos + 13)
  const nameEnd = pos + 14 + nameLength
  const optionsEnd = nameEnd + 4 + data.readUInt32LE(nameEnd)
  const end = optionsEnd + 4 + data.readUInt32LE(optionsEnd)
  return {
    info: {
      id: data.readUInt32LE(pos),
      createdAtMicros: data.readBigUInt64LE(pos + 4),
      status: userStatus(data.readUInt8(pos + 12)),
      username: data.subarray(pos + 14, nameEnd).toString()
    },
    end
  }
}

function decodeUserDetails(data: Buffer): UserInfo {
  const { info, end } = decodeUser(data, 0)
  if (end >= data.length || data.readUInt8(end) === 0) return info
  const length = data.readUInt32LE(end + 1)
  const permissions = deserializePermissions(data.subarray(end + 5, end + 5 + length))
  return { ...info, permissions: fromWirePermissions(permissions) }
}

function decodeUsers(data: Buffer): UserInfo[] {
  const users: UserInfo[] = []
  for (let pos = 0; pos < data.length;) {
    const { info, end } = decodeUser(data, pos)
    users.push(info)
    pos = end
  }
  return users
}

const TRANSPORTS: Readonly<Record<number, string>> = {
  1: "TCP",
  2: "QUIC",
  3: "HTTP",
  4: "WebSocket"
}

// A client record: id u32, user id u32 (u32::MAX before sign-in), transport
// u8, address u32 length and bytes, consumer group count u32. A detailed
// record follows with one stream, topic, and group id triple per group.
function decodeClient(
  data: Buffer,
  pos: number
): { readonly info: ClientInfo; readonly end: number } {
  const addressLength = data.readUInt32LE(pos + 9)
  const userId = data.readUInt32LE(pos + 4)
  const end = pos + 13 + addressLength + 4
  return {
    info: {
      clientId: data.readUInt32LE(pos),
      ...(userId === NO_USER_ID ? {} : { userId }),
      transport: TRANSPORTS[data.readUInt8(pos + 8)] ?? "Unknown",
      address: data.subarray(pos + 13, pos + 13 + addressLength).toString(),
      consumerGroupsCount: data.readUInt32LE(pos + 13 + addressLength),
      consumerGroups: []
    },
    end
  }
}

function decodeClientDetails(data: Buffer): ClientInfo {
  const { info, end } = decodeClient(data, 0)
  const consumerGroups: ClientConsumerGroup[] = []
  for (let pos = end; pos + 12 <= data.length; pos += 12) {
    consumerGroups.push({
      streamId: data.readUInt32LE(pos),
      topicId: data.readUInt32LE(pos + 4),
      groupId: data.readUInt32LE(pos + 8)
    })
  }
  consumerGroups.sort((left, right) => left.groupId - right.groupId)
  return { ...info, consumerGroups }
}

function decodeClients(data: Buffer): ClientInfo[] {
  const clients: ClientInfo[] = []
  for (let pos = 0; pos < data.length;) {
    const { info, end } = decodeClient(data, pos)
    clients.push(info)
    pos = end
  }
  return clients.sort((left, right) => left.clientId - right.clientId)
}

function password(name: string, value: string): Buffer {
  const bytes = Buffer.from(value)
  if (bytes.length < 1 || bytes.length > 255)
    throw new InvalidError(`${name} must be between 1 and 255 bytes`)
  return Buffer.concat([Buffer.from([bytes.length]), bytes])
}

async function mutate(
  client: AdminClient,
  name: string,
  code: number,
  payload: Buffer
): Promise<void> {
  if ((await command(client, code, payload)) === undefined)
    throw new InvalidError(`user \`${name}\` does not exist`)
}

/** @internal */
export async function readUsers(client: AdminClient): Promise<UserInfo[]> {
  return decodeUsers(
    (await command(client, GET_USERS.code, GET_USERS.serialize())) ?? Buffer.alloc(0)
  )
}

/** @internal */
export async function readUser(client: AdminClient, name: string): Promise<UserInfo | undefined> {
  const data = await command(client, GET_USER.code, GET_USER.serialize({ userId: name }))
  return data === undefined || data.length === 0 ? undefined : decodeUserDetails(data)
}

/** @internal */
export async function createUser(
  client: AdminClient,
  name: string,
  secret: string,
  status: UserStatus,
  permissions: Permissions | undefined
): Promise<UserInfo> {
  password("password", secret)
  const payload = CREATE_USER.serialize({
    username: name,
    password: secret,
    status: statusCode(status),
    ...(permissions === undefined ? {} : { permissions: toWirePermissions(permissions) })
  })
  return decodeUserDetails((await command(client, CREATE_USER.code, payload)) ?? Buffer.alloc(0))
}

/** @internal */
export async function updateUser(
  client: AdminClient,
  name: string,
  username: string | undefined,
  status: UserStatus | undefined
): Promise<void> {
  const payload = UPDATE_USER.serialize({
    userId: name,
    ...(username === undefined ? {} : { username }),
    ...(status === undefined ? {} : { status: statusCode(status) })
  })
  await mutate(client, name, UPDATE_USER.code, payload)
}

/** @internal */
export async function updatePermissions(
  client: AdminClient,
  name: string,
  permissions: Permissions | undefined
): Promise<void> {
  const payload =
    permissions === undefined
      ? Buffer.concat([serializeIdentifier(name), Buffer.from([0])])
      : UPDATE_PERMISSIONS.serialize({ userId: name, permissions: toWirePermissions(permissions) })
  await mutate(client, name, UPDATE_PERMISSIONS.code, payload)
}

/** @internal */
export async function changePassword(
  client: AdminClient,
  name: string,
  current: string,
  next: string
): Promise<void> {
  const payload = Buffer.concat([
    serializeIdentifier(name),
    password("currentPassword", current),
    password("newPassword", next)
  ])
  await mutate(client, name, CHANGE_PASSWORD.code, payload)
}

/** @internal */
export async function deleteUser(client: AdminClient, name: string): Promise<boolean> {
  return (
    (await command(client, DELETE_USER.code, DELETE_USER.serialize({ userId: name }))) !== undefined
  )
}

/** @internal */
export async function readTokens(client: AdminClient): Promise<PersonalAccessTokenInfo[]> {
  return decodeTokens(
    (await command(client, GET_TOKENS.code, GET_TOKENS.serialize())) ?? Buffer.alloc(0)
  )
}

/** @internal */
export async function createToken(
  client: AdminClient,
  name: string,
  expiryMicros: bigint
): Promise<string> {
  const data =
    (await command(
      client,
      CREATE_TOKEN.code,
      CREATE_TOKEN.serialize({ name, expiry: expiryMicros })
    )) ?? Buffer.alloc(0)
  return CREATE_TOKEN.deserialize({ status: 0, length: data.length, data }).token
}

/** @internal */
export async function deleteToken(client: AdminClient, name: string): Promise<boolean> {
  if (!(await readTokens(client)).some((token) => token.name === name)) return false
  await command(client, DELETE_TOKEN.code, DELETE_TOKEN.serialize({ name }))
  return true
}

/** @internal */
export async function pingServer(client: AdminClient): Promise<void> {
  await command(client, PING.code, PING.serialize())
}

/** @internal */
export async function readMe(client: AdminClient): Promise<ClientInfo> {
  return decodeClientDetails(
    (await command(client, GET_ME.code, GET_ME.serialize())) ?? Buffer.alloc(0)
  )
}

/** @internal */
export async function readClients(client: AdminClient): Promise<ClientInfo[]> {
  return decodeClients(
    (await command(client, GET_CLIENTS.code, GET_CLIENTS.serialize())) ?? Buffer.alloc(0)
  )
}

/** @internal */
export async function readClient(
  client: AdminClient,
  clientId: number
): Promise<ClientInfo | undefined> {
  const data = await command(client, GET_CLIENT.code, GET_CLIENT.serialize({ clientId }))
  return data === undefined || data.length === 0 ? undefined : decodeClientDetails(data)
}

/** @internal */
export async function readStats(client: AdminClient): Promise<ServerStats> {
  const data = (await command(client, GET_STATS.code, GET_STATS.serialize())) ?? Buffer.alloc(0)
  const raw = GET_STATS.deserialize({ status: 0, length: data.length, data })
  return {
    processId: raw.processId,
    cpuUsage: raw.cpuUsage,
    totalCpuUsage: raw.totalCpuUsage,
    memoryUsageBytes: raw.memoryUsage,
    totalMemoryBytes: raw.totalMemory,
    availableMemoryBytes: raw.availableMemory,
    runTimeMicros: raw.runTime,
    startTimeMicros: raw.startTime,
    readBytes: raw.readBytes,
    writtenBytes: raw.writtenBytes,
    messagesSizeBytes: raw.messagesSizeBytes,
    streamsCount: raw.streamsCount,
    topicsCount: raw.topicsCount,
    partitionsCount: raw.partitionsCount,
    segmentsCount: raw.segmentsCount,
    messagesCount: raw.messagesCount,
    clientsCount: raw.clientsCount,
    consumerGroupsCount: raw.consumersGroupsCount,
    hostname: raw.hostname,
    osName: raw.osName,
    osVersion: raw.osVersion,
    kernelVersion: raw.kernelVersion,
    serverVersion: raw.iggyServerVersion,
    threadsCount: raw.threadsCount,
    freeDiskSpaceBytes: raw.freeDiskSpace,
    totalDiskSpaceBytes: raw.totalDiskSpace,
    openFilesCount: raw.openFilesCount,
    openFilesLimit: raw.openFilesLimit,
    cacheMetrics: raw.cacheMetrics
      .map((metrics) => ({
        streamId: metrics.streamId,
        topicId: metrics.topicId,
        partitionId: metrics.partitionId,
        hits: metrics.hits,
        misses: metrics.misses,
        hitRatio: metrics.hitRatio
      }))
      .sort(
        (left, right) =>
          left.streamId - right.streamId ||
          left.topicId - right.topicId ||
          left.partitionId - right.partitionId
      )
  }
}

const NEVER = 0xffff_ffff_ffff_ffffn

/** @internal The topic settings an update sends, each absent to keep it. */
export interface TopicSettings {
  readonly name?: string
  readonly expiryMicros?: bigint
  readonly neverExpire?: boolean
  readonly maxTopicBytes?: bigint
  readonly unlimitedTopicSize?: boolean
}

/** @internal */
export async function createStream(client: AdminClient, name: string): Promise<void> {
  await command(client, CREATE_STREAM.code, CREATE_STREAM.serialize({ name }))
}

/** @internal */
export async function renameStream(
  client: AdminClient,
  stream: string,
  name: string
): Promise<void> {
  await command(client, UPDATE_STREAM.code, UPDATE_STREAM.serialize({ streamId: stream, name }))
}

/** @internal */
export async function updateTopic(
  client: AdminClient,
  stream: string,
  topic: string,
  settings: TopicSettings
): Promise<void> {
  const messageExpiry = settings.neverExpire === true ? NEVER : settings.expiryMicros
  const maxTopicSize = settings.unlimitedTopicSize === true ? NEVER : settings.maxTopicBytes
  const payload = UPDATE_TOPIC.serialize({
    streamId: stream,
    topicId: topic,
    ...(settings.name === undefined ? {} : { name: settings.name }),
    ...(messageExpiry === undefined ? {} : { messageExpiry }),
    ...(maxTopicSize === undefined ? {} : { maxTopicSize })
  })
  await command(client, UPDATE_TOPIC.code, payload)
}

/** @internal */
export async function deleteSegments(
  client: AdminClient,
  stream: string,
  topic: string,
  partitionId: number,
  segmentsCount: number
): Promise<void> {
  const payload = DELETE_SEGMENTS.serialize({
    streamId: stream,
    topicId: topic,
    partitionId,
    segmentsCount
  })
  await command(client, DELETE_SEGMENTS.code, payload)
}

/** @internal */
export async function readCluster(client: AdminClient): Promise<ClusterInfo> {
  const data =
    (await command(client, GET_CLUSTER_METADATA.code, GET_CLUSTER_METADATA.serialize())) ??
    Buffer.alloc(0)
  const cluster = GET_CLUSTER_METADATA.deserialize({ status: 0, length: data.length, data })
  return {
    name: cluster.name,
    nodes: cluster.nodes.map((node) => ({
      replicaId: node.replicaId,
      name: node.name,
      ip: node.ip,
      tcpPort: node.endpoints.tcp,
      quicPort: node.endpoints.quic,
      httpPort: node.endpoints.http,
      websocketPort: node.endpoints.websocket,
      role: node.role.toLowerCase() as ClusterNodeRole,
      status: node.status.toLowerCase() as ClusterNodeStatus
    }))
  }
}

/** @internal */
export async function resolveOperation(
  client: AdminClient,
  operation: bigint,
  timeoutMs: number
): Promise<void> {
  await client.resolveOperation(operation, { deadline: Date.now() + timeoutMs })
}

/** @internal */
export async function abandonOperation(client: AdminClient, operation: bigint): Promise<void> {
  await client.abandonOperation(operation)
}

/** @internal */
export async function sendCommand(
  client: AdminClient,
  code: number,
  payload: Uint8Array,
  replica?: number
): Promise<Uint8Array> {
  const request = Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength)
  const reply =
    replica === undefined
      ? await client.sendBinaryRequest(code, request)
      : await client.sendBinaryRequestToReplica(code, request, replica)
  return new Uint8Array(reply)
}

/** @internal */
export async function readSnapshot(client: AdminClient): Promise<Uint8Array> {
  const data = (await command(client, SNAPSHOT.code, SNAPSHOT.serialize())) ?? Buffer.alloc(0)
  return new Uint8Array(data)
}

const HEADER_KINDS = [
  "raw",
  "string",
  "bool",
  "int8",
  "int16",
  "int32",
  "int64",
  "int128",
  "uint8",
  "uint16",
  "uint32",
  "uint64",
  "uint128",
  "float32",
  "float64"
] as const

const HEADER_KIND_SIZES: Readonly<Record<string, number>> = {
  bool: 1,
  int8: 1,
  int16: 2,
  int32: 4,
  int64: 8,
  int128: 16,
  uint8: 1,
  uint16: 2,
  uint32: 4,
  uint64: 8,
  uint128: 16,
  float32: 4,
  float64: 8
}

function headerKindName(code: number): string {
  const name = HEADER_KINDS[code - 1]
  if (name === undefined)
    throw new CodecError(`unknown option value kind ${String(code)}`, "streaming", "option catalog")
  return name
}

function int128Le(value: Buffer, signed: boolean): bigint {
  const unsigned = (value.readBigUInt64LE(8) << 64n) | value.readBigUInt64LE(0)
  return signed ? BigInt.asIntN(128, unsigned) : unsigned
}

// The text form of an option default, as Apache Iggy writes option values:
// integers in decimal, floats as the `0x` hex of their bits, raw bytes in
// base64. A value that does not decode under its kind has no text form.
function optionDefaultText(kind: string, value: Buffer): string | undefined {
  if (value.length === 0) return undefined
  const size = HEADER_KIND_SIZES[kind]
  if (size !== undefined && value.length !== size) return undefined
  switch (kind) {
    case "raw":
      return value.toString("base64")
    case "string":
      try {
        return new TextDecoder("utf-8", { fatal: true }).decode(value)
      } catch {
        return undefined
      }
    case "bool":
      return value[0] === 1 ? "true" : value[0] === 0 ? "false" : undefined
    case "int8":
      return String(value.readInt8(0))
    case "int16":
      return String(value.readInt16LE(0))
    case "int32":
      return String(value.readInt32LE(0))
    case "int64":
      return String(value.readBigInt64LE(0))
    case "int128":
      return String(int128Le(value, true))
    case "uint8":
      return String(value.readUInt8(0))
    case "uint16":
      return String(value.readUInt16LE(0))
    case "uint32":
      return String(value.readUInt32LE(0))
    case "uint64":
      return String(value.readBigUInt64LE(0))
    case "uint128":
      return String(int128Le(value, false))
    case "float32":
      return `0x${value.readUInt32LE(0).toString(16).padStart(8, "0")}`
    case "float64":
      return `0x${value.readBigUInt64LE(0).toString(16).padStart(16, "0")}`
    default:
      return undefined
  }
}

/** @internal The Apache Iggy code of `scope`. Throws `InvalidError` for an unknown one. */
export function optionsScopeCode(scope: OptionsScope): IggyOptionsScope {
  const code = {
    topic: IggyOptionsScope.Topic,
    stream: IggyOptionsScope.Stream,
    user: IggyOptionsScope.User
  }[scope] as IggyOptionsScope | undefined
  if (code === undefined) throw new InvalidError(`unknown OptionsScope \`${scope}\``)
  return code
}

/** @internal */
export async function readOptions(
  client: AdminClient,
  scope: IggyOptionsScope
): Promise<OptionInfo[]> {
  const data =
    (await command(client, DESCRIBE_OPTIONS.code, DESCRIBE_OPTIONS.serialize({ scope }))) ??
    Buffer.alloc(0)
  return DESCRIBE_OPTIONS.deserialize({ status: 0, length: data.length, data }).map((spec) => {
    const kind = headerKindName(spec.kind)
    const defaultValue = optionDefaultText(kind, spec.defaultValue)
    return {
      key: spec.key,
      kind,
      updatable: spec.updatable,
      resettable: spec.resettable,
      secret: spec.secret,
      readOnly: spec.readOnly,
      critical: spec.critical,
      ...(defaultValue === undefined ? {} : { defaultValue }),
      description: spec.description
    }
  })
}

const METADATA_GROUPS: readonly MetadataChangeGroup[] = [
  "streams",
  "topics",
  "partitions",
  "consumer_groups",
  "consumer_group_members",
  "users",
  "personal_access_tokens",
  "sessions"
]

function iggyStart(start: MetadataChangeStart | undefined): IggyMetadataChangeStart {
  switch (start?.mode) {
    case undefined:
    case "earliest":
      return IggyMetadataChangeStart.Earliest
    case "latest":
      return IggyMetadataChangeStart.Latest
    case "after":
      try {
        return IggyMetadataChangeStart.After(MetadataChangeCursor.parse(start.cursor))
      } catch (cause) {
        throw new InvalidError(`malformed metadata change cursor \`${start.cursor}\``, undefined, {
          cause
        })
      }
    case "current":
      if (start.pageToken === undefined) return IggyMetadataChangeStart.Current()
      try {
        return IggyMetadataChangeStart.Current(MetadataCurrentPageToken.parse(start.pageToken))
      } catch (cause) {
        throw new InvalidError(`malformed metadata page token \`${start.pageToken}\``, undefined, {
          cause
        })
      }
    default:
      throw new InvalidError(
        `unknown metadata change start \`${String((start as { mode: unknown }).mode)}\``
      )
  }
}

type MetadataBody = Partial<{
  streamId: number
  streamCreatedOp: bigint
  topicId: number
  topicCreatedOp: bigint
  groupId: number
  memberId: number | null
  partitionId: number
  userId: number
  userCreatedAt: bigint
  clientId: bigint
  session: bigint
  name: string
  username: string
  createdAt: bigint
  partitionsCount: number
  partitionIds: number[]
  removedPartitionIds: number[]
  consumerGroupsCount: number
  membersCount: number
  requestedOp: bigint | null
  status: number
  permissions: UserPermissions | null
  expiry: bigint | null
  reason: MetadataChangeReason
}>

function metadataSubject(body: MetadataBody | Buffer): MetadataSubject {
  if (Buffer.isBuffer(body)) return { partitionIds: [], removedPartitionIds: [] }
  const present = <Value>(value: Value | null | undefined): Value | undefined =>
    value === null ? undefined : value
  const fields = {
    streamId: body.streamId,
    streamCreatedOp: body.streamCreatedOp,
    topicId: body.topicId,
    topicCreatedOp: body.topicCreatedOp,
    groupId: body.groupId,
    memberId: present(body.memberId),
    partitionId: body.partitionId,
    userId: body.userId,
    userCreatedAtMicros: body.userCreatedAt,
    clientId: body.clientId,
    session: body.session,
    name: body.name,
    username: body.username,
    createdAtMicros: body.createdAt,
    partitionsCount: body.partitionsCount,
    consumerGroupsCount: body.consumerGroupsCount,
    membersCount: body.membersCount,
    requestedOp: present(body.requestedOp),
    status: body.status === undefined ? undefined : userStatus(body.status),
    permissions:
      body.permissions === undefined || body.permissions === null
        ? undefined
        : fromWirePermissions(body.permissions),
    expiresAtMicros: present(body.expiry),
    reason: body.reason
  }
  return {
    ...Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== undefined)),
    partitionIds: body.partitionIds ?? [],
    removedPartitionIds: body.removedPartitionIds ?? []
  }
}

const INVALID_METADATA_QUERY =
  "a metadata change query needs a limit of 1 to 1000, since no later than until, no time bound with the latest or current start, and every parent group in a current scan"

/** @internal The request bytes of `query`. Throws `InvalidError` for a query
 * that breaks the server rules, before anything is sent. */
export function metadataChangesRequest(query: MetadataChangesQuery): Buffer {
  for (const group of query.groups ?? [])
    if (!METADATA_GROUPS.includes(group))
      throw new InvalidError(`unknown MetadataChangeGroup \`${group}\``)
  const start = iggyStart(query.start)
  try {
    return GET_METADATA_CHANGES.serialize({
      start,
      ...(query.groups === undefined ? {} : { groups: [...query.groups] }),
      ...(query.sinceMicros === undefined ? {} : { since: query.sinceMicros }),
      ...(query.untilMicros === undefined ? {} : { until: query.untilMicros }),
      ...(query.limit === undefined ? {} : { limit: query.limit })
    })
  } catch (cause) {
    throw new InvalidError(INVALID_METADATA_QUERY, undefined, { cause })
  }
}

/** @internal */
export async function readMetadataChanges(
  client: AdminClient,
  payload: Buffer
): Promise<MetadataChanges> {
  const data = (await command(client, GET_METADATA_CHANGES.code, payload)) ?? Buffer.alloc(0)
  const page = GET_METADATA_CHANGES.deserialize({ status: 0, length: data.length, data })
  return {
    status: page.status,
    phase: page.phase,
    headOp: page.headOp,
    oldestOp: page.oldestOp,
    retainedSinceMicros: page.retainedSince,
    ...(page.next === null ? {} : { next: page.next.toString() }),
    hasMore: page.hasMore,
    records: page.records.map((record) => ({
      op: record.op,
      ordinal: record.ordinal,
      timestampMicros: record.timestamp,
      ...(record.actorUserId === null ? {} : { actorUserId: record.actorUserId }),
      ...(record.kind === "unknown" ? {} : { kind: record.kind }),
      subject: metadataSubject(record.body as MetadataBody | Buffer)
    })),
    items: page.items.map((item) => ({
      ...(item.group === "unknown" ? {} : { group: item.group }),
      subject: metadataSubject(item.body as MetadataBody | Buffer)
    })),
    ...(page.currentPageToken === null ? {} : { pageToken: page.currentPageToken.toString() })
  }
}
