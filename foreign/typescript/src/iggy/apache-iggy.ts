import {
  Consumer,
  HeaderKeyFactory,
  HeaderValue as IggyHeaderValueFactory,
  Partitioning,
  PollingStrategy as IggyPollingStrategy,
  SimpleClient,
  getRawClient
} from "apache-iggy"
import type { ClientConfig, ClientCredentials, RawClient } from "apache-iggy"
import { readFileSync } from "node:fs"
import { ConfigError, TransportError } from "../client/errors.js"
import { LASERDATA_ROOT_CA } from "../client/laserdata-ca.js"
import type { PollingStrategy } from "../stream/polling-strategy.js"
import type { Routing } from "../stream/routing.js"
import { Mutex } from "../runtime/mutex.js"

export interface PolledMessage {
  readonly payload: Uint8Array
  readonly partitionId: number
  readonly offset: bigint
  readonly timestampMicros?: bigint
  readonly headers: ReadonlyMap<string, IggyHeaderValue>
}

export type IggyClient = SimpleClient
export type ClientOwnership = "owned" | "borrowed"

const RECONNECT_INTERVAL_MS = 100
const RECONNECT_DEADLINE_MS = 10_000

export function toNodeBuffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)
}

export interface MessageWithHeaders {
  readonly payload: Uint8Array
  readonly headers: ReadonlyMap<string, IggyHeaderValue>
}

// One Iggy user-header value, over every kind the wire protocol supports.
// Provenance (and any future typed-header use) only ever writes `"string"`,
// but a decoder must recognize every kind to tell "a foreign typed header,
// safe to ignore" apart from "a known key with a corrupted value type".
// This is the currency `provenance.ts` encodes to and decodes from, and
// (via `sendMessageWithHeaders`/`pollMessages`) what actually rides the
// wire.
export type IggyHeaderValue =
  | { readonly kind: "raw"; readonly value: Uint8Array }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "int8"; readonly value: number }
  | { readonly kind: "int16"; readonly value: number }
  | { readonly kind: "int32"; readonly value: number }
  | { readonly kind: "int64"; readonly value: bigint }
  | { readonly kind: "int128"; readonly value: Uint8Array }
  | { readonly kind: "uint8"; readonly value: number }
  | { readonly kind: "uint16"; readonly value: number }
  | { readonly kind: "uint32"; readonly value: number }
  | { readonly kind: "uint64"; readonly value: bigint }
  | { readonly kind: "uint128"; readonly value: Uint8Array }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "double"; readonly value: number }

export type ConsumerTarget =
  | { readonly kind: "single"; readonly partitionId: number; readonly name?: string }
  | { readonly kind: "group"; readonly name: string }

export type ConsumerOffsetTarget =
  | { readonly kind: "group"; readonly name: string }
  | { readonly kind: "consumer"; readonly name: string }

export interface LaserTransport {
  readonly kind: "apache-iggy"
  readonly iggyClient: SimpleClient
  sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array>
  ensureStream(name: string): Promise<void>
  ensureTopic(streamId: string, topicId: string, partitions: number): Promise<void>
  findTopicPartitionCount(streamId: string, topicId: string): Promise<number | undefined>
  getTopicPartitionCount(streamId: string, topicId: string): Promise<number>
  resolveStreamTopicIds?(
    streamId: string,
    topicId: string
  ): Promise<{ readonly streamId: number; readonly topicId: number }>
  resolveStreamTopicNames?(
    streamId: number,
    topicId: number
  ): Promise<{ readonly stream: string; readonly topic: string } | undefined>
  sendMessages(
    streamId: string,
    topicId: string,
    payloads: readonly Uint8Array[],
    routing: Routing
  ): Promise<void>
  // One message with explicit user-headers, keyed by `partitionKey` when
  // set (so one conversation/key stays ordered) or balanced across
  // partitions otherwise. The low-level primitive `Provenance` and
  // control-command publishing ride, distinct from the header-less
  // `sendMessages` every other producer path uses today.
  sendMessageWithHeaders(
    streamId: string,
    topicId: string,
    payload: Uint8Array,
    headers: ReadonlyMap<string, IggyHeaderValue>,
    partitionKey?: string | Uint8Array,
    partitionId?: number
  ): Promise<void>
  sendMessagesWithHeaders(
    streamId: string,
    topicId: string,
    messages: readonly MessageWithHeaders[],
    partitionKey?: string | Uint8Array,
    partitionId?: number
  ): Promise<void>
  pollMessages(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    strategy: PollingStrategy,
    count: number,
    autoCommit: boolean
  ): Promise<readonly PolledMessage[]>
  storeOffset(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    partitionId: number,
    offset: bigint
  ): Promise<void>
  getConsumerOffset?(
    streamId: string,
    topicId: string,
    target: ConsumerOffsetTarget,
    partitionId: number
  ): Promise<{ readonly storedOffset: bigint; readonly currentOffset: bigint } | undefined>
  joinConsumerGroup(streamId: string, topicId: string, name: string): Promise<void>
  leaveConsumerGroup(streamId: string, topicId: string, name: string): Promise<void>
  close(): Promise<void>
}

function toIggyConsumer(target: ConsumerTarget) {
  return target.kind === "single"
    ? target.name === undefined
      ? Consumer.Single
      : { kind: 1 as const, id: target.name }
    : Consumer.Group(target.name)
}

function toIggyOffsetConsumer(target: ConsumerOffsetTarget) {
  return target.kind === "group"
    ? Consumer.Group(target.name)
    : { kind: 1 as const, id: target.name }
}

function toPartitioning(routing: Routing) {
  switch (routing.kind) {
    case "balanced":
      return Partitioning.Balanced
    case "key":
      return Partitioning.MessageKey(Buffer.from(routing.key))
    case "partition":
      return Partitioning.PartitionId(routing.partition)
  }
}

function toIggyPollingStrategy(strategy: PollingStrategy) {
  switch (strategy.kind) {
    case "first":
      return IggyPollingStrategy.First
    case "last":
      return IggyPollingStrategy.Last
    case "next":
      return IggyPollingStrategy.Next
    case "offset":
      return IggyPollingStrategy.Offset(strategy.value)
    case "timestamp":
      return IggyPollingStrategy.Timestamp(strategy.value)
  }
}

function toIggyHeaderValue(value: IggyHeaderValue) {
  switch (value.kind) {
    case "raw":
      return IggyHeaderValueFactory.Raw(Buffer.from(value.value))
    case "string":
      return IggyHeaderValueFactory.String(value.value)
    case "bool":
      return IggyHeaderValueFactory.Bool(value.value)
    case "int8":
      return IggyHeaderValueFactory.Int8(value.value)
    case "int16":
      return IggyHeaderValueFactory.Int16(value.value)
    case "int32":
      return IggyHeaderValueFactory.Int32(value.value)
    case "int64":
      return IggyHeaderValueFactory.Int64(value.value)
    case "int128":
      return IggyHeaderValueFactory.Int128(Buffer.from(value.value))
    case "uint8":
      return IggyHeaderValueFactory.Uint8(value.value)
    case "uint16":
      return IggyHeaderValueFactory.Uint16(value.value)
    case "uint32":
      return IggyHeaderValueFactory.Uint32(value.value)
    case "uint64":
      return IggyHeaderValueFactory.Uint64(value.value)
    case "uint128":
      return IggyHeaderValueFactory.Uint128(Buffer.from(value.value))
    case "float":
      return IggyHeaderValueFactory.Float(value.value)
    case "double":
      return IggyHeaderValueFactory.Double(value.value)
  }
}

// The numeric header-kind scheme apache-iggy's wire layer uses internally
// (`HeaderKind` in `wire/message/header.type.ts`: Raw=1, String=2, Bool=3,
// Int8=4, Int16=5, Int32=6, Int64=7, Int128=8, Uint8=9, Uint16=10,
// Uint32=11, Uint64=12, Uint128=13, Float=14, Double=15). Not itself
// exported from the package root (only the `HeaderValue`/`HeaderKeyFactory`
// factories are), so this mirrors it directly rather than importing it,
// to decode a polled message's raw `{kind: number, value}` header entries
// back into `IggyHeaderValue`.
const HEADER_KIND_BY_NUMBER: Readonly<Record<number, IggyHeaderValue["kind"]>> = {
  1: "raw",
  2: "string",
  3: "bool",
  4: "int8",
  5: "int16",
  6: "int32",
  7: "int64",
  8: "int128",
  9: "uint8",
  10: "uint16",
  11: "uint32",
  12: "uint64",
  13: "uint128",
  14: "float",
  15: "double"
}

function fromParsedHeaderValue(kind: number, value: unknown): IggyHeaderValue | undefined {
  const tag = HEADER_KIND_BY_NUMBER[kind]
  if (tag === undefined) return undefined
  switch (tag) {
    case "raw":
    case "int128":
    case "uint128": {
      if (!(value instanceof Buffer)) return undefined
      return { kind: tag, value: new Uint8Array(value) }
    }
    case "string":
      return typeof value === "string" ? { kind: tag, value } : undefined
    case "bool":
      return typeof value === "boolean" ? { kind: tag, value } : undefined
    case "int64":
    case "uint64":
      return typeof value === "bigint" ? { kind: tag, value } : undefined
    case "int8":
    case "int16":
    case "int32":
    case "uint8":
    case "uint16":
    case "uint32":
    case "float":
    case "double":
      return typeof value === "number" ? { kind: tag, value } : undefined
  }
}

function parsedHeadersToMap(
  entries: readonly {
    readonly key: { readonly value: unknown }
    readonly value: { readonly kind: number; readonly value: unknown }
  }[]
): ReadonlyMap<string, IggyHeaderValue> {
  const map = new Map<string, IggyHeaderValue>()
  for (const entry of entries) {
    if (typeof entry.key.value !== "string") continue
    const value = fromParsedHeaderValue(entry.value.kind, entry.value.value)
    if (value !== undefined) map.set(entry.key.value, value)
  }
  return map
}

interface ParsedConnectionString {
  readonly host: string
  readonly port: number
  readonly credentials: ClientCredentials
  readonly tls: boolean
  readonly ca?: string
}

function laserDataHost(host: string): boolean {
  const normalized = host.toLowerCase()
  return (
    normalized === "laserdata.cloud" ||
    normalized.endsWith(".laserdata.cloud") ||
    normalized === "laserdata.com" ||
    normalized.endsWith(".laserdata.com")
  )
}

export function parseConnectionString(
  connectionString: string,
  env: Readonly<Record<string, string | undefined>> = process.env
): ParsedConnectionString {
  let url: URL
  try {
    url = new URL(connectionString)
  } catch (cause) {
    throw new ConfigError(`invalid connection string: ${connectionString}`, { cause })
  }

  if (url.protocol !== "iggy:" && url.protocol !== "iggys:" && url.protocol !== "iggy+tcp:") {
    throw new ConfigError(`unsupported connection scheme: ${url.protocol}`)
  }

  if (!url.hostname) {
    throw new ConfigError(`connection string missing host: ${connectionString}`)
  }

  const port = url.port ? Number(url.port) : 8090
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new ConfigError(`connection string has invalid port: ${connectionString}`)
  }

  const username = decodeURIComponent(url.username)
  const password = decodeURIComponent(url.password)
  const credentials: ClientCredentials =
    username.length === 0
      ? { username: "iggy", password: "iggy" }
      : password.length === 0
        ? { token: username }
        : { username, password }

  const autoTls = env["LASER_NO_TLS"] === undefined && laserDataHost(url.hostname)
  const tls = url.protocol === "iggys:" || url.searchParams.get("tls") === "true" || autoTls
  const caPath = url.searchParams.get("tls_ca_file") ?? env["LASER_TLS_CERT"]
  let ca: string | undefined
  if (caPath !== undefined && caPath.length > 0) {
    try {
      ca = readFileSync(caPath, "utf8")
    } catch (cause) {
      throw new ConfigError(`failed to read TLS CA file: ${caPath}`, { cause })
    }
  } else if (autoTls) {
    ca = LASERDATA_ROOT_CA
  }

  return { host: url.hostname, port, credentials, tls, ...(ca !== undefined ? { ca } : {}) }
}

interface ConnectedClient {
  readonly client: SimpleClient
  readonly raw: RawClient
}

interface RawClientConnection {
  readonly connection: {
    on(event: "error", listener: (cause?: unknown) => void): void
    once(event: "error", listener: (cause?: unknown) => void): void
    off(event: "error", listener: (cause?: unknown) => void): void
  }
}

async function connectSimpleClient(parsed: ParsedConnectionString): Promise<ConnectedClient> {
  const config: ClientConfig = parsed.tls
    ? {
        transport: "TLS",
        options: {
          port: parsed.port,
          host: parsed.host,
          ...(parsed.ca !== undefined ? { ca: parsed.ca } : {})
        },
        credentials: parsed.credentials,
        reconnect: { enabled: false, interval: 0, maxRetries: 0 }
      }
    : {
        transport: "TCP",
        options: { port: parsed.port, host: parsed.host },
        credentials: parsed.credentials,
        reconnect: { enabled: false, interval: 0, maxRetries: 0 }
      }

  let raw: RawClient | undefined
  try {
    raw = getRawClient(config)
    const connection = (raw as RawClient & RawClientConnection).connection
    const client = new SimpleClient(raw)
    // With reconnect disabled a refused or dropped socket surfaces only as a
    // connection `error` event while the upstream connect promise stays
    // pending forever, so race the handshake against that event.
    await new Promise<void>((resolve, reject) => {
      const failed = (cause?: unknown): void => {
        reject(cause instanceof Error ? cause : new Error(String(cause)))
      }
      connection.once("error", failed)
      client.system.ping().then(() => {
        connection.off("error", failed)
        resolve()
      }, reject)
    })
    // A later emitter `error` with no listener would crash the process, the
    // read-stream watchers own disconnect detection, so swallow it here.
    connection.on("error", () => undefined)
    return { client, raw }
  } catch (cause) {
    raw?.destroy()
    throw new TransportError(`failed to connect to ${parsed.host}:${String(parsed.port)}`, false, {
      cause
    })
  }
}

export class ApacheIggyTransport implements LaserTransport {
  readonly kind = "apache-iggy" as const
  private readonly reconnectLock = new Mutex()
  private readonly disconnected = new WeakSet<SimpleClient>()
  private readonly consumerGroups = new Map<
    string,
    { readonly streamId: string; readonly topicId: string; readonly name: string }
  >()
  private closed = false

  private constructor(
    private client: SimpleClient,
    private readonly connection: ParsedConnectionString | undefined,
    private readonly ownership: ClientOwnership
  ) {}

  get iggyClient(): SimpleClient {
    return this.client
  }

  static async connect(connectionString: string): Promise<ApacheIggyTransport> {
    const parsed = parseConnectionString(connectionString)
    const connected = await connectSimpleClient(parsed)
    const transport = new ApacheIggyTransport(connected.client, parsed, "owned")
    transport.watch(connected)
    return transport
  }

  static fromClient(
    client: SimpleClient,
    ownership: ClientOwnership = "borrowed"
  ): ApacheIggyTransport {
    return new ApacheIggyTransport(client, undefined, ownership)
  }

  private async execute<Value>(
    operation: (client: SimpleClient) => Promise<Value>,
    message: string
  ): Promise<Value> {
    const stale = this.client
    if (this.disconnected.has(stale)) {
      await this.reconnect(stale)
      return operation(this.client)
    }
    try {
      return await operation(stale)
    } catch (firstCause) {
      if (!this.disconnected.has(stale)) {
        throw new TransportError(message, true, { cause: firstCause })
      }
      try {
        await this.reconnect(stale)
        return await operation(this.client)
      } catch (cause) {
        throw new TransportError(message, true, { cause: cause ?? firstCause })
      }
    }
  }

  private reconnect(stale: SimpleClient): Promise<void> {
    return this.reconnectLock.runExclusive(async () => {
      if (this.closed) throw new TransportError("transport is closed", false)
      if (this.client !== stale) return
      if (this.connection === undefined) {
        throw new TransportError("an injected client cannot be reconnected by Laser", true)
      }
      await stale.destroy().catch(() => undefined)
      const deadline = performance.now() + RECONNECT_DEADLINE_MS
      let lastError: unknown
      while (performance.now() < deadline) {
        try {
          const connected = await connectSimpleClient(this.connection)
          try {
            for (const group of this.consumerGroups.values()) {
              await connected.client.group.ensureAndJoin(group.streamId, group.topicId, group.name)
            }
          } catch (cause) {
            await connected.client.destroy().catch(() => undefined)
            throw cause
          }
          this.client = connected.client
          this.watch(connected)
          return
        } catch (error) {
          lastError = error
          await new Promise((resolve) => setTimeout(resolve, RECONNECT_INTERVAL_MS))
        }
      }
      throw new TransportError("reconnect deadline exceeded", true, { cause: lastError })
    })
  }

  private watch(connected: ConnectedClient): void {
    const markDisconnected = (): void => {
      this.disconnected.add(connected.client)
    }
    const stream = connected.raw.getReadStream()
    stream.once("error", markDisconnected)
    stream.once("end", markDisconnected)
    stream.once("close", markDisconnected)
  }

  async sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array> {
    const buffer = Buffer.from(payload)
    const reply = await this.execute(
      (client) => client.sendBinaryRequest(code, buffer),
      `managed command ${String(code)} failed`
    )
    return new Uint8Array(reply.buffer, reply.byteOffset, reply.byteLength)
  }

  async ensureStream(name: string): Promise<void> {
    await this.execute(
      (client) => client.stream.ensure(name),
      `failed to ensure stream \`${name}\``
    )
  }

  async ensureTopic(streamId: string, topicId: string, partitions: number): Promise<void> {
    await this.execute(
      (client) => client.topic.ensure(streamId, topicId, partitions),
      `failed to ensure topic \`${topicId}\` on stream \`${streamId}\``
    )
  }

  async findTopicPartitionCount(streamId: string, topicId: string): Promise<number | undefined> {
    const topic = await this.execute(
      (client) => client.topic.get({ streamId, topicId }),
      `failed to read topic \`${topicId}\``
    )
    return topic?.partitionsCount
  }

  async getTopicPartitionCount(streamId: string, topicId: string): Promise<number> {
    const partitions = await this.findTopicPartitionCount(streamId, topicId)
    if (partitions === undefined) {
      throw new TransportError(
        `topic \`${topicId}\` on stream \`${streamId}\` does not exist`,
        false
      )
    }
    return partitions
  }

  async resolveStreamTopicIds(
    streamId: string,
    topicId: string
  ): Promise<{ readonly streamId: number; readonly topicId: number }> {
    const [stream, topic] = await this.execute(
      (client) =>
        Promise.all([client.stream.get({ streamId }), client.topic.get({ streamId, topicId })]),
      `failed to resolve topic \`${topicId}\``
    )
    if (stream === null) {
      throw new TransportError(`stream \`${streamId}\` does not exist`, false)
    }
    if (topic === null) {
      throw new TransportError(
        `topic \`${topicId}\` on stream \`${streamId}\` does not exist`,
        false
      )
    }
    return { streamId: stream.id, topicId: topic.id }
  }

  async resolveStreamTopicNames(
    streamId: number,
    topicId: number
  ): Promise<{ readonly stream: string; readonly topic: string } | undefined> {
    const stream = await this.execute(
      (client) => client.stream.get({ streamId }),
      `failed to resolve stream ${String(streamId)}`
    )
    if (stream === null) return undefined
    const topic = await this.execute(
      (client) => client.topic.get({ streamId, topicId }),
      `failed to resolve topic ${String(topicId)}`
    )
    return topic === null ? undefined : { stream: stream.name, topic: topic.name }
  }

  async sendMessages(
    streamId: string,
    topicId: string,
    payloads: readonly Uint8Array[],
    routing: Routing
  ): Promise<void> {
    await this.execute(
      (client) =>
        client.message.send({
          streamId,
          topicId,
          messages: payloads.map((payload) => ({ payload: Buffer.from(payload) })),
          partition: toPartitioning(routing)
        }),
      `failed to send to topic \`${topicId}\``
    )
  }

  async sendMessageWithHeaders(
    streamId: string,
    topicId: string,
    payload: Uint8Array,
    headers: ReadonlyMap<string, IggyHeaderValue>,
    partitionKey?: string | Uint8Array,
    partitionId?: number
  ): Promise<void> {
    await this.sendMessagesWithHeaders(
      streamId,
      topicId,
      [{ payload, headers }],
      partitionKey,
      partitionId
    )
  }

  async sendMessagesWithHeaders(
    streamId: string,
    topicId: string,
    messages: readonly MessageWithHeaders[],
    partitionKey?: string | Uint8Array,
    partitionId?: number
  ): Promise<void> {
    await this.execute(
      (client) =>
        client.message.send({
          streamId,
          topicId,
          messages: messages.map(({ payload, headers }) => ({
            payload: Buffer.from(payload),
            headers: [...headers].map(([key, value]) => ({
              key: HeaderKeyFactory.String(key),
              value: toIggyHeaderValue(value)
            }))
          })),
          partition:
            partitionId !== undefined
              ? Partitioning.PartitionId(partitionId)
              : partitionKey !== undefined
                ? Partitioning.MessageKey(
                    typeof partitionKey === "string"
                      ? Buffer.from(partitionKey, "utf8")
                      : Buffer.from(partitionKey)
                  )
                : Partitioning.Balanced
        }),
      `failed to send to topic \`${topicId}\``
    )
  }

  async pollMessages(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    strategy: PollingStrategy,
    count: number,
    autoCommit: boolean
  ): Promise<readonly PolledMessage[]> {
    const reply = await this.execute(
      (client) =>
        client.message.poll({
          streamId,
          topicId,
          partitionId: target.kind === "single" ? target.partitionId : null,
          consumer: toIggyConsumer(target),
          pollingStrategy: toIggyPollingStrategy(strategy),
          count,
          autocommit: autoCommit
        }),
      `failed to poll topic \`${topicId}\``
    )
    return reply.messages.map((message) => ({
      payload: new Uint8Array(
        message.payload.buffer,
        message.payload.byteOffset,
        message.payload.byteLength
      ),
      partitionId: reply.partitionId,
      offset: message.headers.offset,
      timestampMicros: BigInt(message.headers.timestamp.getTime()) * 1_000n,
      headers: parsedHeadersToMap(message.userHeaders)
    }))
  }

  // The server rejects `partitionId: null` for `StoreOffset` even for a
  // group consumer (confirmed against a real server, the upstream doc
  // comment calling it optional for groups does not hold): every commit
  // names the exact partition the committed message came from, for both
  // single and group consumers.
  async storeOffset(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    partitionId: number,
    offset: bigint
  ): Promise<void> {
    await this.execute(
      (client) =>
        client.offset.store({
          streamId,
          topicId,
          consumer: toIggyConsumer(target),
          partitionId,
          offset
        }),
      `failed to store offset for topic \`${topicId}\``
    )
  }

  async getConsumerOffset(
    streamId: string,
    topicId: string,
    target: ConsumerOffsetTarget,
    partitionId: number
  ): Promise<{ readonly storedOffset: bigint; readonly currentOffset: bigint } | undefined> {
    const offset = await this.execute(
      (client) =>
        client.offset.get({
          streamId,
          topicId,
          consumer: toIggyOffsetConsumer(target),
          partitionId
        }),
      `failed to read offset for topic \`${topicId}\``
    )
    return offset === null
      ? undefined
      : { storedOffset: offset.storedOffset, currentOffset: offset.currentOffset }
  }

  async joinConsumerGroup(streamId: string, topicId: string, name: string): Promise<void> {
    await this.execute(
      (client) => client.group.ensureAndJoin(streamId, topicId, name),
      `failed to join consumer group \`${name}\``
    )
    this.consumerGroups.set(`${streamId}\0${topicId}\0${name}`, { streamId, topicId, name })
  }

  async leaveConsumerGroup(streamId: string, topicId: string, name: string): Promise<void> {
    await this.execute(
      (client) => client.group.leave({ streamId, topicId, groupId: name }),
      `failed to leave consumer group \`${name}\``
    )
    this.consumerGroups.delete(`${streamId}\0${topicId}\0${name}`)
  }

  async close(): Promise<void> {
    this.closed = true
    this.consumerGroups.clear()
    if (this.ownership === "owned") await this.client.destroy()
  }
}
