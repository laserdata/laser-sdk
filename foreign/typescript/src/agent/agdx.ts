import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { CancelledError, InvalidError, RejectedError, TimeoutError } from "../client/errors.js"
import type {
  IggyHeaderValue,
  LaserTransport,
  MessageWithHeaders,
  PolledMessage
} from "../iggy/apache-iggy.js"
import { mintUlidValue, type UlidSource } from "../runtime/ulid.js"
import { type SigningKey } from "../signing.js"
import {
  type AgentId as SdkAgentId,
  type ConversationId as SdkConversationId
} from "../types/ids.js"
import {
  AgentKind,
  type AgentEnvelope,
  type AgentErrorBody,
  type AgentId as WireAgentId,
  type IdempotencyKey,
  type TaskState,
  type TokenUsage,
  chunkEnvelope,
  commandEnvelope,
  decodeAgentEnvelope,
  decodeAgentErrorBody,
  encodeAgentErrorBody,
  errorEnvelope,
  eventEnvelope,
  responseEnvelope,
  parseAgentId,
  statusEnvelope,
  validateAgentEnvelope,
  withCause,
  withCorrelation,
  withDeadlineMicros,
  withIdempotencyKey,
  withMetadata,
  withOperation,
  withTarget,
  withTaskState,
  withTool,
  withUsage
} from "../wire/agent.js"
import { canonicalAgentRecord, type CanonicalHeader } from "../wire/agent-record.js"
import { decodeOne, encodeNamed, expectMap } from "../wire/cbor.js"
import { AGENT_OP_VERSION } from "../wire/codes.js"
import { ContentType, contentTypeCode } from "../wire/content.js"
import {
  ChannelId,
  ConversationId,
  CorrelationId,
  type LogPosition,
  RecordId
} from "../wire/ids.js"
import type { Value } from "../wire/value.js"

export const DEFAULT_CHUNK_FLUSH_BYTES = 512
export const DEFAULT_CHUNK_LINGER_MS = 20
export const MAX_CHUNK_BODY_BYTES = 64 * 1024

const REPLY_BATCH = 200
const REPLY_POLL_INTERVAL_MS = 50
const textDecoder = new TextDecoder("utf-8", { fatal: true })

export type AgdxLogPosition = LogPosition

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted === true) {
      reject(new CancelledError("AGDX reply wait aborted", { cause: signal.reason }))
      return
    }
    const onAbort = (): void => {
      clearTimeout(timer)
      reject(new CancelledError("AGDX reply wait aborted", { cause: signal?.reason }))
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort)
      resolve()
    }, ms)
    signal?.addEventListener("abort", onAbort, { once: true })
  })
}

function wireAgentId(agent: SdkAgentId): WireAgentId {
  return parseAgentId(agent.asString())
}

function u32FromLittleEndian(header: CanonicalHeader): number {
  if (header.bytes.byteLength !== 4) throw new InvalidError("AGDX u32 header must be 4 bytes")
  return new DataView(
    header.bytes.buffer,
    header.bytes.byteOffset,
    header.bytes.byteLength
  ).getUint32(0, true)
}

function iggyHeader(header: CanonicalHeader): IggyHeaderValue {
  switch (header.kind) {
    case "u32":
      return { kind: "uint32", value: u32FromLittleEndian(header) }
    case "u8": {
      const value = header.bytes[0]
      if (header.bytes.byteLength !== 1 || value === undefined) {
        throw new InvalidError("AGDX u8 header must be 1 byte")
      }
      return { kind: "uint8", value }
    }
    case "uint128":
      if (header.bytes.byteLength !== 16) {
        throw new InvalidError("AGDX uint128 header must be 16 bytes")
      }
      return { kind: "uint128", value: header.bytes.slice() }
    case "string":
      return { kind: "string", value: textDecoder.decode(header.bytes) }
  }
}

function assemble(envelope: AgentEnvelope, contentType: ContentType): MessageWithHeaders {
  validateAgentEnvelope(envelope)
  const record = canonicalAgentRecord(envelope, contentType)
  return {
    payload: record.payload,
    headers: new Map(
      Array.from(record.headers, ([key, header]) => [key, iggyHeader(header)] as const)
    )
  }
}

function mintRecordId(source?: UlidSource): RecordId {
  return RecordId.fromU128(mintUlidValue(source))
}

function mintCorrelationId(source?: UlidSource): CorrelationId {
  return CorrelationId.fromU128(mintUlidValue(source))
}

function mintChannelId(source?: UlidSource): ChannelId {
  return ChannelId.fromU128(mintUlidValue(source))
}

class AgdxReplyReader {
  private partitions: number | undefined
  private offsets: bigint[] = []

  private constructor(
    private readonly transport: LaserTransport,
    private readonly stream: string,
    private readonly topic: string
  ) {}

  static async atTail(
    transport: LaserTransport,
    stream: string,
    topic: string
  ): Promise<AgdxReplyReader> {
    const reader = new AgdxReplyReader(transport, stream, topic)
    const partitions = await transport.findTopicPartitionCount(stream, topic)
    if (partitions === undefined) return reader
    reader.partitions = partitions
    reader.offsets = new Array<bigint>(partitions).fill(0n)
    for (let partitionId = 0; partitionId < partitions; partitionId += 1) {
      const messages = await transport.pollMessages(
        stream,
        topic,
        { kind: "single", partitionId },
        { kind: "last" },
        1,
        false
      )
      const last = messages[messages.length - 1]
      if (last !== undefined) reader.offsets[partitionId] = last.offset + 1n
    }
    return reader
  }

  async next(correlation: CorrelationId): Promise<AgentEnvelope | undefined> {
    if (this.partitions === undefined) {
      this.partitions = await this.transport.findTopicPartitionCount(this.stream, this.topic)
      if (this.partitions === undefined) return undefined
    }
    if (this.offsets.length < this.partitions) {
      this.offsets.push(...new Array<bigint>(this.partitions - this.offsets.length).fill(0n))
    }
    for (let partitionId = 0; partitionId < this.partitions; partitionId += 1) {
      const messages = await this.transport.pollMessages(
        this.stream,
        this.topic,
        { kind: "single", partitionId },
        { kind: "offset", value: this.offsets[partitionId] ?? 0n },
        REPLY_BATCH,
        false
      )
      for (const message of messages) {
        this.offsets[partitionId] = message.offset + 1n
        const envelope = decodeEnvelope(message)
        if (
          envelope !== undefined &&
          envelope.correlation?.equals(correlation) === true &&
          (envelope.kind === AgentKind.Response || envelope.kind === AgentKind.Error)
        ) {
          return envelope
        }
      }
    }
    return undefined
  }
}

function decodeEnvelope(message: PolledMessage): AgentEnvelope | undefined {
  try {
    const context = "AGDX reply"
    return decodeAgentEnvelope(expectMap(decodeOne(message.payload, context), context), context)
  } catch {
    return undefined
  }
}

export interface Agdx {
  readonly topicName: string
  command(correlation: CorrelationId, body: BytesLike): AgdxSend
  respond(correlation: CorrelationId, body: BytesLike): AgdxSend
  emit(body: BytesLike): AgdxSend
  status(operation: string): AgdxSend
  fail(correlation: CorrelationId, error: AgentErrorBody): AgdxSend
  stream(correlation: CorrelationId, purpose: string): AgdxStream
  requestInput(
    replyTopic: string,
    prompt: BytesLike,
    timeoutMs: number,
    options?: { readonly signal?: AbortSignal }
  ): Promise<Uint8Array>
}

interface AgdxPublisher {
  prepare(
    envelope: AgentEnvelope,
    contentType: ContentType,
    signingKey?: SigningKey
  ): Promise<AgentEnvelope>
  publish(envelope: AgentEnvelope, contentType: ContentType): Promise<RecordId | undefined>
  assemble(envelope: AgentEnvelope, contentType: ContentType): MessageWithHeaders
  publishBatch(messages: readonly MessageWithHeaders[]): Promise<void>
}

class AgdxClient implements Agdx, AgdxPublisher {
  constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    readonly topicName: string,
    readonly sourceId: WireAgentId,
    readonly conversationId: ConversationId,
    private readonly ulidSource?: UlidSource,
    private readonly govern?: (envelope: AgentEnvelope, willSign: boolean) => Promise<Uint8Array>
  ) {}

  command(correlation: CorrelationId, body: BytesLike): AgdxSend {
    return this.sendOf(
      commandEnvelope(
        mintRecordId(this.ulidSource),
        this.conversationId,
        this.sourceId,
        correlation,
        ownedBytes(body)
      )
    )
  }

  respond(correlation: CorrelationId, body: BytesLike): AgdxSend {
    return this.sendOf(
      responseEnvelope(
        mintRecordId(this.ulidSource),
        this.conversationId,
        this.sourceId,
        correlation,
        ownedBytes(body)
      )
    )
  }

  emit(body: BytesLike): AgdxSend {
    return this.sendOf(
      eventEnvelope(
        mintRecordId(this.ulidSource),
        this.conversationId,
        this.sourceId,
        ownedBytes(body)
      )
    )
  }

  status(operation: string): AgdxSend {
    return this.sendOf(
      statusEnvelope(mintRecordId(this.ulidSource), this.conversationId, this.sourceId, operation)
    )
  }

  fail(correlation: CorrelationId, error: AgentErrorBody): AgdxSend {
    const body = encodeNamed(encodeAgentErrorBody(error))
    return this.sendOf(
      errorEnvelope(
        mintRecordId(this.ulidSource),
        this.conversationId,
        this.sourceId,
        correlation,
        body
      )
    ).contentType(ContentType.Cbor)
  }

  stream(correlation: CorrelationId, purpose: string): AgdxStream {
    return new AgdxStreamWriter(
      this,
      this.sourceId,
      this.conversationId,
      correlation,
      mintChannelId(this.ulidSource),
      purpose,
      this.ulidSource
    )
  }

  async requestInput(
    replyTopic: string,
    prompt: BytesLike,
    timeoutMs: number,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<Uint8Array> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("requestInput() timeout must be a non-negative finite number")
    }
    const interrupt = mintCorrelationId(this.ulidSource)
    const reader = await AgdxReplyReader.atTail(this.transport, this.streamName, replyTopic)
    await this.command(interrupt, prompt).send()
    const deadline = Date.now() + timeoutMs
    for (;;) {
      if (options.signal?.aborted === true) {
        throw new CancelledError("AGDX reply wait aborted", { cause: options.signal.reason })
      }
      const reply = await reader.next(interrupt)
      if (reply !== undefined) {
        if (reply.kind === AgentKind.Error) {
          let message = "the input request was rejected"
          try {
            const context = "AGDX error body"
            message =
              decodeAgentErrorBody(expectMap(decodeOne(reply.body, context), context), context)
                .message ?? message
          } catch {
            // Keep the stable rejection message when the optional error body is malformed.
          }
          throw new RejectedError(message)
        }
        return reply.body
      }
      const remaining = deadline - Date.now()
      if (remaining <= 0) break
      await delay(Math.min(REPLY_POLL_INTERVAL_MS, remaining), options.signal)
    }
    throw new TimeoutError("the AGDX reply")
  }

  async publish(envelope: AgentEnvelope, contentType: ContentType): Promise<RecordId | undefined> {
    const message = assemble(envelope, contentType)
    await this.transport.sendMessagesWithHeaders(
      this.streamName,
      this.topicName,
      [message],
      this.conversationId.toString()
    )
    return envelope.record
  }

  async prepare(
    envelope: AgentEnvelope,
    contentType: ContentType,
    signingKey?: SigningKey
  ): Promise<AgentEnvelope> {
    const body =
      this.govern === undefined
        ? envelope.body
        : await this.govern(envelope, signingKey !== undefined)
    const governed = body === envelope.body ? envelope : { ...envelope, body }
    if (signingKey === undefined) return governed
    return {
      ...governed,
      signature: signingKey.signWithContext(governed, {
        contentType: contentTypeCode(contentType),
        agentVersion: AGENT_OP_VERSION
      })
    }
  }

  assemble(envelope: AgentEnvelope, contentType: ContentType): MessageWithHeaders {
    return assemble(envelope, contentType)
  }

  async publishBatch(messages: readonly MessageWithHeaders[]): Promise<void> {
    await this.transport.sendMessagesWithHeaders(
      this.streamName,
      this.topicName,
      messages,
      this.conversationId.toString()
    )
  }

  private sendOf(envelope: AgentEnvelope): AgdxSend {
    return new AgdxSendBuilder(this, envelope)
  }
}

export function createAgdx(
  transport: LaserTransport,
  streamName: string,
  topicName: string,
  source: SdkAgentId,
  conversation: SdkConversationId,
  ulidSource?: UlidSource,
  govern?: (envelope: AgentEnvelope, willSign: boolean) => Promise<Uint8Array>
): Agdx {
  return new AgdxClient(
    transport,
    streamName,
    topicName,
    parseAgentId(source.asString()),
    ConversationId.parse(conversation.toString()),
    ulidSource,
    govern
  )
}

export interface AgdxSend {
  withTarget(target: SdkAgentId): this
  withCause(cause: RecordId, causeAt?: LogPosition): this
  withCorrelation(correlation: CorrelationId): this
  withIdempotencyKey(key: IdempotencyKey): this
  withDeadlineMicros(deadlineMicros: bigint): this
  withTaskState(state: TaskState): this
  withOperation(operation: string): this
  withTool(tool: string): this
  withUsage(usage: TokenUsage): this
  withMetadata(key: string, value: Value): this
  last(): this
  contentType(contentType: ContentType): this
  body(body: BytesLike): this
  signedBy(key: SigningKey): this
  send(): Promise<RecordId | undefined>
}

class AgdxSendBuilder implements AgdxSend {
  private contentTypeValue: ContentType = ContentType.Raw
  private sent = false
  private signingKey: SigningKey | undefined

  constructor(
    private readonly agdx: AgdxPublisher,
    private envelope: AgentEnvelope
  ) {}

  withTarget(target: SdkAgentId): this {
    this.envelope = withTarget(this.envelope, wireAgentId(target))
    return this
  }

  withCause(cause: RecordId, causeAt?: LogPosition): this {
    this.envelope = withCause(this.envelope, cause, causeAt)
    return this
  }

  withCorrelation(correlation: CorrelationId): this {
    this.envelope = withCorrelation(this.envelope, correlation)
    return this
  }

  withIdempotencyKey(key: IdempotencyKey): this {
    this.envelope = withIdempotencyKey(this.envelope, key)
    return this
  }

  withDeadlineMicros(deadlineMicros: bigint): this {
    this.envelope = withDeadlineMicros(this.envelope, deadlineMicros)
    return this
  }

  withTaskState(state: TaskState): this {
    this.envelope = withTaskState(this.envelope, state)
    return this
  }

  withOperation(operation: string): this {
    this.envelope = withOperation(this.envelope, operation)
    return this
  }

  withTool(tool: string): this {
    this.envelope = withTool(this.envelope, tool)
    return this
  }

  withUsage(usage: TokenUsage): this {
    this.envelope = withUsage(this.envelope, usage)
    return this
  }

  withMetadata(key: string, value: Value): this {
    this.envelope = withMetadata(this.envelope, key, value)
    return this
  }

  last(): this {
    this.envelope = { ...this.envelope, last: true }
    return this
  }

  contentType(contentType: ContentType): this {
    this.contentTypeValue = contentType
    return this
  }

  body(body: BytesLike): this {
    this.envelope = { ...this.envelope, body: ownedBytes(body) }
    return this
  }

  signedBy(key: SigningKey): this {
    this.signingKey = key
    return this
  }

  async send(): Promise<RecordId | undefined> {
    if (this.sent) throw new InvalidError("an AGDX send can only be performed once")
    this.sent = true
    const envelope = await this.agdx.prepare(this.envelope, this.contentTypeValue, this.signingKey)
    return this.agdx.publish(envelope, this.contentTypeValue)
  }
}

interface ChunkBuffer {
  readonly maxChunks: number
  readonly lingerMs: number
  firstAt: number | undefined
  messages: MessageWithHeaders[]
}

export interface AgdxStream {
  readonly channel: ChannelId
  withDeadlineMicros(deadlineMicros: bigint): this
  withTarget(target: SdkAgentId): this
  contentType(contentType: ContentType): this
  buffered(maxChunks: number, lingerMs: number): this
  write(body: BytesLike): Promise<void>
  flush(): Promise<void>
  finish(finishReason: string, usage?: TokenUsage): Promise<void>
  fail(error: AgentErrorBody): Promise<void>
}

class AgdxStreamWriter implements AgdxStream {
  private sequence = 0n
  private deadlineMicros: bigint | undefined
  private target: WireAgentId | undefined
  private contentTypeValue: ContentType = ContentType.Raw
  private buffer: ChunkBuffer | undefined
  private closed = false

  constructor(
    private readonly agdx: AgdxPublisher,
    private readonly source: WireAgentId,
    private readonly conversation: ConversationId,
    private readonly correlation: CorrelationId,
    readonly channel: ChannelId,
    private readonly purpose: string,
    private readonly ulidSource?: UlidSource
  ) {}

  withDeadlineMicros(deadlineMicros: bigint): this {
    this.deadlineMicros = deadlineMicros
    return this
  }

  withTarget(target: SdkAgentId): this {
    this.target = wireAgentId(target)
    return this
  }

  contentType(contentType: ContentType): this {
    this.contentTypeValue = contentType
    return this
  }

  buffered(maxChunks: number, lingerMs: number): this {
    if (!Number.isInteger(maxChunks) || maxChunks < 0) {
      throw new InvalidError("buffered() maxChunks must be a non-negative integer")
    }
    if (!Number.isFinite(lingerMs) || lingerMs < 0) {
      throw new InvalidError("buffered() lingerMs must be a non-negative finite number")
    }
    this.buffer = {
      maxChunks: Math.max(maxChunks, 1),
      lingerMs,
      firstAt: undefined,
      messages: []
    }
    return this
  }

  async write(body: BytesLike): Promise<void> {
    this.assertOpen()
    const bytes = ownedBytes(body)
    if (bytes.byteLength > MAX_CHUNK_BODY_BYTES) {
      throw new InvalidError(
        `chunk body is ${String(bytes.byteLength)}B, exceeds cap ${String(MAX_CHUNK_BODY_BYTES)}B`
      )
    }
    const envelope = this.chunk(bytes, false)
    this.sequence += 1n
    if (this.buffer === undefined) {
      await this.agdx.publish(envelope, this.contentTypeValue)
      return
    }
    this.buffer.firstAt ??= Date.now()
    this.buffer.messages.push(this.agdx.assemble(envelope, this.contentTypeValue))
    if (
      this.buffer.messages.length >= this.buffer.maxChunks ||
      Date.now() - this.buffer.firstAt >= this.buffer.lingerMs
    ) {
      await this.flush()
    }
  }

  async flush(): Promise<void> {
    if (this.buffer === undefined || this.buffer.messages.length === 0) return
    const messages = this.buffer.messages
    this.buffer.messages = []
    this.buffer.firstAt = undefined
    await this.agdx.publishBatch(messages)
  }

  async finish(finishReason: string, usage?: TokenUsage): Promise<void> {
    this.assertOpen()
    this.closed = true
    let envelope = this.chunk(new Uint8Array(0), true, finishReason)
    if (usage !== undefined) envelope = withUsage(envelope, usage)
    await this.sendTerminal(envelope, this.contentTypeValue)
  }

  async fail(error: AgentErrorBody): Promise<void> {
    this.assertOpen()
    this.closed = true
    let envelope = errorEnvelope(
      mintRecordId(this.ulidSource),
      this.conversation,
      this.source,
      this.correlation,
      encodeNamed(encodeAgentErrorBody(error))
    )
    envelope = { ...envelope, channel: this.channel, sequence: this.sequence }
    if (this.target !== undefined) envelope = withTarget(envelope, this.target)
    await this.sendTerminal(envelope, ContentType.Cbor)
  }

  private async sendTerminal(envelope: AgentEnvelope, contentType: ContentType): Promise<void> {
    if (this.buffer === undefined) {
      await this.agdx.publish(envelope, contentType)
      return
    }
    this.buffer.messages.push(this.agdx.assemble(envelope, contentType))
    const messages = this.buffer.messages
    this.buffer.messages = []
    this.buffer.firstAt = undefined
    await this.agdx.publishBatch(messages)
  }

  private chunk(body: Uint8Array, last: boolean, finishReason?: string): AgentEnvelope {
    let envelope = chunkEnvelope(
      this.conversation,
      this.source,
      this.correlation,
      this.channel,
      this.sequence,
      body
    )
    if (this.sequence === 0n) {
      envelope = withOperation(envelope, this.purpose)
      if (this.deadlineMicros !== undefined) {
        envelope = withDeadlineMicros(envelope, this.deadlineMicros)
      }
    }
    if (this.target !== undefined) envelope = withTarget(envelope, this.target)
    if (last) {
      envelope = {
        ...envelope,
        last: true,
        ...(finishReason !== undefined ? { finishReason } : {})
      }
    }
    return envelope
  }

  private assertOpen(): void {
    if (this.closed) throw new InvalidError("an AGDX stream is already terminal")
  }
}
