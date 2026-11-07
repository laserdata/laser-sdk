import { type BlobStore, checkIn } from "../blob.js"
import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { type CallOptions, abortable, delay, throwIfAborted } from "../client/call-options.js"
import { InvalidError, PolicyBlockedError, RejectedError, TimeoutError } from "../client/errors.js"
import type { LaserTransport, MessageWithHeaders } from "../iggy/apache-iggy.js"
import type { HeaderValue } from "../stream/header-value.js"
import type { UlidSource } from "../runtime/ulid.js"
import { type KeyRegistry, type SigningKey } from "../signing.js"
import { decodeAgentMessage } from "./reliable-consumer.js"
import { replyTopicFor, resolveAgentPartition, resolveAgentTopic } from "./partitioning.js"
import type { SessionLayout } from "../session.js"
import {
  type AgentId as SdkAgentId,
  type ConversationId as SdkConversationId,
  MintUlid
} from "../types/ids.js"
import {
  AgentKind,
  OPERATION_TASK,
  TaskStateName,
  type AgentEnvelope,
  type AgentErrorBody,
  type AgentId as WireAgentId,
  type IdempotencyKey,
  type TaskState,
  type TokenUsage,
  chunkEnvelope,
  commandEnvelope,
  decodeAgentErrorBody,
  encodeAgentErrorBody,
  errorEnvelope,
  eventEnvelope,
  responseEnvelope,
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
  taskStateFromCode,
  withTool,
  withUsage
} from "../wire/agent.js"
import type { ApprovalGrant } from "./context.js"
import { canonicalAgentRecord, type CanonicalHeader } from "../wire/agent-record.js"
import { OPERATION_INPUT_REQUEST } from "../wire/dispatch.js"
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
import { AGENT_CONTROL, AGENT_SESSIONS } from "../wire/topics.js"
import type { Value } from "../wire/value.js"

export const DEFAULT_CHUNK_FLUSH_BYTES = 512
export const DEFAULT_CHUNK_LINGER_MS = 20
export const MAX_CHUNK_BODY_BYTES = 64 * 1024
/** How long after its opening a chunk stream may run before readers abandon
 * it, unless the writer sets its own bound. */
export const DEFAULT_CHUNK_ABANDON_MS = 600_000

const REPLY_BATCH = 200
const REPLY_POLL_INTERVAL_MS = 50
const textDecoder = new TextDecoder("utf-8", { fatal: true })

function u32FromLittleEndian(header: CanonicalHeader): number {
  if (header.bytes.byteLength !== 4) throw new InvalidError("AGDX u32 header must be 4 bytes")
  return new DataView(
    header.bytes.buffer,
    header.bytes.byteOffset,
    header.bytes.byteLength
  ).getUint32(0, true)
}

function iggyHeader(header: CanonicalHeader): HeaderValue {
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

function assemble(
  envelope: AgentEnvelope,
  contentType: ContentType,
  broadcast: boolean
): MessageWithHeaders {
  validateAgentEnvelope(envelope)
  const record = canonicalAgentRecord(envelope, contentType, broadcast)
  return {
    payload: record.payload,
    headers: new Map(
      Array.from(record.headers, ([key, header]) => [key, iggyHeader(header)] as const)
    )
  }
}

function mintRecordId(source?: UlidSource): RecordId {
  return MintUlid.mint(RecordId, source)
}

function mintCorrelationId(source?: UlidSource): CorrelationId {
  return MintUlid.mint(CorrelationId, source)
}

function mintChannelId(source?: UlidSource): ChannelId {
  return MintUlid.mint(ChannelId, source)
}

class AgdxReplyReader {
  private partitions: number | undefined
  private offsets: bigint[] = []
  /** Whether the last pass read any record, so the caller polls again at once. */
  readAny = false

  private constructor(
    private readonly transport: LaserTransport,
    private readonly stream: string,
    private readonly topic: string,
    private readonly verifier?: KeyRegistry,
    private readonly expectedSource?: WireAgentId
  ) {}

  /** A reader at the topic tail. With `expectedSource`, only a reply authored
   * by that agent, and signed by it when a verifier is set, is accepted. */
  static async atTail(
    transport: LaserTransport,
    stream: string,
    topic: string,
    verifier?: KeyRegistry,
    expectedSource?: WireAgentId
  ): Promise<AgdxReplyReader> {
    const reader = new AgdxReplyReader(transport, stream, topic, verifier, expectedSource)
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

  async next(correlation: CorrelationId, signal?: AbortSignal): Promise<AgentEnvelope | undefined> {
    this.readAny = false
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
        false,
        undefined,
        signal
      )
      for (const message of messages) {
        this.readAny = true
        this.offsets[partitionId] = message.offset + 1n
        const decoded = decodeAgentMessage({ ...message, partitionId })
        if (decoded.kind === "error") continue
        const envelope = decoded.message.envelope
        if (
          envelope === undefined ||
          envelope.correlation?.equals(correlation) !== true ||
          (envelope.kind !== AgentKind.Response && envelope.kind !== AgentKind.Error) ||
          (this.expectedSource !== undefined && envelope.source !== this.expectedSource)
        ) {
          continue
        }
        if (this.verifier !== undefined) {
          if (decoded.signatureContext === undefined || decoded.observedAtMicros === undefined) {
            continue
          }
          try {
            const verified = this.verifier.verifyObservedAt(
              envelope,
              decoded.signatureContext,
              decoded.observedAtMicros
            )
            if (this.expectedSource !== undefined && verified.principal !== this.expectedSource) {
              continue
            }
          } catch {
            continue
          }
        }
        return envelope
      }
    }
    return undefined
  }
}

/** Where one published envelope was committed. */
export interface AgdxReceipt {
  /** The envelope's record id. Chunks have none. */
  readonly record?: RecordId
  /** The partition the record landed on, when the server confirmed it. */
  readonly partitionId?: number
  /** The record's offset, when the server confirmed it. */
  readonly offset?: bigint
}

export interface Agdx {
  command(correlation: CorrelationId, body: BytesLike): AgdxSend
  respond(correlation: CorrelationId, body: BytesLike): AgdxSend
  emit(body: BytesLike): AgdxSend
  status(operation: string): AgdxSend
  fail(correlation: CorrelationId, error: AgentErrorBody): AgdxSend
  stream(correlation: CorrelationId, purpose: string): AgdxStream
  /**
   * Publishes an `input-required` task status and the prompt as an
   * `input_request` command, then waits for the correlated response on
   * `replyTopic`. Only an agent that lists `input_request` among its operations
   * receives the prompt as work. `target` addresses the prompt to one approver
   * and accepts only its answer. Without it every agent on a shared session
   * topic receives the prompt.
   */
  requestInput(
    replyTopic: string,
    prompt: BytesLike,
    timeoutMs: number,
    options?: { readonly signal?: AbortSignal; readonly target?: SdkAgentId }
  ): Promise<Uint8Array>
  /** The approving reply envelope of one input request.
   * @internal */
  inputReply(
    replyTopic: string,
    prompt: BytesLike,
    timeoutMs: number,
    options?: { readonly signal?: AbortSignal; readonly target?: SdkAgentId }
  ): Promise<AgentEnvelope>
  /** Publishes an envelope built elsewhere on this producer's topic, after the
   * governor. A signed envelope whose body the governor replaces is refused.
   * @internal */
  publishEnvelope(envelope: AgentEnvelope, options?: CallOptions): Promise<AgdxReceipt>
  /** @internal */
  withLaneGuard(
    check: () => Promise<{
      readonly streamId: number
      readonly topicId: number
      readonly partitions: number
    }>
  ): Agdx
}

interface ClaimCheck {
  readonly store: BlobStore
  readonly thresholdBytes: number
}

interface PreparedSend {
  readonly envelope: AgentEnvelope
  readonly contentType: ContentType
}

interface AgdxPublisher {
  prepare(
    envelope: AgentEnvelope,
    contentType: ContentType,
    signingKey?: SigningKey,
    claimCheck?: ClaimCheck,
    approval?: ApprovalGrant
  ): Promise<PreparedSend>
  publish(
    envelope: AgentEnvelope,
    contentType: ContentType,
    options?: CallOptions
  ): Promise<AgdxReceipt>
  assemble(envelope: AgentEnvelope, contentType: ContentType): MessageWithHeaders
  publishBatch(
    messages: readonly MessageWithHeaders[],
    kind: AgentKind,
    target: WireAgentId | undefined,
    options?: CallOptions
  ): Promise<void>
}

class AgdxClient implements Agdx, AgdxPublisher {
  constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    readonly topicName: string,
    readonly sourceId: WireAgentId,
    readonly conversationId: ConversationId,
    private readonly ulidSource?: UlidSource,
    private readonly govern?: (
      envelope: AgentEnvelope,
      willSign: boolean,
      approval?: ApprovalGrant
    ) => Promise<Uint8Array>,
    private readonly verifier?: KeyRegistry,
    private readonly layout?: () => SessionLayout | undefined,
    private readonly laneGuard?: () => Promise<{
      readonly streamId: number
      readonly topicId: number
      readonly partitions: number
    }>
  ) {}

  withLaneGuard(
    check: () => Promise<{
      readonly streamId: number
      readonly topicId: number
      readonly partitions: number
    }>
  ): Agdx {
    return new AgdxClient(
      this.transport,
      this.streamName,
      this.topicName,
      this.sourceId,
      this.conversationId,
      this.ulidSource,
      this.govern,
      this.verifier,
      this.layout,
      check
    )
  }

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
    options: { readonly signal?: AbortSignal; readonly target?: SdkAgentId } = {}
  ): Promise<Uint8Array> {
    return (await this.inputReply(replyTopic, prompt, timeoutMs, options)).body
  }

  async inputReply(
    replyTopic: string,
    prompt: BytesLike,
    timeoutMs: number,
    options: { readonly signal?: AbortSignal; readonly target?: SdkAgentId } = {}
  ): Promise<AgentEnvelope> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("requestInput() timeout must be a non-negative finite number")
    }
    throwIfAborted(options.signal, "AGDX reply wait")
    const interrupt = mintCorrelationId(this.ulidSource)
    const reader = await abortable(
      AgdxReplyReader.atTail(
        this.transport,
        this.streamName,
        replyTopicFor(this.layout?.(), replyTopic, this.sourceId),
        this.verifier,
        options.target?.wireId()
      ),
      options.signal,
      "AGDX reply wait"
    )
    await this.status(OPERATION_TASK)
      .withCorrelation(interrupt)
      .withTaskState(taskStateFromCode(TaskStateName.InputRequired))
      .send(options)
    const command = this.command(interrupt, prompt).withOperation(OPERATION_INPUT_REQUEST)
    await (options.target === undefined ? command : command.withTarget(options.target)).send(
      options
    )
    const deadline = Date.now() + timeoutMs
    for (;;) {
      throwIfAborted(options.signal, "AGDX reply wait")
      const reply = await reader.next(interrupt, options.signal)
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
        return reply
      }
      const remaining = deadline - Date.now()
      if (remaining <= 0) break
      // A full batch without the reply means more records wait, so only a
      // caught-up pass sleeps.
      if (!reader.readAny) {
        await delay(Math.min(REPLY_POLL_INTERVAL_MS, remaining), options.signal, "AGDX reply wait")
      }
    }
    throw new TimeoutError("the AGDX reply")
  }

  async publishEnvelope(envelope: AgentEnvelope, options: CallOptions = {}): Promise<AgdxReceipt> {
    if (this.govern === undefined) return this.publish(envelope, ContentType.Raw, options)
    const signed = envelope.signature !== undefined
    const body = await this.govern(envelope, signed)
    if (
      body.byteLength === envelope.body.byteLength &&
      body.every((byte, index) => byte === envelope.body[index])
    ) {
      return this.publish(envelope, ContentType.Raw, options)
    }
    // A replacement body would void the signature the caller already attached.
    if (signed) {
      throw new PolicyBlockedError("the governor replaced the body of a signed envelope")
    }
    return this.publish({ ...envelope, body }, ContentType.Raw, options)
  }

  async publish(
    envelope: AgentEnvelope,
    contentType: ContentType,
    options: CallOptions = {}
  ): Promise<AgdxReceipt> {
    const message = this.assemble(envelope, contentType)
    // A declared per-agent topic layout moves addressed work off the lane, and
    // the lane identity guard covers lane records only.
    const destination = this.destinationOf(envelope.kind, envelope.target)
    const guard = destination === undefined ? this.laneGuard : undefined
    let firstSource = await guard?.()
    const sent = await this.transport.sendMessagesWithHeaders(
      this.streamName,
      destination ?? this.topicName,
      [message],
      envelope.conversation.toString(),
      this.partitionOf(envelope.kind, envelope.target),
      guard === undefined
        ? { signal: options.signal }
        : {
            signal: options.signal,
            beforeSend: async () => {
              const source = firstSource
              firstSource = undefined
              if (source !== undefined) return source
              return guard()
            }
          }
    )
    const confirmation = sent.confirmations[0]
    return {
      ...(envelope.record !== undefined ? { record: envelope.record } : {}),
      ...(confirmation !== undefined
        ? { partitionId: confirmation.partitionId, offset: confirmation.baseOffset }
        : {})
    }
  }

  async prepare(
    envelope: AgentEnvelope,
    contentType: ContentType,
    signingKey?: SigningKey,
    claimCheck?: ClaimCheck,
    approval?: ApprovalGrant
  ): Promise<PreparedSend> {
    const body =
      this.govern === undefined
        ? envelope.body
        : await this.govern(envelope, signingKey !== undefined, approval)
    let prepared = body === envelope.body ? envelope : { ...envelope, body }
    let preparedType = contentType
    // Claim-check runs after governance and before signing, so the signature
    // covers the capsule the log carries.
    if (claimCheck !== undefined) {
      const checked = await checkIn(claimCheck.store, claimCheck.thresholdBytes, prepared.body)
      prepared = { ...prepared, body: checked.payload }
      if (checked.contentType !== undefined) preparedType = checked.contentType
    }
    if (signingKey === undefined) return { envelope: prepared, contentType: preparedType }
    return {
      envelope: {
        ...prepared,
        signature: signingKey.signWithContext(prepared, {
          contentType: contentTypeCode(preparedType),
          agentVersion: AGENT_OP_VERSION
        })
      },
      contentType: preparedType
    }
  }

  // Every untargeted record on the shared session and control topics is
  // addressed to every agent, so an addressee-filtered group still reads it.
  assemble(envelope: AgentEnvelope, contentType: ContentType): MessageWithHeaders {
    return assemble(
      envelope,
      contentType,
      this.topicName === AGENT_SESSIONS || this.topicName === AGENT_CONTROL
    )
  }

  async publishBatch(
    messages: readonly MessageWithHeaders[],
    kind: AgentKind,
    target: WireAgentId | undefined,
    options: CallOptions = {}
  ): Promise<void> {
    await this.transport.sendMessagesWithHeaders(
      this.streamName,
      this.destinationOf(kind, target) ?? this.topicName,
      messages,
      this.conversationId.toString(),
      this.partitionOf(kind, target),
      { signal: options.signal }
    )
  }

  // The partition a record of `kind` addressed to `target` lands on under the
  // stream's declared layout, or the conversation's own partition.
  private partitionOf(kind: AgentKind, target: WireAgentId | undefined): number | undefined {
    return resolveAgentPartition(this.layout?.(), this.topicName, kind, target)
  }

  // The declared topic a record of `kind` addressed to `target` moves to under
  // a per-agent topic layout, or `undefined` to stay on this topic.
  private destinationOf(kind: AgentKind, target: WireAgentId | undefined): string | undefined {
    return resolveAgentTopic(this.layout?.(), this.topicName, kind, target)
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
  govern?: (
    envelope: AgentEnvelope,
    willSign: boolean,
    approval?: ApprovalGrant
  ) => Promise<Uint8Array>,
  verifier?: KeyRegistry,
  layout?: () => SessionLayout | undefined
): Agdx {
  return new AgdxClient(
    transport,
    streamName,
    topicName,
    source.wireId(),
    ConversationId.parse(conversation.toString()),
    ulidSource,
    govern,
    verifier,
    layout
  )
}

export interface AgdxSend {
  /** Uses `record` as the envelope's record id, so a retried send repeats the
   * same record.
   * @internal */
  withRecord(record: RecordId): this
  /** Marks the envelope as part of a child session whose parent is `parent`
   * and whose tree is rooted at `root`. */
  withAncestry(parent?: ConversationId, root?: ConversationId): this
  withTarget(target: SdkAgentId): this
  withCause(cause: RecordId, causeAt?: LogPosition): this
  withCorrelation(correlation: CorrelationId): this
  withIdempotencyKey(key: IdempotencyKey): this
  /** The drop-dead time, epoch microseconds. */
  withDeadlineAtMicros(deadlineAtMicros: bigint): this
  withTaskState(state: TaskState): this
  withOperation(operation: string): this
  withTool(tool: string): this
  /** Sets why the model stopped generating. */
  withFinishReason(reason: string): this
  withUsage(usage: TokenUsage): this
  withMetadata(key: string, value: Value): this
  last(): this
  contentType(contentType: ContentType): this
  body(body: BytesLike): this
  signedBy(key: SigningKey): this
  /** Carries `grant` to the action governor, so a step-up it asks for in the
   * grant's conversation proceeds on that approval. */
  withGrant(grant: ApprovalGrant): this
  /**
   * Externalizes a body at or over `thresholdBytes` to `store` at send and
   * replaces it with the body reference capsule (content type `ref`). Applied
   * before signing, so a signature covers the capsule the log carries.
   */
  claimCheck(store: BlobStore, thresholdBytes: number): this
  send(options?: CallOptions): Promise<RecordId | undefined>
  /** Like `send`, and also returns where the record was committed. */
  sendReceipt(options?: CallOptions): Promise<AgdxReceipt>
}

class AgdxSendBuilder implements AgdxSend {
  private contentTypeValue: ContentType = ContentType.Raw
  private sent = false
  private signingKey: SigningKey | undefined
  private claimCheckValue: ClaimCheck | undefined
  private approval: ApprovalGrant | undefined

  constructor(
    private readonly agdx: AgdxPublisher,
    private envelope: AgentEnvelope
  ) {}

  withRecord(record: RecordId): this {
    this.envelope = { ...this.envelope, record }
    return this
  }

  withAncestry(parent?: ConversationId, root?: ConversationId): this {
    const { parent: _parent, root: _root, ...rest } = this.envelope
    this.envelope = {
      ...rest,
      ...(parent !== undefined ? { parent } : {}),
      ...(root !== undefined ? { root } : {})
    }
    return this
  }

  withTarget(target: SdkAgentId): this {
    this.envelope = withTarget(this.envelope, target.wireId())
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

  withDeadlineAtMicros(deadlineAtMicros: bigint): this {
    this.envelope = withDeadlineMicros(this.envelope, deadlineAtMicros)
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

  withFinishReason(reason: string): this {
    this.envelope = { ...this.envelope, finishReason: reason }
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

  withGrant(grant: ApprovalGrant): this {
    this.approval = grant
    return this
  }

  claimCheck(store: BlobStore, thresholdBytes: number): this {
    if (!Number.isSafeInteger(thresholdBytes) || thresholdBytes < 0) {
      throw new InvalidError("claim-check threshold must be a non-negative safe integer")
    }
    this.claimCheckValue = { store, thresholdBytes }
    return this
  }

  async send(options: CallOptions = {}): Promise<RecordId | undefined> {
    return (await this.sendReceipt(options)).record
  }

  async sendReceipt(options: CallOptions = {}): Promise<AgdxReceipt> {
    if (this.sent) throw new InvalidError("an AGDX send can only be performed once")
    if (this.envelope.kind === AgentKind.Error && this.contentTypeValue !== ContentType.Cbor) {
      throw new InvalidError(
        "an error envelope carries a CBOR AgentErrorBody, so its content type cannot change"
      )
    }
    throwIfAborted(options.signal, "AGDX send")
    this.sent = true
    const prepared = await abortable(
      this.agdx.prepare(
        this.envelope,
        this.contentTypeValue,
        this.signingKey,
        this.claimCheckValue,
        this.approval
      ),
      options.signal,
      "AGDX send"
    )
    return abortable(
      this.agdx.publish(prepared.envelope, prepared.contentType, options),
      options.signal,
      "AGDX send"
    )
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
  /** The reader-local abandonment bound, epoch microseconds. */
  withDeadlineAtMicros(deadlineAtMicros: bigint): this
  withTarget(target: SdkAgentId): this
  contentType(contentType: ContentType): this
  buffered(maxChunks: number, lingerMs: number): this
  write(body: BytesLike, options?: CallOptions): Promise<void>
  flush(options?: CallOptions): Promise<void>
  finish(finishReason: string, usage?: TokenUsage, options?: CallOptions): Promise<void>
  fail(error: AgentErrorBody, options?: CallOptions): Promise<void>
}

class AgdxStreamWriter implements AgdxStream {
  private sequence = 0n
  private deadlineMicros: bigint | undefined =
    BigInt(Date.now() + DEFAULT_CHUNK_ABANDON_MS) * 1_000n
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

  withDeadlineAtMicros(deadlineAtMicros: bigint): this {
    this.deadlineMicros = deadlineAtMicros
    return this
  }

  withTarget(target: SdkAgentId): this {
    this.target = target.wireId()
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

  async write(body: BytesLike, options: CallOptions = {}): Promise<void> {
    this.assertOpen()
    const bytes = ownedBytes(body)
    if (bytes.byteLength > MAX_CHUNK_BODY_BYTES) {
      throw new InvalidError(
        `chunk body is ${String(bytes.byteLength)}B, exceeds cap ${String(MAX_CHUNK_BODY_BYTES)}B`
      )
    }
    throwIfAborted(options.signal, "AGDX stream")
    const envelope = this.chunk(bytes, false)
    // A failed publish keeps the sequence, so writing the body again continues
    // the stream without a gap.
    if (this.buffer === undefined) {
      await abortable(
        this.agdx.publish(envelope, this.contentTypeValue, options),
        options.signal,
        "AGDX stream"
      )
      this.sequence += 1n
      return
    }
    const message = this.agdx.assemble(envelope, this.contentTypeValue)
    this.sequence += 1n
    this.buffer.firstAt ??= Date.now()
    this.buffer.messages.push(message)
    if (
      this.buffer.messages.length >= this.buffer.maxChunks ||
      Date.now() - this.buffer.firstAt >= this.buffer.lingerMs
    ) {
      await this.flush(options)
    }
  }

  /** A failed append keeps the chunks buffered, so the next flush sends them. */
  async flush(options: CallOptions = {}): Promise<void> {
    throwIfAborted(options.signal, "AGDX stream")
    if (this.buffer === undefined || this.buffer.messages.length === 0) return
    const messages = [...this.buffer.messages]
    await abortable(
      this.agdx.publishBatch(messages, AgentKind.Chunk, this.target, options),
      options.signal,
      "AGDX stream"
    )
    this.buffer.messages.splice(0, messages.length)
    this.buffer.firstAt = undefined
  }

  async finish(finishReason: string, usage?: TokenUsage, options: CallOptions = {}): Promise<void> {
    this.assertOpen()
    throwIfAborted(options.signal, "AGDX stream")
    this.closed = true
    let envelope = this.chunk(new Uint8Array(0), true, finishReason)
    if (usage !== undefined) envelope = withUsage(envelope, usage)
    await this.sendTerminal(envelope, this.contentTypeValue, options.signal)
  }

  async fail(error: AgentErrorBody, options: CallOptions = {}): Promise<void> {
    this.assertOpen()
    throwIfAborted(options.signal, "AGDX stream")
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
    await this.sendTerminal(envelope, ContentType.Cbor, options.signal)
  }

  private async sendTerminal(
    envelope: AgentEnvelope,
    contentType: ContentType,
    signal: AbortSignal | undefined
  ): Promise<void> {
    if (this.buffer === undefined) {
      await abortable(this.agdx.publish(envelope, contentType, { signal }), signal, "AGDX stream")
      return
    }
    this.buffer.messages.push(this.agdx.assemble(envelope, contentType))
    const messages = this.buffer.messages
    this.buffer.messages = []
    this.buffer.firstAt = undefined
    await abortable(
      this.agdx.publishBatch(messages, envelope.kind, envelope.target, { signal }),
      signal,
      "AGDX stream"
    )
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
