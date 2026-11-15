import { type IncomingMessage, type Server, type ServerResponse, createServer } from "node:http"
import { isIP } from "node:net"
import { type CallOptions, throwIfAborted } from "../client/call-options.js"
import { InvalidError } from "../client/errors.js"

/** @internal Where `A2aBridge.serve` and `McpBridge.serve` listen. `host`
 * defaults to `127.0.0.1` and `port` to 0, which picks a free port. */
export interface BridgeServeOptions {
  readonly host?: string
  readonly port?: number
}

/** A host hook that names the authenticated caller of one A2A JSON-RPC
 * request from its headers, keyed by lowercase name, or returns `undefined`
 * for none. A hook that throws or returns anything else refuses the request
 * with 401. */
export type A2aCallerHook = (
  headers: Readonly<Record<string, string>>
) => string | undefined | Promise<string | undefined>

/** @internal One answered bridge request. */
export interface BridgeReply {
  readonly status: number
  readonly body?: unknown
  readonly headers?: Readonly<Record<string, string>>
}

/** @internal One JSON-RPC request: its headers keyed by lowercase name, its
 * first `Origin` and `Host` values, and the caller the host hook named. */
export interface BridgeRequest {
  readonly headers: Readonly<Record<string, string>>
  readonly origin: string | undefined
  readonly host: string | undefined
  readonly caller: string | undefined
}

/** @internal What a bridge serves: its JSON-RPC handler and any extra GET routes. */
export interface BridgeRoutes {
  readonly rpc: (body: unknown, request: BridgeRequest) => Promise<BridgeReply>
  readonly get?: Readonly<Record<string, () => BridgeReply>>
  readonly caller?: A2aCallerHook
}

const BODY_LIMIT = 2 * 1024 * 1024

/** A running bridge HTTP server. `port` is the bound port, the one the system
 * picked when `serve` asked for port 0. `close` stops accepting connections
 * and waits for the requests in flight. */
export class BridgeServer {
  private closing: Promise<void> | undefined

  private constructor(
    private readonly server: Server,
    readonly host: string,
    readonly port: number
  ) {}

  /** @internal */
  static async listen(
    routes: BridgeRoutes,
    options: BridgeServeOptions & CallOptions = {}
  ): Promise<BridgeServer> {
    throwIfAborted(options.signal, "bridge serve")
    const server = createServer((request, response) => {
      answer(routes, request, response).catch(() => {
        if (!response.headersSent) response.writeHead(500)
        response.end()
      })
    })
    const host = options.host ?? "127.0.0.1"
    const port = options.port ?? 0
    await new Promise<void>((resolve, reject) => {
      const refuse = (cause: Error) => {
        reject(
          new InvalidError(
            `cannot listen on ${host}:${String(port)}: ${cause.message}`,
            undefined,
            {
              cause
            }
          )
        )
      }
      server.once("error", refuse)
      server.listen(port, host, () => {
        server.off("error", refuse)
        resolve()
      })
    })
    const address = server.address()
    const bound = typeof address === "object" && address !== null ? address.port : 0
    return new BridgeServer(server, host, bound)
  }

  /** The `http://host:port` base URL of the JSON-RPC endpoint. */
  get url(): string {
    return `http://${isIP(this.host) === 6 ? `[${this.host}]` : this.host}:${String(this.port)}`
  }

  /** Stops the server. A second call resolves with the first. */
  close(): Promise<void> {
    this.closing ??= new Promise((resolve) => {
      this.server.close(() => {
        resolve()
      })
      this.server.closeIdleConnections()
    })
    return this.closing
  }
}

async function answer(
  routes: BridgeRoutes,
  request: IncomingMessage,
  response: ServerResponse
): Promise<void> {
  const reply = await route(routes, request)
  send(response, reply, request.method === "HEAD")
}

async function route(routes: BridgeRoutes, request: IncomingMessage): Promise<BridgeReply> {
  const path = new URL(request.url ?? "/", "http://bridge").pathname
  const method = request.method ?? "GET"
  const get = routes.get?.[path]
  if (get !== undefined) {
    if (method !== "GET" && method !== "HEAD")
      return { status: 405, headers: { allow: "GET,HEAD" } }
    return get()
  }
  if (path !== "/") return { status: 404 }
  if (method !== "POST") return { status: 405, headers: { allow: "POST" } }
  const headers = flatHeaders(request)
  let caller: unknown
  if (routes.caller !== undefined) {
    try {
      caller = await routes.caller(headers)
    } catch {
      return { status: 401 }
    }
    if (caller !== undefined && typeof caller !== "string") return { status: 401 }
  }
  if (!jsonContentType(headers["content-type"])) return { status: 415 }
  const raw = await readBody(request)
  if (raw === undefined) return { status: 413 }
  let body: unknown
  try {
    body = JSON.parse(raw.toString("utf8"))
  } catch {
    return { status: 400 }
  }
  return routes.rpc(body, {
    headers,
    origin: request.headersDistinct["origin"]?.[0],
    host: request.headersDistinct["host"]?.[0],
    caller: caller as string | undefined
  })
}

function send(response: ServerResponse, reply: BridgeReply, headOnly = false): void {
  const headers: Record<string, string> = { ...reply.headers }
  if (reply.body === undefined) {
    response.writeHead(reply.status, headers)
    response.end()
    return
  }
  const text = JSON.stringify(reply.body)
  headers["content-type"] = "application/json"
  headers["content-length"] = String(Buffer.byteLength(text))
  response.writeHead(reply.status, headers)
  response.end(headOnly ? undefined : text)
}

function flatHeaders(request: IncomingMessage): Record<string, string> {
  const headers: Record<string, string> = {}
  for (const [name, values] of Object.entries(request.headersDistinct)) {
    if (values !== undefined) headers[name] = values.join(", ")
  }
  return headers
}

// `application/json` or any `application/*+json`, with parameters ignored.
function jsonContentType(value: string | undefined): boolean {
  if (value === undefined) return false
  const essence = value.split(";", 1)[0]?.trim().toLowerCase() ?? ""
  const [type, subtype] = essence.split("/", 2)
  return (
    type === "application" &&
    subtype !== undefined &&
    (subtype === "json" || subtype.endsWith("+json"))
  )
}

async function readBody(request: IncomingMessage): Promise<Buffer | undefined> {
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of request) {
    const bytes = chunk as Buffer
    size += bytes.length
    if (size > BODY_LIMIT) {
      request.resume()
      return undefined
    }
    chunks.push(bytes)
  }
  return Buffer.concat(chunks)
}

/** @internal Whether a request may reach a bridge JSON-RPC endpoint. A
 * request without an `Origin` header comes from a non-browser client. A
 * browser request passes when its origin is allowlisted, or when it is
 * same-origin with a `Host` a DNS rebinding cannot name: an IP literal or
 * `localhost`. A rebound page sends its own name as both origin and host, so
 * a same-origin check on any other host would pass it. */
export function originAllowed(
  origin: string | undefined,
  host: string | undefined,
  allowed: readonly string[]
): boolean {
  if (origin === undefined) return true
  if (!visibleAscii(origin)) return false
  if (allowed.includes(origin)) return true
  const authority = origin.startsWith("http://")
    ? origin.slice("http://".length)
    : origin.startsWith("https://")
      ? origin.slice("https://".length)
      : undefined
  if (authority === undefined || host === undefined || !visibleAscii(host)) return false
  return host.toLowerCase() === authority.toLowerCase() && unrebindable(host)
}

function visibleAscii(value: string): boolean {
  return /^[\t\x20-\x7e]*$/.test(value)
}

// Whether a `Host` value names an IP literal or `localhost`, a host no DNS
// answer can point a foreign page at.
function unrebindable(host: string): boolean {
  let name: string
  if (host.startsWith("[")) {
    name = host.slice(1).split("]", 1)[0] ?? ""
  } else {
    const colon = host.lastIndexOf(":")
    name = colon === -1 ? host : host.slice(0, colon)
  }
  return name.toLowerCase() === "localhost" || (!name.includes("%") && isIP(name) !== 0)
}
