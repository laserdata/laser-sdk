export interface LlmClient {
  complete(prompt: string, options?: { readonly signal?: AbortSignal }): Promise<string>
}

export class MockLlm implements LlmClient {
  complete(prompt: string, options: { readonly signal?: AbortSignal } = {}): Promise<string> {
    if (options.signal?.aborted === true) {
      return Promise.reject(options.signal.reason)
    }
    return Promise.resolve(`summary: ${prompt.trim().slice(0, 48)}`)
  }
}

interface ProviderReply {
  readonly output: string
}

function providerReply(value: unknown): ProviderReply {
  if (typeof value !== "object" || value === null || !("output" in value)) {
    throw new Error("model provider returned an invalid response")
  }
  const output = (value as { readonly output?: unknown }).output
  if (typeof output !== "string") throw new Error("model provider response has no output")
  return { output }
}

export class HttpLlm implements LlmClient {
  constructor(
    private readonly endpoint: URL,
    private readonly bearerToken: string,
    private readonly timeoutMs = 30_000
  ) {}

  async complete(prompt: string, options: { readonly signal?: AbortSignal } = {}): Promise<string> {
    const timeout = AbortSignal.timeout(this.timeoutMs)
    const signal =
      options.signal === undefined ? timeout : AbortSignal.any([options.signal, timeout])
    const response = await fetch(this.endpoint, {
      method: "POST",
      headers: {
        authorization: `Bearer ${this.bearerToken}`,
        "content-type": "application/json"
      },
      body: JSON.stringify({ prompt }),
      signal
    })
    if (!response.ok) throw new Error(`model provider returned HTTP ${String(response.status)}`)
    return providerReply(await response.json()).output
  }
}
