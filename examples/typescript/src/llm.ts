/** Model seam owned by examples. The SDK itself never invokes a model. */
export interface LlmClient {
  complete(prompt: string, options?: { readonly signal?: AbortSignal }): Promise<string>
}

export class MockLlm implements LlmClient {
  complete(prompt: string, options: { readonly signal?: AbortSignal } = {}): Promise<string> {
    if (options.signal?.aborted === true) {
      return Promise.reject(abortReason(options.signal))
    }
    return Promise.resolve(`[mock-llm] ${prompt}`)
  }
}

/** Selects an explicitly configured provider or the deterministic mock. */
export function defaultLlm(env: NodeJS.ProcessEnv = process.env): LlmClient {
  return AnthropicLlm.fromEnv(env) ?? OpenAiLlm.fromEnv(env) ?? new MockLlm()
}

const ANTHROPIC_ENDPOINT = "https://api.anthropic.com/v1/messages"
const ANTHROPIC_API_VERSION = "2023-06-01"
const ANTHROPIC_DEFAULT_MODEL = "claude-sonnet-4-6"
const ANTHROPIC_MAX_TOKENS = 1024

const OPENAI_ENDPOINT = "https://api.openai.com/v1/chat/completions"
const OPENAI_DEFAULT_MODEL = "gpt-4o"

const REQUEST_TIMEOUT_MS = 30_000

export class AnthropicLlm implements LlmClient {
  private constructor(
    private readonly apiKey: string,
    private readonly model: string
  ) {}

  static fromEnv(env: NodeJS.ProcessEnv = process.env): AnthropicLlm | undefined {
    const apiKey = env["ANTHROPIC_API_KEY"]?.trim() ?? ""
    if (apiKey.length === 0) return undefined
    const model = env["ANTHROPIC_MODEL"]?.trim() || ANTHROPIC_DEFAULT_MODEL
    return new AnthropicLlm(apiKey, model)
  }

  async complete(prompt: string, options: { readonly signal?: AbortSignal } = {}): Promise<string> {
    let body: unknown
    try {
      const response = await fetch(ANTHROPIC_ENDPOINT, {
        method: "POST",
        headers: {
          "x-api-key": this.apiKey,
          "anthropic-version": ANTHROPIC_API_VERSION,
          "content-type": "application/json"
        },
        body: JSON.stringify({
          model: this.model,
          max_tokens: ANTHROPIC_MAX_TOKENS,
          messages: [{ role: "user", content: prompt }]
        }),
        signal: requestSignal(options.signal)
      })
      if (!response.ok) {
        return `[anthropic-request-error] HTTP ${String(response.status)}`
      }
      body = await response.json()
    } catch (error) {
      return `[anthropic-request-error] ${errorMessage(error)}`
    }
    const text = anthropicText(body)
    return text ?? "[anthropic-decode-error] unexpected response shape"
  }
}

export class OpenAiLlm implements LlmClient {
  private constructor(
    private readonly apiKey: string,
    private readonly model: string
  ) {}

  static fromEnv(env: NodeJS.ProcessEnv = process.env): OpenAiLlm | undefined {
    const apiKey = env["OPENAI_API_KEY"]?.trim() ?? ""
    if (apiKey.length === 0) return undefined
    const model = env["OPENAI_MODEL"]?.trim() || OPENAI_DEFAULT_MODEL
    return new OpenAiLlm(apiKey, model)
  }

  async complete(prompt: string, options: { readonly signal?: AbortSignal } = {}): Promise<string> {
    let body: unknown
    try {
      const response = await fetch(OPENAI_ENDPOINT, {
        method: "POST",
        headers: {
          authorization: `Bearer ${this.apiKey}`,
          "content-type": "application/json"
        },
        body: JSON.stringify({
          model: this.model,
          messages: [{ role: "user", content: prompt }]
        }),
        signal: requestSignal(options.signal)
      })
      if (!response.ok) {
        return `[openai-request-error] HTTP ${String(response.status)}`
      }
      body = await response.json()
    } catch (error) {
      return `[openai-request-error] ${errorMessage(error)}`
    }
    const text = openAiText(body)
    return text ?? "[openai-decode-error] unexpected response shape"
  }
}

function requestSignal(signal: AbortSignal | undefined): AbortSignal {
  const timeout = AbortSignal.timeout(REQUEST_TIMEOUT_MS)
  return signal === undefined ? timeout : AbortSignal.any([signal, timeout])
}

function anthropicText(body: unknown): string | undefined {
  if (typeof body !== "object" || body === null) return undefined
  const content = (body as { readonly content?: unknown }).content
  if (!Array.isArray(content)) return undefined
  const parts: string[] = []
  for (const block of content as readonly unknown[]) {
    if (typeof block !== "object" || block === null) return undefined
    const text = (block as { readonly text?: unknown }).text
    if (typeof text === "string") parts.push(text)
  }
  return parts.join("")
}

function openAiText(body: unknown): string | undefined {
  if (typeof body !== "object" || body === null) return undefined
  const choices = (body as { readonly choices?: unknown }).choices
  if (!Array.isArray(choices)) return undefined
  const first: unknown = choices[0]
  if (first === undefined) return ""
  if (typeof first !== "object" || first === null) return undefined
  const message = (first as { readonly message?: unknown }).message
  if (typeof message !== "object" || message === null) return undefined
  const content = (message as { readonly content?: unknown }).content
  return typeof content === "string" ? content : ""
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function abortReason(signal: AbortSignal): Error {
  const reason: unknown = signal.reason
  return reason instanceof Error ? reason : new Error(String(reason))
}
