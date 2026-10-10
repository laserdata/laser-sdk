import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import { randomUUID } from "node:crypto"
import { join } from "node:path"
import { StateStoreError } from "./client/errors.js"
import type { Kv } from "./managed/kv.js"

export interface StateStore {
  get(key: string): Promise<Uint8Array | undefined>
  set(key: string, value: Uint8Array): Promise<void>
  delete(key: string): Promise<void>
}

export class InMemoryStore implements StateStore {
  private readonly entries = new Map<string, Uint8Array>()

  get(key: string): Promise<Uint8Array | undefined> {
    const value = this.entries.get(key)
    return Promise.resolve(value === undefined ? undefined : value.slice())
  }

  set(key: string, value: Uint8Array): Promise<void> {
    this.entries.set(key, value.slice())
    return Promise.resolve()
  }

  delete(key: string): Promise<void> {
    this.entries.delete(key)
    return Promise.resolve()
  }
}

/** The managed `Kv` as a `StateStore`, like Rust's `impl StateStore for Kv`
 * and Python `KvStore`. Keys are stored as their UTF-8 bytes and `set` never
 * expires: use the `Kv` builder for a TTL. */
export class KvStore implements StateStore {
  constructor(private readonly kv: Kv) {}

  get(key: string): Promise<Uint8Array | undefined> {
    return this.kv.get(UTF8.encode(key))
  }

  async set(key: string, value: Uint8Array): Promise<void> {
    await this.kv.set(UTF8.encode(key)).bytes(value).send()
  }

  async delete(key: string): Promise<void> {
    await this.kv.delete(UTF8.encode(key))
  }
}

const UTF8 = new TextEncoder()

function hexKey(key: string): string {
  return [...new TextEncoder().encode(key)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("")
}

export class FileStore implements StateStore {
  constructor(private readonly root: string) {}

  async get(key: string): Promise<Uint8Array | undefined> {
    try {
      return new Uint8Array(await readFile(this.pathFor(key)))
    } catch (cause) {
      if (isNotFound(cause)) return undefined
      throw new StateStoreError(`failed to read state key \`${key}\``, { cause })
    }
  }

  async set(key: string, value: Uint8Array): Promise<void> {
    await mkdir(this.root, { recursive: true })
    const target = this.pathFor(key)
    const staging = `${target}.${randomUUID()}.tmp`
    try {
      await writeFile(staging, value)
      await rename(staging, target)
    } catch (cause) {
      await rm(staging, { force: true })
      throw new StateStoreError(`failed to write state key \`${key}\``, { cause })
    }
  }

  async delete(key: string): Promise<void> {
    try {
      await rm(this.pathFor(key), { force: true })
    } catch (cause) {
      throw new StateStoreError(`failed to delete state key \`${key}\``, { cause })
    }
  }

  private pathFor(key: string): string {
    return join(this.root, hexKey(key))
  }
}

function isNotFound(error: unknown): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    (error as Error & { readonly code?: string }).code === "ENOENT"
  )
}
