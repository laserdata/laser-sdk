import { chmod, mkdir, open, readFile, rename, rm } from "node:fs/promises"
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
    const target = this.pathFor(key)
    const staging = `${target}.${randomUUID()}.tmp`
    try {
      // Dedup keys, cursors, and conversation state are this store's content,
      // so the directory is restricted to its owner rather than the umask.
      await mkdir(this.root, { recursive: true, mode: 0o700 })
      if (process.platform !== "win32") await chmod(this.root, 0o700)
      // The staged bytes reach disk before the rename, or a crash after the
      // rename could still surface an empty file.
      const staged = await open(staging, "w", 0o600)
      try {
        await staged.writeFile(value)
        await staged.sync()
      } finally {
        await staged.close()
      }
      await rename(staging, target)
      await syncDirectory(this.root)
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

// Makes the rename itself durable. Best effort, as Rust: a platform that
// cannot open a directory for syncing still has the synced file.
async function syncDirectory(directory: string): Promise<void> {
  try {
    const handle = await open(directory, "r")
    try {
      await handle.sync()
    } finally {
      await handle.close()
    }
  } catch {
    // The rename already happened, only its durability is unconfirmed.
  }
}

function isNotFound(error: unknown): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    (error as Error & { readonly code?: string }).code === "ENOENT"
  )
}
