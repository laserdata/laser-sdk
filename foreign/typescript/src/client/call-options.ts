import { CancelledError } from "./errors.js"

/** The trailing options of an awaiting call. An aborted `signal` rejects the
 * call with `CancelledError` before it sends anything and while it waits. */
export interface CallOptions {
  readonly signal?: AbortSignal | undefined
}

/**
 * Throws `CancelledError` for `what` when `signal` is aborted.
 * @internal
 */
export function throwIfAborted(signal: AbortSignal | undefined, what: string): void {
  if (signal?.aborted === true) throw cancelled(signal, what)
}

/**
 * Settles as `work` does, or rejects with `CancelledError` once `signal`
 * aborts. Work that is abandoned keeps running and its outcome is dropped.
 * @internal
 */
export function abortable<T>(
  work: Promise<T>,
  signal: AbortSignal | undefined,
  what: string
): Promise<T> {
  if (signal === undefined) return work
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => {
      reject(cancelled(signal, what))
    }
    work.then(
      (value) => {
        signal.removeEventListener("abort", onAbort)
        resolve(value)
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort)
        reject(error instanceof Error ? error : new Error(String(error)))
      }
    )
    if (signal.aborted) onAbort()
    else signal.addEventListener("abort", onAbort, { once: true })
  })
}

/**
 * Runs `run` unless `signal` is already aborted, and rejects with
 * `CancelledError` once it aborts before `run` settles.
 * @internal
 */
export function cancellable<T>(
  signal: AbortSignal | undefined,
  what: string,
  run: () => Promise<T>
): Promise<T> {
  if (signal?.aborted === true) return Promise.reject(cancelled(signal, what))
  return abortable(run(), signal, what)
}

/**
 * Waits `ms` milliseconds, or rejects with `CancelledError` once `signal`
 * aborts.
 * @internal
 */
export function delay(ms: number, signal: AbortSignal | undefined, what: string): Promise<void> {
  if (signal?.aborted === true) return Promise.reject(cancelled(signal, what))
  return new Promise((resolve, reject) => {
    const onAbort = (): void => {
      clearTimeout(timer)
      reject(cancelled(signal, what))
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort)
      resolve()
    }, ms)
    signal?.addEventListener("abort", onAbort, { once: true })
  })
}

function cancelled(signal: AbortSignal | undefined, what: string): CancelledError {
  return new CancelledError(`${what} aborted`, { cause: signal?.reason })
}
