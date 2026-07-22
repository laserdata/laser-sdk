// Memoizes one async computation so concurrent callers share the same
// in-flight promise instead of triggering it twice (a duplicate connection
// probe, a duplicate producer initialization). A failed attempt clears the
// cache so the next call retries rather than being stuck replaying the same
// rejection forever.
export class AsyncOnce<T> {
  private promise: Promise<T> | undefined

  static resolved<T>(value: T): AsyncOnce<T> {
    const once = new AsyncOnce<T>()
    once.promise = Promise.resolve(value)
    return once
  }

  async get(compute: () => Promise<T>): Promise<T> {
    this.promise ??= compute()
    try {
      return await this.promise
    } catch (error) {
      this.promise = undefined
      throw error
    }
  }
}
