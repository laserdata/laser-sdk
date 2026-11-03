export class AsyncOnce<T> {
  private promise: Promise<T> | undefined
  private settled: { readonly value: T } | undefined

  static resolved<T>(value: T): AsyncOnce<T> {
    const once = new AsyncOnce<T>()
    once.promise = Promise.resolve(value)
    return once
  }

  async get(compute: () => Promise<T>): Promise<T> {
    const promise = (this.promise ??= compute())
    try {
      const value = await promise
      if (this.promise === promise) this.settled = { value }
      return value
    } catch (error) {
      if (this.promise === promise) this.promise = undefined
      throw error
    }
  }

  clear(): void {
    this.promise = undefined
    this.settled = undefined
  }

  /** Clears only while `value` is still the held result, so concurrent callers share one recompute. */
  invalidate(value: T): void {
    if (this.settled?.value === value) this.clear()
  }
}
