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
