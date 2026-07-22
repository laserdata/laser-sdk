function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let resolve: () => void = () => undefined
  const promise = new Promise<void>((res) => {
    resolve = res
  })
  return { promise, resolve }
}

// Serializes access to a shared mutable region across concurrent async
// callers. Each `runExclusive` call queues behind every earlier one, in
// call order, regardless of how long each holder's body takes.
export class Mutex {
  private tail: Promise<void> = Promise.resolve()

  async runExclusive<T>(body: () => Promise<T>): Promise<T> {
    const previousTail = this.tail
    const next = deferred()
    this.tail = next.promise
    await previousTail
    try {
      return await body()
    } finally {
      next.resolve()
    }
  }
}
