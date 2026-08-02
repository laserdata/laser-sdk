export async function eventual<Value>(
  read: () => Promise<Value | undefined>,
  description: string,
  timeoutMs = 5_000
): Promise<Value> {
  const deadline = performance.now() + timeoutMs
  let delayMs = 5
  let last: Value | undefined
  while (performance.now() < deadline) {
    last = await read()
    if (last !== undefined) return last
    await new Promise((resolve) => setTimeout(resolve, delayMs))
    delayMs = Math.min(100, delayMs * 2)
  }
  throw new Error(`${description} did not converge within ${String(timeoutMs)}ms; last=${String(last)}`)
}
