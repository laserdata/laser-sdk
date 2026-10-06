const MAX_TIMER_MS = 2_147_483_647

/** A one-shot timer for any delay. Node fires a delay above 2^31 - 1 ms at
 * once, so a longer one is chained in chunks. */
export function longTimeout(callback: () => void, ms: number): { cancel(): void } {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = Date.now() + ms
  const tick = (): void => {
    const remaining = deadline - Date.now()
    if (remaining <= 0) callback()
    else timer = setTimeout(tick, Math.min(remaining, MAX_TIMER_MS))
  }
  timer = setTimeout(tick, Math.min(Math.max(0, ms), MAX_TIMER_MS))
  return {
    cancel: () => {
      clearTimeout(timer)
    }
  }
}
