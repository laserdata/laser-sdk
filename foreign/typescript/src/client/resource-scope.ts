/** How a handle names the managed resources it sends, derived from its
 * `Laser`.
 * @internal */
export interface ResourceScope {
  /** The name sent for the managed resource `local`. */
  name(local: string): string
  /** The caller's name for a listed `name`, `undefined` when it belongs to
   * another stream. */
  local(name: string): string | undefined
  /** The stream names are scoped to, `undefined` when names stay bare. */
  readonly stream: string | undefined
}

/** A scope that sends every name as written.
 * @internal */
export const BARE_SCOPE: ResourceScope = Object.freeze({
  name: (local: string) => local,
  local: (name: string) => name,
  stream: undefined
})
