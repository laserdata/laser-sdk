/** Report a condition the caller should know about but that does not fail
 * the operation, as a Node process warning of type `LaserWarning`.
 * @internal */
export function warn(message: string): void {
  process.emitWarning(message, { type: "LaserWarning" })
}
