import { type BytesLike, ownedBytes } from "../client/bytes.js"

/** Per-send partition selection. `balanced` lets Apache Iggy spread records
 * across the topic partitions, `key` hashes a non-empty key so records with
 * the same key keep their order, and `partition` sends to one partition. */
export type Routing =
  | { readonly kind: "balanced" }
  | { readonly kind: "key"; readonly key: Uint8Array }
  | { readonly kind: "partition"; readonly partition: number }

export const Routing: {
  /** Let Apache Iggy balance records across the topic partitions. */
  readonly balanced: Routing
  /** Route records by a stable, non-empty key. */
  readonly key: (value: BytesLike) => Routing
  /** Send directly to one partition. */
  readonly partition: (id: number) => Routing
} = {
  balanced: { kind: "balanced" },
  key: (value) => ({ kind: "key", key: ownedBytes(value) }),
  partition: (id) => ({ kind: "partition", partition: id })
}
