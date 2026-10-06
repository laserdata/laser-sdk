/** Polling position for a consumer. `next`, the default, resumes after the
 * server-stored consumer offset. `timestampMicros` begins at a microsecond
 * Unix timestamp. */
export type ConsumerStart =
  | { readonly kind: "first" }
  | { readonly kind: "last" }
  | { readonly kind: "next" }
  | { readonly kind: "offset"; readonly value: bigint }
  | { readonly kind: "timestampMicros"; readonly value: bigint }
