export type PollingStrategy =
  | { readonly kind: "first" }
  | { readonly kind: "last" }
  | { readonly kind: "next" }
  | { readonly kind: "offset"; readonly value: bigint }
  | { readonly kind: "timestamp"; readonly value: bigint }
