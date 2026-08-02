export type Routing =
  | { readonly kind: "balanced" }
  | { readonly kind: "key"; readonly key: Uint8Array }
  | { readonly kind: "partition"; readonly partition: number }
