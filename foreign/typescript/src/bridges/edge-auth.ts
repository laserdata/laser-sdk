import type { ResultCode } from "../wire/result.js"

export interface EdgeClaims {
  readonly audience: readonly string[]
  readonly scopes: readonly string[]
}

export type EdgeDenial =
  | { readonly kind: "wrongAudience"; readonly expected: string }
  | { readonly kind: "stepUp"; readonly requiredScope: string }

export function edgeDenialCode(denial: EdgeDenial): ResultCode {
  return {
    kind: "known",
    name: denial.kind === "wrongAudience" ? "Unauthenticated" : "StepUpRequired"
  }
}

export function edgeDenialChallenge(denial: EdgeDenial): string | undefined {
  return denial.kind === "stepUp" ? `Bearer scope="${denial.requiredScope}"` : undefined
}

export function authorizeEdge(
  claims: EdgeClaims,
  expectedAudience: string,
  requiredScope: string
): EdgeDenial | undefined {
  if (!claims.audience.includes(expectedAudience)) {
    return { kind: "wrongAudience", expected: expectedAudience }
  }
  if (!claims.scopes.includes(requiredScope)) {
    return { kind: "stepUp", requiredScope }
  }
  return undefined
}
