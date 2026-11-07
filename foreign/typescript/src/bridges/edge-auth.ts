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

/**
 * The RFC 6750 section 3 `WWW-Authenticate` challenge: `invalid_token` for a
 * token minted for another audience, `insufficient_scope` with the scope to
 * acquire for a step-up, the error MCP clients read to start re-authorization.
 */
export function edgeDenialChallenge(denial: EdgeDenial): string {
  if (denial.kind === "wrongAudience") return 'Bearer error="invalid_token"'
  // RFC 9110 section 5.6.4 quoted-string: escape `"` and `\` so a scope cannot end the parameter.
  const scope = denial.requiredScope.replace(/["\\]/g, (character) => `\\${character}`)
  return `Bearer error="insufficient_scope", scope="${scope}"`
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
