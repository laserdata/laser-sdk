export const ResultCodeName = {
  Ok: 0,
  Unsupported: 1,
  NotFound: 2,
  InvalidArgument: 3,
  TooLarge: 4,
  Conflict: 5,
  Stale: 6,
  VersionSkew: 7,
  Unauthenticated: 8,
  Backend: 9,
  Forbidden: 10,
  StepUpRequired: 11
} as const

export type ResultCode =
  | { readonly kind: "known"; readonly name: keyof typeof ResultCodeName }
  | { readonly kind: "unrecognized"; readonly code: number }

const NAME_BY_CODE: ReadonlyMap<number, keyof typeof ResultCodeName> = new Map(
  Object.entries(ResultCodeName).map(([name, code]) => [code, name as keyof typeof ResultCodeName])
)

export function resultCodeFromCode(code: number): ResultCode {
  const name = NAME_BY_CODE.get(code)
  return name === undefined ? { kind: "unrecognized", code } : { kind: "known", name }
}

export function resultCodeToCode(value: ResultCode): number {
  return value.kind === "known" ? ResultCodeName[value.name] : value.code
}

export function resultCodeHttpStatus(value: ResultCode): number {
  if (value.kind === "unrecognized") return 500
  switch (value.name) {
    case "Ok":
      return 200
    case "Unsupported":
      return 501
    case "NotFound":
      return 404
    case "InvalidArgument":
      return 400
    case "TooLarge":
      return 413
    case "Conflict":
      return 409
    case "Stale":
      return 503
    case "VersionSkew":
      return 400
    case "Unauthenticated":
      return 401
    case "Backend":
      return 502
    case "Forbidden":
    case "StepUpRequired":
      return 403
  }
}
