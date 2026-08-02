use laser_wire::result::ResultCode;

/// The bearer-token claims an incoming edge request asserts, decoded by the
/// transport (the SDK does not parse tokens, it decides on the decoded claims).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EdgeClaims {
    /// The audiences the token was minted for.
    pub audience: Vec<String>,
    /// The scopes the token grants.
    pub scopes: Vec<String>,
}

/// Why an external-edge request was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EdgeDenial {
    /// The token was not minted for this server: its audience does not include
    /// `expected`. A server accepts only tokens minted for itself, so this is a
    /// hard reject, never a step-up.
    WrongAudience { expected: String },
    /// The token is valid for this server but lacks `required_scope`. The caller
    /// re-authorizes for the scope (a `403` carrying it), so it is a step-up, not
    /// a permanent denial.
    StepUp { required_scope: String },
}

impl EdgeDenial {
    /// The result code a refusal maps to: a wrong-audience token is not a valid
    /// credential for this server (`Unauthenticated`, `401`), while a missing
    /// scope is a step-up (`StepUpRequired`, a `403` naming the scope to acquire).
    pub fn code(&self) -> ResultCode {
        match self {
            EdgeDenial::WrongAudience { .. } => ResultCode::Unauthenticated,
            EdgeDenial::StepUp { .. } => ResultCode::StepUpRequired,
        }
    }

    /// The `WWW-Authenticate` challenge for a step-up, naming the scope the
    /// caller must acquire, or `None` for a wrong-audience reject (nothing to
    /// step up to).
    pub fn challenge(&self) -> Option<String> {
        match self {
            EdgeDenial::StepUp { required_scope } => {
                Some(format!("Bearer scope=\"{required_scope}\""))
            }
            EdgeDenial::WrongAudience { .. } => None,
        }
    }
}

/// Authorize an external-edge (MCP / A2A) request against a decoded token:
/// strict audience validation (the token must name `expected_audience`) then a
/// scope check (missing `required_scope` is a step-up). Never inspect a token
/// minted for another audience, and never forward the caller's token upstream:
/// a bridge acts under its own enrolled identity, minting a fresh upstream
/// credential rather than passing this one through.
pub fn authorize_edge(
    claims: &EdgeClaims,
    expected_audience: &str,
    required_scope: &str,
) -> Result<(), EdgeDenial> {
    if !claims.audience.iter().any(|a| a == expected_audience) {
        return Err(EdgeDenial::WrongAudience {
            expected: expected_audience.to_owned(),
        });
    }
    if !claims.scopes.iter().any(|s| s == required_scope) {
        return Err(EdgeDenial::StepUp {
            required_scope: required_scope.to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(audience: &[&str], scopes: &[&str]) -> EdgeClaims {
        EdgeClaims {
            audience: audience.iter().map(|s| s.to_string()).collect(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn given_a_matching_audience_and_scope_when_authorized_then_should_pass() {
        let c = claims(&["mcp.laserdata"], &["tool:read"]);
        assert!(authorize_edge(&c, "mcp.laserdata", "tool:read").is_ok());
    }

    #[test]
    fn given_a_foreign_audience_when_authorized_then_should_reject_not_step_up() {
        let c = claims(&["other.server"], &["tool:read"]);
        let denial = authorize_edge(&c, "mcp.laserdata", "tool:read").unwrap_err();
        assert!(matches!(denial, EdgeDenial::WrongAudience { .. }));
        assert_eq!(denial.challenge(), None);
        assert_eq!(denial.code(), ResultCode::Unauthenticated);
    }

    #[test]
    fn given_a_missing_scope_when_authorized_then_should_step_up_with_the_scope() {
        let c = claims(&["mcp.laserdata"], &["tool:read"]);
        let denial = authorize_edge(&c, "mcp.laserdata", "tool:write").unwrap_err();
        assert!(matches!(denial, EdgeDenial::StepUp { .. }));
        assert_eq!(denial.code(), ResultCode::StepUpRequired);
        assert_eq!(
            denial.challenge().as_deref(),
            Some("Bearer scope=\"tool:write\"")
        );
    }
}
