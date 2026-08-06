use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::BenchError;

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ClaimsRegister {
    pub schema_version: u32,
    pub claim: Vec<Claim>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Claim {
    pub id: String,
    pub statement: String,
    pub evidence: Vec<String>,
    #[serde(default)]
    pub throughput_ratio_lower_bound: Option<f64>,
    #[serde(default)]
    pub latency_ratio_upper_bound: Option<f64>,
}

impl ClaimsRegister {
    /// Load and validate a claims register.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, decoded, or validated.
    pub fn load(path: &Path) -> Result<Self, BenchError> {
        let source = fs::read_to_string(path).map_err(|source| BenchError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let register: Self = toml::from_str(&source)?;
        register.validate()?;
        Ok(register)
    }

    /// Validate claim identity, evidence, and the direct-streaming bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported schema, duplicate claim, incomplete claim, or invalid C2 bound.
    pub fn validate(&self) -> Result<(), BenchError> {
        if self.schema_version != 1 {
            return Err(BenchError::Invalid(format!(
                "unsupported claims schema version {}",
                self.schema_version
            )));
        }
        let mut ids = BTreeSet::new();
        for claim in &self.claim {
            if !ids.insert(&claim.id) {
                return Err(BenchError::Invalid(format!(
                    "duplicate claim id `{}`",
                    claim.id
                )));
            }
            if claim.statement.trim().is_empty() || claim.evidence.is_empty() {
                return Err(BenchError::Invalid(format!(
                    "claim `{}` requires a statement and evidence",
                    claim.id
                )));
            }
        }
        let c2 = self
            .claim
            .iter()
            .find(|claim| claim.id == "C2")
            .ok_or_else(|| BenchError::Invalid("claim C2 is required".to_owned()))?;
        if c2.throughput_ratio_lower_bound != Some(0.99)
            || c2.latency_ratio_upper_bound != Some(1.01)
        {
            return Err(BenchError::Invalid(
                "claim C2 must pin throughput 0.99 and latency 1.01".to_owned(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_duplicate_claim_when_validated_then_should_reject_it() {
        let claim = Claim {
            id: "C2".to_owned(),
            statement: "direct streaming".to_owned(),
            evidence: vec!["L2".to_owned()],
            throughput_ratio_lower_bound: Some(0.99),
            latency_ratio_upper_bound: Some(1.01),
        };
        let register = ClaimsRegister {
            schema_version: 1,
            claim: vec![claim.clone(), claim],
        };
        assert!(register.validate().is_err());
    }
}
