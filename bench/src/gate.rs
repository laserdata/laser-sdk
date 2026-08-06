use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use crate::BenchError;
use crate::report::DeterministicGateEvidence;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GateCommand {
    pub name: String,
    pub program: String,
    pub arguments: Vec<String>,
}

impl GateCommand {
    /// Execute a deterministic benchmark prerequisite without invoking a shell.
    ///
    /// # Errors
    ///
    /// Returns an error when the command cannot start. A nonzero exit is returned as failed evidence.
    pub fn run(&self, working_directory: &Path) -> Result<DeterministicGateEvidence, BenchError> {
        if self.name.trim().is_empty() || self.program.trim().is_empty() {
            return Err(BenchError::Invalid(
                "gate name and program are required".to_owned(),
            ));
        }
        let mut process = Command::new(&self.program);
        process.args(&self.arguments).current_dir(working_directory);
        if self.program == "cargo" {
            process
                .env("CARGO_TARGET_DIR", "target/laser-bench-gates")
                .env_remove("RUSTFLAGS");
        }
        let status = process.status().map_err(|error| {
            BenchError::Invalid(format!(
                "failed to execute deterministic gate `{}`: {error}",
                self.name
            ))
        })?;
        let mut command = Vec::with_capacity(self.arguments.len() + 1);
        command.push(self.program.clone());
        command.extend(self.arguments.iter().cloned());
        let mut observations = BTreeMap::new();
        observations.insert(
            "exit_code".to_owned(),
            status
                .code()
                .map_or(serde_json::Value::Null, serde_json::Value::from),
        );
        if self.program == "cargo" {
            observations.insert(
                "cargo_target_dir".to_owned(),
                serde_json::Value::String("target/laser-bench-gates".to_owned()),
            );
        }
        Ok(DeterministicGateEvidence {
            name: self.name.clone(),
            command,
            passed: status.success(),
            observations,
        })
    }
}

/// The SDK allocation and pointer-identity regression gate.
pub fn sdk_allocation_gate() -> GateCommand {
    GateCommand {
        name: "sdk-allocation-test".to_owned(),
        program: "cargo".to_owned(),
        arguments: ["test", "--locked", "-p", "laser-sdk", "--test", "alloc"]
            .map(str::to_owned)
            .to_vec(),
    }
}

/// The direct Laser-to-Iggy payload pointer-identity regression gate.
pub fn sdk_zero_copy_gate() -> GateCommand {
    GateCommand {
        name: "sdk-direct-zero-copy-test".to_owned(),
        program: "cargo".to_owned(),
        arguments: [
            "test",
            "--locked",
            "-p",
            "laser-sdk",
            "--lib",
            "stream::transport::tests::given_bytes_payload_when_lowered_to_iggy_then_should_preserve_pointer_identity",
        ]
        .map(str::to_owned)
        .to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_allocation_gate_when_constructed_then_should_pin_the_sdk_test() {
        let gate = sdk_allocation_gate();
        assert_eq!(gate.name, "sdk-allocation-test");
        assert_eq!(
            gate.arguments,
            ["test", "--locked", "-p", "laser-sdk", "--test", "alloc"]
        );
    }

    #[test]
    fn given_zero_copy_gate_when_constructed_then_should_pin_the_direct_sdk_test() {
        let gate = sdk_zero_copy_gate();
        assert_eq!(gate.name, "sdk-direct-zero-copy-test");
        assert!(gate.arguments.iter().any(|argument| argument.contains(
            "given_bytes_payload_when_lowered_to_iggy_then_should_preserve_pointer_identity"
        )));
    }

    #[test]
    fn given_a_successful_command_when_run_then_should_record_passing_evidence() {
        let gate = GateCommand {
            name: "rustc-version".to_owned(),
            program: "rustc".to_owned(),
            arguments: vec!["--version".to_owned()],
        };
        let evidence = gate
            .run(Path::new("."))
            .expect("rustc should execute for the benchmark crate");
        assert!(evidence.passed);
        assert_eq!(evidence.command, ["rustc", "--version"]);
    }
}
