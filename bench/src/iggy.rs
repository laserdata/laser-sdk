use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::binary::{BinaryManifest, sha256_file};

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_benchmark_kind
)]
pub enum IggyBenchmarkKind {
    PinnedProducer,
    PinnedConsumer,
    PinnedProducerAndConsumer,
    BalancedProducer,
    BalancedConsumerGroup,
    BalancedProducerAndConsumerGroup,
    EndToEndProducingConsumer,
    EndToEndProducingConsumerGroup,
}

impl IggyBenchmarkKind {
    fn command(self) -> &'static str {
        match self {
            Self::PinnedProducer => "pinned-producer",
            Self::PinnedConsumer => "pinned-consumer",
            Self::PinnedProducerAndConsumer => "pinned-producer-and-consumer",
            Self::BalancedProducer => "balanced-producer",
            Self::BalancedConsumerGroup => "balanced-consumer-group",
            Self::BalancedProducerAndConsumerGroup => "balanced-producer-and-consumer-group",
            Self::EndToEndProducingConsumer => "end-to-end-producing-consumer",
            Self::EndToEndProducingConsumerGroup => "end-to-end-producing-consumer-group",
        }
    }

    fn has_producers(self) -> bool {
        !matches!(self, Self::PinnedConsumer | Self::BalancedConsumerGroup)
    }

    fn has_consumers(self) -> bool {
        matches!(
            self,
            Self::PinnedConsumer
                | Self::PinnedProducerAndConsumer
                | Self::BalancedConsumerGroup
                | Self::BalancedProducerAndConsumerGroup
        )
    }

    fn supports_partitions(self) -> bool {
        !matches!(self, Self::PinnedProducer)
    }
}

fn invalid_benchmark_kind(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported Iggy benchmark kind `{value}`"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IggyBenchmarkRun {
    pub kind: IggyBenchmarkKind,
    pub message_size: usize,
    pub messages_per_batch: usize,
    pub message_batches: u64,
    pub warmup_seconds: u64,
    pub streams: u32,
    pub partitions: u32,
    pub producers: u32,
    pub consumers: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ImportedIggyReport {
    pub source_path: PathBuf,
    pub copied_path: PathBuf,
    pub sha256: String,
    #[serde(skip_serializing)]
    pub report: serde_json::Value,
}

impl IggyBenchmarkRun {
    /// Build the exact TCP VSR invocation arguments for the upstream benchmark binary.
    ///
    /// # Errors
    ///
    /// Returns an error when any workload dimension is zero.
    pub fn arguments(
        &self,
        address: SocketAddr,
        output_directory: &Path,
    ) -> Result<Vec<String>, BenchError> {
        if self.message_size == 0
            || self.messages_per_batch == 0
            || self.message_batches == 0
            || self.streams == 0
            || self.partitions == 0
            || self.producers == 0
        {
            return Err(BenchError::Invalid(
                "Iggy benchmark dimensions must be nonzero".to_owned(),
            ));
        }
        let mut arguments = vec![
            "--message-size".to_owned(),
            self.message_size.to_string(),
            "--messages-per-batch".to_owned(),
            self.messages_per_batch.to_string(),
            "--message-batches".to_owned(),
            self.message_batches.to_string(),
            "--warmup-time".to_owned(),
            format!("{}s", self.warmup_seconds),
            self.kind.command().to_owned(),
            "--streams".to_owned(),
            self.streams.to_string(),
        ];
        if self.kind.supports_partitions() {
            arguments.extend(["--partitions".to_owned(), self.partitions.to_string()]);
        }
        if self.kind.has_producers() {
            arguments.extend(["--producers".to_owned(), self.producers.to_string()]);
        }
        if self.kind.has_consumers() {
            if self.consumers == 0 {
                return Err(BenchError::Invalid(
                    "consumer benchmark requires consumers".to_owned(),
                ));
            }
            arguments.extend(["--consumers".to_owned(), self.consumers.to_string()]);
        }
        arguments.extend([
            "tcp".to_owned(),
            "--server-address".to_owned(),
            address.to_string(),
            "--nodelay".to_owned(),
            "output".to_owned(),
            "--output-dir".to_owned(),
            output_directory.to_string_lossy().into_owned(),
            "--identifier".to_owned(),
            "laser-bench".to_owned(),
        ]);
        Ok(arguments)
    }

    /// Run one upstream Iggy TCP VSR benchmark and copy its JSON report unchanged.
    ///
    /// # Errors
    ///
    /// Returns an error when binary validation, process execution, report discovery, or import fails.
    pub fn execute(
        &self,
        binary: &BinaryManifest,
        address: SocketAddr,
        output_directory: &Path,
    ) -> Result<ImportedIggyReport, BenchError> {
        if binary.name != "iggy-bench" {
            return Err(BenchError::Invalid(format!(
                "expected iggy-bench, resolved `{}`",
                binary.name
            )));
        }
        binary.verify()?;
        fs::create_dir(output_directory).map_err(|source| BenchError::Write {
            path: output_directory.to_path_buf(),
            source,
        })?;
        let stdout = create_log(&output_directory.join("iggy-bench.stdout.log"))?;
        let stderr = create_log(&output_directory.join("iggy-bench.stderr.log"))?;
        let status = Command::new(&binary.path)
            .args(self.arguments(address, output_directory)?)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .status()
            .map_err(|error| BenchError::Invalid(format!("failed to start iggy-bench: {error}")))?;
        if !status.success() {
            return Err(BenchError::Invalid(format!(
                "iggy-bench failed with {status}"
            )));
        }
        let source = find_report(output_directory)?;
        import_report(&source, &output_directory.join("iggy-report.json"))
    }
}

/// Copy an upstream report byte for byte and retain a decoded view for adapters.
///
/// # Errors
///
/// Returns an error when the source is invalid JSON, the destination exists, or file I/O fails.
pub fn import_report(source: &Path, destination: &Path) -> Result<ImportedIggyReport, BenchError> {
    let bytes = fs::read(source).map_err(|source_error| BenchError::Read {
        path: source.to_path_buf(),
        source: source_error,
    })?;
    let report: serde_json::Value = serde_json::from_slice(&bytes)?;
    if !report.is_object() {
        return Err(BenchError::Invalid(
            "Iggy report root must be a JSON object".to_owned(),
        ));
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(|source| BenchError::Write {
            path: destination.to_path_buf(),
            source,
        })?;
    file.write_all(&bytes).map_err(|source| BenchError::Write {
        path: destination.to_path_buf(),
        source,
    })?;
    Ok(ImportedIggyReport {
        source_path: source.to_path_buf(),
        copied_path: destination.to_path_buf(),
        sha256: sha256_file(destination)?,
        report,
    })
}

fn find_report(root: &Path) -> Result<PathBuf, BenchError> {
    let mut directories = vec![root.to_path_buf()];
    let mut reports = Vec::new();
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory).map_err(|source| BenchError::Read {
            path: directory.clone(),
            source,
        })? {
            let entry = entry.map_err(|source| BenchError::Read {
                path: directory.clone(),
                source,
            })?;
            let path = entry.path();
            if path.is_dir() {
                directories.push(path);
            } else if path.file_name().is_some_and(|name| name == "report.json") {
                reports.push(path);
            }
        }
    }
    if reports.len() != 1 {
        return Err(BenchError::Invalid(format!(
            "expected one Iggy report under `{}`, found {}",
            root.display(),
            reports.len()
        )));
    }
    Ok(reports.remove(0))
}

fn create_log(path: &Path) -> Result<std::fs::File, BenchError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| BenchError::Write {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_l1_run_when_arguments_are_built_then_should_use_only_tcp_vsr() {
        let run = IggyBenchmarkRun {
            kind: IggyBenchmarkKind::PinnedProducerAndConsumer,
            message_size: 1_024,
            messages_per_batch: 100,
            message_batches: 10,
            warmup_seconds: 1,
            streams: 1,
            partitions: 8,
            producers: 2,
            consumers: 2,
        };
        let arguments = run
            .arguments(
                "127.0.0.1:8090".parse().expect("address should parse"),
                Path::new("results"),
            )
            .expect("arguments should be valid");
        assert!(arguments.contains(&"tcp".to_owned()));
        assert!(
            !arguments
                .iter()
                .any(|argument| { matches!(argument.as_str(), "http" | "quic" | "web-socket") })
        );
    }

    #[test]
    fn given_upstream_report_when_imported_then_should_preserve_exact_bytes() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let source = directory.path().join("report.json");
        let destination = directory.path().join("iggy-report.json");
        fs::write(&source, b"{\n  \"value\": 7\n}\n").expect("report fixture should be written");
        let imported = import_report(&source, &destination).expect("report should import");
        assert_eq!(
            fs::read(&source).expect("source should read"),
            fs::read(&destination).expect("copy should read")
        );
        assert_eq!(imported.report["value"], 7);
    }
}
