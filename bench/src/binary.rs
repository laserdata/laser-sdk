use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::BenchError;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SourceSnapshot {
    pub root: PathBuf,
    pub revision: String,
    pub dirty: bool,
    pub dirty_digest: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BuildIdentity {
    pub profile: String,
    #[serde(default)]
    pub release_profile: String,
    pub target_triple: String,
    pub cpu_target: String,
    pub rustflags: String,
    pub cargo_version: String,
    pub rustc_version: String,
    pub command: Vec<String>,
    pub environment: BTreeMap<String, String>,
}

/// Summarize the `[profile.release]` overrides declared by the workspace
/// manifest that governs a build, so reports can distinguish an LTO build
/// from a Cargo-default release build.
#[must_use]
pub fn release_profile_summary(workspace_manifest: &Path) -> String {
    let Ok(source) = fs::read_to_string(workspace_manifest) else {
        return "unknown".to_owned();
    };
    let Ok(value) = source.parse::<toml::Value>() else {
        return "unknown".to_owned();
    };
    let Some(profile) = value
        .get("profile")
        .and_then(|profile| profile.get("release"))
    else {
        return "cargo-default".to_owned();
    };
    let lto = profile
        .get("lto")
        .map_or("default".to_owned(), ToString::to_string);
    let codegen_units = profile
        .get("codegen-units")
        .map_or("default".to_owned(), ToString::to_string);
    format!("lto={lto},codegen-units={codegen_units}")
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BinaryManifest {
    pub schema_version: u32,
    pub name: String,
    pub path: PathBuf,
    pub source: String,
    pub version: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub minisign_verified: bool,
    #[serde(default)]
    pub reused_local_binary: bool,
    pub source_snapshot: Option<SourceSnapshot>,
    pub build: Option<BuildIdentity>,
}

impl SourceSnapshot {
    /// Capture the Git revision and a digest of every tracked and untracked worktree change.
    ///
    /// # Errors
    ///
    /// Returns an error when Git cannot inspect the source tree or an untracked file cannot be read.
    pub fn capture(root: &Path) -> Result<Self, BenchError> {
        let revision = command_text(root, "git", ["rev-parse", "HEAD"])?;
        let status = command_output(
            root,
            "git",
            ["status", "--porcelain=v1", "--untracked-files=all"],
        )?;
        let dirty = !status.stdout.is_empty();
        let dirty_digest = if dirty {
            let diff = command_output(root, "git", ["diff", "--binary", "HEAD"])?;
            let mut digest = Sha256::new();
            digest.update(&status.stdout);
            digest.update(&diff.stdout);
            for line in String::from_utf8_lossy(&status.stdout).lines() {
                if let Some(relative) = line.strip_prefix("?? ") {
                    let path = root.join(relative);
                    if path.is_file() {
                        digest.update(relative.as_bytes());
                        digest.update(fs::read(&path).map_err(|source| BenchError::Read {
                            path: path.clone(),
                            source,
                        })?);
                    }
                }
            }
            Some(hex_digest(digest.finalize()))
        } else {
            None
        };
        Ok(Self {
            root: root.to_path_buf(),
            revision,
            dirty,
            dirty_digest,
        })
    }

    /// Confirm that a source tree still matches the state recorded before its build.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot be inspected or its revision or patch digest changed.
    pub fn verify_current(&self) -> Result<(), BenchError> {
        let current = Self::capture(&self.root)?;
        if current.revision != self.revision || current.dirty_digest != self.dirty_digest {
            return Err(BenchError::Invalid(format!(
                "source tree `{}` changed after the binary was built",
                self.root.display()
            )));
        }
        Ok(())
    }
}

impl BinaryManifest {
    /// Inspect the running release-built benchmark executable and bind it to its source and toolchain.
    ///
    /// # Errors
    ///
    /// Returns an error when the executable, source snapshot, or toolchain identity cannot be inspected.
    pub fn running_benchmark(root: &Path) -> Result<Self, BenchError> {
        let path = std::env::current_exe().map_err(|error| {
            BenchError::Invalid(format!(
                "failed to inspect running benchmark binary: {error}"
            ))
        })?;
        let source_snapshot = SourceSnapshot::capture(root)?;
        let cargo_version = command_text(root, "cargo", ["--version"])?;
        let rustc_verbose = command_text(root, "rustc", ["-vV"])?;
        let target_triple = rustc_verbose
            .lines()
            .find_map(|line| line.strip_prefix("host: "))
            .ok_or_else(|| BenchError::Invalid("rustc did not report a host triple".to_owned()))?
            .to_owned();
        let rustflags = std::env::var("RUSTFLAGS").unwrap_or_default();
        let environment = ["CARGO_TARGET_DIR", "RUSTFLAGS"]
            .into_iter()
            .filter_map(|name| {
                std::env::var(name)
                    .ok()
                    .map(|value| (name.to_owned(), value))
            })
            .collect();
        let effective_cpu_target = rustflags
            .split_whitespace()
            .find_map(|argument| argument.strip_prefix("target-cpu="))
            .unwrap_or("generic")
            .to_owned();
        let build = BuildIdentity {
            profile: "release".to_owned(),
            release_profile: release_profile_summary(&root.join("bench/Cargo.toml")),
            target_triple,
            cpu_target: effective_cpu_target,
            rustflags,
            cargo_version,
            rustc_version: rustc_verbose.lines().next().unwrap_or("rustc").to_owned(),
            command: vec![
                "cargo".to_owned(),
                "build".to_owned(),
                "--locked".to_owned(),
                "--release".to_owned(),
                "--manifest-path".to_owned(),
                "bench/Cargo.toml".to_owned(),
                "--bin".to_owned(),
                "laser-bench".to_owned(),
            ],
            environment,
        };
        Self::inspect(
            "laser-bench",
            &path,
            "source",
            env!("CARGO_PKG_VERSION"),
            false,
            Some(source_snapshot),
            Some(build),
        )
    }

    /// Inspect a resolved binary and bind its bytes to build and source provenance.
    ///
    /// # Errors
    ///
    /// Returns an error when the binary is missing, not executable, or cannot be read.
    pub fn inspect(
        name: &str,
        path: &Path,
        source: &str,
        version: &str,
        minisign_verified: bool,
        source_snapshot: Option<SourceSnapshot>,
        build: Option<BuildIdentity>,
    ) -> Result<Self, BenchError> {
        validate_executable(path)?;
        let metadata = fs::metadata(path).map_err(|source| BenchError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(Self {
            schema_version: 1,
            name: name.to_owned(),
            path: path.to_path_buf(),
            source: source.to_owned(),
            version: version.to_owned(),
            size_bytes: metadata.len(),
            sha256: sha256_file(path)?,
            minisign_verified,
            reused_local_binary: false,
            source_snapshot,
            build,
        })
    }

    /// Verify that the binary and its source still match this manifest.
    ///
    /// # Errors
    ///
    /// Returns an error for changed bytes, non-release source builds, or changed source state.
    pub fn verify(&self) -> Result<(), BenchError> {
        validate_executable(&self.path)?;
        if sha256_file(&self.path)? != self.sha256 {
            return Err(BenchError::Invalid(format!(
                "binary digest changed for `{}`",
                self.path.display()
            )));
        }
        if let Some(build) = &self.build
            && (build.profile != "release"
                || !self
                    .path
                    .components()
                    .any(|part| part.as_os_str() == "release"))
        {
            return Err(BenchError::Invalid(format!(
                "timed binary `{}` is not a release build",
                self.path.display()
            )));
        }
        if let Some(source) = &self.source_snapshot {
            source.verify_current()?;
        }
        Ok(())
    }

    /// Write the manifest without replacing existing evidence.
    ///
    /// # Errors
    ///
    /// Returns an error when serialization fails, the path exists, or writing fails.
    pub fn write_immutable(&self, path: &Path) -> Result<(), BenchError> {
        let bytes = serde_json::to_vec_pretty(self)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|source| BenchError::Write {
                path: path.to_path_buf(),
                source,
            })?;
        file.write_all(&bytes).map_err(|source| BenchError::Write {
            path: path.to_path_buf(),
            source,
        })
    }
}

/// Compute a SHA-256 digest for a file.
///
/// # Errors
///
/// Returns an error when the file cannot be read.
pub fn sha256_file(path: &Path) -> Result<String, BenchError> {
    let bytes = fs::read(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(hex_digest(Sha256::digest(bytes)))
}

/// Run a command and return its output only when it exits successfully.
///
/// # Errors
///
/// Returns an error when the process cannot start or exits unsuccessfully.
pub fn command_output<I, S>(root: &Path, program: &str, arguments: I) -> Result<Output, BenchError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .output()
        .map_err(|error| BenchError::Invalid(format!("failed to execute `{program}`: {error}")))?;
    if !output.status.success() {
        return Err(BenchError::Invalid(format!(
            "`{program}` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output)
}

fn command_text<I, S>(root: &Path, program: &str, arguments: I) -> Result<String, BenchError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = command_output(root, program, arguments)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn validate_executable(path: &Path) -> Result<(), BenchError> {
    if !path.is_file() {
        return Err(BenchError::Invalid(format!(
            "binary `{}` does not exist",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(path)
            .map_err(|source| BenchError::Read {
                path: path.to_path_buf(),
                source,
            })?
            .permissions()
            .mode();
        if mode & 0o111 == 0 {
            return Err(BenchError::Invalid(format!(
                "binary `{}` is not executable",
                path.display()
            )));
        }
    }
    Ok(())
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    let digest = digest.as_ref();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn given_executable_bytes_when_inspected_then_should_detect_later_changes() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("tool");
        fs::write(&path, b"first").expect("fixture binary should be written");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755))
            .expect("fixture binary should become executable");
        let manifest = BinaryManifest::inspect("tool", &path, "path", "fixture", false, None, None)
            .expect("fixture binary should be inspected");
        manifest.verify().expect("unchanged binary should verify");
        fs::write(&path, b"second").expect("fixture binary should change");
        assert!(manifest.verify().is_err());
    }

    #[test]
    fn given_repository_when_captured_then_should_include_a_revision() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let snapshot = SourceSnapshot::capture(&root).expect("repository should be inspectable");
        assert_eq!(snapshot.revision.len(), 40);
        assert_eq!(snapshot.dirty, snapshot.dirty_digest.is_some());
    }
}
