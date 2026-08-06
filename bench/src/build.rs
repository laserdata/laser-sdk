use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::BenchError;
use crate::binary::{BinaryManifest, BuildIdentity, SourceSnapshot, command_output};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalBinary {
    IggyServer,
    IggyBench,
    Plane,
}

impl LocalBinary {
    fn package(self) -> &'static str {
        match self {
            Self::IggyServer => "server-ng",
            Self::IggyBench => "iggy-bench",
            Self::Plane => "plane",
        }
    }

    fn binary(self) -> &'static str {
        match self {
            Self::IggyServer => "iggy-server-ng",
            Self::IggyBench => "iggy-bench",
            Self::Plane => "plane",
        }
    }

    fn cargo_manifest(self) -> &'static str {
        match self {
            Self::IggyServer => "core/server-ng/Cargo.toml",
            Self::IggyBench => "core/bench/Cargo.toml",
            Self::Plane => "plane/Cargo.toml",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalBuild {
    pub source_root: PathBuf,
    pub target_root: PathBuf,
    pub cpu_target: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct CachedBinaryManifest {
    manifest: BinaryManifest,
}

impl LocalBuild {
    /// Build one locked CPU-targeted release binary and return an immutable identity for its bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe CPU target, source inspection failure, Cargo failure, or missing binary.
    pub fn build(&self, binary: LocalBinary) -> Result<BinaryManifest, BenchError> {
        validate_cpu_target(&self.cpu_target)?;
        verify_vsr_graph(&self.source_root, binary)?;
        let source_snapshot = SourceSnapshot::capture(&self.source_root)?;
        let target_triple = host_target_triple()?;
        let version = package_version(&self.source_root.join(binary.cargo_manifest()))?;
        let build = self.build_identity(binary, &target_triple)?;
        let path = self.binary_path(binary, &target_triple);
        let cache_path = self.cache_path(binary, &target_triple);
        if let Some(manifest) = Self::try_cached_manifest(
            &cache_path,
            binary,
            &version,
            &source_snapshot,
            &build,
            &path,
        )? {
            return Ok(manifest);
        }
        let status = Command::new("cargo")
            .args(&build.command[1..])
            .current_dir(&self.source_root)
            .env("CARGO_TARGET_DIR", self.target_directory())
            .env("RUSTFLAGS", &build.rustflags)
            .status()
            .map_err(|error| {
                BenchError::Invalid(format!("failed to start Cargo build: {error}"))
            })?;
        if !status.success() {
            return Err(BenchError::Invalid(format!(
                "Cargo failed to build {}",
                binary.binary()
            )));
        }
        let manifest = BinaryManifest::inspect(
            binary.binary(),
            &path,
            "source",
            &version,
            false,
            Some(source_snapshot),
            Some(build),
        )?;
        write_cached_manifest(&cache_path, &manifest)?;
        Ok(manifest)
    }

    fn build_identity(
        &self,
        binary: LocalBinary,
        target_triple: &str,
    ) -> Result<BuildIdentity, BenchError> {
        let cargo_version = tool_version("cargo")?;
        let rustc_version = tool_version("rustc")?;
        let rustflags = format!("-C target-cpu={}", self.cpu_target);
        let target_directory = self.target_directory();
        let arguments = [
            "build".to_owned(),
            "--locked".to_owned(),
            "--release".to_owned(),
            "--target".to_owned(),
            target_triple.to_owned(),
            "-p".to_owned(),
            binary.package().to_owned(),
            "--bin".to_owned(),
            binary.binary().to_owned(),
        ];
        let environment = BTreeMap::from([
            (
                "CARGO_TARGET_DIR".to_owned(),
                target_directory.to_string_lossy().into_owned(),
            ),
            ("RUSTFLAGS".to_owned(), rustflags.clone()),
        ]);
        let mut command = vec!["cargo".to_owned()];
        command.extend(arguments);
        Ok(BuildIdentity {
            profile: "release".to_owned(),
            release_profile: crate::binary::release_profile_summary(
                &self.source_root.join("Cargo.toml"),
            ),
            target_triple: target_triple.to_owned(),
            cpu_target: self.cpu_target.clone(),
            rustflags,
            cargo_version,
            rustc_version,
            command,
            environment,
        })
    }

    /// Return the exact build command without executing it.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe CPU target or unavailable host target triple.
    pub fn command(&self, binary: LocalBinary) -> Result<Vec<String>, BenchError> {
        validate_cpu_target(&self.cpu_target)?;
        Ok(vec![
            "cargo".to_owned(),
            "build".to_owned(),
            "--locked".to_owned(),
            "--release".to_owned(),
            "--target".to_owned(),
            host_target_triple()?,
            "-p".to_owned(),
            binary.package().to_owned(),
            "--bin".to_owned(),
            binary.binary().to_owned(),
        ])
    }

    fn target_directory(&self) -> PathBuf {
        self.target_root
            .join(format!("laser-bench-{}", self.cpu_target))
    }

    fn binary_path(&self, binary: LocalBinary, target_triple: &str) -> PathBuf {
        self.target_directory()
            .join(target_triple)
            .join("release")
            .join(binary.binary())
    }

    fn cache_path(&self, binary: LocalBinary, target_triple: &str) -> PathBuf {
        self.binary_path(binary, target_triple)
            .with_extension("laser-bench.json")
    }

    fn try_cached_manifest(
        cache_path: &Path,
        binary: LocalBinary,
        version: &str,
        source_snapshot: &SourceSnapshot,
        build: &BuildIdentity,
        path: &Path,
    ) -> Result<Option<BinaryManifest>, BenchError> {
        let Some(cached) = read_cached_manifest(cache_path)? else {
            return Ok(None);
        };
        let manifest = cached.manifest;
        if manifest.name != binary.binary()
            || manifest.version != version
            || manifest.path != path
            || manifest.source_snapshot.as_ref() != Some(source_snapshot)
            || manifest.build.as_ref() != Some(build)
        {
            return Ok(None);
        }
        let mut manifest = manifest;
        manifest.verify()?;
        manifest.reused_local_binary = true;
        Ok(Some(manifest))
    }
}

fn read_cached_manifest(path: &Path) -> Result<Option<CachedBinaryManifest>, BenchError> {
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = fs::read(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn write_cached_manifest(path: &Path, manifest: &BinaryManifest) -> Result<(), BenchError> {
    let bytes = serde_json::to_vec_pretty(&CachedBinaryManifest {
        manifest: manifest.clone(),
    })?;
    fs::write(path, bytes).map_err(|source| BenchError::Write {
        path: path.to_path_buf(),
        source,
    })
}

fn host_target_triple() -> Result<String, BenchError> {
    let output = command_output(Path::new("."), "rustc", ["-vV"])?;
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .map(str::to_owned)
        .ok_or_else(|| BenchError::Invalid("rustc did not report a host target".to_owned()))
}

fn tool_version(program: &str) -> Result<String, BenchError> {
    let output = command_output(Path::new("."), program, ["--version"])?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn package_version(path: &Path) -> Result<String, BenchError> {
    let source = fs::read_to_string(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let manifest: toml::Value = toml::from_str(&source)?;
    manifest
        .get("package")
        .and_then(|package| package.get("version"))
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            BenchError::Invalid(format!(
                "package version is missing from `{}`",
                path.display()
            ))
        })
}

fn validate_cpu_target(cpu_target: &str) -> Result<(), BenchError> {
    if cpu_target.is_empty()
        || !cpu_target
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(BenchError::Invalid(format!(
            "unsafe CPU target `{cpu_target}`"
        )));
    }
    Ok(())
}

fn verify_vsr_graph(root: &Path, binary: LocalBinary) -> Result<(), BenchError> {
    let dependency = match binary {
        LocalBinary::IggyServer => "iggy_common",
        LocalBinary::IggyBench => "iggy",
        LocalBinary::Plane => return Ok(()),
    };
    let output = command_output(
        root,
        "cargo",
        [
            "tree",
            "--locked",
            "-p",
            binary.package(),
            "-e",
            "features",
            "-i",
            dependency,
        ],
    )?;
    if !String::from_utf8_lossy(&output.stdout).contains("feature \"vsr\"") {
        return Err(BenchError::Invalid(format!(
            "{} does not enable the mandatory VSR feature",
            binary.binary()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_native_iggy_build_when_planned_then_should_be_locked_release_server_ng() {
        let build = LocalBuild {
            source_root: PathBuf::from("/path/to/iggy-source"),
            target_root: PathBuf::from("/path/to/iggy-source/target"),
            cpu_target: "native".to_owned(),
        };
        let command = build
            .command(LocalBinary::IggyServer)
            .expect("build command should be valid");
        assert!(command.windows(2).any(|pair| pair == ["-p", "server-ng"]));
        assert!(
            command
                .windows(2)
                .any(|pair| pair == ["--bin", "iggy-server-ng"])
        );
        assert!(command.contains(&"--locked".to_owned()));
        assert!(command.contains(&"--release".to_owned()));
    }

    #[test]
    fn given_classic_server_name_when_selecting_local_binary_then_should_have_no_variant() {
        let supported = [
            LocalBinary::IggyServer.binary(),
            LocalBinary::IggyBench.binary(),
            LocalBinary::Plane.binary(),
        ];
        assert!(!supported.contains(&"iggy-server"));
    }

    #[cfg(unix)]
    #[test]
    fn given_changed_cached_binary_when_reused_then_should_reject_provenance() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary build directory should exist");
        let release = directory.path().join("release");
        fs::create_dir(&release).expect("release directory should exist");
        let binary_path = release.join("iggy-server-ng");
        fs::write(&binary_path, b"original").expect("fixture binary should write");
        fs::set_permissions(&binary_path, fs::Permissions::from_mode(0o755))
            .expect("fixture binary should be executable");
        let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let source_snapshot =
            SourceSnapshot::capture(&source_root).expect("source snapshot should exist");
        let build = BuildIdentity {
            profile: "release".to_owned(),
            release_profile: "lto=true,codegen-units=1".to_owned(),
            target_triple: "fixture-target".to_owned(),
            cpu_target: "native".to_owned(),
            rustflags: "-C target-cpu=native".to_owned(),
            cargo_version: "cargo fixture".to_owned(),
            rustc_version: "rustc fixture".to_owned(),
            command: vec!["cargo".to_owned(), "build".to_owned()],
            environment: BTreeMap::new(),
        };
        let manifest = BinaryManifest::inspect(
            "iggy-server-ng",
            &binary_path,
            "source",
            "fixture",
            false,
            Some(source_snapshot.clone()),
            Some(build.clone()),
        )
        .expect("fixture manifest should inspect");
        let cache_path = binary_path.with_extension("laser-bench.json");
        write_cached_manifest(&cache_path, &manifest).expect("cache manifest should write");
        fs::write(&binary_path, b"changed").expect("fixture binary should change");

        let result = LocalBuild::try_cached_manifest(
            &cache_path,
            LocalBinary::IggyServer,
            "fixture",
            &source_snapshot,
            &build,
            &binary_path,
        );

        assert!(result.is_err());
    }
}
