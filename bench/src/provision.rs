use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::BenchError;
use crate::artifact::{ArtifactKind, ArtifactSource};
use crate::binary::BinaryManifest;
use crate::build::{LocalBinary, LocalBuild};
use crate::manifest::{ProvisionMode, SuiteManifest};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResolvedStack {
    pub mode: ProvisionMode,
    pub benchmark: Option<BinaryManifest>,
    pub iggy_server: Option<BinaryManifest>,
    pub iggy_bench: Option<BinaryManifest>,
    pub plane: Option<BinaryManifest>,
    pub authoritative: bool,
    pub warning: Option<String>,
}

impl ResolvedStack {
    /// Verify every resolved binary before a process starts.
    ///
    /// # Errors
    ///
    /// Returns an error when a required binary is absent or any binary changed after resolution.
    pub fn verify(&self, requires_plane: bool) -> Result<(), BenchError> {
        if let Some(benchmark) = &self.benchmark {
            benchmark.verify()?;
        }
        if self.mode == ProvisionMode::Compose {
            return Ok(());
        }
        let server = self.iggy_server.as_ref().ok_or_else(|| {
            BenchError::Invalid("resolved stack has no native Iggy server".to_owned())
        })?;
        let bench = self.iggy_bench.as_ref().ok_or_else(|| {
            BenchError::Invalid("resolved stack has no native Iggy benchmark".to_owned())
        })?;
        server.verify()?;
        bench.verify()?;
        if requires_plane {
            self.plane
                .as_ref()
                .ok_or_else(|| {
                    BenchError::Invalid("resolved stack has no plane binary".to_owned())
                })?
                .verify()?;
        }
        Ok(())
    }
}

/// Resolve the suite's source, path, signed artifact, or Compose provider.
///
/// # Errors
///
/// Returns an error when the selected provider is incomplete or cannot build or validate its binaries.
pub fn resolve(
    manifest: &SuiteManifest,
    benchmark_root: &Path,
) -> Result<ResolvedStack, BenchError> {
    match manifest.provisioning.mode {
        ProvisionMode::Source => resolve_source(manifest),
        ProvisionMode::Path => resolve_paths(manifest),
        ProvisionMode::Artifact => resolve_artifacts(manifest, benchmark_root),
        ProvisionMode::Compose => resolve_compose(manifest),
    }
}

fn resolve_source(manifest: &SuiteManifest) -> Result<ResolvedStack, BenchError> {
    let iggy_root = from_current_directory(&required_path(
        manifest.provisioning.iggy_root.as_ref(),
        "iggy_root",
    )?)?;
    let iggy_build = LocalBuild {
        target_root: iggy_root.join("target"),
        source_root: iggy_root.clone(),
        cpu_target: manifest.provisioning.cpu_target.clone(),
    };
    let iggy_server = iggy_build.build(LocalBinary::IggyServer)?;
    let iggy_bench = iggy_build.build(LocalBinary::IggyBench)?;
    let plane = if manifest.requires_plane() {
        let plane_root = from_current_directory(&required_path(
            manifest.provisioning.plane_root.as_ref(),
            "plane_root",
        )?)?;
        Some(
            LocalBuild {
                target_root: plane_root.join("target"),
                source_root: plane_root,
                cpu_target: manifest.provisioning.cpu_target.clone(),
            }
            .build(LocalBinary::Plane)?,
        )
    } else {
        None
    };
    Ok(ResolvedStack {
        mode: ProvisionMode::Source,
        benchmark: None,
        iggy_server: Some(iggy_server),
        iggy_bench: Some(iggy_bench),
        plane,
        authoritative: manifest.authoritative,
        warning: None,
    })
}

fn resolve_paths(manifest: &SuiteManifest) -> Result<ResolvedStack, BenchError> {
    if manifest.authoritative {
        return Err(BenchError::Invalid(
            "authoritative path mode requires source or signed artifact provenance".to_owned(),
        ));
    }
    let cpu_target = &manifest.provisioning.cpu_target;
    let server_path = from_current_directory(&required_path(
        manifest.provisioning.iggy_server.as_ref(),
        "iggy_server",
    )?)?;
    let server = BinaryManifest::inspect(
        "iggy-server",
        &server_path,
        "path",
        "caller-provided",
        false,
        None,
        None,
    )?;
    let bench_path = from_current_directory(&required_path(
        manifest.provisioning.iggy_bench.as_ref(),
        "iggy_bench",
    )?)?;
    let bench = BinaryManifest::inspect(
        "iggy-bench",
        &bench_path,
        "path",
        "caller-provided",
        false,
        None,
        None,
    )?;
    let plane = manifest
        .provisioning
        .plane
        .as_ref()
        .map(|path| {
            let path = from_current_directory(path)?;
            BinaryManifest::inspect("plane", &path, "path", "caller-provided", false, None, None)
        })
        .transpose()?;
    let resolved = ResolvedStack {
        mode: ProvisionMode::Path,
        benchmark: None,
        iggy_server: Some(server),
        iggy_bench: Some(bench),
        plane,
        authoritative: false,
        warning: Some(format!(
            "path mode records binary digests but has no source provenance for CPU target `{cpu_target}`"
        )),
    };
    resolved.verify(manifest.requires_plane())?;
    Ok(resolved)
}

fn resolve_artifacts(
    manifest: &SuiteManifest,
    benchmark_root: &Path,
) -> Result<ResolvedStack, BenchError> {
    let source = ArtifactSource::laserdata();
    let cache_root = manifest
        .provisioning
        .cache_root
        .as_ref()
        .map_or_else(default_cache_root, |path| {
            absolute_from(benchmark_root, path)
        });
    let server_version = required_text(
        manifest.provisioning.iggy_server_version.as_deref(),
        "iggy_server_version",
    )?;
    let bench_version = required_text(
        manifest.provisioning.iggy_bench_version.as_deref(),
        "iggy_bench_version",
    )?;
    let cpu_target = &manifest.provisioning.cpu_target;
    let plane = manifest
        .requires_plane()
        .then(|| {
            let version = required_text(
                manifest.provisioning.plane_version.as_deref(),
                "plane_version",
            )?;
            source.resolve(ArtifactKind::Plane, version, cpu_target, &cache_root)
        })
        .transpose()?;
    let resolved = ResolvedStack {
        mode: ProvisionMode::Artifact,
        benchmark: None,
        iggy_server: Some(source.resolve(
            ArtifactKind::IggyServer,
            server_version,
            cpu_target,
            &cache_root,
        )?),
        iggy_bench: Some(source.resolve(
            ArtifactKind::IggyBench,
            bench_version,
            cpu_target,
            &cache_root,
        )?),
        plane,
        authoritative: manifest.authoritative,
        warning: None,
    };
    resolved.verify(manifest.requires_plane())?;
    Ok(resolved)
}

fn resolve_compose(manifest: &SuiteManifest) -> Result<ResolvedStack, BenchError> {
    if manifest.authoritative {
        return Err(BenchError::Invalid(
            "Docker Compose runs cannot be authoritative".to_owned(),
        ));
    }
    let compose_file = required_path(manifest.provisioning.compose_file.as_ref(), "compose_file")?;
    let status = Command::new("docker")
        .args(["compose", "-f"])
        .arg(compose_file)
        .arg("config")
        .arg("--quiet")
        .status()
        .map_err(|error| BenchError::Invalid(format!("failed to start Docker Compose: {error}")))?;
    if !status.success() {
        return Err(BenchError::Invalid(
            "Docker Compose configuration is invalid".to_owned(),
        ));
    }
    Ok(ResolvedStack {
        mode: ProvisionMode::Compose,
        benchmark: None,
        iggy_server: None,
        iggy_bench: None,
        plane: None,
        authoritative: false,
        warning: Some(
            "Docker Compose is a non-authoritative convenience tier because container scheduling, networking, cgroups, and filesystem layers can affect results"
                .to_owned(),
        ),
    })
}

fn required_path(path: Option<&PathBuf>, name: &str) -> Result<PathBuf, BenchError> {
    path.cloned()
        .ok_or_else(|| BenchError::Invalid(format!("provisioning field `{name}` is required")))
}

fn required_text<'a>(value: Option<&'a str>, name: &str) -> Result<&'a str, BenchError> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| BenchError::Invalid(format!("provisioning field `{name}` is required")))
}

fn absolute_from(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn default_cache_root() -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME").map_or_else(
        || {
            std::env::var_os("HOME").map_or_else(
                || PathBuf::from(".cache/laser-bench"),
                |home| PathBuf::from(home).join(".cache/laser-bench"),
            )
        },
        |cache| PathBuf::from(cache).join("laser-bench"),
    )
}

fn from_current_directory(path: &Path) -> Result<PathBuf, BenchError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|root| root.join(path))
            .map_err(|error| {
                BenchError::Invalid(format!("failed to inspect current directory: {error}"))
            })?
    };
    absolute.canonicalize().map_err(|error| {
        BenchError::Invalid(format!(
            "failed to resolve `{}`: {error}",
            absolute.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::BenchmarkLayer;

    #[test]
    fn given_managed_artifact_suite_without_plane_identity_when_validated_then_should_reject_it() {
        let mut manifest = SuiteManifest::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/suite-minimal.toml"),
        )
        .expect("fixture suite should load");
        manifest.provisioning.mode = ProvisionMode::Artifact;
        manifest.provisioning.iggy_server_version = Some("selected-server-version".to_owned());
        manifest.provisioning.iggy_bench_version = Some("selected-bench-version".to_owned());
        manifest.scenarios[0].layer = BenchmarkLayer::L4;
        let error = manifest
            .validate()
            .expect_err("managed artifact mode should require a plane identity");
        assert!(error.to_string().contains("plane_version"));
    }
}
