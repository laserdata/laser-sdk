use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use minisign_verify::{PublicKey, Signature};
use serde::{Deserialize, Serialize};
use strum::{Display, IntoStaticStr};

use crate::BenchError;
use crate::binary::BinaryManifest;

#[derive(Clone, Copy, Debug, Deserialize, Display, IntoStaticStr, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ArtifactKind {
    IggyServer,
    IggyBench,
    Plane,
}

impl ArtifactKind {
    fn directory(self) -> &'static str {
        match self {
            Self::IggyServer => "iggy-server",
            Self::IggyBench => "iggy-bench",
            Self::Plane => "plane",
        }
    }

    fn manifest_name(self) -> &'static str {
        match self {
            Self::IggyServer => "iggy-server",
            Self::IggyBench => "iggy-bench",
            Self::Plane => "plane",
        }
    }

    fn filename(self, cpu_target: &str) -> Result<String, BenchError> {
        validate_component(cpu_target, "CPU target")?;
        let architecture = if cpu_target == "arm64" {
            "linux-arm64".to_owned()
        } else {
            format!("linux-amd64-{cpu_target}")
        };
        Ok(format!("{}-{architecture}", self.directory()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactSource {
    base_url: &'static str,
    public_key: PathBuf,
}

impl ArtifactSource {
    /// Use the fixed artifact origin and repository verification key.
    #[must_use]
    pub fn laserdata() -> Self {
        Self {
            base_url: "https://artifacts.laserdata.com",
            public_key: Path::new(env!("CARGO_MANIFEST_DIR")).join("keys/minisign.pub"),
        }
    }

    /// Resolve the exact caller-selected version, verify its signature, and record its digest.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe version, download failure, signature failure, or cache failure.
    pub fn resolve(
        &self,
        kind: ArtifactKind,
        version: &str,
        cpu_target: &str,
        cache_root: &Path,
    ) -> Result<BinaryManifest, BenchError> {
        validate_component(version, "version")?;
        let filename = kind.filename(cpu_target)?;
        let destination = cache_root
            .join(kind.directory())
            .join(version)
            .join(cpu_target);
        let binary = destination.join(&filename);
        let signature = destination.join(format!("{filename}.minisig"));
        if destination.exists() {
            if !binary.is_file() || !signature.is_file() {
                return Err(BenchError::Invalid(format!(
                    "artifact cache `{}` is incomplete",
                    destination.display()
                )));
            }
            verify_minisign(&binary, &signature, &self.public_key)?;
        } else {
            self.download(kind, version, cpu_target, &destination, &self.public_key)?;
        }
        BinaryManifest::inspect(
            kind.manifest_name(),
            &binary,
            &self.url(kind, version, cpu_target)?,
            version,
            true,
            None,
            None,
        )
    }

    /// Construct the public URL for one caller-selected artifact.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe version or CPU target.
    pub fn url(
        &self,
        kind: ArtifactKind,
        version: &str,
        cpu_target: &str,
    ) -> Result<String, BenchError> {
        validate_component(version, "version")?;
        Ok(format!(
            "{}/{}/{}/{}",
            self.base_url,
            kind.directory(),
            version,
            kind.filename(cpu_target)?
        ))
    }

    fn download(
        &self,
        kind: ArtifactKind,
        version: &str,
        cpu_target: &str,
        destination: &Path,
        public_key: &Path,
    ) -> Result<(), BenchError> {
        let parent = destination.parent().ok_or_else(|| {
            BenchError::Invalid("artifact destination has no parent directory".to_owned())
        })?;
        fs::create_dir_all(parent).map_err(|source| BenchError::Write {
            path: parent.to_path_buf(),
            source,
        })?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| BenchError::Invalid(format!("system clock is invalid: {error}")))?
            .as_nanos();
        let staging = parent.join(format!(".download-{}-{nonce}", std::process::id()));
        fs::create_dir(&staging).map_err(|source| BenchError::Write {
            path: staging.clone(),
            source,
        })?;
        let filename = kind.filename(cpu_target)?;
        let binary = staging.join(&filename);
        let signature = staging.join(format!("{filename}.minisig"));
        let url = self.url(kind, version, cpu_target)?;
        run(
            "curl",
            vec![
                "--fail".to_owned(),
                "--location".to_owned(),
                "--retry".to_owned(),
                "3".to_owned(),
                "--output".to_owned(),
                binary.to_string_lossy().into_owned(),
                url.clone(),
            ],
        )?;
        run(
            "curl",
            vec![
                "--fail".to_owned(),
                "--location".to_owned(),
                "--retry".to_owned(),
                "3".to_owned(),
                "--output".to_owned(),
                signature.to_string_lossy().into_owned(),
                format!("{url}.minisig"),
            ],
        )?;
        verify_minisign(&binary, &signature, public_key)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).map_err(|source| {
                BenchError::Write {
                    path: binary.clone(),
                    source,
                }
            })?;
        }
        fs::rename(&staging, destination).map_err(|source| BenchError::Write {
            path: destination.to_path_buf(),
            source,
        })
    }
}

/// Verify one file against its Minisign signature without an external command.
///
/// # Errors
///
/// Returns an error when the key, signature, or message cannot be read or verification fails.
pub fn verify_minisign(
    message: &Path,
    signature: &Path,
    public_key: &Path,
) -> Result<(), BenchError> {
    let public_key = PublicKey::from_file(public_key).map_err(|error| {
        BenchError::Invalid(format!("failed to load Minisign public key: {error}"))
    })?;
    let signature = Signature::from_file(signature).map_err(|error| {
        BenchError::Invalid(format!("failed to load Minisign signature: {error}"))
    })?;
    let contents = fs::read(message).map_err(|source| BenchError::Read {
        path: message.to_path_buf(),
        source,
    })?;
    public_key
        .verify(&contents, &signature, false)
        .map_err(|error| BenchError::Invalid(format!("Minisign verification failed: {error}")))
}

fn validate_component(value: &str, label: &str) -> Result<(), BenchError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(BenchError::Invalid(format!(
            "unsafe artifact {label} `{value}`"
        )));
    }
    Ok(())
}

fn run(program: &str, arguments: Vec<String>) -> Result<(), BenchError> {
    let output = Command::new(program)
        .args(arguments)
        .output()
        .map_err(|error| BenchError::Invalid(format!("failed to execute `{program}`: {error}")))?;
    if !output.status.success() {
        return Err(BenchError::Invalid(format!(
            "`{program}` failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_selected_version_and_cpu_when_url_is_built_then_should_use_fixed_origin() {
        let source = ArtifactSource::laserdata();
        let url = source
            .url(ArtifactKind::IggyBench, "9.8.7-edge.6", "icelake")
            .expect("rolling version should form a URL");
        assert_eq!(
            url,
            "https://artifacts.laserdata.com/iggy-bench/9.8.7-edge.6/iggy-bench-linux-amd64-icelake"
        );
    }

    #[test]
    fn given_unsafe_version_when_url_is_built_then_should_reject_it() {
        let source = ArtifactSource::laserdata();
        assert!(
            source
                .url(ArtifactKind::IggyBench, "../moving", "skylake")
                .is_err()
        );
    }

    #[test]
    fn given_plane_version_and_cpu_when_url_is_built_then_should_match_release_contract() {
        let source = ArtifactSource::laserdata();
        let url = source
            .url(ArtifactKind::Plane, "0.14.0", "skylake")
            .expect("plane release should form a URL");
        assert_eq!(
            url,
            "https://artifacts.laserdata.com/plane/0.14.0/plane-linux-amd64-skylake"
        );
    }
}
