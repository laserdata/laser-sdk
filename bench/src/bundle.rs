use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::BenchError;
use crate::artifact::verify_minisign;
use crate::binary::sha256_file;

const MANIFEST_NAME: &str = "publication-manifest.json";
const SIGNATURE_NAME: &str = "publication-manifest.json.minisig";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PublicationFile {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PublicationManifest {
    pub schema_version: u32,
    pub source_suite_digest: String,
    pub files: Vec<PublicationFile>,
}

/// Copy sanitized publishable evidence into an immutable bundle and write its integrity manifest.
///
/// # Errors
///
/// Returns an error when the destination exists, source evidence contains unsafe files, sanitation fails, or a copied file cannot be hashed.
pub fn prepare_publication(
    source: &Path,
    destination: &Path,
) -> Result<PublicationManifest, BenchError> {
    if destination.exists() {
        return Err(BenchError::Invalid(format!(
            "publication destination `{}` already exists",
            destination.display()
        )));
    }
    let suite_index = read_json(&source.join("suite-index.json"))?;
    if suite_index.get("authoritative") != Some(&Value::Bool(true)) {
        return Err(BenchError::Invalid(
            "only an authoritative suite can become a publication bundle".to_owned(),
        ));
    }
    if suite_index
        .get("invalid_repetitions")
        .and_then(Value::as_u64)
        != Some(0)
        || suite_index.get("invalid_analyses").and_then(Value::as_u64) != Some(0)
    {
        return Err(BenchError::Invalid(
            "invalid authoritative evidence cannot become a publication bundle".to_owned(),
        ));
    }
    fs::create_dir(destination).map_err(|source| BenchError::Write {
        path: destination.to_path_buf(),
        source,
    })?;
    let mut files = Vec::new();
    copy_evidence(source, source, destination, &mut files)?;
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let manifest = PublicationManifest {
        schema_version: 1,
        source_suite_digest: suite_index
            .get("suite_digest")
            .and_then(Value::as_str)
            .ok_or_else(|| BenchError::Invalid("suite index has no digest".to_owned()))?
            .to_owned(),
        files,
    };
    write_json(
        &destination.join(MANIFEST_NAME),
        &serde_json::to_value(&manifest)?,
    )?;
    verify_publication(destination, false)?;
    Ok(manifest)
}

/// Verify every publication file and optionally require its adjacent Minisign signature.
///
/// # Errors
///
/// Returns an error for an unsafe or unexpected path, digest mismatch, missing signature, or failed signature verification.
pub fn verify_publication(
    directory: &Path,
    require_signature: bool,
) -> Result<PublicationManifest, BenchError> {
    let manifest_path = directory.join(MANIFEST_NAME);
    let manifest: PublicationManifest = serde_json::from_value(read_json(&manifest_path)?)?;
    if manifest.schema_version != 1 {
        return Err(BenchError::Invalid(format!(
            "unsupported publication manifest version {}",
            manifest.schema_version
        )));
    }
    let mut expected = BTreeSet::from([MANIFEST_NAME.to_owned()]);
    for file in &manifest.files {
        let path = safe_path(directory, &file.path)?;
        let metadata = fs::metadata(&path).map_err(|source| BenchError::Read {
            path: path.clone(),
            source,
        })?;
        if metadata.len() != file.bytes || sha256_file(&path)? != file.sha256 {
            return Err(BenchError::Invalid(format!(
                "publication file `{}` failed integrity validation",
                file.path
            )));
        }
        expected.insert(file.path.clone());
    }
    let signature_path = directory.join(SIGNATURE_NAME);
    if signature_path.is_file() {
        expected.insert(SIGNATURE_NAME.to_owned());
        verify_minisign(
            &manifest_path,
            &signature_path,
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("keys/minisign.pub"),
        )?;
    } else if require_signature {
        return Err(BenchError::Invalid(format!(
            "publication bundle is unsigned, expected {}",
            signature_path.display()
        )));
    }
    let actual = publication_paths(directory)?;
    if actual != expected {
        return Err(BenchError::Invalid(
            "publication bundle contains unmanifested files".to_owned(),
        ));
    }
    Ok(manifest)
}

fn copy_evidence(
    source_root: &Path,
    source: &Path,
    destination_root: &Path,
    files: &mut Vec<PublicationFile>,
) -> Result<(), BenchError> {
    for entry in fs::read_dir(source).map_err(|source_error| BenchError::Read {
        path: source.to_path_buf(),
        source: source_error,
    })? {
        let entry = entry.map_err(|error| BenchError::Invalid(error.to_string()))?;
        let path = entry.path();
        let relative = path.strip_prefix(source_root).map_err(|error| {
            BenchError::Invalid(format!("publication path escaped source root: {error}"))
        })?;
        let file_type = entry
            .file_type()
            .map_err(|error| BenchError::Invalid(error.to_string()))?;
        if file_type.is_symlink() {
            return Err(BenchError::Invalid(format!(
                "publication source contains symlink `{}`",
                relative.display()
            )));
        }
        if file_type.is_dir() {
            if entry.file_name() == "services" || entry.file_name() == "publication" {
                continue;
            }
            copy_evidence(source_root, &path, destination_root, files)?;
            continue;
        }
        if path.extension().is_some_and(|extension| extension == "log")
            || entry.file_name() == MANIFEST_NAME
            || entry.file_name() == SIGNATURE_NAME
        {
            continue;
        }
        let destination = destination_root.join(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|source| BenchError::Write {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        copy_file(&path, &destination)?;
        audit_public_file(&destination)?;
        let metadata = fs::metadata(&destination).map_err(|source| BenchError::Read {
            path: destination.clone(),
            source,
        })?;
        files.push(PublicationFile {
            path: relative.to_string_lossy().into_owned(),
            sha256: sha256_file(&destination)?,
            bytes: metadata.len(),
        });
    }
    Ok(())
}

fn copy_file(source: &Path, destination: &Path) -> Result<(), BenchError> {
    if source
        .extension()
        .is_some_and(|extension| extension == "json")
    {
        let value = sanitize_json(read_json(source));
        return write_json(destination, &value?);
    }
    fs::copy(source, destination)
        .map(|_| ())
        .map_err(|source| BenchError::Write {
            path: destination.to_path_buf(),
            source,
        })
}

fn sanitize_json(value: Result<Value, BenchError>) -> Result<Value, BenchError> {
    fn sanitize(value: Value) -> Value {
        match value {
            Value::Object(object) => Value::Object(
                object
                    .into_iter()
                    .map(|(key, value)| {
                        let sensitive = key.to_ascii_lowercase();
                        let value = if ["password", "secret", "token", "credential", "private_key"]
                            .iter()
                            .any(|fragment| sensitive.contains(fragment))
                        {
                            Value::String("[REDACTED]".to_owned())
                        } else {
                            sanitize(value)
                        };
                        (key, value)
                    })
                    .collect(),
            ),
            Value::Array(values) => Value::Array(values.into_iter().map(sanitize).collect()),
            Value::String(value) if value.starts_with('/') || value.starts_with("file://") => {
                Value::String("[REDACTED_ABSOLUTE_PATH]".to_owned())
            }
            Value::String(value) => Value::String(redact_url_credentials(&value)),
            value => value,
        }
    }
    value.map(sanitize)
}

fn redact_url_credentials(value: &str) -> String {
    let Some(scheme_end) = value.find("://") else {
        return value.to_owned();
    };
    let authority_start = scheme_end + 3;
    let authority_end = value[authority_start..]
        .find('/')
        .map_or(value.len(), |offset| authority_start + offset);
    let authority = &value[authority_start..authority_end];
    let Some(at) = authority.rfind('@') else {
        return value.to_owned();
    };
    format!(
        "{}[REDACTED]{}",
        &value[..authority_start],
        &value[authority_start + at..]
    )
}

fn audit_public_file(path: &Path) -> Result<(), BenchError> {
    let extension = path.extension().and_then(|extension| extension.to_str());
    if !matches!(
        extension,
        Some("json" | "toml" | "csv" | "html" | "rs" | "md")
    ) && path.file_name().and_then(|name| name.to_str()) != Some("Cargo.lock")
    {
        return Ok(());
    }
    let value = fs::read_to_string(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    if value.contains("/home/") || value.contains("/Users/") {
        return Err(BenchError::Invalid(format!(
            "publication file `{}` contains a private home path",
            path.display()
        )));
    }
    Ok(())
}

fn publication_paths(root: &Path) -> Result<BTreeSet<String>, BenchError> {
    fn collect(
        root: &Path,
        directory: &Path,
        paths: &mut BTreeSet<String>,
    ) -> Result<(), BenchError> {
        for entry in fs::read_dir(directory).map_err(|source| BenchError::Read {
            path: directory.to_path_buf(),
            source,
        })? {
            let entry = entry.map_err(|error| BenchError::Invalid(error.to_string()))?;
            let path = entry.path();
            if entry
                .file_type()
                .map_err(|error| BenchError::Invalid(error.to_string()))?
                .is_dir()
            {
                collect(root, &path, paths)?;
            } else {
                paths.insert(
                    path.strip_prefix(root)
                        .map_err(|error| BenchError::Invalid(error.to_string()))?
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
        Ok(())
    }
    let mut paths = BTreeSet::new();
    collect(root, root, &mut paths)?;
    Ok(paths)
}

fn safe_path(root: &Path, relative: &str) -> Result<PathBuf, BenchError> {
    let path = Path::new(relative);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(BenchError::Invalid(format!(
            "unsafe publication path `{relative}`"
        )));
    }
    Ok(root.join(path))
}

fn read_json(path: &Path) -> Result<Value, BenchError> {
    let bytes = fs::read(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(Into::into)
}

fn write_json(path: &Path, value: &Value) -> Result<(), BenchError> {
    fs::write(path, serde_json::to_vec_pretty(value)?).map_err(|source| BenchError::Write {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{prepare_publication, verify_publication};

    #[test]
    fn given_authoritative_evidence_when_bundled_then_should_redact_paths_and_exclude_logs() {
        let temporary = tempdir().expect("temporary root should exist");
        let source = temporary.path().join("source");
        let output = temporary.path().join("publication");
        fs::create_dir_all(source.join("scenario/repetition-000/services"))
            .expect("source directories should exist");
        fs::write(
            source.join("suite-index.json"),
            serde_json::to_vec(&json!({
                "authoritative": true,
                "suite_digest": "a".repeat(64),
                "invalid_repetitions": 0,
                "invalid_analyses": 0
            }))
            .expect("suite index should encode"),
        )
        .expect("suite index should write");
        fs::write(
            source.join("resolved-stack.json"),
            br#"{"path":"/private/workspace/binary","sha256":"abc"}"#,
        )
        .expect("stack should write");
        fs::write(
            source.join("scenario/repetition-000/report.json"),
            br#"{"analysis":{"valid":true,"publishable":true}}"#,
        )
        .expect("report should write");
        fs::write(
            source.join("scenario/repetition-000/services/server.log"),
            b"private",
        )
        .expect("log should write");

        prepare_publication(&source, &output).expect("bundle should prepare");
        verify_publication(&output, false).expect("unsigned bundle integrity should verify");

        let stack = fs::read_to_string(output.join("resolved-stack.json"))
            .expect("sanitized stack should read");
        assert!(!stack.contains("/private/"));
        assert!(
            !output
                .join("scenario/repetition-000/services/server.log")
                .exists()
        );
    }
}
