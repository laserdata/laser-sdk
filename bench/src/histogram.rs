use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, Write};
use std::path::{Path, PathBuf};

use hdrhistogram::Histogram;
use hdrhistogram::serialization::{Deserializer, Serializer, V2DeflateSerializer};
use sha2::{Digest, Sha256};

use crate::BenchError;
use crate::report::HistogramRef;

/// Persist one compressed HDR histogram without replacing an existing sidecar.
///
/// # Errors
///
/// Returns an error for an unsafe class name, serialization failure, existing path, or file-system failure.
pub fn write_sidecar(
    directory: &Path,
    class: &str,
    histogram: &Histogram<u64>,
) -> Result<HistogramRef, BenchError> {
    if class.is_empty()
        || !class
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(BenchError::Invalid(format!(
            "histogram class `{class}` is not a safe file name"
        )));
    }
    fs::create_dir_all(directory).map_err(|source| BenchError::Write {
        path: directory.to_path_buf(),
        source,
    })?;
    let path = directory.join(format!("{class}.hdr"));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|source| BenchError::Write {
            path: path.clone(),
            source,
        })?;
    let mut bytes = Vec::new();
    V2DeflateSerializer::new()
        .serialize(histogram, &mut bytes)
        .map_err(|error| BenchError::Invalid(format!("HDR serialization failed: {error}")))?;
    let mut writer = BufWriter::new(file);
    writer
        .write_all(&bytes)
        .and_then(|()| writer.flush())
        .map_err(|source| BenchError::Write {
            path: path.clone(),
            source,
        })?;
    Ok(HistogramRef {
        class: class.to_owned(),
        path: path.to_string_lossy().into_owned(),
        sha256: digest_hex(&bytes),
        samples: histogram.len(),
    })
}

/// Read a compressed HDR histogram sidecar.
///
/// # Errors
///
/// Returns an error when the sidecar cannot be read or decoded.
pub fn read_sidecar(path: &Path) -> Result<Histogram<u64>, BenchError> {
    let bytes = fs::read(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Deserializer::new()
        .deserialize(&mut BufReader::new(Cursor::new(bytes)))
        .map_err(|error| BenchError::Invalid(format!("HDR deserialization failed: {error}")))
}

/// Verify the digest stored for a histogram sidecar.
///
/// # Errors
///
/// Returns an error when the sidecar cannot be read or its digest differs.
pub fn verify_sidecar(reference: &HistogramRef) -> Result<(), BenchError> {
    verify_sidecar_at(Path::new("."), reference)
}

/// Verify a histogram reference relative to its report directory.
///
/// # Errors
///
/// Returns an error when the sidecar cannot be read or its digest differs.
pub fn verify_sidecar_at(root: &Path, reference: &HistogramRef) -> Result<(), BenchError> {
    let declared = PathBuf::from(&reference.path);
    let path = if declared.is_absolute() {
        declared
    } else {
        root.join(declared)
    };
    let bytes = fs::read(&path).map_err(|source| BenchError::Read {
        path: path.clone(),
        source,
    })?;
    let actual = digest_hex(&bytes);
    if actual != reference.sha256 {
        return Err(BenchError::Invalid(format!(
            "histogram digest mismatch for `{}`",
            path.display()
        )));
    }
    Ok(())
}

fn digest_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_histogram_when_written_then_should_roundtrip_and_verify_digest() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
            .expect("histogram bounds should be valid");
        histogram
            .record_n(1_000, 25)
            .expect("latency sample should fit");
        histogram
            .record_n(9_000_000, 5)
            .expect("tail sample should fit");
        let reference = write_sidecar(directory.path(), "scheduled_response", &histogram)
            .expect("histogram should serialize");
        verify_sidecar(&reference).expect("histogram digest should match");
        let decoded = read_sidecar(Path::new(&reference.path)).expect("histogram should decode");
        assert_eq!(decoded.len(), 30);
        assert_eq!(
            decoded.value_at_quantile(0.99),
            histogram.value_at_quantile(0.99)
        );
    }

    #[test]
    fn given_existing_sidecar_when_written_again_then_should_reject_replacement() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let histogram = Histogram::<u64>::new(3).expect("histogram should be valid");
        write_sidecar(directory.path(), "service", &histogram)
            .expect("first sidecar should be written");
        assert!(write_sidecar(directory.path(), "service", &histogram).is_err());
    }
}
