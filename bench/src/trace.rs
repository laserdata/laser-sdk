use std::fmt::Write;
use std::fs;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::BenchError;

const TRACE_MAGIC: &[u8; 8] = b"LBTRC001";
const NANOS_PER_SECOND: u64 = 1_000_000_000;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SharedTraceRef {
    pub schema_version: u32,
    pub path: PathBuf,
    pub sha256: String,
    pub seed: u64,
    pub entries: u64,
    pub rate_per_second: u64,
    pub payload_bytes: usize,
    pub payload_sha256: String,
}

/// Persist the exact fixed-rate sequence and arrival offsets consumed by comparison arms.
///
/// # Errors
///
/// Returns an error for zero dimensions, arithmetic overflow, or an I/O failure.
pub fn write_shared_trace(
    output: &Path,
    seed: u64,
    duration_seconds: u64,
    rate_per_second: u64,
    payload: &[u8],
) -> Result<SharedTraceRef, BenchError> {
    if duration_seconds == 0 || rate_per_second == 0 || payload.is_empty() {
        return Err(BenchError::Invalid(
            "shared trace duration, rate, and payload must be nonzero".to_owned(),
        ));
    }
    let entries = duration_seconds
        .checked_mul(rate_per_second)
        .ok_or_else(|| BenchError::Invalid("shared trace entry count overflowed".to_owned()))?;
    let payload_digest = Sha256::digest(payload);
    let path = output.join("shared-trace.bin");
    let file = fs::File::create_new(&path).map_err(|source| BenchError::Write {
        path: path.clone(),
        source,
    })?;
    let mut writer = BufWriter::new(file);
    let mut digest = Sha256::new();
    write_trace_bytes(&mut writer, &mut digest, TRACE_MAGIC)?;
    write_trace_bytes(&mut writer, &mut digest, &seed.to_le_bytes())?;
    write_trace_bytes(&mut writer, &mut digest, &entries.to_le_bytes())?;
    write_trace_bytes(&mut writer, &mut digest, &rate_per_second.to_le_bytes())?;
    write_trace_bytes(
        &mut writer,
        &mut digest,
        &u64::try_from(payload.len())
            .map_err(|_| BenchError::Invalid("trace payload length exceeds u64".to_owned()))?
            .to_le_bytes(),
    )?;
    write_trace_bytes(&mut writer, &mut digest, &payload_digest)?;
    for sequence in 0..entries {
        write_trace_bytes(&mut writer, &mut digest, &sequence.to_le_bytes())?;
        write_trace_bytes(
            &mut writer,
            &mut digest,
            &fixed_arrival_ns(sequence, rate_per_second)?.to_le_bytes(),
        )?;
    }
    writer.flush().map_err(|source| BenchError::Write {
        path: path.clone(),
        source,
    })?;
    Ok(SharedTraceRef {
        schema_version: 1,
        path: PathBuf::from("shared-trace.bin"),
        sha256: digest_state_hex(digest),
        seed,
        entries,
        rate_per_second,
        payload_bytes: payload.len(),
        payload_sha256: digest_hex(payload),
    })
}

fn fixed_arrival_ns(sequence: u64, rate_per_second: u64) -> Result<u64, BenchError> {
    sequence
        .checked_mul(NANOS_PER_SECOND)
        .ok_or_else(|| BenchError::Invalid("shared trace arrival offset overflowed".to_owned()))
        .map(|nanos| nanos / rate_per_second)
}

fn digest_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        })
}

fn digest_state_hex(digest: Sha256) -> String {
    digest
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        })
}

fn write_trace_bytes(
    writer: &mut BufWriter<fs::File>,
    digest: &mut Sha256,
    bytes: &[u8],
) -> Result<(), BenchError> {
    writer
        .write_all(bytes)
        .map_err(|source| BenchError::Write {
            path: PathBuf::from("shared-trace.bin"),
            source,
        })?;
    digest.update(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_fixed_trace_when_written_then_should_preserve_exact_arrivals() {
        let directory = tempfile::tempdir().expect("temporary directory should exist");
        let reference =
            write_shared_trace(directory.path(), 7, 1, 10, b"laser").expect("trace should write");
        let bytes =
            fs::read(directory.path().join(&reference.path)).expect("trace should be readable");

        assert_eq!(reference.entries, 10);
        assert_eq!(&bytes[..8], TRACE_MAGIC);
        assert_eq!(
            fixed_arrival_ns(1, 10).expect("arrival should fit"),
            100_000_000
        );
        assert_eq!(reference.sha256, digest_hex(&bytes));
    }
}
