use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use laser_bench::BenchError;

pub struct ExecutionLock {
    path: PathBuf,
    owner: String,
}

impl ExecutionLock {
    pub fn acquire() -> Result<Self, BenchError> {
        Self::acquire_at(&Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))
    }

    fn acquire_at(directory: &Path) -> Result<Self, BenchError> {
        fs::create_dir_all(directory).map_err(|source| BenchError::Write {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = directory.join("laser-bench.lock");
        let owner = std::process::id().to_string();
        for _ in 0..2 {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(owner.as_bytes())
                        .and_then(|()| file.sync_all())
                        .map_err(|source| BenchError::Write {
                            path: path.clone(),
                            source,
                        })?;
                    return Ok(Self { path, owner });
                }
                Err(source) if source.kind() == ErrorKind::AlreadyExists => {
                    let existing =
                        fs::read_to_string(&path).map_err(|source| BenchError::Read {
                            path: path.clone(),
                            source,
                        })?;
                    let pid = existing.trim().parse::<u32>().map_err(|_| {
                        BenchError::Invalid(format!(
                            "benchmark execution lock is malformed: {}",
                            path.display()
                        ))
                    })?;
                    if Path::new("/proc").join(pid.to_string()).exists() {
                        return Err(BenchError::Invalid(format!(
                            "another benchmark campaign is running with PID {pid}"
                        )));
                    }
                    fs::remove_file(&path).map_err(|source| BenchError::Write {
                        path: path.clone(),
                        source,
                    })?;
                }
                Err(source) => {
                    return Err(BenchError::Write {
                        path: path.clone(),
                        source,
                    });
                }
            }
        }
        Err(BenchError::Invalid(
            "benchmark execution lock changed while it was being acquired".to_owned(),
        ))
    }
}

impl Drop for ExecutionLock {
    fn drop(&mut self) {
        if fs::read_to_string(&self.path).is_ok_and(|owner| owner.trim() == self.owner) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::ExecutionLock;

    #[test]
    fn given_active_lock_when_acquiring_again_then_should_reject_concurrent_campaign() {
        let directory = tempdir().expect("temporary directory should be created");
        let _lock =
            ExecutionLock::acquire_at(directory.path()).expect("first lock should be acquired");

        let error = ExecutionLock::acquire_at(directory.path())
            .err()
            .expect("second lock should be rejected");

        assert!(error.to_string().contains("another benchmark campaign"));
    }

    #[test]
    fn given_stale_lock_when_acquiring_then_should_replace_it() {
        let directory = tempdir().expect("temporary directory should be created");
        let path = directory.path().join("laser-bench.lock");
        std::fs::write(&path, u32::MAX.to_string()).expect("stale lock should be written");

        let lock =
            ExecutionLock::acquire_at(directory.path()).expect("stale lock should be replaced");

        assert_eq!(
            std::fs::read_to_string(path).expect("replacement lock should be readable"),
            std::process::id().to_string()
        );
        drop(lock);
    }
}
