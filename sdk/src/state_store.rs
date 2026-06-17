use crate::error::LaserError;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use tokio::fs;

/// A durable key/value seam for agent state: dedup persistence, conversation
/// checkpoints, stream-cursor offsets, arbitrary per-agent state. `get`/`set`/`delete`
/// is the one point-store vocabulary, shared with `Kv` (which implements this trait,
/// so the managed KV store is the durable drop-in). `InMemoryStore` and `FileStore`
/// are the self-contained defaults. Values are owned `Vec<u8>` so the trait never
/// leaks the `bytes` crate to implementors.
///
/// Backend contract: keys SHOULD be non-empty and implementations MAY reject empty
/// keys or impose key/value size limits, so the same code can fail on one backend and
/// not another. In particular `Kv` rejects empty keys, keys over 512 B, and values
/// over 8 MiB (surfaced as `LaserError::Kv`), while `InMemoryStore` / `FileStore` are
/// unbounded (and report `LaserError::StateStore`). Keep keys short and non-empty for
/// portability across backends.
#[trait_variant::make(StateStore: Send)]
pub trait LocalStateStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, LaserError>;
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), LaserError>;
    async fn delete(&self, key: &str) -> Result<(), LaserError>;
}

/// An in-memory `StateStore` (a `HashMap`). Fast, not durable across restarts.
#[derive(Default)]
pub struct InMemoryStore {
    entries: Mutex<HashMap<String, Vec<u8>>>,
}

impl InMemoryStore {
    /// An empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<u8>>> {
        self.entries
            .lock()
            .expect("the state store mutex is not poisoned")
    }
}

impl StateStore for InMemoryStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, LaserError> {
        Ok(self.lock().get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), LaserError> {
        self.lock().insert(key.to_owned(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), LaserError> {
        self.lock().remove(key);
        Ok(())
    }
}

/// File-backed store under a single directory. Keys are hex-encoded into file
/// names, so any key is safe and no path traversal is possible. Suitable for the
/// on-box disk the platform mounts onto a deployment.
pub struct FileStore {
    root: PathBuf,
}

impl FileStore {
    /// A file-backed store rooted at `root` (keys are hex-encoded into file names).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, key: &str) -> PathBuf {
        let mut name = String::with_capacity(key.len() * 2);
        for byte in key.bytes() {
            name.push_str(&format!("{byte:02x}"));
        }
        self.root.join(name)
    }
}

impl StateStore for FileStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, LaserError> {
        match fs::read(self.path_for(key)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(LaserError::StateStore(error.to_string())),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), LaserError> {
        fs::create_dir_all(&self.root)
            .await
            .map_err(|error| LaserError::StateStore(error.to_string()))?;
        // Atomic write: stage in a `<file>.<ulid>.tmp` file then `rename` into
        // place. A crash partway through `fs::write` would otherwise leave a
        // partial or zero-length file that the next `get` returns as if it
        // were valid state. The Ulid suffix avoids a race between two
        // concurrent `set(k, ...)` callers on the same key clobbering each
        // other's staging file (rename order is then defined by the OS, and
        // each rename atomically replaces the previous one).
        let final_path = self.path_for(key);
        let tmp_path = final_path.with_extension(format!("{}.tmp", ulid::Ulid::new()));
        let cleanup_on_err = |error: std::io::Error| {
            let _ = std::fs::remove_file(&tmp_path);
            LaserError::StateStore(error.to_string())
        };
        fs::write(&tmp_path, &value).await.map_err(cleanup_on_err)?;
        fs::rename(&tmp_path, &final_path)
            .await
            .map_err(|error| LaserError::StateStore(error.to_string()))
    }

    async fn delete(&self, key: &str) -> Result<(), LaserError> {
        match fs::remove_file(self.path_for(key)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(LaserError::StateStore(error.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn round_trips<S: StateStore>(store: S) {
        assert_eq!(store.get("missing").await.expect("get"), None);
        store
            .set("k", b"v".to_vec())
            .await
            .expect("set should succeed");
        assert_eq!(
            store.get("k").await.expect("get should succeed"),
            Some(b"v".to_vec())
        );
        store.delete("k").await.expect("delete should succeed");
        assert_eq!(store.get("k").await.expect("get should succeed"), None);
    }

    #[tokio::test]
    async fn given_an_in_memory_store_when_used_then_should_round_trip() {
        round_trips(InMemoryStore::new()).await;
    }

    #[tokio::test]
    async fn given_a_file_store_when_used_then_should_round_trip() {
        let dir = std::env::temp_dir().join(format!("laser-statestore-{}", std::process::id()));
        round_trips(FileStore::new(&dir)).await;
        let _ = fs::remove_dir_all(&dir).await;
    }
}
