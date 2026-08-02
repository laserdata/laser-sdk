use crate::error::LaserError;
use async_trait::async_trait;
use laser_wire::agent::BodyRef;
use laser_wire::content::ContentType;
use laser_wire::framing::{decode_named, encode_named};
use sha2::{Digest, Sha256};

/// Where claim-checked bodies live. The seam rule applies: no default store
/// ships, the deployment chooses (an S3-compatible bucket, the kv surface for
/// small overflow, a filesystem in dev), and the same trait shape as
/// [`Deduplicator`](crate::agent::Deduplicator) keeps it `dyn`-usable behind
/// a plain reference.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Store `payload` and return the reference a [`BodyRef`] will carry
    /// (a URI, object key, or kv key, bounded by the capsule's cap).
    async fn put(&self, payload: Vec<u8>) -> Result<String, LaserError>;
    /// Fetch the bytes behind `reference`.
    async fn get(&self, reference: &str) -> Result<Vec<u8>, LaserError>;
}

/// Claim-check `payload` against `store` when it is at or over
/// `threshold_bytes`: the bytes are `put`, hashed, and replaced by the encoded
/// [`BodyRef`] capsule with content-type `ref`. Under the threshold the
/// payload comes back untouched, byte-identical to no claim check at all.
/// Returns the (possibly replaced) payload and the content type to stamp when
/// the check fired.
pub async fn check_in(
    store: &dyn BlobStore,
    threshold_bytes: usize,
    payload: Vec<u8>,
) -> Result<(Vec<u8>, Option<ContentType>), LaserError> {
    if payload.len() < threshold_bytes {
        return Ok((payload, None));
    }
    let sha256: [u8; 32] = Sha256::digest(&payload).into();
    let size_bytes = payload.len() as u64;
    let reference = store.put(payload).await?;
    let capsule = BodyRef::new(reference, size_bytes, sha256);
    let encoded = encode_named(&capsule)
        .map_err(|error| LaserError::Codec(format!("encode body ref: {error}")))?;
    Ok((encoded, Some(ContentType::Ref)))
}

/// Resolve a claim-checked body: decode the [`BodyRef`] capsule from
/// `payload`, fetch the referenced bytes, and verify their SHA-256 against the
/// capsule's digest. A mismatch is the typed integrity refusal, never
/// unverified payload: the capsule's digest is the reader's proof the store
/// returned what the producer externalized.
pub async fn resolve_body(store: &dyn BlobStore, payload: &[u8]) -> Result<Vec<u8>, LaserError> {
    let capsule: BodyRef = decode_named(payload)
        .map_err(|error| LaserError::Codec(format!("decode body ref: {error}")))?;
    let payload = store.get(&capsule.reference).await?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    if digest.as_slice() != capsule.sha256.as_slice() {
        return Err(LaserError::Integrity {
            reference: capsule.reference,
        });
    }
    Ok(payload)
}

impl crate::agent::AgentMessage {
    /// The real body of a claim-checked message: when the content type is
    /// `ref`, decode the capsule, fetch, and digest-verify through
    /// [`resolve_body`]. Any other content type returns the payload as-is.
    /// The consume-side pairing of the publish builder's `claim_check`.
    pub async fn resolve_body(&self, store: &dyn BlobStore) -> Result<Vec<u8>, LaserError> {
        if self.content_type == Some(ContentType::Ref) {
            return resolve_body(store, &self.payload).await;
        }
        Ok(self.payload.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemoryStore {
        blobs: Mutex<HashMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl BlobStore for MemoryStore {
        async fn put(&self, payload: Vec<u8>) -> Result<String, LaserError> {
            let mut blobs = self.blobs.lock().expect("store lock");
            let reference = format!("blob-{}", blobs.len());
            blobs.insert(reference.clone(), payload);
            Ok(reference)
        }

        async fn get(&self, reference: &str) -> Result<Vec<u8>, LaserError> {
            self.blobs
                .lock()
                .expect("store lock")
                .get(reference)
                .cloned()
                .ok_or_else(|| LaserError::Invalid(format!("no blob at `{reference}`")))
        }
    }

    #[tokio::test]
    async fn given_a_small_body_when_checked_in_then_should_pass_through_byte_identical() {
        let store = MemoryStore::default();
        let payload = b"small".to_vec();
        let (out, content_type) = check_in(&store, 1024, payload.clone())
            .await
            .expect("passes through");
        assert_eq!(out, payload);
        assert!(content_type.is_none());
        assert!(store.blobs.lock().expect("store lock").is_empty());
    }

    #[tokio::test]
    async fn given_a_large_body_when_checked_in_then_should_round_trip_through_the_store() {
        let store = MemoryStore::default();
        let payload = vec![7u8; 4096];
        let (capsule, content_type) = check_in(&store, 1024, payload.clone())
            .await
            .expect("externalizes");
        assert_eq!(content_type, Some(ContentType::Ref));
        let resolved = resolve_body(&store, &capsule).await.expect("resolves");
        assert_eq!(resolved, payload);
    }

    #[tokio::test]
    async fn given_a_tampered_blob_when_resolved_then_should_refuse_with_the_integrity_error() {
        let store = MemoryStore::default();
        let payload = vec![7u8; 4096];
        let (capsule, _) = check_in(&store, 1024, payload).await.expect("externalizes");
        for blob in store.blobs.lock().expect("store lock").values_mut() {
            *blob = b"swapped".to_vec();
        }
        let refused = resolve_body(&store, &capsule).await;
        assert!(matches!(refused, Err(LaserError::Integrity { .. })));
    }
}
