use crate::error::LaserError;
use ed25519_dalek::{Signer, SigningKey as DalekSigningKey, Verifier, VerifyingKey};
use laser_wire::agent::{AgentEnvelope, SIGNATURE_DOMAIN, SIGNATURE_SCHEME_ED25519, Signature};
use laser_wire::framing::encode_named;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// The key id length: the first 8 bytes of the public key's SHA-256.
const KEY_ID_BYTES: usize = 8;
/// The Ed25519 detached-signature length.
const SIGNATURE_BYTES: usize = 64;

/// The canonical bytes a signature covers: the domain separator followed by the
/// named-field encoding of the envelope with its signature cleared. Sign and
/// verify build the identical input, so a decode-then-re-encode must be
/// byte-identical (the round-trip property the wire fixtures pin).
fn signing_input(envelope: &AgentEnvelope) -> Result<Vec<u8>, LaserError> {
    let mut bare = envelope.clone();
    bare.signature = None;
    let body =
        encode_named(&bare).map_err(|error| LaserError::Codec(format!("sign encode: {error}")))?;
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + body.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(&body);
    Ok(input)
}

/// The key id of a public key: the first [`KEY_ID_BYTES`] of its SHA-256.
fn key_id_of(verifying: &VerifyingKey) -> Vec<u8> {
    Sha256::digest(verifying.as_bytes())[..KEY_ID_BYTES].to_vec()
}

/// An Ed25519 signing key. Sign an envelope to produce the detached
/// [`Signature`] to attach with [`AgentEnvelope::with_signature`].
pub struct SigningKey {
    inner: DalekSigningKey,
    key_id: Vec<u8>,
}

impl SigningKey {
    /// Build from a 32-byte Ed25519 secret seed.
    pub fn from_bytes(secret: &[u8; 32]) -> Self {
        let inner = DalekSigningKey::from_bytes(secret);
        let key_id = key_id_of(&inner.verifying_key());
        Self { inner, key_id }
    }

    /// This key's 8-byte id, the [`Signature::key_id`] it stamps.
    pub fn key_id(&self) -> &[u8] {
        &self.key_id
    }

    /// The public verifying key, to enroll in a [`KeyRegistry`].
    pub fn verifying_key(&self) -> VerifyingKey {
        self.inner.verifying_key()
    }

    /// Sign `envelope`, returning the detached signature.
    pub fn sign(&self, envelope: &AgentEnvelope) -> Result<Signature, LaserError> {
        let input = signing_input(envelope)?;
        let signature = self.inner.sign(&input);
        Ok(Signature {
            scheme: SIGNATURE_SCHEME_ED25519,
            key_id: self.key_id.clone(),
            bytes: signature.to_bytes().to_vec(),
        })
    }
}

/// A registry of enrolled verifying keys, each bound to the authenticated
/// principal that enrolled it. Verifying a signed envelope returns the bound
/// principal, so a caller asserts it equals the server-stamped identity (the
/// authorship binding: a key proves its enrolled principal, never the
/// self-asserted `source`).
#[derive(Default)]
pub struct KeyRegistry {
    keys: HashMap<Vec<u8>, (String, VerifyingKey)>,
}

impl KeyRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enroll `verifying` bound to `principal` (the authenticated `user_id`). A
    /// later enroll under the same key id replaces the binding.
    pub fn enroll(&mut self, principal: impl Into<String>, verifying: VerifyingKey) {
        self.keys
            .insert(key_id_of(&verifying), (principal.into(), verifying));
    }

    /// Verify the envelope's signature, returning the enrolled principal on
    /// success. Errors on an unsigned envelope, an unknown key id, a non-Ed25519
    /// scheme, a malformed signature, or a failed check. The caller compares the
    /// returned principal to the trusted server-stamped identity.
    pub fn verify(&self, envelope: &AgentEnvelope) -> Result<String, LaserError> {
        let signature = envelope
            .signature
            .as_ref()
            .ok_or_else(|| LaserError::Signature("envelope is not signed".to_owned()))?;
        if signature.scheme != SIGNATURE_SCHEME_ED25519 {
            return Err(LaserError::Signature(format!(
                "unsupported signature scheme {}",
                signature.scheme
            )));
        }
        let (principal, verifying) = self
            .keys
            .get(signature.key_id.as_slice())
            .ok_or_else(|| LaserError::Signature("signing key is not enrolled".to_owned()))?;
        let bytes: [u8; SIGNATURE_BYTES] = signature
            .bytes
            .as_slice()
            .try_into()
            .map_err(|_| LaserError::Signature("malformed Ed25519 signature".to_owned()))?;
        let signature = ed25519_dalek::Signature::from_bytes(&bytes);
        let input = signing_input(envelope)?;
        verifying
            .verify(&input, &signature)
            .map_err(|_| LaserError::Signature("signature verification failed".to_owned()))?;
        Ok(principal.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::agent::{AgentEnvelope, ConversationId, CorrelationId, RecordId};

    fn envelope() -> AgentEnvelope {
        AgentEnvelope::command(
            RecordId::from_u128(1),
            ConversationId::from_u128(2),
            "planner".parse().expect("valid agent id"),
            CorrelationId::from_u128(3),
            b"{}".to_vec(),
        )
    }

    #[test]
    fn given_a_signed_envelope_when_verified_then_should_return_the_enrolled_principal() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let mut registry = KeyRegistry::new();
        registry.enroll("user-42", key.verifying_key());

        let signed = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        assert_eq!(registry.verify(&signed).expect("verifies"), "user-42");
    }

    #[test]
    fn given_a_tampered_body_when_verified_then_should_fail() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let mut registry = KeyRegistry::new();
        registry.enroll("user-42", key.verifying_key());

        let signature = key.sign(&envelope()).expect("signs");
        let mut tampered = envelope();
        tampered.body = b"{\"evil\":true}".to_vec();
        let tampered = tampered.with_signature(signature);
        assert!(matches!(
            registry.verify(&tampered),
            Err(LaserError::Signature(_))
        ));
    }

    #[test]
    fn given_an_unenrolled_key_when_verified_then_should_fail() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let signed = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        assert!(matches!(
            KeyRegistry::new().verify(&signed),
            Err(LaserError::Signature(_))
        ));
    }

    #[test]
    fn given_an_unsigned_envelope_when_verified_then_should_fail() {
        assert!(matches!(
            KeyRegistry::new().verify(&envelope()),
            Err(LaserError::Signature(_))
        ));
    }
}
