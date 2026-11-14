use crate::error::LaserError;
use iggy::prelude::{Aes256GcmEncryptor, EncryptorKind, IggyMessage};
use std::fmt;
use std::sync::Arc;

const AES_256_KEY_BYTES: usize = 32;

/// Client-side encryption in Apache Iggy's AES-256-GCM format: a random
/// 12-byte nonce, the ciphertext, then the 16-byte tag. A producer encrypts
/// the payload and the user-header block of each record, and a consumer with
/// the same key decrypts both, so the server stores only ciphertext. Every
/// Laser client and Apache Iggy's own encryptor read each other's records.
#[derive(Clone)]
pub struct Encryption(Arc<EncryptorKind>);

impl Encryption {
    /// AES-256-GCM with a 32-byte key. Any other key length fails with
    /// [`LaserError::Invalid`].
    pub fn aes256_gcm(key: impl AsRef<[u8]>) -> Result<Self, LaserError> {
        let key = key.as_ref();
        if key.len() != AES_256_KEY_BYTES {
            return Err(LaserError::Invalid(format!(
                "an AES-256-GCM key holds {AES_256_KEY_BYTES} bytes, not {}",
                key.len()
            )));
        }
        Ok(Self(Arc::new(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::new(key)?,
        ))))
    }

    /// Encrypts `plaintext` with a fresh nonce.
    pub fn encrypt(&self, plaintext: impl AsRef<[u8]>) -> Result<Vec<u8>, LaserError> {
        Ok(self.0.encrypt(plaintext.as_ref())?)
    }

    /// Decrypts what [`encrypt`](Self::encrypt) returned. A ciphertext that is
    /// short, altered, or sealed with another key fails with Apache Iggy
    /// error 72.
    pub fn decrypt(&self, ciphertext: impl AsRef<[u8]>) -> Result<Vec<u8>, LaserError> {
        Ok(self.0.decrypt(ciphertext.as_ref())?)
    }

    pub(crate) fn encryptor(&self) -> Arc<EncryptorKind> {
        Arc::clone(&self.0)
    }

    pub(crate) fn decrypt_message(&self, message: &mut IggyMessage) -> Result<(), LaserError> {
        Ok(self.0.decrypt_message(message)?)
    }
}

impl fmt::Debug for Encryption {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Encryption(Aes256Gcm)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = *b"laser-encryption-test-key-32byte";
    const PLAINTEXT: &[u8] = br#"{"host":"node-7","cpu":82}"#;
    // Sealed with `KEY` by Apache Iggy's Rust encryptor and by Node's AES-256-GCM.
    const IGGY_CIPHERTEXT: &str = "2cf9899e78c2d7e11900d208ccc590f5ca112bc7f67b1d5180fba3d30354613fa3c954477cc01d73545c7105dce1ac30680fe7a79699";
    const NODE_CIPHERTEXT: &str = "bfafee07d0742f433ed130c340127691377c4f54776eda6e40a841af10d5b4c950612150a76700ff3b017a802971514632695045f015";

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex digit pair"))
            .collect()
    }

    fn encryption() -> Encryption {
        Encryption::aes256_gcm(KEY).expect("a 32-byte key")
    }

    #[test]
    fn given_a_key_when_encrypting_then_should_decrypt_to_the_plaintext() {
        let sealed = encryption().encrypt(PLAINTEXT).expect("encrypt");
        assert_eq!(sealed.len(), 12 + PLAINTEXT.len() + 16);
        assert_eq!(encryption().decrypt(&sealed).expect("decrypt"), PLAINTEXT);
    }

    #[test]
    fn given_one_plaintext_when_encrypted_twice_then_should_use_a_fresh_nonce() {
        let first = encryption().encrypt(PLAINTEXT).expect("encrypt");
        let second = encryption().encrypt(PLAINTEXT).expect("encrypt");
        assert_ne!(first, second);
    }

    #[test]
    fn given_the_shared_vectors_when_decrypting_then_should_read_the_plaintext() {
        for vector in [IGGY_CIPHERTEXT, NODE_CIPHERTEXT] {
            assert_eq!(
                encryption().decrypt(hex(vector)).expect("decrypt vector"),
                PLAINTEXT
            );
        }
    }

    #[test]
    fn given_another_key_when_decrypting_then_should_fail_with_iggy_error_72() {
        let sealed = encryption().encrypt(PLAINTEXT).expect("encrypt");
        let other = Encryption::aes256_gcm([7u8; 32]).expect("a 32-byte key");
        let error = other
            .decrypt(&sealed)
            .expect_err("another key must not decrypt");
        assert_eq!(error.iggy_error_code(), Some(72));
    }

    #[test]
    fn given_a_short_key_when_building_then_should_refuse_it() {
        assert!(matches!(
            Encryption::aes256_gcm([0u8; 16]),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_an_encryption_when_debugged_then_should_not_print_the_key() {
        assert_eq!(format!("{:?}", encryption()), "Encryption(Aes256Gcm)");
    }
}
