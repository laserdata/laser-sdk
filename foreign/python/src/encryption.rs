use crate::convert::payload_bytes;
use crate::errors::to_pyerr;
use laser_sdk::stream::Encryption;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// Client-side encryption in Apache Iggy's AES-256-GCM format: a random
/// 12-byte nonce, the ciphertext, then the 16-byte tag. Pass it as
/// `encryption=` to `Topic.producer` to encrypt the payload and the
/// user-header block of every record, and to a consumer with the same key to
/// decrypt them. The Rust and TypeScript clients and Apache Iggy's own
/// encryptor read the same records.
#[gen_stub_pyclass]
#[pyclass(name = "Encryption", frozen)]
#[derive(Clone)]
pub struct PyEncryption {
    pub(crate) inner: Encryption,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyEncryption {
    /// AES-256-GCM with a 32-byte key. Any other length raises `InvalidError`.
    #[staticmethod]
    fn aes256_gcm(key: &Bound<'_, PyBytes>) -> PyResult<Self> {
        Ok(Self {
            inner: Encryption::aes256_gcm(key.as_bytes()).map_err(to_pyerr)?,
        })
    }

    /// Encrypt `plaintext` (bytes, bytearray, or str) with a fresh nonce.
    fn encrypt<'py>(
        &self,
        py: Python<'py>,
        plaintext: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let sealed = self
            .inner
            .encrypt(payload_bytes(plaintext)?)
            .map_err(to_pyerr)?;
        Ok(PyBytes::new(py, &sealed))
    }

    /// Decrypt what `encrypt` returned. A ciphertext that is short, altered,
    /// or sealed with another key raises `TransportError` with Apache Iggy
    /// error code 72.
    fn decrypt<'py>(
        &self,
        py: Python<'py>,
        ciphertext: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let opened = self
            .inner
            .decrypt(payload_bytes(ciphertext)?)
            .map_err(to_pyerr)?;
        Ok(PyBytes::new(py, &opened))
    }

    fn __repr__(&self) -> &'static str {
        "Encryption(Aes256Gcm)"
    }
}
