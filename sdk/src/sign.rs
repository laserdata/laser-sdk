use crate::error::LaserError;
use ed25519_dalek::{Signer, SigningKey as DalekSigningKey, Verifier, VerifyingKey};
use laser_wire::agent::{
    AgentEnvelope, SIGNATURE_DOMAIN, SIGNATURE_SCHEME_ED25519, Signature, SignatureContext,
};
use laser_wire::framing::encode_named;
pub use laser_wire::keys::KeyKind;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

/// The key id length: the first 8 bytes of the public key's SHA-256.
const KEY_ID_BYTES: usize = 8;
/// The Ed25519 detached-signature length.
const SIGNATURE_BYTES: usize = 64;
/// The JWS algorithm of a card signature (RFC 8037 section 3.1).
#[cfg(feature = "a2a-bridge")]
const CARD_SIGNATURE_ALGORITHM: &str = "EdDSA";
/// The largest integer magnitude an IEEE 754 double holds exactly, the JCS
/// number domain a card is held to.
#[cfg(feature = "a2a-bridge")]
const MAX_JCS_INTEGER: u64 = (1 << 53) - 1;
/// The key-value namespace [`KvKeyRegistry::new`] uses.
#[cfg(feature = "kv")]
pub const DEFAULT_KEY_NAMESPACE: &str = "agent.keys";

/// The canonical bytes a signature covers: the domain separator followed by the
/// named-field encoding of the envelope with its signature cleared. Sign and
/// verify build the identical input, so a decode-then-re-encode must be
/// byte-identical (the round-trip property the wire fixtures pin).
fn signing_input(
    envelope: &AgentEnvelope,
    context: Option<&SignatureContext>,
) -> Result<Vec<u8>, LaserError> {
    let mut bare = envelope.clone();
    bare.signature = None;
    let body =
        encode_named(&bare).map_err(|error| LaserError::Codec(format!("sign encode: {error}")))?;
    // domain || [context] || body. The context (agdx.ct / agdx.av) is prepended
    // so a signed record's interpretation attributes cannot be flipped by an
    // intermediary. A `None` context reproduces the pre-context preimage exactly,
    // so an unsigned-context signature verifies unchanged.
    let context_bytes = match context {
        Some(context) => encode_named(context)
            .map_err(|error| LaserError::Codec(format!("sign context encode: {error}")))?,
        None => Vec::new(),
    };
    let mut input = Vec::with_capacity(SIGNATURE_DOMAIN.len() + context_bytes.len() + body.len());
    input.extend_from_slice(SIGNATURE_DOMAIN);
    input.extend_from_slice(&context_bytes);
    input.extend_from_slice(&body);
    Ok(input)
}

/// The key id of a public key: the first [`KEY_ID_BYTES`] of its SHA-256.
fn key_id_of(verifying: &VerifyingKey) -> Vec<u8> {
    Sha256::digest(verifying.as_bytes())[..KEY_ID_BYTES].to_vec()
}

/// An Ed25519 signing key. Sign an envelope to produce the detached
/// [`Signature`] to attach with [`AgentEnvelope::with_signature`].
#[derive(Clone)]
pub struct SigningKey {
    inner: DalekSigningKey,
    key_id: Vec<u8>,
}

impl SigningKey {
    /// A fresh key from 32 bytes of the operating system's random source.
    /// Store its seed ([`to_bytes`](Self::to_bytes)) the way any secret is
    /// stored, and enroll [`verifying_key`](Self::verifying_key).
    pub fn generate() -> Result<Self, LaserError> {
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).map_err(|error| {
            LaserError::Signature(format!(
                "the operating system random source failed: {error}"
            ))
        })?;
        Ok(Self::from_bytes(&secret))
    }

    /// Build from a 32-byte Ed25519 secret seed.
    pub fn from_bytes(secret: &[u8; 32]) -> Self {
        let inner = DalekSigningKey::from_bytes(secret);
        let key_id = key_id_of(&inner.verifying_key());
        Self { inner, key_id }
    }

    /// The 32-byte secret seed, the value [`from_bytes`](Self::from_bytes)
    /// rebuilds this key from.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.inner.to_bytes()
    }

    /// This key's 8-byte id, the [`Signature::key_id`] it stamps.
    pub fn key_id(&self) -> &[u8] {
        &self.key_id
    }

    /// The public verifying key, to enroll in a [`KeyRegistry`].
    pub fn verifying_key(&self) -> VerifyingKey {
        self.inner.verifying_key()
    }

    /// Sign `envelope`, returning the detached signature (no interpretation
    /// context: the preimage covers the envelope body alone).
    pub fn sign(&self, envelope: &AgentEnvelope) -> Result<Signature, LaserError> {
        self.sign_inner(envelope, None)
    }

    /// Sign `envelope` binding `context` (the `agdx.ct` / `agdx.av` interpretation
    /// attributes) into the preimage, so an intermediary cannot flip the codec or
    /// wire version on the signed record. The context is stored on the capsule so
    /// a verifier reconstructs the same preimage.
    pub fn sign_with_context(
        &self,
        envelope: &AgentEnvelope,
        context: SignatureContext,
    ) -> Result<Signature, LaserError> {
        self.sign_inner(envelope, Some(context))
    }

    fn sign_inner(
        &self,
        envelope: &AgentEnvelope,
        context: Option<SignatureContext>,
    ) -> Result<Signature, LaserError> {
        let input = signing_input(envelope, context.as_ref())?;
        let signature = self.inner.sign(&input);
        Ok(Signature {
            scheme: SIGNATURE_SCHEME_ED25519,
            key_id: self.key_id.clone(),
            bytes: signature.to_bytes().to_vec(),
            context,
        })
    }
}

/// Sign a v1.0 A2A card value as a detached JWS (`sign` feature): EdDSA over
/// `base64url(protected) || "." || base64url(JCS(card without signatures))`,
/// the RFC 7515 signing input with the RFC 8785 canonical form as the payload.
/// The protected header names the algorithm and the signing key's id (`kid`,
/// the lowercase hex of [`SigningKey::key_id`]).
#[cfg(feature = "a2a-bridge")]
pub fn sign_card_value(
    key: &SigningKey,
    card: &serde_json::Value,
) -> Result<crate::a2a::AgentCardSignature, LaserError> {
    let payload = jcs_bytes(&without_signatures(card))?;
    // Already canonical: `alg` sorts before `kid` and a hex id needs no escape.
    let protected = base64url(
        format!(
            r#"{{"alg":"{CARD_SIGNATURE_ALGORITHM}","kid":"{}"}}"#,
            key_id_hex(key.key_id())
        )
        .as_bytes(),
    );
    let input = jws_signing_input(&protected, &payload);
    let signature = key.inner.sign(input.as_bytes());
    Ok(crate::a2a::AgentCardSignature {
        protected,
        signature: base64url(&signature.to_bytes()),
    })
}

/// Verify one detached JWS signature over a v1.0 A2A card value against
/// `verifying`. The payload is reconstructed from the card itself (minus
/// `signatures`), so a tampered field fails, exactly the discovery-integrity
/// property the signature exists for. The protected header must name `EdDSA`,
/// carry no `crit` extension (none is understood, RFC 7515 section 4.1.11),
/// and, when it names a `kid`, name the id of `verifying`.
#[cfg(feature = "a2a-bridge")]
pub fn verify_card(
    card: &serde_json::Value,
    signature: &crate::a2a::AgentCardSignature,
    verifying: &VerifyingKey,
) -> Result<(), LaserError> {
    check_card_header(&signature.protected, verifying)?;
    let payload = jcs_bytes(&without_signatures(card))?;
    let input = jws_signing_input(&signature.protected, &payload);
    let payload = base64url_decode(&signature.signature)?;
    let payload: [u8; SIGNATURE_BYTES] = payload
        .try_into()
        .map_err(|_| LaserError::Signature("card signature is not 64 bytes".to_owned()))?;
    verifying
        .verify(
            input.as_bytes(),
            &ed25519_dalek::Signature::from_bytes(&payload),
        )
        .map_err(|_| LaserError::Signature("card signature does not verify".to_owned()))
}

/// A managed key registry: versioned, lifecycle-aware verifying-key records in
/// a reserved namespace (default `agent.keys`), keyed by canonical key id. The
/// kv sibling of the in-memory [`KeyRegistry`]: enroll keys through the
/// platform, then [`registry`](Self::registry) snapshots them into the same
/// [`KeyRegistry`] the verifier folds with. Managed: unsupported without the
/// key-value surface.
#[cfg(feature = "kv")]
#[derive(Clone)]
pub struct KvKeyRegistry {
    laser: crate::laser::Laser,
    namespace: String,
}

#[cfg(feature = "kv")]
impl KvKeyRegistry {
    /// A registry over the default `agent.keys` namespace.
    pub fn new(laser: crate::laser::Laser) -> Self {
        Self::in_namespace(laser, DEFAULT_KEY_NAMESPACE)
    }

    /// A registry over `namespace`.
    pub fn in_namespace(laser: crate::laser::Laser, namespace: impl Into<String>) -> Self {
        Self {
            laser,
            namespace: namespace.into(),
        }
    }

    /// Enroll `verifying` as `principal`'s agent key.
    pub async fn enroll(
        &self,
        principal: impl Into<String>,
        verifying: &VerifyingKey,
    ) -> Result<(), LaserError> {
        self.enroll_record(&KeyRecord::agent(principal, *verifying))
            .await
            .map(|_| ())
    }

    /// Enroll or replace a lifecycle-aware key record with compare-and-swap protection.
    pub async fn enroll_record(&self, record: &KeyRecord) -> Result<u64, LaserError> {
        let stored = stored_key_record(record);
        stored
            .validate()
            .map_err(|error| LaserError::Invalid(error.to_owned()))?;
        let payload = encode_named(&stored)?;
        let kv = self.laser.kv(&self.namespace);
        let key = key_id_hex(&stored.key_id);
        let current = kv.get_entry(&key).await?;
        let write = kv.set(&key).bytes(payload);
        match current {
            Some(entry) => write.expect_version(entry.version).commit().await,
            None => write.expect_absent().commit().await,
        }
    }

    /// Revoke an enrolled key with compare-and-swap protection.
    pub async fn revoke(&self, key_id: impl AsRef<[u8]>) -> Result<u64, LaserError> {
        let key_id = key_id.as_ref();
        if key_id.len() != KEY_ID_BYTES {
            return Err(LaserError::Invalid(
                "Ed25519 key id must be 8 bytes".to_owned(),
            ));
        }
        let kv = self.laser.kv(&self.namespace);
        let key = key_id_hex(key_id);
        let entry = kv
            .get_entry(&key)
            .await?
            .ok_or_else(|| LaserError::Invalid("signing key is not enrolled".to_owned()))?;
        let mut stored: laser_wire::keys::KeyRecord =
            laser_wire::framing::decode_named(&entry.value)?;
        stored
            .validate()
            .map_err(|error| LaserError::Invalid(error.to_owned()))?;
        if stored.key_id != key_id {
            return Err(LaserError::Invalid(
                "key record identifier differs from its storage key".to_owned(),
            ));
        }
        if stored.revoked {
            return Ok(entry.version);
        }
        stored.revoked = true;
        kv.set(&key)
            .bytes(encode_named(&stored)?)
            .expect_version(entry.version)
            .commit()
            .await
    }

    /// Snapshot every enrolled key into a [`KeyRegistry`]. An entry whose
    /// value is not a valid 32-byte Ed25519 key is skipped (a corrupt
    /// enrollment must not poison every other principal's verification).
    pub async fn registry(&self) -> Result<KeyRegistry, LaserError> {
        let entries = self.laser.kv(&self.namespace).scan().entries().await?;
        let registry = KeyRegistry::default();
        for entry in entries {
            if let Ok(stored) =
                laser_wire::framing::decode_named::<laser_wire::keys::KeyRecord>(&entry.value)
                && stored.validate().is_ok()
                && entry.key == key_id_hex(&stored.key_id).into_bytes()
                && let Ok(payload) = <[u8; 32]>::try_from(stored.verifying_key.as_slice())
                && let Ok(verifying) = VerifyingKey::from_bytes(&payload)
                && stored.key_id == key_id_of(&verifying)
            {
                registry.enroll_record(KeyRecord {
                    principal: stored.principal,
                    verifying,
                    kind: stored.kind,
                    valid_from_micros: stored.valid_from_micros,
                    valid_to_micros: stored.valid_to_micros,
                    revoked: stored.revoked,
                });
            }
        }
        Ok(registry)
    }
}

#[cfg(feature = "kv")]
fn stored_key_record(record: &KeyRecord) -> laser_wire::keys::KeyRecord {
    laser_wire::keys::KeyRecord {
        v: laser_wire::keys::KEY_RECORD_VERSION,
        principal: record.principal.clone(),
        key_id: record.key_id(),
        verifying_key: record.verifying.as_bytes().to_vec(),
        kind: record.kind,
        valid_from_micros: record.valid_from_micros,
        valid_to_micros: record.valid_to_micros,
        revoked: record.revoked,
    }
}

#[cfg(any(feature = "kv", feature = "a2a-bridge"))]
fn key_id_hex(key_id: &[u8]) -> String {
    key_id
        .iter()
        .copied()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

/// RFC 8785 (JCS) canonical bytes of `value`: object members sorted by the
/// UTF-16 code units of their names (section 3.2.3), whatever order the map
/// iterates in, strings escaped as ECMAScript `JSON.stringify` does, no
/// whitespace. Numbers are limited to integers an IEEE 754 double holds
/// exactly, which is every number a card carries, so the serialization of
/// section 3.2.2.3 is the decimal integer.
#[cfg(feature = "a2a-bridge")]
fn jcs_bytes(value: &serde_json::Value) -> Result<Vec<u8>, LaserError> {
    let mut out = String::new();
    write_jcs(value, &mut out)?;
    Ok(out.into_bytes())
}

#[cfg(feature = "a2a-bridge")]
fn write_jcs(value: &serde_json::Value, out: &mut String) -> Result<(), LaserError> {
    match value {
        serde_json::Value::Null => out.push_str("null"),
        serde_json::Value::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
        serde_json::Value::Number(number) => {
            let integer = number
                .as_i64()
                .filter(|integer| integer.unsigned_abs() <= MAX_JCS_INTEGER)
                .ok_or_else(|| {
                    LaserError::Signature(
                        "card canonicalization only accepts integers up to 2^53 - 1".to_owned(),
                    )
                })?;
            out.push_str(&integer.to_string());
        }
        serde_json::Value::String(text) => write_jcs_string(text, out)?,
        serde_json::Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_jcs(item, out)?;
            }
            out.push(']');
        }
        serde_json::Value::Object(fields) => {
            let mut members: Vec<(&String, &serde_json::Value)> = fields.iter().collect();
            members.sort_by(|(left, _), (right, _)| left.encode_utf16().cmp(right.encode_utf16()));
            out.push('{');
            for (index, (name, item)) in members.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_jcs_string(name, out)?;
                out.push(':');
                write_jcs(item, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

// serde_json escapes exactly the JCS set: `"`, `\`, the short forms of
// U+0008, U+0009, U+000A, U+000C, U+000D, and `\u00xx` in lowercase for the
// other controls, everything else literal.
#[cfg(feature = "a2a-bridge")]
fn write_jcs_string(text: &str, out: &mut String) -> Result<(), LaserError> {
    let quoted = serde_json::to_string(text)
        .map_err(|error| LaserError::Codec(format!("canonicalize: {error}")))?;
    out.push_str(&quoted);
    Ok(())
}

// The protected header a card signature must carry, see [`verify_card`]. The
// header is an open parameter set (RFC 7515 section 4), so it is read as a map:
// a `crit` of any value, `null` included, is refused.
#[cfg(feature = "a2a-bridge")]
fn check_card_header(protected: &str, verifying: &VerifyingKey) -> Result<(), LaserError> {
    let header: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&base64url_decode(protected)?).map_err(|_| {
            LaserError::Signature("card signature header does not decode".to_owned())
        })?;
    if header.get("alg").and_then(serde_json::Value::as_str) != Some(CARD_SIGNATURE_ALGORITHM) {
        return Err(LaserError::Signature(format!(
            "card signature header must name alg {CARD_SIGNATURE_ALGORITHM}"
        )));
    }
    if header.contains_key("crit") {
        return Err(LaserError::Signature(
            "card signature header names a critical extension this verifier does not understand"
                .to_owned(),
        ));
    }
    match header.get("kid") {
        Some(kid) if kid.as_str() != Some(key_id_hex(&key_id_of(verifying)).as_str()) => Err(
            LaserError::Signature("card signature names another key".to_owned()),
        ),
        _ => Ok(()),
    }
}

/// The card value with its `signatures` slot removed: the JWS payload covers
/// everything else, so a signature never signs itself.
#[cfg(feature = "a2a-bridge")]
fn without_signatures(card: &serde_json::Value) -> serde_json::Value {
    let mut bare = card.clone();
    if let Some(fields) = bare.as_object_mut() {
        fields.remove("signatures");
    }
    bare
}

/// The RFC 7515 signing input: `BASE64URL(protected) || '.' || BASE64URL(payload)`.
#[cfg(feature = "a2a-bridge")]
fn jws_signing_input(protected_b64: &str, payload: &[u8]) -> String {
    format!("{protected_b64}.{}", base64url(payload))
}

/// Unpadded base64url (RFC 4648 section 5), dependency-free.
#[cfg(feature = "a2a-bridge")]
fn base64url(payload: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(payload.len().div_ceil(3) * 4);
    for chunk in payload.chunks(3) {
        let buffer = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let index = u32::from_be_bytes([0, buffer[0], buffer[1], buffer[2]]);
        out.push(ALPHABET[(index >> 18) as usize & 63] as char);
        out.push(ALPHABET[(index >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(index >> 6) as usize & 63] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[index as usize & 63] as char);
        }
    }
    out
}

/// Inverse of [`base64url`], rejecting non-alphabet input.
#[cfg(feature = "a2a-bridge")]
fn base64url_decode(text: &str) -> Result<Vec<u8>, LaserError> {
    fn value_of(byte: u8) -> Result<u32, LaserError> {
        match byte {
            b'A'..=b'Z' => Ok(u32::from(byte - b'A')),
            b'a'..=b'z' => Ok(u32::from(byte - b'a') + 26),
            b'0'..=b'9' => Ok(u32::from(byte - b'0') + 52),
            b'-' => Ok(62),
            b'_' => Ok(63),
            _ => Err(LaserError::Signature("invalid base64url".to_owned())),
        }
    }
    let payload = text.as_bytes();
    let mut out = Vec::with_capacity(payload.len() * 3 / 4);
    for chunk in payload.chunks(4) {
        if chunk.len() == 1 {
            return Err(LaserError::Signature("truncated base64url".to_owned()));
        }
        let mut index = 0u32;
        for &byte in chunk {
            index = (index << 6) | value_of(byte)?;
        }
        index <<= 6 * (4 - chunk.len()) as u32;
        out.push((index >> 16) as u8);
        if chunk.len() > 2 {
            out.push((index >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(index as u8);
        }
    }
    Ok(out)
}

/// Whether an enrolled key may sign privileged control facts (`Operator`) or only
/// its own agent messages (`Agent`). A quarantine/unquarantine fold requires an
/// `Operator` signer, so an enrolled agent cannot quarantine another agent.
/// A verified signer: the enrolled principal and the kind of key it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPrincipal {
    /// The authenticated principal the key was enrolled under.
    pub principal: String,
    /// Whether the key is an operator or agent key.
    pub kind: KeyKind,
}

/// An enrolled key with its lifecycle. `valid_from`/`valid_to` bound when the key
/// may sign (epoch micros, `valid_to` `None` is open-ended), and `revoked` hard
/// disables it regardless of window. A rotation enrolls the new key while the old
/// one's `valid_to` still covers in-flight records (the overlap window).
#[derive(Debug, Clone)]
pub struct KeyRecord {
    /// The authenticated principal this key is bound to.
    pub principal: String,
    /// The public key.
    pub verifying: VerifyingKey,
    /// Whether it is an operator or agent key.
    pub kind: KeyKind,
    /// Not valid before this epoch-micros time.
    pub valid_from_micros: u64,
    /// Not valid at or after this epoch-micros time (`None` is open-ended).
    pub valid_to_micros: Option<u64>,
    /// Hard-disabled: never verifies, whatever the window.
    pub revoked: bool,
}

impl KeyRecord {
    /// The 8-byte identifier derived from this record's public key.
    pub fn key_id(&self) -> Vec<u8> {
        key_id_of(&self.verifying)
    }

    /// An always-valid agent key.
    pub fn agent(principal: impl Into<String>, verifying: VerifyingKey) -> Self {
        Self::new(principal, verifying, KeyKind::Agent)
    }

    /// An always-valid operator key (may sign privileged control facts).
    pub fn operator(principal: impl Into<String>, verifying: VerifyingKey) -> Self {
        Self::new(principal, verifying, KeyKind::Operator)
    }

    /// Build a record from a 32-byte Ed25519 public verifying key.
    pub fn from_verifying_bytes(
        principal: impl Into<String>,
        verifying: &[u8],
        kind: KeyKind,
    ) -> Result<Self, LaserError> {
        let bytes: [u8; 32] = verifying.try_into().map_err(|_| {
            LaserError::Invalid("an Ed25519 public key requires exactly 32 bytes".to_owned())
        })?;
        let verifying = VerifyingKey::from_bytes(&bytes)
            .map_err(|_| LaserError::Invalid("invalid Ed25519 public key".to_owned()))?;
        Ok(Self::new(principal, verifying, kind))
    }

    fn new(principal: impl Into<String>, verifying: VerifyingKey, kind: KeyKind) -> Self {
        Self {
            principal: principal.into(),
            verifying,
            kind,
            valid_from_micros: 0,
            valid_to_micros: None,
            revoked: false,
        }
    }

    /// Bound the validity window (epoch micros). An end at or before the start
    /// is refused as invalid, since such a key could never verify.
    pub fn valid_window(
        mut self,
        from_micros: u64,
        to_micros: Option<u64>,
    ) -> Result<Self, LaserError> {
        if to_micros.is_some_and(|end| end <= from_micros) {
            return Err(LaserError::Invalid(
                "key validity end must be after its start".to_owned(),
            ));
        }
        self.valid_from_micros = from_micros;
        self.valid_to_micros = to_micros;
        Ok(self)
    }

    /// Mark the key revoked.
    #[must_use]
    pub fn revoked(mut self) -> Self {
        self.revoked = true;
        self
    }
}

/// A registry of enrolled verifying keys, each bound to the authenticated
/// principal that enrolled it. Verifying a signed envelope returns the bound
/// principal, so a caller asserts it equals the server-stamped identity (the
/// authorship binding: a key proves its enrolled principal, never the
/// self-asserted `source`). A key carries a kind (agent vs operator) and a
/// lifecycle window, so a privileged-fact verifier can require an operator key
/// valid at fold time. A registry is a shared handle: its clones, and every
/// verifier configured with it, read the same enrollments, so enrolling a
/// revoked record reaches a running client at once.
#[derive(Clone, Default)]
pub struct KeyRegistry {
    keys: Arc<RwLock<HashMap<Vec<u8>, KeyRecord>>>,
}

impl KeyRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enroll `verifying` as an always-valid agent key bound to `principal` (the
    /// authenticated `user_id`). A later enroll under the same key id replaces it.
    pub fn enroll(&self, principal: impl Into<String>, verifying: VerifyingKey) {
        self.enroll_record(KeyRecord::agent(principal, verifying));
    }

    /// Enroll `verifying` as an always-valid operator key (may sign privileged
    /// control facts).
    pub fn enroll_operator(&self, principal: impl Into<String>, verifying: VerifyingKey) {
        self.enroll_record(KeyRecord::operator(principal, verifying));
    }

    /// Enroll a key with an explicit kind and lifecycle. Enrolling a record
    /// under an enrolled key id replaces it, which is how a key is revoked or
    /// its window changed.
    pub fn enroll_record(&self, record: KeyRecord) {
        self.keys
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key_id_of(&record.verifying), record);
    }

    /// Verify the envelope's signature, returning the enrolled principal on
    /// success. Errors on an unsigned envelope, an unknown or revoked key, a
    /// non-Ed25519 scheme, a malformed signature, or a failed check. Ignores the
    /// validity window (no clock): a time-bound check is [`verify_at`](Self::verify_at).
    pub fn verify(&self, envelope: &AgentEnvelope) -> Result<String, LaserError> {
        self.check(envelope, None)
            .map(|verified| verified.principal)
    }

    /// Verify as of `at_micros`, returning the signer's principal and key kind.
    /// Errors additionally when the key is outside its validity window at that
    /// time. The privileged-fact path uses this to require an operator key valid
    /// at fold time.
    pub fn verify_at(
        &self,
        envelope: &AgentEnvelope,
        at_micros: u64,
    ) -> Result<VerifiedPrincipal, LaserError> {
        self.check(envelope, Some(at_micros))
    }

    /// Verify a log-resident envelope against the interpretation headers that
    /// were actually observed on the record, using the record timestamp for the
    /// key lifecycle check. A signed context that differs from the observed
    /// headers is rejected before cryptographic verification.
    pub fn verify_observed_at(
        &self,
        envelope: &AgentEnvelope,
        observed: &SignatureContext,
        at_micros: u64,
    ) -> Result<VerifiedPrincipal, LaserError> {
        let signed = envelope
            .signature
            .as_ref()
            .and_then(|signature| signature.context.as_ref())
            .ok_or_else(|| {
                LaserError::Signature("signature does not bind record headers".to_owned())
            })?;
        if signed != observed {
            return Err(LaserError::Signature(
                "signature context differs from record headers".to_owned(),
            ));
        }
        self.check(envelope, Some(at_micros))
    }

    fn check(
        &self,
        envelope: &AgentEnvelope,
        at_micros: Option<u64>,
    ) -> Result<VerifiedPrincipal, LaserError> {
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
        let keys = self.keys.read().unwrap_or_else(PoisonError::into_inner);
        let record = keys
            .get(signature.key_id.as_slice())
            .ok_or_else(|| LaserError::Signature("signing key is not enrolled".to_owned()))?;
        if record.revoked {
            return Err(LaserError::Signature("signing key is revoked".to_owned()));
        }
        if let Some(now) = at_micros {
            if now < record.valid_from_micros {
                return Err(LaserError::Signature(
                    "signing key is not yet valid".to_owned(),
                ));
            }
            if record.valid_to_micros.is_some_and(|to| now >= to) {
                return Err(LaserError::Signature("signing key has expired".to_owned()));
            }
        }
        let payload: [u8; SIGNATURE_BYTES] = signature
            .bytes
            .as_slice()
            .try_into()
            .map_err(|_| LaserError::Signature("malformed Ed25519 signature".to_owned()))?;
        let signature_bytes = ed25519_dalek::Signature::from_bytes(&payload);
        let input = signing_input(envelope, signature.context.as_ref())?;
        record
            .verifying
            .verify(&input, &signature_bytes)
            .map_err(|_| LaserError::Signature("signature verification failed".to_owned()))?;
        Ok(VerifiedPrincipal {
            principal: record.principal.clone(),
            kind: record.kind,
        })
    }
}

/// Verify an on-behalf-of envelope and return `(signer, delegated_user)`. The
/// signature must verify through [`KeyRegistry::verify_observed_at`] against the
/// headers `observed` on the record and its server timestamp `at_micros` (so an
/// expired or revoked key, or a rewritten header, cannot carry the claim), and
/// the delegated user is read from the signed metadata span. `None` when the
/// envelope carries no delegation. The server authorizes only the authenticated
/// caller, so the caller authorizes with [`laser_wire::authz::delegated_allow`],
/// intersecting the two principals' grants so the agent can never exceed the
/// user it acts for.
pub fn verify_delegation(
    registry: &KeyRegistry,
    envelope: &AgentEnvelope,
    observed: &SignatureContext,
    at_micros: u64,
) -> Result<Option<(String, String)>, LaserError> {
    let signer = registry
        .verify_observed_at(envelope, observed, at_micros)?
        .principal;
    let delegated = envelope
        .metadata
        .as_ref()
        .and_then(|meta| meta.get(laser_wire::agent::METADATA_DELEGATED_BY))
        .and_then(|value| match value {
            laser_wire::query::Value::Str(user) => Some(user.clone()),
            _ => None,
        });
    Ok(delegated.map(|user| (signer, user)))
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
    fn given_generated_keys_when_used_then_should_differ_sign_and_rebuild_from_their_seed() {
        let first = SigningKey::generate().expect("the random source answers");
        let second = SigningKey::generate().expect("the random source answers");
        assert_ne!(first.to_bytes(), second.to_bytes());
        let rebuilt = SigningKey::from_bytes(&first.to_bytes());
        assert_eq!(rebuilt.key_id(), first.key_id());
        let registry = KeyRegistry::new();
        registry.enroll("generated", first.verifying_key());
        let signed = envelope().with_signature(rebuilt.sign(&envelope()).expect("signs"));
        assert_eq!(registry.verify(&signed).expect("verifies"), "generated");
    }

    #[test]
    fn given_a_signed_envelope_when_verified_then_should_return_the_enrolled_principal() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let registry = KeyRegistry::new();
        registry.enroll("user-42", key.verifying_key());

        let signed = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        assert_eq!(registry.verify(&signed).expect("verifies"), "user-42");
    }

    #[test]
    fn given_a_tampered_body_when_verified_then_should_fail() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let registry = KeyRegistry::new();
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

    #[test]
    fn given_a_signed_delegation_when_verified_then_should_return_the_signer_and_user() {
        let key = SigningKey::from_bytes(&[9u8; 32]);
        let registry = KeyRegistry::new();
        registry.enroll("agent-a", key.verifying_key());
        let context = delegation_context();

        let env = envelope().with_metadata(laser_wire::agent::METADATA_DELEGATED_BY, "user-7");
        let signed = env
            .clone()
            .with_signature(key.sign_with_context(&env, context.clone()).expect("signs"));
        assert_eq!(
            verify_delegation(&registry, &signed, &context, 1).expect("verifies"),
            Some(("agent-a".to_owned(), "user-7".to_owned()))
        );

        // No delegation claim: verifies, reports None.
        let plain = envelope().with_signature(
            key.sign_with_context(&envelope(), context.clone())
                .expect("signs"),
        );
        assert_eq!(
            verify_delegation(&registry, &plain, &context, 1).expect("verifies"),
            None
        );

        // A forged claim (unsigned mutation) fails signature verification.
        let forged = signed.with_metadata(laser_wire::agent::METADATA_DELEGATED_BY, "user-999");
        assert!(matches!(
            verify_delegation(&registry, &forged, &context, 1),
            Err(LaserError::Signature(_))
        ));
    }

    #[test]
    fn given_an_expired_key_or_rewritten_headers_when_a_delegation_is_verified_then_should_reject_it()
     {
        let key = SigningKey::from_bytes(&[10u8; 32]);
        let registry = KeyRegistry::new();
        registry.enroll_record(
            KeyRecord::agent("agent-a", key.verifying_key())
                .valid_window(100, Some(200))
                .expect("the window is ordered"),
        );
        let context = delegation_context();
        let env = envelope().with_metadata(laser_wire::agent::METADATA_DELEGATED_BY, "user-7");
        let signed = env
            .clone()
            .with_signature(key.sign_with_context(&env, context.clone()).expect("signs"));
        assert!(verify_delegation(&registry, &signed, &context, 150).is_ok());
        assert!(matches!(
            verify_delegation(&registry, &signed, &context, 200),
            Err(LaserError::Signature(_))
        ));
        let rewritten = SignatureContext {
            content_type: Some(3),
            agent_version: Some(1),
        };
        assert!(matches!(
            verify_delegation(&registry, &signed, &rewritten, 150),
            Err(LaserError::Signature(_))
        ));
    }

    #[cfg(feature = "a2a-bridge")]
    #[test]
    fn given_a_signed_card_when_verified_then_should_pass_and_fail_on_tamper() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let card = serde_json::json!({
            "name": "a2a-bridge",
            "version": "0.0.1",
            "supportedInterfaces": [
                {"url": "/", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}
            ],
            "skills": []
        });
        let signature = sign_card_value(&key, &card).expect("signs");
        verify_card(&card, &signature, &key.verifying_key()).expect("verifies");

        // Any field change breaks the detached payload.
        let mut tampered = card.clone();
        tampered["name"] = serde_json::json!("impostor");
        assert!(verify_card(&tampered, &signature, &key.verifying_key()).is_err());

        // A signature already on the card is excluded from its own payload, so
        // attaching it does not invalidate verification.
        let mut signed = card.clone();
        signed["signatures"] = serde_json::json!([{
            "protected": signature.protected,
            "signature": signature.signature,
        }]);
        verify_card(&signed, &signature, &key.verifying_key()).expect("self-carrying verifies");
    }

    #[test]
    fn given_a_context_signature_when_the_interpretation_flips_then_should_fail() {
        let key = SigningKey::from_bytes(&[21u8; 32]);
        let registry = KeyRegistry::new();
        registry.enroll("user-1", key.verifying_key());
        let context = SignatureContext {
            content_type: Some(1),
            agent_version: Some(1),
        };
        let signed =
            envelope().with_signature(key.sign_with_context(&envelope(), context).expect("signs"));
        assert_eq!(registry.verify(&signed).expect("verifies"), "user-1");
        // Flip the bound content-type: the preimage no longer matches, so a hop
        // that reinterprets the codec on the signed record is rejected.
        let mut flipped = signed.clone();
        if let Some(signature) = flipped.signature.as_mut() {
            signature.context = Some(SignatureContext {
                content_type: Some(3),
                agent_version: Some(1),
            });
        }
        assert!(registry.verify(&flipped).is_err());
    }

    #[test]
    fn given_an_operator_key_when_verified_at_then_should_report_the_operator_kind() {
        let key = SigningKey::from_bytes(&[11u8; 32]);
        let registry = KeyRegistry::new();
        registry.enroll_operator("op-1", key.verifying_key());
        let signed = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        let verified = registry.verify_at(&signed, 1_000).expect("verifies");
        assert_eq!(verified.principal, "op-1");
        assert_eq!(verified.kind, KeyKind::Operator);
        // A plain enroll is an agent key, distinguishable at fold time.
        let agent_key = SigningKey::from_bytes(&[12u8; 32]);
        registry.enroll("agent-x", agent_key.verifying_key());
        let agent_signed = envelope().with_signature(agent_key.sign(&envelope()).expect("signs"));
        assert_eq!(
            registry
                .verify_at(&agent_signed, 1_000)
                .expect("verifies")
                .kind,
            KeyKind::Agent
        );
    }

    #[test]
    fn given_a_key_outside_its_window_when_verified_at_then_should_fail() {
        let key = SigningKey::from_bytes(&[13u8; 32]);
        let registry = KeyRegistry::new();
        registry.enroll_record(
            KeyRecord::operator("op-2", key.verifying_key())
                .valid_window(100, Some(200))
                .expect("the window is ordered"),
        );
        let signed = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        assert!(
            registry.verify_at(&signed, 150).is_ok(),
            "inside the window"
        );
        assert!(
            registry.verify_at(&signed, 50).is_err(),
            "before valid_from"
        );
        assert!(registry.verify_at(&signed, 200).is_err(), "at valid_to");
        // `verify` (no clock) still passes: the window only binds `verify_at`.
        assert!(registry.verify(&signed).is_ok());
    }

    #[test]
    fn given_a_registry_held_by_a_verifier_when_its_key_is_revoked_then_should_refuse_it_at_once() {
        let key = SigningKey::from_bytes(&[15u8; 32]);
        let registry = KeyRegistry::new();
        registry.enroll_operator("op-4", key.verifying_key());
        let held = std::sync::Arc::new(registry.clone());
        let signed = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        assert!(held.verify(&signed).is_ok());
        registry.enroll_record(KeyRecord::operator("op-4", key.verifying_key()).revoked());
        assert!(matches!(
            held.verify(&signed),
            Err(LaserError::Signature(_))
        ));
    }

    #[test]
    fn given_an_inverted_validity_window_when_a_record_is_built_then_should_refuse_it_as_invalid() {
        let key = SigningKey::from_bytes(&[16u8; 32]);
        for (from, to) in [(200, Some(100)), (100, Some(100))] {
            assert!(matches!(
                KeyRecord::agent("agent-b", key.verifying_key()).valid_window(from, to),
                Err(LaserError::Invalid(_))
            ));
        }
        assert!(
            KeyRecord::agent("agent-b", key.verifying_key())
                .valid_window(100, None)
                .is_ok()
        );
    }

    #[test]
    fn given_a_revoked_key_when_verified_then_should_fail_regardless_of_time() {
        let key = SigningKey::from_bytes(&[14u8; 32]);
        let registry = KeyRegistry::new();
        registry.enroll_record(KeyRecord::operator("op-3", key.verifying_key()).revoked());
        let signed = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        assert!(registry.verify(&signed).is_err());
        assert!(registry.verify_at(&signed, 1_000).is_err());
    }

    #[cfg(feature = "a2a-bridge")]
    #[test]
    fn given_the_shared_card_vector_when_signed_then_should_sign_the_rfc_8785_canonical_form() {
        let vector: CardVector =
            serde_json::from_str(include_str!("../tests/fixtures/card_signature.json"))
                .expect("the card vector parses");
        let key = SigningKey::from_bytes(&[7u8; 32]);
        assert_eq!(vector.secret_seed_hex, "07".repeat(32));
        assert_eq!(
            String::from_utf8(jcs_bytes(&without_signatures(&vector.card)).expect("canonicalizes"))
                .expect("utf8"),
            vector.canonical
        );
        let signature = sign_card_value(&key, &vector.card).expect("signs");
        assert_eq!(signature.protected, vector.protected);
        assert_eq!(signature.signature, vector.signature);
        verify_card(&vector.card, &signature, &key.verifying_key()).expect("verifies");
    }

    #[cfg(feature = "a2a-bridge")]
    #[test]
    fn given_a_card_signature_header_when_verified_then_should_require_eddsa_no_crit_and_the_key_id()
     {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let other = SigningKey::from_bytes(&[8u8; 32]);
        let card = serde_json::json!({ "name": "planner", "skills": [] });
        let signed = sign_card_value(&key, &card).expect("signs");
        verify_card(&card, &signed, &key.verifying_key()).expect("verifies");
        let reheader = |header: &str| {
            let protected = base64url(header.as_bytes());
            let input = jws_signing_input(&protected, br#"{"name":"planner","skills":[]}"#);
            crate::a2a::AgentCardSignature {
                protected,
                signature: base64url(&key.inner.sign(input.as_bytes()).to_bytes()),
            }
        };
        verify_card(&card, &reheader(r#"{"alg":"EdDSA"}"#), &key.verifying_key())
            .expect("a header without kid verifies");
        for header in [
            r#"{"alg":"none"}"#,
            r#"{"alg":"EdDSA","crit":["exp"],"exp":1}"#,
            r#"{"alg":"EdDSA","crit":null}"#,
            r#"["alg"]"#,
        ] {
            assert!(
                matches!(
                    verify_card(&card, &reheader(header), &key.verifying_key()),
                    Err(LaserError::Signature(_))
                ),
                "{header}"
            );
        }
        assert!(matches!(
            verify_card(&card, &signed, &other.verifying_key()),
            Err(LaserError::Signature(_))
        ));
    }

    #[cfg(feature = "a2a-bridge")]
    #[test]
    fn given_numbers_past_the_double_integer_range_when_canonicalized_then_should_refuse_them() {
        for number in [
            serde_json::json!(9_007_199_254_740_992_u64),
            serde_json::json!(-9_007_199_254_740_992_i64),
            serde_json::json!(1.5),
        ] {
            assert!(matches!(jcs_bytes(&number), Err(LaserError::Signature(_))));
        }
        assert_eq!(
            jcs_bytes(&serde_json::json!(-9_007_199_254_740_991_i64)).expect("in range"),
            b"-9007199254740991"
        );
    }

    #[test]
    fn given_base64url_when_round_tripped_then_should_be_lossless() {
        for input in [&b""[..], b"f", b"fo", b"foo", b"foob", &[0xff, 0x00, 0x7f]] {
            #[cfg(feature = "a2a-bridge")]
            assert_eq!(
                base64url_decode(&base64url(input)).expect("decodes"),
                input.to_vec()
            );
        }
    }

    fn delegation_context() -> SignatureContext {
        SignatureContext {
            content_type: Some(1),
            agent_version: Some(1),
        }
    }

    #[cfg(feature = "a2a-bridge")]
    #[derive(serde::Deserialize)]
    struct CardVector {
        secret_seed_hex: String,
        card: serde_json::Value,
        canonical: String,
        protected: String,
        signature: String,
    }
}
