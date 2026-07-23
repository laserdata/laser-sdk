use crate::error::LaserError;
use ed25519_dalek::{Signer, SigningKey as DalekSigningKey, Verifier, VerifyingKey};
use laser_wire::agent::{
    AgentEnvelope, SIGNATURE_DOMAIN, SIGNATURE_SCHEME_ED25519, Signature, SignatureContext,
};
use laser_wire::framing::encode_named;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// The key id length: the first 8 bytes of the public key's SHA-256.
const KEY_ID_BYTES: usize = 8;
/// The Ed25519 detached-signature length.
const SIGNATURE_BYTES: usize = 64;
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
#[cfg(feature = "a2a-bridge")]
pub fn sign_card_value(
    key: &SigningKey,
    card: &serde_json::Value,
) -> Result<crate::a2a::AgentCardSignature, LaserError> {
    let payload = jcs_bytes(&without_signatures(card))?;
    let protected = base64url(br#"{"alg":"EdDSA"}"#);
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
/// property the signature exists for.
#[cfg(feature = "a2a-bridge")]
pub fn verify_card(
    card: &serde_json::Value,
    signature: &crate::a2a::AgentCardSignature,
    verifying: &VerifyingKey,
) -> Result<(), LaserError> {
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

/// A managed key registry: verifying keys as key-value entries in a reserved
/// namespace (default `agent.keys`), one 32-byte Ed25519 public key per
/// principal. The kv sibling of the in-memory [`KeyRegistry`]: enroll keys
/// through the platform, then [`registry`](Self::registry) snapshots them into
/// the same [`KeyRegistry`] the verifier folds with, so verification composes
/// with the plane instead of requiring a side file. Managed: unsupported
/// without the key-value surface.
#[cfg(feature = "kv")]
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

    /// Enroll `verifying` as `principal`'s key (an overwrite rotates it).
    pub async fn enroll(
        &self,
        principal: impl Into<String>,
        verifying: &VerifyingKey,
    ) -> Result<(), LaserError> {
        self.laser
            .kv(&self.namespace)
            .set(principal.into())
            .bytes(verifying.as_bytes())
            .send()
            .await
    }

    /// Snapshot every enrolled key into a [`KeyRegistry`]. An entry whose
    /// value is not a valid 32-byte Ed25519 key is skipped (a corrupt
    /// enrollment must not poison every other principal's verification).
    pub async fn registry(&self) -> Result<KeyRegistry, LaserError> {
        let entries = self.laser.kv(&self.namespace).scan().entries().await?;
        let mut registry = KeyRegistry::default();
        for entry in entries {
            let Ok(principal) = String::from_utf8(entry.key.clone()) else {
                continue;
            };
            let Ok(payload) = <[u8; 32]>::try_from(entry.value.as_slice()) else {
                continue;
            };
            let Ok(verifying) = VerifyingKey::from_bytes(&payload) else {
                continue;
            };
            registry.enroll(principal, verifying);
        }
        Ok(registry)
    }
}

/// RFC 8785 (JCS) canonical bytes of `value` for the card's value domain:
/// serde_json's plain `Value` already holds objects sorted (BTreeMap) and
/// serializes compact, which IS the canonical form for a tree of strings,
/// booleans, integers, arrays, and objects. A non-integer number is refused
/// rather than canonicalized wrong: the card schema carries none, so hitting
/// one means the input is not a card.
#[cfg(feature = "a2a-bridge")]
fn jcs_bytes(value: &serde_json::Value) -> Result<Vec<u8>, LaserError> {
    fn assert_no_floats(value: &serde_json::Value) -> Result<(), LaserError> {
        match value {
            serde_json::Value::Number(number) if !number.is_i64() && !number.is_u64() => {
                Err(LaserError::Signature(
                    "card canonicalization does not cover non-integer numbers".to_owned(),
                ))
            }
            serde_json::Value::Array(items) => items.iter().try_for_each(assert_no_floats),
            serde_json::Value::Object(fields) => fields.values().try_for_each(assert_no_floats),
            _ => Ok(()),
        }
    }
    assert_no_floats(value)?;
    serde_json::to_vec(value).map_err(|error| LaserError::Codec(format!("canonicalize: {error}")))
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    /// An agent key: signs the agent's own messages.
    Agent,
    /// An operator key: authorized to sign privileged control facts.
    Operator,
}

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
    /// An always-valid agent key.
    pub fn agent(principal: impl Into<String>, verifying: VerifyingKey) -> Self {
        Self::new(principal, verifying, KeyKind::Agent)
    }

    /// An always-valid operator key (may sign privileged control facts).
    pub fn operator(principal: impl Into<String>, verifying: VerifyingKey) -> Self {
        Self::new(principal, verifying, KeyKind::Operator)
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

    /// Bound the validity window (epoch micros).
    #[must_use]
    pub fn valid_window(mut self, from_micros: u64, to_micros: Option<u64>) -> Self {
        self.valid_from_micros = from_micros;
        self.valid_to_micros = to_micros;
        self
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
/// valid at fold time.
#[derive(Clone, Default)]
pub struct KeyRegistry {
    keys: HashMap<Vec<u8>, KeyRecord>,
}

impl KeyRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enroll `verifying` as an always-valid agent key bound to `principal` (the
    /// authenticated `user_id`). A later enroll under the same key id replaces it.
    pub fn enroll(&mut self, principal: impl Into<String>, verifying: VerifyingKey) {
        self.enroll_record(KeyRecord::agent(principal, verifying));
    }

    /// Enroll `verifying` as an always-valid operator key (may sign privileged
    /// control facts).
    pub fn enroll_operator(&mut self, principal: impl Into<String>, verifying: VerifyingKey) {
        self.enroll_record(KeyRecord::operator(principal, verifying));
    }

    /// Enroll a key with an explicit kind and lifecycle.
    pub fn enroll_record(&mut self, record: KeyRecord) {
        self.keys.insert(key_id_of(&record.verifying), record);
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
        let record = self
            .keys
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
/// signature must verify against an enrolled key (so the signer cannot forge the
/// claim), and the delegated user is read from the signed metadata span. `None`
/// when the envelope carries no delegation. The caller then authorizes with
/// [`laser_wire::authz::delegated_allow`], intersecting the two principals'
/// grants so the agent can never exceed the user it acts for.
pub fn verify_delegation(
    registry: &KeyRegistry,
    envelope: &AgentEnvelope,
) -> Result<Option<(String, String)>, LaserError> {
    let signer = registry.verify(envelope)?;
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

    #[test]
    fn given_a_signed_delegation_when_verified_then_should_return_the_signer_and_user() {
        let key = SigningKey::from_bytes(&[9u8; 32]);
        let mut registry = KeyRegistry::new();
        registry.enroll("agent-a", key.verifying_key());

        let env = envelope().with_metadata(laser_wire::agent::METADATA_DELEGATED_BY, "user-7");
        let signed = env.clone().with_signature(key.sign(&env).expect("signs"));
        assert_eq!(
            verify_delegation(&registry, &signed).expect("verifies"),
            Some(("agent-a".to_owned(), "user-7".to_owned()))
        );

        // No delegation claim: verifies, reports None.
        let plain = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        assert_eq!(
            verify_delegation(&registry, &plain).expect("verifies"),
            None
        );

        // A forged claim (unsigned mutation) fails signature verification.
        let mut forged = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        forged = forged.with_metadata(laser_wire::agent::METADATA_DELEGATED_BY, "user-999");
        assert!(matches!(
            verify_delegation(&registry, &forged),
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
        let mut registry = KeyRegistry::new();
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
        let mut registry = KeyRegistry::new();
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
        let mut registry = KeyRegistry::new();
        registry.enroll_record(
            KeyRecord::operator("op-2", key.verifying_key()).valid_window(100, Some(200)),
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
    fn given_a_revoked_key_when_verified_then_should_fail_regardless_of_time() {
        let key = SigningKey::from_bytes(&[14u8; 32]);
        let mut registry = KeyRegistry::new();
        registry.enroll_record(KeyRecord::operator("op-3", key.verifying_key()).revoked());
        let signed = envelope().with_signature(key.sign(&envelope()).expect("signs"));
        assert!(registry.verify(&signed).is_err());
        assert!(registry.verify_at(&signed, 1_000).is_err());
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
}
