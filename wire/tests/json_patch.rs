#![cfg(feature = "cbor")]

use laser_wire::agent::{ContextManifest, Fragment, StateDelta, StateSnapshot, apply_json_patch};
use laser_wire::framing::decode_named;
use laser_wire::schema::Digest32;
use laser_wire::validate::Validate;
use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    name: String,
    document: serde_json::Value,
    patch: Vec<json_patch::PatchOperation>,
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: bool,
}

#[test]
fn shared_json_patch_cases_match_the_rust_engine() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/json_patch/cases.json")).expect("cases");
    for case in cases {
        let actual = apply_json_patch(&case.document, &case.patch);
        if case.error {
            assert!(actual.is_err(), "{} must fail", case.name);
        } else {
            assert_eq!(
                actual.expect(&case.name),
                case.result.expect("result"),
                "{}",
                case.name
            );
        }
    }
}

#[test]
fn state_patch_limits_apply_before_and_after_the_patch() {
    let operation: json_patch::PatchOperation =
        serde_json::from_value(serde_json::json!({"op": "remove", "path": "/a"}))
            .expect("operation");
    assert!(apply_json_patch(&serde_json::json!({"a": 1}), &vec![operation; 257]).is_err());
    let large = serde_json::json!({"large": "x".repeat(8 * 1024 * 1024)});
    assert!(apply_json_patch(&large, &[]).is_err());
}

#[test]
fn context_digest_and_patch_operations_reject_invalid_inputs() {
    let mut manifest: ContextManifest =
        decode_named(include_bytes!("../fixtures/context_manifest.bin")).expect("manifest fixture");
    let memory = manifest
        .fragments
        .iter_mut()
        .find(|fragment| matches!(fragment, Fragment::Memory { .. }))
        .expect("memory fragment");
    if let Fragment::Memory { digest, .. } = memory {
        *digest = Some(Digest32(vec![0; 31]));
    }
    assert!(manifest.validate().is_err());
    assert!(
        serde_json::from_value::<StateDelta>(serde_json::json!({
            "base_revision": 0,
            "patch": [{"op": "unknown", "path": "/a"}],
            "op_id": "patch-1"
        }))
        .is_err()
    );
}

#[test]
fn state_json_rejects_integers_that_typescript_cannot_represent_exactly() {
    let document = serde_json::json!({"large": 9_007_199_254_740_992_u64});
    let snapshot = StateSnapshot {
        base_revision: 0,
        document: document.clone(),
    };
    assert!(snapshot.validate().is_err());
    assert!(apply_json_patch(&document, &[]).is_err());
    let delta: StateDelta = serde_json::from_value(serde_json::json!({
        "base_revision": 0,
        "patch": [{"op": "add", "path": "/large", "value": 9_007_199_254_740_992_u64}],
        "op_id": "patch-1"
    }))
    .expect("patch shape");
    assert!(delta.validate().is_err());
}
