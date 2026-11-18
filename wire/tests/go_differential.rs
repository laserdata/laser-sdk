//! Differential check against the Go port.
//!
//! The fixture corpus pins the bytes of the shapes it carries. The shapes it
//! does not carry are proven in each client only against itself: a round trip
//! shows an encoder and a decoder agree, not that either agrees with this
//! crate.
//!
//! This closes that. The Go port writes one populated value per unfixtured
//! shape, and each is decoded here into its own typed peer and re-encoded. A
//! value that decodes and returns the same bytes is one both implementations
//! agree on, which is what a fixture proves, for a shape no fixture exists
//! for.
//!
//! The vectors are generated, not committed, so the two halves run together:
//!
//! ```text
//! LASER_GO_VECTORS=/tmp/go-vectors go test ./wire -run Export
//! LASER_GO_VECTORS=/tmp/go-vectors cargo test -p laser-wire --test go_differential
//! ```
//!
//! Without the variable the test skips, so a checkout without the Go port
//! still builds and passes.

#![cfg(feature = "cbor")]

use laser_wire::framing::{decode_named, encode_named};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};

/// Decode one vector into `T` and re-encode it, returning the bytes that came
/// back so the caller can compare them with what Go wrote.
fn round_trip<T: DeserializeOwned + Serialize>(payload: &[u8]) -> Result<Vec<u8>, String> {
    let value: T = decode_named(payload).map_err(|error| format!("decode: {error}"))?;
    encode_named(&value).map_err(|error| format!("encode: {error}"))
}

/// One shape's verifier: decode the bytes as the named type and re-encode.
type Verifier = fn(&[u8]) -> Result<Vec<u8>, String>;

/// Every shape the Go port exports, paired with the contract type it must
/// decode as. A name with no entry here is reported rather than skipped, so a
/// shape added on the Go side is noticed instead of silently unchecked.
fn verifiers() -> Vec<(&'static str, Verifier)> {
    use laser_wire::agent::SessionPauseRequest;
    use laser_wire::authz::{
        DefineRoleReq, DeleteRoleReq, GetBindingsReq, GetRoleReq, ListRolesReq, RoleBinding,
        WhoamiReq,
    };
    use laser_wire::browse::{GetSchema, ListSchemas, MaterializationReadiness};
    use laser_wire::filter::HeaderScalar;
    use laser_wire::filter::{
        FilterCatalogVersion, FilterHeader, FilterPolicyRef, FilterPreviewRequest,
        FilterTestRequest, GetFilter, GetFilterBinding, GetFilterOperation, ListFilterBindings,
        ListFilterRevisions, ListFilters, ResolveFilterPolicy, WatchFilterCatalog,
    };
    use laser_wire::fork::{ForkCreate, ForkDelete, ForkList, ForkPromote};
    use laser_wire::http::{
        CasCommittedView, ClientMetadataListView, ClientMetadataView, ClientsQuery,
        DecodeRecordBody, DeletedManyView, FilterBindingListQuery, FilterListQuery,
        FilterRevisionListQuery, FilterTestBody, ForkCreateBody, ForkPutBody, GraphEdgeView,
        GraphNeighborsQuery, GraphNodeView, GraphResultView, KvCasQuery, KvPutQuery, KvScanQuery,
        OperationQuery, PathView, ProjectionListQuery, PromotedView, RegisterSchemaBody,
        RemoveBindingBody, SchemaListQuery, SchemaQuery, SessionChangesQuery, SessionEventsQuery,
        SessionLinksQuery, SessionStateQuery, SessionsQuery, StreamQuery,
    };
    use laser_wire::kv::{KvDeleteMany, KvExists, KvExpire};
    use laser_wire::schema::{BinaryValue, UuidValue};

    macro_rules! shapes {
        ($($name:literal => $type:ty),* $(,)?) => {
            vec![$(($name, round_trip::<$type> as Verifier)),*]
        };
    }

    shapes![
        "BinaryValue" => BinaryValue,
        "CasCommittedView" => CasCommittedView,
        "ClientMetadataListView" => ClientMetadataListView,
        "ClientMetadataView" => ClientMetadataView,
        "ClientsQuery" => ClientsQuery,
        "DecodeRecordBody" => DecodeRecordBody,
        "DefineRoleReq" => DefineRoleReq,
        "DeleteRoleReq" => DeleteRoleReq,
        "DeletedManyView" => DeletedManyView,
        "FilterBindingListQuery" => FilterBindingListQuery,
        "FilterCatalogVersion" => FilterCatalogVersion,
        "FilterHeader" => FilterHeader,
        "FilterListQuery" => FilterListQuery,
        "FilterPolicyRef" => FilterPolicyRef,
        "FilterPreviewRequest" => FilterPreviewRequest,
        "FilterRevisionListQuery" => FilterRevisionListQuery,
        "FilterTestBody" => FilterTestBody,
        "FilterTestRequest" => FilterTestRequest,
        "ForkCreate" => ForkCreate,
        "ForkCreateBody" => ForkCreateBody,
        "ForkDelete" => ForkDelete,
        "ForkList" => ForkList,
        "ForkPromote" => ForkPromote,
        "ForkPutBody" => ForkPutBody,
        "GetBindingsReq" => GetBindingsReq,
        "GetFilter" => GetFilter,
        "GetFilterBinding" => GetFilterBinding,
        "GetFilterOperation" => GetFilterOperation,
        "GetRoleReq" => GetRoleReq,
        "GetSchema" => GetSchema,
        "HeaderScalar" => HeaderScalar,
        "ListSchemas" => ListSchemas,
        "MaterializationReadiness" => MaterializationReadiness,
        "SessionPauseRequest" => SessionPauseRequest,
        "GraphEdgeView" => GraphEdgeView,
        "GraphNeighborsQuery" => GraphNeighborsQuery,
        "GraphNodeView" => GraphNodeView,
        "GraphResultView" => GraphResultView,
        "KvCasQuery" => KvCasQuery,
        "KvDeleteMany" => KvDeleteMany,
        "KvExists" => KvExists,
        "KvExpire" => KvExpire,
        "KvPutQuery" => KvPutQuery,
        "KvScanQuery" => KvScanQuery,
        "ListFilterBindings" => ListFilterBindings,
        "ListFilterRevisions" => ListFilterRevisions,
        "ListFilters" => ListFilters,
        "ListRolesReq" => ListRolesReq,
        "OperationQuery" => OperationQuery,
        "PathView" => PathView,
        "ProjectionListQuery" => ProjectionListQuery,
        "PromotedView" => PromotedView,
        "RegisterSchemaBody" => RegisterSchemaBody,
        "RemoveBindingBody" => RemoveBindingBody,
        "ResolveFilterPolicy" => ResolveFilterPolicy,
        "RoleBinding" => RoleBinding,
        "SchemaListQuery" => SchemaListQuery,
        "SchemaQuery" => SchemaQuery,
        "SessionChangesQuery" => SessionChangesQuery,
        "SessionEventsQuery" => SessionEventsQuery,
        "SessionLinksQuery" => SessionLinksQuery,
        "SessionStateQuery" => SessionStateQuery,
        "SessionsQuery" => SessionsQuery,
        "StreamQuery" => StreamQuery,
        "UuidValue" => UuidValue,
        "WatchFilterCatalog" => WatchFilterCatalog,
        "WhoamiReq" => WhoamiReq,
    ]
}

fn vector_directory() -> Option<PathBuf> {
    std::env::var_os("LASER_GO_VECTORS").map(PathBuf::from)
}

#[test]
fn given_the_go_ports_vectors_when_decoded_then_should_re_encode_byte_identically() {
    let Some(directory) = vector_directory() else {
        eprintln!("set LASER_GO_VECTORS to run the Go differential check");
        return;
    };
    assert!(
        directory.is_dir(),
        "the vector directory {} does not exist, run the Go exporter first",
        directory.display()
    );

    let mut failures = Vec::new();
    let mut checked = 0_usize;
    for (name, verify) in verifiers() {
        let path = directory.join(format!("{name}.bin"));
        let Ok(payload) = std::fs::read(&path) else {
            failures.push(format!("{name}: the Go port exported no vector"));
            continue;
        };
        checked += 1;
        match verify(&payload) {
            Ok(again) if again == payload => {}
            Ok(again) => failures.push(format!(
                "{name}: this crate re-encodes {} bytes where Go wrote {}\n     go: {}\n   rust: {}",
                again.len(),
                payload.len(),
                hex(&payload),
                hex(&again)
            )),
            Err(error) => failures.push(format!("{name}: {error}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {checked} shapes differ between this crate and the Go port:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
    assert!(checked > 0, "no vectors were checked");
    eprintln!("{checked} shapes agree byte for byte with the Go port");
}

/// Every vector the Go port writes must be verified here, so a shape added
/// there is checked rather than quietly ignored.
#[test]
fn given_every_exported_vector_when_checked_then_should_have_a_verifier() {
    let Some(directory) = vector_directory() else {
        return;
    };
    let verified: Vec<&str> = verifiers().into_iter().map(|(name, _)| name).collect();
    let mut unverified = Vec::new();
    for entry in std::fs::read_dir(&directory).expect("read the vector directory") {
        let path = entry.expect("read a vector").path();
        if let Some(name) = shape_name(&path)
            && !verified.contains(&name.as_str())
            && !NOT_CONTRACT_SHAPES.contains(&name.as_str())
        {
            unverified.push(name);
        }
    }
    unverified.sort();
    assert!(
        unverified.is_empty(),
        "{} exported shapes have no verifier here: {}",
        unverified.len(),
        unverified.join(" ")
    );
}

/// The exported shapes that are not contract types: a borrowed evaluator
/// input, and the pair a rendered attribute rides as, which this crate writes
/// as a tuple and so has no named type for.
const NOT_CONTRACT_SHAPES: &[&str] = &["FilterRecord", "NamedText"];

fn shape_name(path: &Path) -> Option<String> {
    (path.extension()? == "bin").then(|| path.file_stem()?.to_str().map(str::to_owned))?
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}
