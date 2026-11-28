import contextlib
import importlib.util
import pathlib
import tempfile
import textwrap
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("parity", pathlib.Path(__file__).with_name("check-parity.py"))
parity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(parity)

TABLES = ("PY_PEERS", "TS_PEERS", "PY_ERROR_CLASSES", "TS_ERROR_CLASSES", "PY_ONLY", "TS_ONLY")


@contextlib.contextmanager
def workspace(rust="", wire="", stub="", report="", capabilities=(), retired=(), omissions=None, raw=None, **tables):
    """A throwaway repository with one Rust crate, a stub, and an API report, the mapping tables replaced, and raw clients with the given operations."""
    with tempfile.TemporaryDirectory() as directory, contextlib.ExitStack() as stack:
        root = pathlib.Path(directory)
        files = {"sdk/src/lib.rs": rust, "wire/src/lib.rs": wire, "foreign/python/laser_sdk.pyi": stub, "foreign/typescript/api/laser-sdk.api.md": report}
        for name, text in files.items():
            if text or name.startswith(("sdk", "foreign")):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(textwrap.dedent(text))
        (root / "scripts").mkdir()
        stack.enter_context(patch.object(parity, "ROOT", root))
        for table in TABLES:
            stack.enter_context(patch.dict(getattr(parity, table), tables.get(table.lower(), {}), clear=True))
        stack.enter_context(patch.dict(parity.OMISSION_CAPABILITIES, omissions or {}, clear=True))
        stack.enter_context(patch.dict(parity.CAPABILITY_SIGNOFFS, {}, clear=True))
        stack.enter_context(patch.object(parity, "CAPABILITIES", tuple(capabilities)))
        # The surface rows name real Go declarations, which a throwaway repository lacks.
        stack.enter_context(patch.object(parity, "GO_SURFACES", ()))
        # The Go table is keyed by capability, so a fake capability list needs a
        # fake Go standing to match it: the two are one thing stated twice.
        stack.enter_context(patch.dict(
            parity.GO_CAPABILITIES,
            {key: parity.MISSING for key, *_ in capabilities},
            clear=True,
        ))
        stack.enter_context(patch.object(parity, "RETIRED_RAW_CALLS", tuple(retired)))
        clients = {label: {"source": "fixture", "version": "1", "operations": sorted((raw or {}).get(label, ()))} for label in ("Rust", "TypeScript")}
        stack.enter_context(patch.object(parity, "raw_clients", lambda: (clients, [])))
        yield root


def rows():
    return {row.display: row for row in parity.rust_surface()}


def matrix():
    return parity.compare(parity.rust_surface(), parity.python_surface(), parity.typescript_surface())


def problems():
    return matrix().problems


def cells(display):
    found = {row.display: row for row in matrix().rows}[display]
    return found.python, found.typescript


class TypeScriptInheritanceTests(unittest.TestCase):
    def test_given_a_generic_base_when_extracting_then_should_preserve_inherited_members(self):
        surface = parity.TsSurface()
        parity.parse_report('export abstract class WireId<Tag extends string> {\n    asU128(): bigint;\n}\nexport class RecordId extends WireId<"RecordId"> {\n    static fromU128(value: bigint): RecordId;\n}\n', surface)
        lookup = parity.Lookup("TypeScript", surface, {}, {}, set(), {})
        self.assertIn("asU128", lookup.members("RecordId"))

    def test_given_a_wire_namespace_alias_when_extracting_then_should_resolve_the_aliased_declaration(self):
        surface = parity.TsSurface()
        parity.parse_report('interface WireQueryCapabilities {\n    readonly cancellation: boolean;\n}\ndeclare namespace wire {\n    export { WireQueryCapabilities as QueryCapabilities }\n}\n', surface)
        self.assertIn("cancellation", surface.decls["wire.QueryCapabilities"].members)

    def test_given_an_unexported_base_when_extracting_then_should_preserve_inherited_members(self):
        surface = parity.TsSurface()
        parity.parse_report('declare abstract class WireId<Tag extends string> {\n    asU128(): bigint;\n}\nexport class RecordId extends WireId<"RecordId"> {\n    static fromU128(value: bigint): RecordId;\n}\n', surface)
        lookup = parity.Lookup("TypeScript", surface, {}, {}, set(), {})
        self.assertIn("asU128", lookup.members("RecordId"))
        self.assertNotIn("WireId", surface.decls)

    def test_given_a_combined_peer_spelling_when_resolving_then_should_claim_every_part_and_fail_on_a_missing_one(self):
        surface = parity.TsSurface()
        parity.parse_report("export interface CreateOptions {\n    readonly filter?: string;\n    readonly filterName?: string;\n}\n", surface)
        lookup = parity.Lookup("TypeScript", surface, {}, {}, set(), {})
        found, accepted, _, _ = lookup.resolve("CreateOptions.filter + CreateOptions.filterName")
        self.assertTrue(found and accepted)
        self.assertTrue({"CreateOptions.filter", "CreateOptions.filterName"} <= lookup.claims)
        self.assertFalse(lookup.resolve("CreateOptions.filter + CreateOptions.missing")[0])

    def test_given_an_interface_and_companion_const_in_two_reports_when_extracting_then_should_keep_both_member_sets(self):
        surface = parity.TsSurface()
        report = 'export interface Filter {\n    readonly expr: string;\n}\nexport const Filter: {\n    readonly json: (expr: string) => Filter;\n};\n'
        parity.parse_report(report, surface)
        parity.parse_report(report, surface)
        self.assertEqual({"expr", "json"}, set(surface.decls["Filter"].members))


class RustExtractionTests(unittest.TestCase):
    def test_given_a_struct_with_public_and_private_fields_when_extracting_then_should_row_only_the_public_ones(self):
        with workspace(rust="""
            pub struct Record {
                pub key: String,
                #[doc(hidden)]
                pub internal: u8,
                secret: u8,
            }
            struct Hidden { pub field: u8 }
        """):
            found = rows()
            self.assertIn("Record.key", found)
            self.assertNotIn("Record.internal", found)
            self.assertNotIn("Record.secret", found)
            self.assertNotIn("Hidden", found)

    def test_given_a_laser_error_with_struct_variants_when_extracting_then_should_row_variants_and_payload_fields(self):
        with workspace(rust="""
            pub enum LaserError {
                Timeout(&'static str),
                FenceViolation { stale: u64, current: u64 },
            }
        """):
            found = rows()
            self.assertEqual(found["LaserError::Timeout"].kind, "variant")
            self.assertEqual(found["LaserError.FenceViolation.stale"].kind, "variant-field")
            self.assertIn("LaserError.FenceViolation.current", found)

    def test_given_items_re_exported_from_the_wire_crate_when_extracting_then_should_row_them_at_the_re_export(self):
        with workspace(
            rust="""
                pub mod filters {
                    pub use laser_wire::filter::{CompiledFilter, MAX_FILTER_BYTES};
                }
            """,
            wire="""
                pub mod filter {
                    pub const MAX_FILTER_BYTES: usize = 4096;
                    pub struct CompiledFilter { pub digest: [u8; 32] }
                    impl CompiledFilter {
                        pub fn compile() -> Self { todo!() }
                    }
                    pub struct NotExported;
                }
            """,
        ):
            found = rows()
            self.assertEqual(found["CompiledFilter"].section, "filters")
            self.assertIn("CompiledFilter.digest", found)
            self.assertIn("CompiledFilter::compile", found)
            self.assertEqual(found["filters::MAX_FILTER_BYTES"].kind, "const")
            self.assertNotIn("NotExported", found)

    def test_given_consts_statics_aliases_and_associated_consts_when_extracting_then_should_row_each(self):
        with workspace(rust="""
            pub const DEFAULT_LINGER: Duration = Duration::from_millis(1);
            pub static CONTROL_TOPIC: &str = "control";
            pub type Headers = HashMap<String, String>;
            pub struct Capabilities;
            impl Capabilities {
                pub const OPEN: Self = Self;
            }
        """):
            found = rows()
            self.assertEqual(found["DEFAULT_LINGER"].kind, "const")
            self.assertIn("Duration", found["DEFAULT_LINGER"].value_type)
            self.assertEqual(found["CONTROL_TOPIC"].kind, "static")
            self.assertEqual(found["Headers"].kind, "type")
            self.assertEqual(found["Capabilities::OPEN"].kind, "assoc-const")

    def test_given_a_bon_derived_builder_when_extracting_then_should_row_setters_maybe_setters_and_the_finish(self):
        with workspace(rust="""
            #[derive(bon::Builder)]
            pub struct ContextAssembler {
                conversation_id: u128,
                #[builder(default)]
                from_offsets: Vec<u64>,
                #[builder(field)]
                internal_buffer: Vec<u8>,
            }
        """):
            found = rows()
            self.assertEqual(found["ContextAssembler::builder"].start_of, "ContextAssemblerBuilder")
            self.assertEqual(found["ContextAssemblerBuilder::conversation_id"].kind, "setter")
            self.assertEqual(found["ContextAssemblerBuilder::maybe_from_offsets"].setter_of, "from_offsets")
            self.assertTrue(found["ContextAssemblerBuilder::build"].finish)
            self.assertNotIn("ContextAssemblerBuilder::internal_buffer", found)
            self.assertNotIn("ContextAssemblerBuilder::maybe_conversation_id", found)

    def test_given_a_bon_function_builder_on_new_when_extracting_then_should_row_the_builder_and_no_phantom_new(self):
        with workspace(rust="""
            pub struct Intent { pub body: Vec<u8> }
            #[bon::bon]
            impl Intent {
                #[builder]
                pub fn new(body: Vec<u8>, #[builder(default)] mandatory_voters: Vec<u8>) -> Result<Self, IntentError> {
                    todo!()
                }
            }
        """):
            found = rows()
            self.assertNotIn("Intent::new", found)
            self.assertIn("Intent::builder", found)
            self.assertIn("IntentBuilder::body", found)
            self.assertIn("IntentBuilder::maybe_mandatory_voters", found)

    def test_given_a_type_only_reachable_as_a_return_value_when_extracting_then_should_row_it(self):
        with workspace(rust="""
            mod client {
                pub struct KvCasFencedRequest;
                impl KvCasFencedRequest {
                    pub fn commit(self) {}
                }
            }
            pub use client::Kv;
            mod client2 {}
            pub struct Kv;
            impl Kv {
                pub fn cas_fenced(&self) -> crate::client::KvCasFencedRequest { todo!() }
            }
        """):
            self.assertIn("KvCasFencedRequest::commit", rows())

    def test_given_public_traits_when_extracting_then_should_row_generated_variants_and_skip_hidden_methods(self):
        with workspace(rust="""
            #[trait_variant::make(Memory: Send)]
            pub trait LocalMemory {
                async fn remember(&self);
                #[doc(hidden)]
                fn internal(&self);
            }
            impl LocalMemory for Store {
                fn private_method(&self) {}
            }
            #[cfg(test)]
            mod tests {
                pub fn helper() {}
            }
        """):
            found = rows()
            self.assertIn("LocalMemory::remember", found)
            self.assertIn("Memory::remember", found)
            self.assertEqual(found["LocalMemory"].variant_of, "Memory")
            self.assertNotIn("LocalMemory::internal", found)
            self.assertNotIn("tests::helper", found)


class PeerExtractionTests(unittest.TestCase):
    def test_given_a_python_stub_when_extracting_then_should_read_classes_constructors_functions_and_constants(self):
        with workspace(stub="""
            DEFAULT_LINGER_MS: int
            class Topic:
                def __new__(cls, name: str) -> Topic: ...
                @property
                def name(self) -> str: ...
                def producer(self, *, linger_ms: int = 1) -> Producer: ...
            def decode_snapshot(data: bytes) -> dict: ...
        """):
            surface = parity.python_surface()
            self.assertEqual(surface.classes["Topic"].members["name"]["kind"], "property")
            self.assertEqual(surface.classes["Topic"].members["producer"]["params"][0]["name"], "linger_ms")
            self.assertEqual(surface.classes["Topic"].constructor[0]["name"], "name")
            self.assertIn("decode_snapshot", surface.functions)
            self.assertIn("DEFAULT_LINGER_MS", surface.constants)

    def test_given_an_api_report_when_extracting_then_should_keep_public_members_aliases_and_interface_bases(self):
        with workspace(report="""
            class Record_2 {
                contentType(value: number): this;
                private secret;
                protected hidden(): void;
            }
            export { Record_2 as Record }
            export interface ConsumedMessage {
                readonly payload: Uint8Array;
            }
            export interface ConsumerMessage extends ConsumedMessage {
                readonly position: string;
            }
            export const ActionKind: {
                readonly Send: "send";
            };
            export type ActionKind = (typeof ActionKind)[keyof typeof ActionKind];
            export function filterAll(filters: readonly Filter[]): Filter;
        """):
            surface = parity.typescript_surface()
            lookup = parity.Lookup("TypeScript", surface, {}, {}, set(), {})
            self.assertEqual(set(surface.decls["Record"].members), {"contentType"})
            self.assertNotIn("Record_2", surface.decls)
            self.assertIn("payload", lookup.members("ConsumerMessage"))
            self.assertIn("keyof typeof", surface.decls["ActionKind"].text)
            self.assertEqual(surface.functions["filterAll"][0]["name"], "filters")


class ComparisonTests(unittest.TestCase):
    RUST = """
        pub struct Topic;
        impl Topic {
            pub fn send(&self, payload: Vec<u8>, timeout: Duration) {}
        }
    """

    def test_given_matching_peers_when_comparing_then_should_report_no_problem(self):
        with workspace(
            rust=self.RUST,
            stub="""
                class Topic:
                    def send(self, payload: bytes, timeout_ms: int) -> None: ...
            """,
            report="""
                export class Topic {
                    private constructor();
                    send(data: Uint8Array, ms: number): void;
                }
            """,
        ):
            self.assertEqual(problems(), [])

    def test_given_variant_payload_getters_when_comparing_then_should_claim_them(self):
        with workspace(
            rust="""
                pub enum Verdict {
                    Allow,
                    StepUp { scope: String },
                }
            """,
            stub="""
                class Verdict:
                    Allow: Verdict
                    StepUp: Verdict
                    @property
                    def scope(self) -> str: ...
            """,
            report="""
                export class Verdict {
                    private constructor();
                    static readonly Allow: Verdict;
                    static readonly StepUp: Verdict;
                    readonly scope: string;
                }
            """,
        ):
            self.assertEqual(problems(), [])

    def test_given_complex_enum_variants_as_nested_python_classes_when_comparing_then_should_resolve_them(self):
        with workspace(
            rust="""
                pub enum Contract {
                    Completed(String),
                    TimedOut,
                }
            """,
            stub="""
                class Contract:
                    class Completed(Contract):
                        @property
                        def _0(self) -> str: ...
                    class TimedOut(Contract): ...
            """,
            report="""
                export type Contract = { readonly kind: "completed"; readonly reply: string } | { readonly kind: "timedOut" };
            """,
        ):
            self.assertEqual([problem for problem in problems() if "Python" in problem], [])

    def test_given_a_field_and_a_method_of_one_name_when_the_method_is_renamed_then_should_keep_the_field_name(self):
        with workspace(
            rust="""
                pub struct KeyRecord {
                    pub revoked: bool,
                }
                impl KeyRecord {
                    pub fn revoked(self) -> Self { self }
                }
            """,
            stub="""
                class KeyRecord:
                    revoked: bool
                    def revoke(self) -> KeyRecord: ...
            """,
            report="""
                export class KeyRecord {
                    private constructor();
                    readonly revoked: boolean;
                    revoke(): KeyRecord;
                }
            """,
            py_peers={("KeyRecord", "revoked"): ("KeyRecord.revoke", "name-collision")},
            ts_peers={("KeyRecord", "revoked"): ("KeyRecord.revoke", "name-collision")},
        ):
            self.assertEqual(problems(), [])

    def test_given_a_python_keyword_that_differs_from_rust_when_comparing_then_should_report_the_parameters(self):
        with workspace(
            rust=self.RUST,
            stub="""
                class Topic:
                    def send(self, value: bytes, timeout_ms: int) -> None: ...
            """,
            report="""
                export class Topic {
                    private constructor();
                    send(payload: Uint8Array, timeoutMs: number): void;
                }
            """,
        ):
            self.assertEqual(problems(), ["Topic::send: Python `Topic.send` takes (value, timeout_ms), Rust takes (payload, timeout) (sdk/src/lib.rs:4)"])

    def test_given_a_typescript_method_with_too_few_parameters_when_comparing_then_should_report_it(self):
        with workspace(
            rust=self.RUST,
            stub="""
                class Topic:
                    def send(self, payload: bytes, timeout_ms: int) -> None: ...
            """,
            report="""
                export class Topic {
                    private constructor();
                    send(payload: Uint8Array): void;
                }
            """,
        ):
            self.assertEqual(problems(), ["Topic::send: TypeScript `Topic.send` takes (payload), Rust takes (payload, timeout) (sdk/src/lib.rs:4)"])

    def test_given_a_struct_unpacked_into_keywords_when_comparing_then_should_accept_its_field_names(self):
        with workspace(
            rust="""
                pub struct MemoryScope { pub agent: String, pub user: String }
                pub struct Memory;
                impl Memory {
                    pub fn forget(&self, scope: MemoryScope) {}
                }
            """,
            stub="""
                class Memory:
                    def forget(self, *, agent: str = None, user: str = None) -> None: ...
                class MemoryScope:
                    agent: str
                    user: str
            """,
            report="""
                export interface MemoryScope {
                    readonly agent: string;
                    readonly user: string;
                }
                export class Memory {
                    private constructor();
                    forget(scope: MemoryScope): void;
                }
            """,
        ):
            self.assertEqual(problems(), [])

    def test_given_a_missing_member_when_comparing_then_should_report_it_and_fold_members_of_a_missing_type(self):
        with workspace(
            rust="""
                pub struct Topic { pub name: String }
                pub struct Kv { pub namespace: String }
            """,
            stub="""
                class Topic:
                    pass
            """,
            report="""
                export interface Topic {
                    readonly name: string;
                }
                export interface Kv {
                    readonly namespace: string;
                }
            """,
        ):
            self.assertEqual(problems(), [
                "Kv: Python MISSING `Kv` and 1 member(s) (sdk/src/lib.rs:3)",
                "Topic.name: Python MISSING `Topic.name` (sdk/src/lib.rs:2)",
            ])

    def test_given_a_hint_entry_when_the_peer_lacks_the_rust_name_then_should_report_where_it_lives_today(self):
        with workspace(
            rust="""
                pub struct Laser;
                impl Laser {
                    pub fn dlq_topic(&self) -> String { todo!() }
                }
            """,
            stub="""
                class Laser:
                    dlq_topic: str
            """,
            report="""
                export class Laser {
                    private constructor();
                    readonly deadLetterTopic: string;
                }
            """,
            ts_peers={("Laser", "dlq_topic"): ("~Laser.deadLetterTopic", None)},
        ):
            self.assertEqual(problems(), ["Laser::dlq_topic: TypeScript MISSING, today `Laser.deadLetterTopic` (sdk/src/lib.rs:4)"])

    def test_given_a_hint_entry_when_the_peer_adopts_the_rust_name_then_should_pass_without_a_table_edit(self):
        with workspace(
            rust="""
                pub struct Laser;
                impl Laser {
                    pub fn dlq_topic(&self) -> String { todo!() }
                }
            """,
            stub="""
                class Laser:
                    dlq_topic: str
            """,
            report="""
                export class Laser {
                    private constructor();
                    readonly dlqTopic: string;
                }
            """,
            ts_peers={("Laser", "dlq_topic"): ("~Laser.deadLetterTopic", None)},
        ):
            self.assertEqual(problems(), [])

    def test_given_an_omission_when_the_reason_does_not_allow_one_then_should_report_it(self):
        with workspace(
            rust="""
                pub struct Topic;
                impl Topic {
                    pub fn iggy_producer(&self) {}
                    pub fn flush(&self) {}
                }
            """,
            stub="""
                class Topic:
                    pass
            """,
            report="""
                export class Topic {
                    private constructor();
                    flush(): void;
                }
            """,
            py_peers={("Topic", "iggy_producer"): ("-", "rust-crate"), ("Topic", "flush"): ("-", "keywords")},
            ts_peers={("Topic", "iggy_producer"): ("-", "rust-crate")},
            capabilities=[("message.send", "Send messages", (), (), "Topic", "Topic", "Topic")],
            omissions={("Topic", "iggy_producer"): ("message.send",)},
        ):
            self.assertEqual(problems(), ["Topic::flush: Python omitted without an omission reason (keywords) (sdk/src/lib.rs:5)"])

    def test_given_an_override_for_one_type_when_comparing_then_should_not_bleed_into_another_type(self):
        with workspace(
            rust="""
                pub struct Producer;
                impl Producer { pub fn send(&self) {} }
                pub struct Consumer;
                impl Consumer { pub fn send(&self) {} }
            """,
            stub="""
                class Producer:
                    def publish(self) -> None: ...
                class Consumer:
                    pass
            """,
            report="""
                export class Producer {
                    private constructor();
                    send(): void;
                }
                export class Consumer {
                    private constructor();
                    send(): void;
                }
            """,
            py_peers={("Producer", "send"): ("Producer.publish", "one-call")},
        ):
            self.assertEqual(problems(), ["Consumer::send: Python MISSING `Consumer.send` (sdk/src/lib.rs:5)"])

    def test_given_a_rust_constructor_when_the_peer_class_has_no_public_one_then_should_report_it(self):
        with workspace(
            rust="""
                pub struct Window;
                impl Window {
                    pub fn new(size: usize) -> Self { todo!() }
                }
            """,
            stub="""
                class Window:
                    def __init__(self, size: int) -> None: ...
            """,
            report="""
                export class Window {
                    private constructor();
                }
            """,
        ):
            self.assertEqual(problems(), ["Window::new: TypeScript MISSING `new Window()` (sdk/src/lib.rs:4)"])

    def test_given_a_builder_setter_when_python_takes_keywords_then_should_check_each_keyword_name(self):
        with workspace(
            rust="""
                pub struct ProducerBuilder;
                impl ProducerBuilder {
                    pub fn linger(self, linger: Duration) -> Self { self }
                    pub fn retries(self, retries: u32) -> Self { self }
                    pub fn build(self) {}
                }
            """,
            stub="""
                class Topic:
                    def producer(self, *, linger_ms: int = 1) -> None: ...
            """,
            report="""
                export interface ProducerOptions {
                    readonly lingerMs?: number;
                    readonly retries?: number;
                }
                export class Topic {
                    private constructor();
                    producer(options?: ProducerOptions): void;
                }
            """,
            py_peers={("ProducerBuilder", None): ("Topic.producer", "keywords")},
            ts_peers={("ProducerBuilder", None): ("ProducerOptions", "keywords"), ("ProducerBuilder", "build"): ("Topic.producer", "one-call")},
        ):
            self.assertEqual(problems(), ["ProducerBuilder::retries: Python MISSING `Topic.producer(retries=)`, a keyword or option is absent (sdk/src/lib.rs:5)"])
            python, typescript = cells("ProducerBuilder::build")
            self.assertEqual((python.spelling, python.reason), ("Topic.producer", "one-call"))
            self.assertEqual(typescript.spelling, "Topic.producer")

    def test_given_a_capability_setter_when_mapped_to_a_nested_field_then_should_check_that_field(self):
        with workspace(
            rust="""
                pub struct KvCaps { pub cas: bool }
                pub struct Capabilities { pub kv: KvCaps }
                impl Capabilities {
                    pub fn with_kv_cas(mut self, value: bool) -> Self { self }
                }
            """,
            stub="""
                class KvCaps:
                    cas: bool
                class Capabilities:
                    kv: KvCaps
                class Laser:
                    def with_capabilities(self, *, kv_cas: bool = None) -> None: ...
            """,
            report="""
                export interface KvCapabilities {
                    readonly available: boolean;
                }
                export interface Capabilities {
                    readonly kv: KvCapabilities;
                }
            """,
            py_peers={("Capabilities", "with_kv_cas"): ("Laser.with_capabilities(kv_cas=)", "keywords")},
            ts_peers={("KvCaps", None): ("~KvCapabilities", None), ("Capabilities", "with_kv_cas"): ("{KvCaps}.cas", "keywords")},
        ):
            found = problems()
            self.assertIn("Capabilities::with_kv_cas: TypeScript MISSING `KvCapabilities.cas` (sdk/src/lib.rs:5)", found)
            self.assertNotIn("Python", " ".join(found))

    def test_given_a_free_function_for_a_class_method_when_comparing_then_should_report_a_gap(self):
        with workspace(
            rust="""
                pub struct LaserError;
                impl LaserError { pub fn is_retryable(&self) -> bool { true } }
                pub struct EdgeDenial;
                impl EdgeDenial { pub fn code(&self) -> u8 { 0 } }
            """,
            stub="""
                class LaserError:
                    def is_retryable(self) -> bool: ...
                class EdgeDenial:
                    def code(self) -> int: ...
            """,
            report="""
                export class LaserError { private constructor(); }
                export function isRetryable(error: LaserError): boolean;
                export type EdgeDenial = { readonly kind: "unauthenticated"; };
                export function edgeDenialCode(denial: EdgeDenial): number;
            """,
            ts_peers={("LaserError", "is_retryable"): ("fn:isRetryable", "free-function")},
        ):
            self.assertEqual(problems(), ["LaserError::is_retryable: TypeScript MISSING, free function `fn:isRetryable` where the peer type is a class (sdk/src/lib.rs:3)"])

    def test_given_a_serde_record_handed_out_as_a_python_dict_when_comparing_then_should_check_the_derive_api_and_keys(self):
        rust = """
            #[derive(Serialize)]
            pub struct Summary {
                pub name: String,
                #[serde(skip)]
                pub cache: u64,
            }
            pub struct Plain {
                pub name: String,
            }
            pub struct Group;
            impl Group {
                pub fn summary(&self) -> Summary { Summary }
                pub fn plain(&self) -> Plain { Plain }
            }
        """
        stub = """
            class Group:
                def summary(self) -> dict: ...
                def plain(self) -> dict: ...
        """
        report = """
            export class Group {
                private constructor();
                summary(): Summary;
                plain(): Plain;
            }
            export interface Summary { readonly name: string; readonly cache: bigint; }
            export interface Plain { readonly name: string; }
        """
        peers = {("Summary", None): ("dict:Group.summary", "serde-dict"), ("Plain", None): ("dict:Group.plain", "serde-dict")}
        with workspace(rust=rust, stub=stub, report=report, py_peers=peers):
            python, _ = cells("Summary")
            self.assertFalse(python.gap)
            self.assertFalse(cells("Summary.name")[0].gap)
            self.assertEqual([problem for problem in problems() if ": Python " in problem], [
                "Plain: Python MISSING dict via `Group.plain`, the Rust type derives no serde Serialize or Deserialize and 1 member(s) (sdk/src/lib.rs:8)",
                "Summary.cache: Python MISSING dict key, serde writes no key for it (sdk/src/lib.rs:6)",
            ])
        with workspace(rust=rust, stub=stub, report=report, py_peers={("Summary", None): ("dict:Group.absent", "serde-dict")}):
            self.assertTrue(cells("Summary")[0].gap)
        with workspace(rust=rust, stub=stub, report="export class Group { private constructor(); }", ts_peers={("Summary", None): ("dict:Group.summary", "serde-dict")}):
            self.assertIn("a serde dict is a Python representation", cells("Summary")[1].problem)

    def test_given_an_error_classifier_function_when_comparing_then_should_check_the_function_and_parameters(self):
        with workspace(
            rust="pub enum LaserError { Invalid(String) } impl LaserError { pub fn is_retryable(&self) -> bool { false } }",
            stub="class LaserError: ...\nclass InvalidError(LaserError): ...\nLaserError.retryable: bool",
            report="export class LaserError { private constructor(); }\nexport class InvalidError extends LaserError { constructor(message: string); }\nexport function isRetryable(error: LaserError): boolean;",
            py_peers={("LaserError", "is_retryable"): ("LaserError.retryable", "property")},
            ts_peers={("LaserError", "is_retryable"): ("fn:isRetryable", "error-function")},
        ) as root:
            # Only the TypeScript classifier cell is under test.
            self.assertFalse(cells("LaserError::is_retryable")[1].gap)
            report = root / "foreign/typescript/api/laser-sdk.api.md"
            report.write_text("export class LaserError { private constructor(); }")
            self.assertTrue(cells("LaserError::is_retryable")[1].gap)

    def test_given_laser_error_variants_when_comparing_then_should_check_error_classes_and_payload_fields(self):
        with workspace(
            rust="""
                pub enum LaserError {
                    Query(QueryError),
                    FenceViolation { stale: u64, current: u64 },
                }
                pub struct QueryError;
            """,
            stub="""
                class LaserError(Exception): ...
                class QueryError(LaserError): ...
                class FenceViolationError(LaserError):
                    held: int
                    current: int
            """,
            report="""
                export class LaserError extends Error {
                    protected constructor(message: string);
                }
                export class QueryExecutionError extends LaserError {
                    constructor(message: string, detail: QueryError);
                    readonly detail: QueryError;
                }
                export type QueryError = { readonly kind: "backend"; };
                export class FenceViolationError extends LaserError {
                    constructor(stale: bigint, current: bigint);
                    readonly stale: bigint;
                    readonly current: bigint;
                }
            """,
            ts_error_classes={"Query": "QueryExecutionError"},
        ):
            self.assertEqual(problems(), [
                "LaserError.FenceViolation.stale: Python MISSING `FenceViolationError.stale` (sdk/src/lib.rs:4)",
                "Python only: FenceViolationError.held has no Rust row",
            ])

    def test_given_a_peer_api_with_no_rust_row_when_comparing_then_should_report_it_unless_allowlisted(self):
        with workspace(
            rust="""
                pub struct Consumer;
                impl Consumer { pub fn commit(&self) {} }
            """,
            stub="""
                class Consumer:
                    def commit(self) -> None: ...
                    def init(self) -> None: ...
                    def name(self) -> str: ...
                    def __repr__(self) -> str: ...
            """,
            report="""
                export class Consumer {
                    private constructor();
                    commit(): void;
                    returnDelivery(message: unknown): void;
                    [Symbol.asyncIterator](): AsyncIterator<unknown>;
                }
                export class Stream { constructor(name: string); }
            """,
            py_only={"Consumer.init": "lazy-init"},
        ):
            found = matrix()
            self.assertEqual(found.python_only, ["Consumer.name"])
            self.assertEqual(found.typescript_only, ["Consumer.returnDelivery", "Stream"])
            self.assertIn(("PY", "Consumer.init", "lazy-init"), found.accepted)

    def test_given_a_stale_peers_entry_when_comparing_then_should_report_that_it_names_no_rust_item(self):
        with workspace(
            rust="pub struct Topic;",
            stub="class Topic: ...",
            report="export class Topic { private constructor(); }",
            py_peers={("Topic", "gone"): ("Topic.gone", None)},
        ):
            self.assertIn("PY_PEERS entry ('Topic', 'gone') names no public Rust item", problems())

    def test_given_a_rendered_matrix_when_checking_then_should_fail_on_a_stale_doc_and_pass_after_write(self):
        with workspace(rust="pub struct Topic;", stub="class Topic: ...", report="export class Topic { private constructor(); }") as root:
            (root / "docs").mkdir()
            with patch.object(parity, "DOC", root / "docs" / "parity.md"), patch("sys.argv", ["check-parity.py"]), patch("builtins.print"):
                self.assertEqual(parity.main(), 1)
                with patch("sys.argv", ["check-parity.py", "--write"]):
                    self.assertEqual(parity.main(), 0)
                self.assertEqual(parity.main(), 0)
            self.assertIn("| `Topic` | `Topic` | `Topic` |  |", (root / "docs" / "parity.md").read_text())


class CapabilityTests(unittest.TestCase):
    def test_go_capabilities_require_the_named_public_member(self):
        caps = (("probe", "probe", (), (), "probe", "probe", "probe"),)
        with workspace(capabilities=caps) as root:
            package = root / "foreign/go/laser"
            package.mkdir(parents=True)
            (package / "client.go").write_text("package laser\ntype Probe struct{}\nfunc (p *Probe) Run() {}\n")
            with patch.dict(parity.GO_CAPABILITIES, {"probe": "Probe.Run"}):
                self.assertEqual([], parity.go_capability_problems())
            with patch.dict(parity.GO_CAPABILITIES, {"probe": "Probe.Missing"}):
                self.assertIn("Probe.Missing", parity.go_capability_problems()[0])

    def test_go_surfaces_require_the_named_public_member(self):
        with workspace() as root:
            package = root / "foreign/go/laser"
            package.mkdir(parents=True)
            (package / "client.go").write_text(
                "package laser\ntype Probe struct{}\nfunc (p *Probe) Run() {}\ntype Box[T any] struct{}\nfunc (b *Box[T]) Open() {}\nfunc NewProbe() {}\n"
            )
            helpers = root / "foreign/go/laser/lasertest"
            helpers.mkdir()
            (helpers / "lasertest.go").write_text("package lasertest\nfunc AgentMessage() {}\n")
            with patch.object(parity, "GO_SURFACES", (("Probe", "Probe.Run, Box.Open, laser.NewProbe and lasertest.AgentMessage"),)):
                self.assertEqual([], parity.go_capability_problems())
            with patch.object(parity, "GO_SURFACES", (("Probe", "Probe.Run and Probe.Missing"),)):
                self.assertEqual(
                    ["Go surface `Probe` names missing declaration `Probe.Missing`"], parity.go_capability_problems()
                )

    RUST = """
        pub struct Laser;
        impl Laser {
            pub async fn ping(&self) {}
        }
    """
    STUB = """
        class Laser:
            def ping(self) -> typing.Awaitable[None]: ...
    """
    REPORT = """
        export class Laser {
            private constructor();
            ping(): Promise<void>;
        }
    """
    PING = ("system.ping", "Ping the server", ("ping",), ("system.ping",), "Laser::ping", "Laser.ping", "Laser.ping")
    RAW = {"Rust": ["ping"], "TypeScript": ["system.ping"]}

    def test_given_every_raw_call_served_in_every_client_when_checking_then_should_report_no_problem(self):
        with workspace(rust=self.RUST, stub=self.STUB, report=self.REPORT, capabilities=[self.PING], raw=self.RAW):
            self.assertEqual(problems(), [])

    def test_given_a_removed_capability_api_when_checking_then_should_fail(self):
        report = "export class Laser {\n    private constructor();\n}\n"
        with workspace(rust=self.RUST, stub=self.STUB, report=report, capabilities=[self.PING], raw=self.RAW):
            self.assertIn("capability `system.ping` (Ping the server): TypeScript has no first-class API, `Laser.ping` is absent", problems())

    def test_given_an_unlisted_raw_call_when_checking_then_should_fail(self):
        raw = {"Rust": ["ping", "next_producer_id"], "TypeScript": ["system.ping", "sendBinaryRequest"]}
        with workspace(rust=self.RUST, stub=self.STUB, report=self.REPORT, capabilities=[self.PING], raw=raw):
            found = problems()
        self.assertIn("raw call `next_producer_id` (Rust fixture 1) is neither a capability nor retired", found)
        self.assertIn("raw call `sendBinaryRequest` (TypeScript fixture 1) is neither a capability nor retired", found)

    def test_given_a_raw_call_on_the_retired_list_when_checking_then_should_need_no_api(self):
        raw = {"Rust": ["ping"], "TypeScript": ["system.ping", "stream.purge"]}
        retired = [("stream.purge", "Remove every message of a stream", (), ("stream.purge",), 205)]
        with workspace(rust=self.RUST, stub=self.STUB, report=self.REPORT, capabilities=[self.PING], retired=retired, raw=raw):
            self.assertEqual(problems(), [])

    def test_given_a_raw_call_named_twice_or_absent_from_the_client_when_checking_then_should_fail(self):
        twice = ("system.health", "Check the server", ("ping", "get_health"), (), "Laser::ping", "Laser.ping", "Laser.ping")
        with workspace(rust=self.RUST, stub=self.STUB, report=self.REPORT, capabilities=[self.PING, twice], raw=self.RAW):
            found = problems()
        self.assertIn("raw call `ping` (Rust fixture 1) belongs to 2 rows: `system.ping`, `system.health`", found)
        self.assertIn("`system.health` names raw call `get_health`, which the Rust fixture 1 client does not have", found)

    def test_given_an_omitted_zero_argument_method_that_reaches_the_raw_client_when_checking_then_should_fail(self):
        rust = """
            pub struct Laser;
            impl Laser {
                pub async fn ping(&self) {}
                pub async fn reconnect(&self) -> Result<(), String> {}
                pub fn name(&self) -> &str {}
            }
        """
        omitted = {("Laser", "reconnect"): ("-", "plain-value"), ("Laser", "name"): ("-", "plain-value")}
        with workspace(rust=rust, stub=self.STUB, report=self.REPORT, capabilities=[self.PING], raw=self.RAW, py_peers=omitted, ts_peers=omitted):
            found = problems()
        self.assertIn("Laser::reconnect: Python omits it as `plain-value`, but it reaches the raw client or does more than convert, construct, or read a plain value, so OMISSION_CAPABILITIES must name the capabilities it reaches (sdk/src/lib.rs:5)", found)
        self.assertFalse([problem for problem in found if problem.startswith("Laser::name")])

    def test_given_an_omitted_zero_argument_method_with_its_capabilities_when_checking_then_should_pass(self):
        rust = """
            pub struct Laser;
            impl Laser {
                pub async fn ping(&self) {}
                pub async fn reconnect(&self) -> Result<(), String> {}
            }
        """
        omitted = {("Laser", "reconnect"): ("-", "plain-value")}
        with workspace(rust=rust, stub=self.STUB, report=self.REPORT, capabilities=[self.PING], raw=self.RAW, py_peers=omitted, ts_peers=omitted, omissions={("Laser", "reconnect"): ("system",)}):
            self.assertEqual(problems(), [])


class RawClientDerivationTests(unittest.TestCase):
    def test_given_a_client_trait_with_supertraits_when_deriving_then_should_list_every_method(self):
        with tempfile.TemporaryDirectory() as directory:
            src = pathlib.Path(directory)
            (src / "client.rs").write_text(textwrap.dedent("""
                pub trait Client: StreamClient + Sync + Send {
                    async fn connect(&self) -> Result<(), IggyError>;
                    /// fn not_a_method(&self);
                    async fn abandon_operation(&self, _op: u64) -> Result<(), IggyError> {
                        let inner = |value: u64| { value };
                        Err(IggyError::FeatureUnavailable)
                    }
                }
            """))
            (src / "stream_client.rs").write_text("pub trait StreamClient {\n    async fn get_stream(&self) -> u32;\n}\npub trait Unrelated {\n    fn other(&self);\n}\n")
            self.assertEqual(parity.trait_operations(src, "Client"), ["abandon_operation", "connect", "get_stream"])

    def test_given_client_declarations_when_deriving_then_should_list_commands_of_groups_methods_and_bases(self):
        with tempfile.TemporaryDirectory() as directory:
            dist = pathlib.Path(directory)
            (dist / "command-set.d.ts").write_text(textwrap.dedent("""
                declare const streamAPI: (c: ClientProvider) => {
                    get: (arg: GetStream) => Promise<Stream | null>;
                    ensure: (streamName: string) => Promise<Stream>;
                };
                type StreamAPI = ReturnType<typeof streamAPI>;
                export declare abstract class AbstractAPI {
                    clientProvider: ClientProvider;
                    constructor(getClient: ClientProvider);
                }
                export declare abstract class CommandAPI extends AbstractAPI {
                    stream: StreamAPI;
                    consumer: ({ name, streamId }: ConsumerRequest) => Promise<IggyConsumer>;
                    /** sendRaw(code: number): void; */
                    sendBinaryRequest(code: number, payload: Buffer): Promise<Buffer>;
                    private sendRawCommand;
                }
            """))
            (dist / "client.d.ts").write_text("export declare class SimpleClient extends CommandAPI {\n    constructor(client: RawClient);\n    destroy(): Promise<void>;\n}\n")
            self.assertEqual(parity.client_commands(dist, "SimpleClient"), ["clientProvider", "consumer", "destroy", "sendBinaryRequest", "stream.ensure", "stream.get"])

    def test_given_iggy_client_impls_when_deriving_inherent_methods_then_should_list_only_public_inherent_ones(self):
        with tempfile.TemporaryDirectory() as directory:
            src = pathlib.Path(directory)
            (src / "client.rs").write_text(textwrap.dedent("""
                impl IggyClient {
                    pub fn builder() -> IggyClientBuilder { IggyClientBuilder::new() }
                    /// pub fn not_a_method(&self);
                    pub async fn send_binary_request(&self, code: u32) -> Result<Bytes, IggyError> {
                        let inner = || { "pub fn quoted()" };
                        Ok(Bytes::new())
                    }
                    pub(crate) fn restricted(&self) {}
                    fn private(&self) {}
                }
                impl Client for IggyClient {
                    async fn connect(&self) -> Result<(), IggyError> { Ok(()) }
                }
                impl IggyClientBuilder {
                    pub fn build(self) -> IggyClient { IggyClient }
                }
            """))
            (src / "binary.rs").write_text("impl IggyClient {\n    pub async fn send_http_request(&self) {}\n}\n")
            self.assertEqual(parity.inherent_operations(src, "IggyClient"), ["builder", "send_binary_request", "send_http_request"])

    def test_given_pinned_iggy_crates_when_deriving_rust_operations_then_should_join_trait_and_inherent_methods(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as cargo, patch.object(parity, "ROOT", pathlib.Path(directory)), patch.dict(parity.os.environ, {"CARGO_HOME": cargo}):
            (pathlib.Path(directory) / "Cargo.lock").write_text('[[package]]\nname = "iggy"\nversion = "0.12.0"\n\n[[package]]\nname = "iggy_common"\nversion = "0.12.0"\n')
            registry = pathlib.Path(cargo) / "registry" / "src" / "index"
            (registry / "iggy_common-0.12.0" / "src").mkdir(parents=True)
            (registry / "iggy-0.12.0" / "src").mkdir(parents=True)
            (registry / "iggy_common-0.12.0" / "src" / "client.rs").write_text("pub trait Client {\n    async fn ping(&self);\n}\n")
            (registry / "iggy-0.12.0" / "src" / "client.rs").write_text("impl IggyClient {\n    pub async fn send_binary_request(&self) {}\n}\n")
            derived = parity.rust_raw_operations()
        self.assertEqual(derived, ("iggy_common and iggy", "0.12.0", ["ping", "send_binary_request"], None))

    def test_given_a_static_member_when_deriving_client_commands_then_should_leave_it_out(self):
        with tempfile.TemporaryDirectory() as directory:
            dist = pathlib.Path(directory)
            (dist / "client.d.ts").write_text("export declare class SimpleClient {\n    static create(config: string): SimpleClient;\n    get connected(): boolean;\n    destroy(): Promise<void>;\n}\n")
            self.assertEqual(parity.client_commands(dist, "SimpleClient"), ["connected", "destroy"])

    def test_given_a_derived_raw_call_without_a_row_when_checking_then_should_fail(self):
        clients = {"Rust": {"source": "iggy_common and iggy", "version": "0.12.0", "operations": ["ping", "send_http_request"]}, "TypeScript": {"source": "apache-iggy", "version": "0.11.0", "operations": ["system.ping"]}}
        with patch.object(parity, "CAPABILITIES", (("system.ping", "Ping the server", ("ping",), ("system.ping",), "Laser::ping", "Laser.ping", "Laser.ping"),)):
            found = parity.raw_call_problems(clients)
        self.assertEqual(found, ["raw call `send_http_request` (Rust iggy_common and iggy 0.12.0) is neither a capability nor retired"])

    def test_given_a_missing_source_when_the_stored_version_differs_from_the_pin_then_should_fail(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(parity, "ROOT", pathlib.Path(directory)):
            root = pathlib.Path(directory)
            (root / "scripts").mkdir()
            (root / "scripts" / "iggy-client-operations.json").write_text('{"Rust": {"source": "iggy_common", "version": "0.11.0", "operations": ["ping"]}}')
            with patch.object(parity, "rust_raw_operations", lambda: ("iggy_common", "0.12.0", None, None)), patch.object(parity, "typescript_raw_operations", lambda: ("apache-iggy", "0.11.0", ["system.ping"], None)):
                clients, found = parity.raw_clients()
        self.assertEqual(clients["TypeScript"]["operations"], ["system.ping"])
        self.assertEqual(found, ["Rust raw client iggy_common 0.12.0 is not installed and scripts/iggy-client-operations.json holds iggy_common 0.11.0, so install it and run `python3 scripts/check-parity.py --write`"])

    def test_given_a_missing_source_when_the_stored_version_matches_the_pin_then_should_read_the_stored_operations(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(parity, "ROOT", pathlib.Path(directory)):
            root = pathlib.Path(directory)
            (root / "scripts").mkdir()
            (root / "scripts" / "iggy-client-operations.json").write_text('{"Rust": {"source": "iggy_common", "version": "0.12.0", "operations": ["ping"]}}')
            with patch.object(parity, "rust_raw_operations", lambda: ("iggy_common", "0.12.0", None, None)), patch.object(parity, "typescript_raw_operations", lambda: ("apache-iggy", "0.11.0", ["system.ping"], None)):
                clients, found = parity.raw_clients()
        self.assertEqual((clients["Rust"]["operations"], found), (["ping"], []))


class ReturnKindTests(unittest.TestCase):
    RUST = """
        pub struct Workflow;
        impl Workflow {
            pub fn budget(self, budget: u32) -> Self { todo!() }
            pub async fn run(self) -> Result<u32, LaserError> { todo!() }
        }
    """

    def test_given_a_peer_method_that_returns_nothing_where_rust_chains_when_compared_then_should_report_it(self):
        with workspace(
            rust=self.RUST,
            stub="""
                import typing
                class Workflow:
                    def budget(self, budget: int) -> None: ...
                    def run(self) -> None: ...
            """,
        ):
            found = problems()
            self.assertTrue(any("Workflow::budget: Python `Workflow.budget` returns nothing where Rust returns itself to chain" in problem for problem in found), found)
            self.assertTrue(any("Workflow::run: Python `Workflow.run` returns nothing where Rust returns an awaitable" in problem for problem in found), found)

    def test_given_peer_methods_that_chain_and_await_like_rust_when_compared_then_should_report_no_return_kind(self):
        with workspace(
            rust=self.RUST,
            stub="""
                import typing
                class Workflow:
                    def budget(self, budget: int) -> Workflow: ...
                    def run(self) -> typing.Awaitable[int]: ...
            """,
            report="""
                export class Workflow {
                    budget(budget: number): this;
                    run(): Promise<number>;
                }
            """,
        ):
            self.assertFalse([problem for problem in problems() if "where Rust returns" in problem])


if __name__ == "__main__":
    unittest.main()
