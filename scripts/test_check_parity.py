import importlib.util
import pathlib
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("parity", pathlib.Path(__file__).with_name("check-parity.py"))
parity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(parity)


class ParityExtractionTests(unittest.TestCase):
    def test_given_multiline_inherent_impls_when_extracting_then_should_keep_the_public_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "sdk" / "src" / "lib.rs"
            path.parent.mkdir(parents=True)
            path.write_text("""pub struct Agent<H> { handler: H }
impl<H: Iterator<Item = Vec<u8>>> Agent<H>
where
    H: Send + Sync,
{
    pub fn spawn(self) {}
}
impl SomeTrait for Agent<()> {
    fn private_trait_method(&self) {}
}
""")
            with patch.object(parity, "ROOT", root):
                self.assertEqual(parity.rust_surface()["Agent"], {"spawn"})

    def test_given_public_free_functions_when_extracting_then_should_keep_module_owners_and_skip_hidden_helpers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "sdk" / "src" / "memory.rs"
            path.parent.mkdir(parents=True)
            path.write_text("""pub fn fuse_reciprocal_rank() {}
pub async fn resolve_body() {}
#[doc(hidden)]
pub fn internal() {}
fn private() {}
""")
            with patch.object(parity, "ROOT", root):
                self.assertEqual(parity.rust_surface()["memory"], {"fuse_reciprocal_rank", "resolve_body"})
                self.assertIn("memory", parity.public_rust_types())

    def test_given_public_traits_when_extracting_then_should_cover_required_default_and_generated_methods(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "sdk" / "src" / "lib.rs"
            path.parent.mkdir(parents=True)
            path.write_text("""#[trait_variant::make(Memory: Send)]
pub trait LocalMemory {
    async fn remember(&self);
    fn append(&self) -> impl Future<Output = ()> {
        async {}
    }
    #[doc(hidden)]
    fn internal(&self);
}
pub trait Policy {
    fn select(&self);
}
impl Policy for Internal {
    fn select(&self) {}
}
""")
            with patch.object(parity, "ROOT", root):
                surface = parity.rust_surface()
                self.assertEqual(surface["LocalMemory"], {"remember", "append"})
                self.assertEqual(surface["Memory"], {"remember", "append"})
                self.assertEqual(surface["Policy"], {"select"})
                self.assertNotIn("Internal", surface)
                self.assertTrue({"LocalMemory", "Memory", "Policy"} <= parity.public_rust_types())

    def test_given_a_bon_builder_when_extracting_then_should_cover_private_setters_and_skip_internal_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "sdk" / "src" / "lib.rs"
            path.parent.mkdir(parents=True)
            path.write_text("""#[derive(bon::Builder)]
pub struct ContextAssembler {
    conversation_id: u128,
    #[builder(default)]
    from_offsets: Vec<u64>,
    #[builder(field)]
    internal_buffer: Vec<u8>,
}
""")
            with patch.object(parity, "ROOT", root):
                surface = parity.rust_surface()
                self.assertEqual(surface["ContextAssembler"], {"builder"})
                self.assertEqual(surface["ContextAssemblerBuilder"], {"build", "conversation_id", "from_offsets"})
                self.assertIn("ContextAssemblerBuilder", parity.public_rust_types())

    def test_given_an_exported_typescript_alias_when_extracting_then_should_count_only_public_members(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "foreign" / "typescript" / "api" / "laser-sdk-full.api.md"
            path.parent.mkdir(parents=True)
            path.write_text("""class Record_2 {
    contentType(value: number): this;
    private secret;
    protected hidden(): void;
}
export { Record_2 as Record }
class Internal {
    send(): void;
}
export interface Options {
    readonly timeoutMs?: number;
}
""")
            with patch.object(parity, "ROOT", root):
                surface = parity.typescript_surface()
                self.assertEqual(surface["Record"], {"contentType"})
                self.assertEqual(surface["Options"], {"timeoutMs"})
                self.assertNotIn("Record_2", surface)
                self.assertNotIn("Internal", surface)


if __name__ == "__main__":
    unittest.main()
