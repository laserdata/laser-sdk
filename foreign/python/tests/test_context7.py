import json
import unittest
from pathlib import Path
from urllib.parse import urlparse

CONFIG = Path(__file__).parents[3] / "context7.json"
# The limits of https://context7.com/schema/context7.json, which refuses any other key.
TEXT = {
    "projectTitle": (1, 100),
    "description": (10, 500),
    "branch": (1, 100),
    "redirect": (1, 500),
    "url": (1, 500),
    "public_key": (1, 100),
}
LISTS = {
    "folders": (50, 255, None),
    "excludeFolders": (50, 255, None),
    "excludeFiles": (100, 255, r"[^/\\]+"),
    "includeExtensions": (10, 17, r"\.[a-z0-9]{1,16}"),
    "rules": (50, 255, None),
}
KEYS = {"$schema", "disallow", "previousVersions", *TEXT, *LISTS}


class Context7ConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = json.loads(CONFIG.read_text())

    def test_given_the_config_when_read_then_should_use_only_schema_keys(self) -> None:
        self.assertLessEqual(set(self.config), KEYS)
        self.assertEqual(urlparse(self.config["$schema"]).scheme, "https")
        self.assertNotIsInstance(self.config.get("disallow", False), str)

    def test_given_the_claim_fields_when_read_then_should_come_as_a_pair(self) -> None:
        self.assertEqual("url" in self.config, "public_key" in self.config)
        if "url" in self.config:
            self.assertEqual(urlparse(self.config["url"]).scheme, "https")

    def test_given_text_fields_when_measured_then_should_fit_the_schema(self) -> None:
        for key, (shortest, longest) in TEXT.items():
            if key in self.config:
                with self.subTest(key=key):
                    self.assertIsInstance(self.config[key], str)
                    self.assertTrue(shortest <= len(self.config[key]) <= longest)

    def test_given_list_fields_when_measured_then_should_fit_the_schema(self) -> None:
        for key, (most, longest, pattern) in LISTS.items():
            items = self.config.get(key, [])
            with self.subTest(key=key):
                self.assertLessEqual(len(items), most)
                if key != "rules":
                    self.assertEqual(len(items), len(set(items)))
                for item in items:
                    self.assertIsInstance(item, str)
                    self.assertTrue(
                        1 <= len(item) <= longest, f"{key} item of {len(item)} characters: {item}"
                    )
                    if pattern is not None:
                        self.assertRegex(item, f"^{pattern}$")

    def test_given_the_rust_setup_rule_when_read_then_should_name_serde_with_derive(self) -> None:
        setup = [rule for rule in self.config.get("rules", []) if rule.startswith("Rust adds")]
        self.assertEqual(len(setup), 1)
        self.assertIn("serde with derive", setup[0])

    def test_given_previous_versions_when_read_then_should_name_a_tag_or_a_branch(self) -> None:
        versions = self.config.get("previousVersions", [])
        self.assertLessEqual(len(versions), 20)
        for version in versions:
            if isinstance(version, str):
                self.assertTrue(1 <= len(version) <= 50)
            else:
                self.assertLessEqual(set(version), {"tag", "branch"})
                self.assertEqual(len(version), 1)
                self.assertTrue(all(1 <= len(value) <= 50 for value in version.values()))


if __name__ == "__main__":
    unittest.main()
