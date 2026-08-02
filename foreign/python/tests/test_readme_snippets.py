import ast
import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).parents[3]
README_PATHS = (
    REPOSITORY_ROOT / "README.md",
    REPOSITORY_ROOT / "foreign/python/README.md",
)


def python_blocks(path: Path) -> list[str]:
    blocks: list[str] = []
    current: list[str] | None = None
    for line in path.read_text().splitlines():
        if line == "```python":
            current = []
        elif line == "```" and current is not None:
            blocks.append("\n".join(current))
            current = None
        elif current is not None:
            current.append(line)
    assert current is None, f"unclosed Python fence in {path}"
    return blocks


class ReadmeSnippetTests(unittest.TestCase):
    def test_python_blocks_compile(self) -> None:
        for path in README_PATHS:
            for index, block in enumerate(python_blocks(path), start=1):
                with self.subTest(path=path, block=index):
                    compile(
                        block,
                        f"{path}:python-block-{index}",
                        "exec",
                        flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
                    )


if __name__ == "__main__":
    unittest.main()
