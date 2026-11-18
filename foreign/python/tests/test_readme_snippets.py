import ast
import asyncio
import os
import sys
import unittest
from pathlib import Path

try:
    import pytest
except ModuleNotFoundError:
    # `just python-docs` compiles the blocks with the bare interpreter, which has no pytest.
    pytest = None

REPOSITORY_ROOT = Path(__file__).parents[3]
README_PATHS = (
    REPOSITORY_ROOT / "README.md",
    REPOSITORY_ROOT / "foreign/python/README.md",
)
PROGRAM_MARKER = "asyncio.run("
PROGRAM_TIMEOUT_SECONDS = 60
EXAMPLES = REPOSITORY_ROOT / "examples"
STANDALONE_PROGRAMS = {
    "```python\n": (EXAMPLES / "python/workflow.py", EXAMPLES / "python/memory_correction.py"),
    "```rust,no_run\n": (
        EXAMPLES / "rust/src/workflow/main.rs",
        EXAMPLES / "rust/src/memory-correction/main.rs",
    ),
    "```ts\n": (
        EXAMPLES / "typescript/src/workflow/main.ts",
        EXAMPLES / "typescript/src/memory-correction/main.ts",
    ),
}
STANDALONE_READMES = {
    REPOSITORY_ROOT / "README.md": ("```python\n", "```rust,no_run\n", "```ts\n"),
    REPOSITORY_ROOT / "foreign/python/README.md": ("```python\n",),
    REPOSITORY_ROOT / "sdk/README.md": ("```rust,no_run\n",),
    REPOSITORY_ROOT / "foreign/typescript/README.md": ("```ts\n",),
}


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


def programs() -> list[tuple[str, str]]:
    return [
        (f"{path.relative_to(REPOSITORY_ROOT)}:python-block-{index}", block)
        for path in README_PATHS
        for index, block in enumerate(python_blocks(path), start=1)
        if PROGRAM_MARKER in block
    ]


class ReadmeSnippetTests(unittest.TestCase):
    def test_given_the_readmes_when_read_then_should_show_the_standalone_programs_verbatim(
        self,
    ) -> None:
        for readme, fences in STANDALONE_READMES.items():
            text = readme.read_text()
            for fence in fences:
                for program in STANDALONE_PROGRAMS[fence]:
                    with self.subTest(readme=readme.name, program=program.name):
                        self.assertIn(fence + program.read_text() + "```\n", text)

    def test_given_the_workflow_and_memory_docstrings_when_read_then_should_show_the_standalone_programs(  # noqa: E501
        self,
    ) -> None:
        stub = (REPOSITORY_ROOT / "foreign/python/laser_sdk.pyi").read_text()
        for program in STANDALONE_PROGRAMS["```python\n"]:
            lines = [line for line in program.read_text().splitlines() if not line.startswith("#")]
            body = "\n".join(lines).strip("\n")
            shown = "\n".join(("        " + line).rstrip() for line in body.splitlines())
            self.assertIn(f"        ```python\n{shown}\n        ```", stub, program.name)

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


if pytest is not None:
    import laser_sdk as ls

    def test_given_the_readmes_when_read_then_should_hold_whole_programs():
        assert len(programs()) >= 4

    @pytest.mark.integration
    @pytest.mark.timeout(PROGRAM_TIMEOUT_SECONDS * 2)
    @pytest.mark.parametrize(("name", "program"), programs(), ids=[name for name, _ in programs()])
    async def test_given_a_readme_program_when_run_alone_then_should_exit_zero_and_leave_no_stream(
        iggy_endpoint, tmp_path, name, program
    ):
        before = await stream_names(iggy_endpoint)
        script = tmp_path / "program.py"
        script.write_text(program)
        environment = {key: value for key, value in os.environ.items() if key != "LASER_STREAM"}
        environment["LASER_CONNECTION_STRING"] = iggy_endpoint
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(script),
            cwd=tmp_path,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), PROGRAM_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            pytest.fail(f"{name} ran past {PROGRAM_TIMEOUT_SECONDS} seconds")
        assert process.returncode == 0, f"{name} exited {process.returncode}:\n{stderr.decode()}"
        assert stdout.strip(), f"{name} printed nothing"
        assert await stream_names(iggy_endpoint) - before == set()

    async def stream_names(endpoint: str) -> set[str]:
        async with await ls.Laser.connect(endpoint) as laser:
            return {stream.name for stream in await laser.streams()} - {laser.ops_stream}


if __name__ == "__main__":
    unittest.main()
