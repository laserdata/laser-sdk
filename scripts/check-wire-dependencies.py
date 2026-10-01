#!/usr/bin/env python3
"""Check the published wire crate without native workspace feature unification."""

import json
from pathlib import Path
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
FEATURES = "cbor,codecs,fixtures,builders,http-client"


def main() -> None:
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"], cwd=ROOT,
    ))
    wire = next(package for package in metadata["packages"] if package["name"] == "laser-wire")
    subprocess.run(
        ["cargo", "package", "-p", "laser-wire", "--locked", "--allow-dirty", "--no-verify"],
        cwd=ROOT, check=True,
    )
    name = f"laser-wire-{wire['version']}"
    archive_path = Path(metadata["target_directory"]) / "package" / f"{name}.crate"
    # Cargo metadata unifies optional evaluator features from native workspace
    # consumers. The archive has the exact published manifest and its own graph.
    with tempfile.TemporaryDirectory(prefix="laser-wire-dependencies-") as directory:
        with tarfile.open(archive_path) as archive:
            archive.extractall(directory, filter="data")
        subprocess.run([
            "cargo", "deny", "--manifest-path", str(Path(directory) / name / "Cargo.toml"),
            "--target", "wasm32-unknown-unknown", "--no-default-features",
            "--features", FEATURES, "check", "--config", str(ROOT / "deny-wire.toml"), "bans",
        ], cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
