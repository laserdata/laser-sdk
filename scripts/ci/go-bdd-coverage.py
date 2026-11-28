"""Record Go BDD outcomes and verify that every scenario passed on a deployment."""

import argparse
import json
import sys
from pathlib import Path


def record(path):
    outcomes = {}
    for line in sys.stdin:
        event = json.loads(line)
        action = event.get("Action")
        name = event.get("Test", "")
        if action in {"output", "build-output"}:
            text = event.get("Output", "")
            if not text.startswith(("=== RUN", "--- PASS", "    --- PASS", "--- SKIP", "    --- SKIP")):
                sys.stdout.write(text)
        if name.startswith("TestGivenTheShared") and "/" in name and action in {"pass", "skip", "fail"}:
            outcomes[name] = action
    if not outcomes:
        raise SystemExit("Go BDD produced no scenario outcomes")
    path.write_text(json.dumps(outcomes, indent=2, sort_keys=True) + "\n")
    print(f"{path.stem}: {sum(value == 'pass' for value in outcomes.values())}/{len(outcomes)} scenarios passed")


def check(paths):
    reports = [json.loads(path.read_text()) for path in paths]
    names = set().union(*reports)
    if not names:
        raise SystemExit("Go BDD coverage has no scenarios")
    problems = []
    for name in sorted(names):
        results = [report.get(name) for report in reports]
        if None in results or "fail" in results or "pass" not in results:
            problems.append(f"{name}: " + ", ".join(f"{p.stem}={r}" for p, r in zip(paths, results)))
    if problems:
        raise SystemExit("\n".join(problems))
    print(f"All {len(names)} Go BDD scenarios passed across {len(reports)} deployment profiles")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    modes.add_parser("record").add_argument("path", type=Path)
    modes.add_parser("check").add_argument("paths", nargs="+", type=Path)
    args = parser.parse_args()
    if args.mode == "record":
        record(args.path)
    else:
        check(args.paths)
