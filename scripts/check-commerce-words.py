#!/usr/bin/env python3
"""Fail when a tracked text file uses commerce vocabulary in an example.

Examples, docs, tests, and fixtures use neutral systems vocabulary (telemetry,
hosts, services, incidents). Pass paths to limit the scan, otherwise every file
git knows about is scanned.
"""

import re
import subprocess
import sys
from pathlib import Path

WORDS = [
    r"order[-_]?books?",
    r"orders_v1",
    r"orders_rows",
    r"orders-lakehouse",
    r"orders-history",
    r"order[_.]id",
    r"order\.v1",
    r"order_created",
    r"ORDER_AVRO\w*",
    r"shop\.Order",
    r"struct Order",
    r"class Order",
    r"interface Order",
    r"topic\(\s*\"orders\"\s*\)",
    r"query\(\s*\"orders\"\s*\)",
    r"\"orders",
    r"'orders",
    r"orders\.v\d",
    r"orders\.publish",
    r"order #",
    r"shops?",
    r"checkouts?",
    r"carts?",
    r"payments?",
    r"paid",
    r"invoices?",
    r"billing",
    r"skus?",
    r"reserve inventory",
    r"inventory[- ](?:drift|adjustment)",
    r"pricing",
    r"prices?",
    r"price_cents",
    r"customers?",
    r"customer_id",
    r"refunds?",
    r"purchases?",
    r"purchased",
    r"aisle",
    r"bookings?",
    r"concierge",
    r"trip[-_]planner",
    r"search_flights",
    r"e-?commerce",
    r"commerce",
    r"shipments?",
    r"shipping",
    r"merchants?",
    r"storefront",
    r"subscription_json",
]

PATTERN = re.compile(r"(?<![A-Za-z0-9])(?:" + "|".join(WORDS) + r")(?![A-Za-z0-9])", re.IGNORECASE)

# Type names are matched with their case, so prose such as "in order {" never trips.
TYPED = re.compile(r"\bOrder\s*[{(<]|<Order>|\bOrder\b(?=\s*=)")

# Phrases where a listed word carries a non-commerce meaning. A hit inside one of
# these spans is ignored.
ALLOWED = [
    r"actions/checkout",
    r"git checkout",
    r"sdk checkout",
    r"repo(?:sitory)? checkout",
    r"local checkout",
    r"the checkout",
    r"a checkout",
    r"checkout of",
    r"source checkout",
    r"fresh checkout",
    r"upstream checkout",
    r"billing evidence",
    r"not billing",
    r"for billing",
    r"paid once",
    r"paid up front",
    r"is paid",
    r"cost is paid",
    r"paid for",
    r"price of",
    r"billing facts",
    r"billing or access",
    r"billing, or authorization",
    r"nor billing",
    r"its checkout",
    r"Stack(?:\]\([^)]*\))? checkout",
    r"commerce-check",
    r"check-commerce-words",
    r"^.*commerce vocabulary.*$",
    r"of shipping",
    r"before shipping",
]
ALLOWED_RE = re.compile("|".join(ALLOWED), re.IGNORECASE)

# A camelCase or PascalCase identifier is scanned word by word, so `customerId`
# and `PaymentEvent` are caught like `customer_id`.
CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")

ROOT = Path(__file__).resolve().parent.parent

SKIP_PARTS = {"target", "node_modules", "dist", ".git", ".ruff_cache", ".venv", "__pycache__"}
SKIP_NAMES = {"Cargo.lock", "package-lock.json", "uv.lock", "check-commerce-words.py"}
SKIP_SUFFIXES = {".bin", ".desc", ".png", ".jpg", ".jpeg", ".gif", ".pdf", ".ico", ".woff", ".woff2", ".zip", ".gz", ".hdr"}


def tracked(paths):
    command = ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", *paths]
    output = subprocess.run(command, check=True, capture_output=True, text=True, cwd=ROOT).stdout
    return [ROOT / line for line in output.splitlines() if line]


def scanned(path):
    if path.name in SKIP_NAMES or path.suffix.lower() in SKIP_SUFFIXES:
        return False
    if any(part in SKIP_PARTS for part in path.parts):
        return False
    return path.is_file()


def hits(path):
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return
    for number, line in enumerate(text.splitlines(), start=1):
        allowed = [match.span() for match in ALLOWED_RE.finditer(line)]
        seen = set()
        for match in [*PATTERN.finditer(line), *TYPED.finditer(line)]:
            start, end = match.span()
            if any(low <= start and end <= high for low, high in allowed):
                continue
            seen.add(match.group(0).lower())
            yield number, match.group(0), line.strip()
        if allowed:
            continue
        for match in PATTERN.finditer(CAMEL.sub(" ", line)):
            if match.group(0).lower() not in seen:
                seen.add(match.group(0).lower())
                yield number, match.group(0), line.strip()


def main():
    found = 0
    for path in tracked(sys.argv[1:]):
        if not scanned(path):
            continue
        for number, word, line in hits(path):
            found += 1
            print(f"{path.relative_to(ROOT)}:{number}: {word}: {line[:160]}")
    if found:
        print(f"\n{found} commerce word(s) found. Use neutral systems vocabulary.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
