"""Reference query and key-value engines: the executable specification of the
query and compare-and-swap semantics every LaserData SDK and the managed backend
must reproduce. Pure and transport-free (no Iggy, no client), so the shared
Gherkin pins one cross-language contract. The Rust counterparts live in the Rust
BDD crate. This Python port must return the same answers for the same inputs."""

from __future__ import annotations

from dataclasses import dataclass, field

# A full page when a query sets no limit. Not exercised by the scenarios (they
# set explicit limits), kept so the paging path matches the Rust engine.
MAX_PAGE_SIZE = 1000


def _as_number(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _compare(field_value, op, value):
    """Compare a row's string field against a bound. Numeric when the bound is a
    number, otherwise lexical, matching the Rust reference engine."""
    if op == "contains":
        return str(value) in field_value
    if op == "prefix":
        return field_value.startswith(str(value))
    if op == "in":
        return any(field_value == str(item) for item in value)

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        left, right = _as_number(field_value), float(value)
        order = None if left is None else (left > right) - (left < right)
    else:
        text = str(value)
        order = (field_value > text) - (field_value < text)

    if order is None:
        return False
    return {
        "eq": order == 0,
        "ne": order != 0,
        "lt": order < 0,
        "lte": order <= 0,
        "gt": order > 0,
        "gte": order >= 0,
    }[op]


@dataclass
class QueryEngine:
    """An in-memory materialized index. Rows are dicts of indexed string fields."""

    indexes: dict = field(default_factory=dict)

    def insert(self, index, row):
        self.indexes.setdefault(index, []).append(dict(row))

    def execute(self, index, *, predicate=None, order=None, limit=0, offset=0, aggregate=None):
        rows = self.indexes.get(index)
        if rows is None:
            return {"rows": [], "total": 0, "limit": limit, "offset": offset, "has_more": False}

        matched = [row for row in rows if self._matches(row, predicate)]
        if aggregate is not None:
            return self._aggregate(matched, aggregate)

        for sort_field, direction in reversed(order or []):
            matched.sort(
                key=lambda row: self._sort_key(row, sort_field), reverse=direction == "desc"
            )

        total = len(matched)
        page_limit = MAX_PAGE_SIZE if limit == 0 else limit
        page = matched[offset : offset + page_limit]
        return {
            "rows": page,
            "total": total,
            "limit": page_limit,
            "offset": offset,
            "has_more": offset + len(page) < total,
        }

    @staticmethod
    def _matches(row, predicate):
        if predicate is None:
            return True
        field_name, op, value = predicate
        if field_name not in row:
            return False
        return _compare(row[field_name], op, value)

    @staticmethod
    def _sort_key(row, field_name):
        # Numeric when the field parses as a number, else lexical, so ordering is
        # "30" < "550" < "900", not the lexical "10" < "30" < "550" < "900".
        value = row.get(field_name, "")
        number = _as_number(value)
        return (0, number, "") if number is not None else (1, 0.0, value)

    @staticmethod
    def _aggregate(matched, aggregate):
        group_by, func, alias = aggregate
        groups: dict = {}
        for row in matched:
            key = tuple(row.get(name, "") for name in group_by)
            groups.setdefault(key, []).append(row)
        rows = []
        for key, members in sorted(groups.items()):
            headers = dict(zip(group_by, key, strict=True))
            headers[alias] = _aggregate_value(members, func)
            rows.append(headers)
        total = len(rows)
        return {
            "rows": rows,
            "total": total,
            "limit": max(total, 1),
            "offset": 0,
            "has_more": False,
        }


def _aggregate_value(members, func):
    if func == "count":
        return str(len(members))
    raise ValueError(f"unsupported aggregate function '{func}'")


@dataclass
class _Stored:
    value: bytes
    version: int
    expires_at: int | None


@dataclass
class CasOutcome:
    """A compare-and-swap result: a committed version, or a conflict carrying the
    live version (or `None` when the key is absent or expired)."""

    committed: int | None = None
    conflict_current: int | None = None
    is_conflict: bool = False


@dataclass
class KvEngine:
    """An in-memory namespace. Each key carries a version that increases by one on
    every successful mutation, resetting to 1 over an absent or expired key.
    Expiry reads as absence. Time is an explicit argument, so expiry is
    deterministic with no wall clock."""

    entries: dict = field(default_factory=dict)

    def _live_version(self, key, now):
        stored = self.entries.get(key)
        if stored is None or (stored.expires_at is not None and now >= stored.expires_at):
            return None
        return stored.version

    def set(self, key, value, expires_at, now):
        version = (self._live_version(key, now) or 0) + 1
        self.entries[key] = _Stored(value=value, version=version, expires_at=expires_at)
        return version

    def cas(self, key, value, expect, expires_at, now):
        live = self._live_version(key, now)
        if expect == "absent":
            committable = live is None
        else:
            committable = live == expect
        if not committable:
            return CasOutcome(is_conflict=True, conflict_current=live)
        return CasOutcome(committed=self.set(key, value, expires_at, now))
