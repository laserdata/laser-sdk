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


# Agent-memory and knowledge-graph reference engines: the Python port of the Rust
# reference engines, returning the same answers for the same inputs. The content
# hashes mirror the Rust SDK exactly, so a deduped id and a converged node id are
# identical across the two SDKs.

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_U64_MASK = 0xFFFFFFFFFFFFFFFF
_FNV_OFFSET = 0xCBF29CE484222325
_FNV_PRIME = 0x100000001B3


def _fnv1a(salt, data):
    """One salted FNV-1a pass over `data`, the hash the SDK content ids use."""
    value = _FNV_OFFSET
    for byte in bytes([salt]) + data:
        value ^= byte
        value = (value * _FNV_PRIME) & _U64_MASK
    return value


def _crockford(value):
    """A 128-bit value as a 26-character Crockford base32 string, the ULID form
    the SDK renders a memory id as."""
    out = [""] * 26
    for index in range(25, -1, -1):
        out[index] = _CROCKFORD[value & 0x1F]
        value >>= 5
    return "".join(out)


def memory_content_id(agent, kind_code, body):
    """The content-addressed memory id for an owner and body, byte-for-byte the
    Rust `MemoryId::content`. Stream is unset in these scenarios."""
    seed = bytearray()
    seed.append(0)  # empty stream segment
    if agent:
        seed += agent.encode()
    seed.append(0)
    seed.append(kind_code)
    seed += body
    seed = bytes(seed)
    value = (_fnv1a(0x4D, seed) << 64) | _fnv1a(0xC7, seed)
    return _crockford(value)


@dataclass
class MemoryEngine:
    """An in-memory agent-memory store under one durable owner. Items keep arrival
    order, so the most recent are the tail. Mirrors the Rust `MemoryEngine`."""

    agent: str = "agent"
    items: list = field(default_factory=list)
    forgotten: set = field(default_factory=set)
    feedback: dict = field(default_factory=dict)
    counter: int = 0

    def remember(self, body, dedup):
        if dedup:
            mid = memory_content_id(self.agent, 1, body)
            if any(item_id == mid for item_id, _ in self.items):
                return mid
        else:
            self.counter += 1
            salted = body + self.counter.to_bytes(16, "big")
            mid = memory_content_id(self.agent, 1, salted)
        self.items.append((mid, body))
        return mid

    def live_len(self):
        return sum(1 for item_id, _ in self.items if item_id not in self.forgotten)

    def improve(self, target, weight):
        self.feedback[target] = self.feedback.get(target, 0.0) + weight

    def forget(self, mid):
        self.forgotten.add(mid)

    def recall(self, limit):
        live = [item for item in self.items if item[0] not in self.forgotten]
        live.reverse()
        if self.feedback:
            live.sort(key=lambda item: self.feedback.get(item[0], 0.0), reverse=True)
        return [body.decode() for _, body in live[:limit]]


def _node_id(value):
    """A node's content-addressed id, the same salted FNV-1a the Rust graph engine
    uses, so the same entity converges on one node across SDKs."""
    return _fnv1a(0x6E, value.encode())


def _valid_at(valid_from, valid_to, at):
    """Whether an edge's valid-time window contains `at` (half-open
    [valid_from, valid_to)), or always when `at` is None. An open bound is
    unbounded on that side. Mirrors the Rust `Edge::valid_at`."""
    if at is None:
        return True
    return (valid_from is None or at >= valid_from) and (valid_to is None or at < valid_to)


@dataclass
class GraphEngine:
    """An in-memory graph of labelled nodes and typed edges, keyed by node value
    so re-adding a value is the same node. Mirrors the Rust `GraphEngine`."""

    nodes: dict = field(default_factory=dict)
    edges: list = field(default_factory=list)
    # Provenance: a node's source is first-writer (the first record it was seen
    # in), an edge's is last-writer (the most recent record that asserted it).
    node_sources: dict = field(default_factory=dict)
    edge_sources: dict = field(default_factory=dict)

    def upsert_node(self, value, source=None):
        nid = _node_id(value)
        self.nodes.setdefault(nid, value)
        if source is not None:
            self.node_sources.setdefault(nid, source)
        return nid

    def add_edge(self, from_id, edge_type, to_id, valid_from=None, valid_to=None, source=None):
        # An edge carries an optional valid-time window (open-ended when a bound
        # is None), the bitemporal write path.
        self.edges.append((from_id, edge_type, to_id, valid_from, valid_to))
        if source is not None:
            self.edge_sources[(from_id, edge_type, to_id)] = source

    def node_count(self):
        return len(self.nodes)

    def node_source(self, value):
        return self.node_sources.get(_node_id(value))

    def edge_source(self, from_value, edge_type, to_value):
        key = (_node_id(from_value), edge_type, _node_id(to_value))
        return self.edge_sources.get(key)

    def traverse(self, start, hops, as_of=None):
        # `as_of` (epoch micros) follows only edges whose valid-time window
        # contains that instant (half-open [valid_from, valid_to)). None reads
        # the current graph.
        frontier = {_node_id(start)}
        for edge_type, direction in hops:
            nxt = set()
            for from_id, etype, to_id, valid_from, valid_to in self.edges:
                if etype != edge_type or not _valid_at(valid_from, valid_to, as_of):
                    continue
                if direction == "out" and from_id in frontier:
                    nxt.add(to_id)
                if direction == "in" and to_id in frontier:
                    nxt.add(from_id)
            frontier = nxt
        return sorted(self.nodes[i] for i in frontier if i in self.nodes)
