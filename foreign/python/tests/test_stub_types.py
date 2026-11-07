import ast
import pathlib

import laser_sdk as ls
import pytest

PACKAGE = pathlib.Path(__file__).resolve().parents[1]
STUB = PACKAGE / "laser_sdk.pyi"
SOURCES = PACKAGE / "src"
INTEGER_SEQUENCES = {"schema_refs", "partitions", "offsets", "from_offsets"}
POLLING = ("first", "last", "next", "offset", "timestamp")
AUTO_COMMIT = ("disabled", "interval", "polling", "all", "each", "every")
CONSISTENCY = ("eventual", "read_your_writes")
MEMORY_KIND = ("fact", "message", "summary", "entity", "feedback", "procedure")
MEMORY_BACKEND = ("auto", "log", "vector")
FAULT_POLICY = ("stop", "pass", "drop")
GOVERNOR_MODE = ("observe", "enforce")
TEXT_MATCH = ("equals", "prefix", "suffix", "contains", "glob", "regex")


def functions():
    tree = ast.parse(STUB.read_text())
    found = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            found[(None, node.name)] = node
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    found[(node.name, item.name)] = item
    return found


def returns(owner, name):
    return ast.unparse(functions()[(owner, name)].returns)


def parameter(owner, name, argument):
    node = functions()[(owner, name)]
    arguments = node.args.posonlyargs + node.args.args + node.args.kwonlyargs
    return next(item for item in arguments if item.arg == argument)


def literal(words):
    return "typing.Literal[" + ", ".join(repr(word) for word in words) + "]"


def keyword_default(owner, name, argument):
    node = functions()[(owner, name)]
    for item, default in zip(node.args.kwonlyargs, node.args.kw_defaults, strict=True):
        if item.arg == argument:
            return default
    raise KeyError(argument)


@pytest.mark.parametrize(
    ("owner", "name", "expected"),
    [
        ("Laser", "connect", "typing.Awaitable[Laser]"),
        ("Laser", "connect_env", "typing.Awaitable[Laser]"),
        ("Laser", "close", "typing.Awaitable[None]"),
        ("Kv", "get", "typing.Awaitable[typing.Optional[builtins.bytes]]"),
        ("Kv", "delete", "typing.Awaitable[builtins.bool]"),
        ("Consumer", "next_within", "typing.Awaitable[ConsumerMessage]"),
        ("QueryRequest", "fetch_all", "typing.Awaitable[QueryRowSet]"),
        ("QueryRows", "next", "typing.Awaitable[typing.Optional[QueryRow]]"),
        ("QueryRows", "__anext__", "typing.Awaitable[QueryRow]"),
    ],
)
def test_given_a_coroutine_method_when_typed_then_should_await_what_it_resolves_to(
    owner, name, expected
):
    assert returns(owner, name) == expected


def test_given_a_walked_row_when_typed_then_should_read_by_name_and_report_the_frontier():
    assert ast.unparse(parameter("QueryRow", "__getitem__", "key").annotation) == (
        "builtins.str | builtins.int"
    )
    assert returns("QueryRowSet", "__iter__") == "typing.Iterator[QueryRow]"
    assert returns("QueryRowSet", "frontier_moved") == "builtins.bool"
    assert returns("QueryRows", "frontier_moved") == "builtins.bool"
    assert returns("TypedQueryRows", "frontier_moved") == "builtins.bool"


def test_given_the_bindings_when_returning_a_coroutine_then_should_use_the_typed_awaitable():
    untyped = [
        f"{path.name}:{number}"
        for path in sorted(SOURCES.glob("*.rs"))
        if path.name != "async_bridge.rs"
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if "future_into_py(" in line or "future_into_py_returning(" in line
    ]
    assert untyped == []


def test_given_the_generated_stub_when_read_then_should_type_bytes_as_bytes():
    sequences = []
    lists = []
    for (owner, name), node in functions().items():
        arguments = node.args.posonlyargs + node.args.args + node.args.kwonlyargs
        for item in arguments:
            annotation = ast.unparse(item.annotation) if item.annotation is not None else ""
            if "Sequence[builtins.int]" in annotation and item.arg not in INTEGER_SEQUENCES:
                sequences.append(f"{owner}.{name}({item.arg})")
        result = ast.unparse(node.returns) if node.returns is not None else ""
        if "list[builtins.int]" in result and name not in INTEGER_SEQUENCES:
            lists.append(f"{owner}.{name}")
    assert sequences == []
    assert lists == []


@pytest.mark.parametrize(
    ("owner", "name", "argument", "words"),
    [
        ("Topic", "consumer", "polling", POLLING),
        ("Topic", "consumer", "auto_commit", AUTO_COMMIT),
        ("ConsumerGroup", "consumer", "polling", POLLING),
        ("ConsumerGroup", "consumer", "auto_commit", AUTO_COMMIT),
        ("BackgroundConfig", "__new__", "sharding", ("ordered", "balanced")),
        (
            "BackgroundConfig",
            "__new__",
            "failure_mode",
            ("block", "block_with_timeout", "fail_immediately"),
        ),
        ("ConsumerFilter", "json", "fault_policy", FAULT_POLICY),
        ("ConsumerFilter", "headers_only", "fault_policy", FAULT_POLICY),
        ("FilterExpr", "text", "kind", TEXT_MATCH),
        ("FilterExpr", "header_text", "kind", TEXT_MATCH),
        (
            "FilterExpr",
            "pred_as",
            "coerce",
            ("number", "rfc3339", "epoch_seconds", "epoch_millis", "epoch_micros"),
        ),
        ("Laser", "with_governor", "mode", GOVERNOR_MODE),
        ("Laser", "spawn_agent", "governor_mode", GOVERNOR_MODE),
        ("AgentCtx", "fan_out", "gather", ("require_all", "quorum", "best_effort")),
        ("Laser", "memory_with", "backend", MEMORY_BACKEND),
        ("MemoryHandle", "remember", "kind", MEMORY_KIND),
        ("GraphHandle", "neighbors", "dir", ("out", "in", "both")),
        ("Workflow", "step", "on_timeout", ("fail", "reassign")),
        ("KeyRecord", "__new__", "kind", ("agent", "operator")),
        ("Grant", "__new__", "effect", ("allow", "deny")),
        ("ResourcePattern", "__new__", "kind", ("all", "literal", "prefix")),
    ],
)
def test_given_a_word_vocabulary_when_typed_then_should_be_a_literal(owner, name, argument, words):
    assert ast.unparse(parameter(owner, name, argument).annotation) == literal(words)


@pytest.mark.parametrize(
    ("build", "words"),
    [
        (
            lambda word: ls.ConsumerFilter.json(ls.FilterExpr.pred("/cpu", "eq", 1), word),
            FAULT_POLICY,
        ),
        (lambda word: ls.KeyRecord("planner", bytes(32), kind=word), ("agent", "operator")),
        (lambda word: ls.ResourcePattern(word, "orders"), ("all", "literal", "prefix")),
        (lambda word: ls.BackgroundConfig(sharding=word), ("ordered", "balanced")),
    ],
)
def test_given_a_literal_vocabulary_when_used_at_runtime_then_should_accept_exactly_its_words(
    build, words
):
    for word in words:
        build(word)
    with pytest.raises(ValueError):
        build("unlisted")


def test_given_a_topic_consumer_when_typed_then_should_require_the_partition():
    assert keyword_default("Topic", "consumer", "partition") is None
