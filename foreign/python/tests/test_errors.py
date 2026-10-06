import ast
import asyncio
import pathlib

import laser_sdk as ls
import pytest

STUB = pathlib.Path(__file__).resolve().parents[1] / "laser_sdk.pyi"


def raised_by(error):
    def embed(text):
        raise error

    return embed


def with_fields(error, **fields):
    for name, value in fields.items():
        setattr(error, name, value)
    return error


@pytest.mark.parametrize(
    ("name", "parent"),
    [
        ("HandlerError", "LaserError"),
        ("HandlerConfigError", "ConfigError"),
        ("RejectedError", "LaserError"),
        ("AmbiguousMutationError", "LaserError"),
        ("StateStoreError", "LaserError"),
        ("AgentError", "LaserError"),
        ("CheckpointError", "LaserError"),
        ("FilterFaultError", "FilterError"),
        ("FilterOversizedRecordError", "FilterError"),
        ("ConsumerGroupSetupError", "FilterError"),
        ("IdError", "InvalidError"),
        ("PublishFailedError", "LaserError"),
        ("IntegrityError", "LaserError"),
        ("NoCapableAgentError", "RoutingError"),
        ("NoInboxError", "RoutingError"),
        ("RoutePrincipalMismatchError", "RoutingError"),
        ("RoutingError", "LaserError"),
        ("PresenceConflictError", "LaserError"),
        ("FenceViolationError", "LaserError"),
        ("QuarantinedError", "LaserError"),
        ("ProvenanceError", "LaserError"),
    ],
)
def test_given_a_rust_error_variant_when_importing_its_class_then_should_sit_under_its_parent(
    name, parent
):
    assert getattr(ls, parent) in getattr(ls, name).__bases__


def test_given_a_rejection_when_classified_then_should_not_be_a_validation_error():
    assert not issubclass(ls.RejectedError, ls.InvalidError)
    assert issubclass(ls.IdError, ValueError)


@pytest.mark.parametrize(
    ("error", "code", "retryable", "flag"),
    [
        (ls.LaserError("x"), "Backend", False, None),
        (ls.InvalidError("x"), "InvalidArgument", False, None),
        (ls.ConfigError("x"), "InvalidArgument", False, None),
        (ls.NoStreamError("x"), "Unsupported", False, None),
        (ls.TimeoutError("x"), "Backend", True, None),
        (ls.HandlerError("x"), "Backend", True, None),
        (ls.PolicyDeferredError("x"), "Backend", True, None),
        (ls.SignatureError("x"), "Unauthenticated", False, None),
        (ls.StepUpRequiredError("x"), "StepUpRequired", False, None),
        (ls.UnsupportedError("x"), "Unsupported", False, "unsupported"),
        (ls.AmbiguousMutationError("x"), "Backend", False, "ambiguous_mutation"),
        (ls.FenceViolationError("x"), "Conflict", False, "fence_violation"),
        (ls.BudgetExceededError("x"), "Backend", False, "budget_exceeded"),
        (ls.QuarantinedError("x"), "Forbidden", False, "quarantined"),
        (ls.NoCapableAgentError("x"), "NotFound", True, "no_capable_agent"),
        (ls.RoutePrincipalMismatchError("x"), "Forbidden", False, "permission_denied"),
        (ls.FilterOversizedRecordError("x"), "TooLarge", False, None),
    ],
)
def test_given_an_error_raised_by_application_code_when_read_then_should_carry_class_defaults(
    error, code, retryable, flag
):
    assert error.code == code
    assert error.retryable is retryable
    flags = ("unavailable", "not_found", "stale", "permission_denied", "not_leader")
    for name in flags + ("unsupported", "fence_violation", "quarantined"):
        assert getattr(error, name) is (name == flag)


def test_given_a_binding_validation_failure_when_raised_then_should_carry_code_and_retryable():
    with pytest.raises(ls.InvalidError) as failure:
        ls.Filter.pred("cpu", "almost", 90)
    assert failure.value.code == "InvalidArgument"
    assert failure.value.retryable is False


@pytest.mark.parametrize(
    ("raised", "expected", "fields"),
    [
        (
            with_fields(ls.BudgetExceededError("over"), ceiling=10, spent=12),
            ls.BudgetExceededError,
            {"ceiling": 10, "spent": 12, "budget_exceeded": True, "retryable": False},
        ),
        (
            with_fields(ls.FenceViolationError("lost"), held=3, current=5),
            ls.FenceViolationError,
            {"held": 3, "current": 5, "fence_violation": True, "retryable": False},
        ),
        (
            with_fields(ls.QuarantinedError("q"), agent="planner"),
            ls.QuarantinedError,
            {"agent": "planner", "quarantined": True, "retryable": False},
        ),
        (
            with_fields(ls.NoCapableAgentError("none"), skill="triage"),
            ls.NoCapableAgentError,
            {"skill": "triage", "no_capable_agent": True, "retryable": True},
        ),
        (
            with_fields(ls.StepUpRequiredError("approve"), scope="storage:rotate"),
            ls.StepUpRequiredError,
            {"scope": "storage:rotate", "code": "StepUpRequired"},
        ),
        (
            with_fields(ls.KvError("lost"), detail="LeaseLost"),
            ls.KvError,
            {"detail": "LeaseLost", "lease_lost": True},
        ),
        (ls.SignatureError("forged"), ls.SignatureError, {"retryable": False}),
        (ls.RejectedError("no"), ls.RejectedError, {"retryable": False}),
        (ls.PolicyBlockedError("no"), ls.PolicyBlockedError, {"retryable": False}),
        (asyncio.CancelledError("stop"), ls.CancelledError, {"retryable": False}),
        (TimeoutError("slow"), ls.TimeoutError, {"retryable": True}),
        (RuntimeError("boom"), ls.HandlerError, {"retryable": True}),
    ],
)
async def test_given_a_callback_error_when_it_crosses_rust_then_should_keep_its_class_and_fields(
    raised, expected, fields
):
    memory = ls.MemoryHandle.vector(raised_by(raised))
    with pytest.raises(ls.LaserError) as failure:
        await memory.remember("auth is slow")
    assert type(failure.value) is expected
    for name, value in fields.items():
        assert getattr(failure.value, name) == value


async def test_given_a_refused_memory_operation_when_raised_then_should_name_the_surface():
    memory = ls.MemoryHandle.vector(lambda text: [1.0])
    with pytest.raises(ls.UnsupportedError) as failure:
        await memory.set("plan", b"rotate")
    assert isinstance(failure.value.surface, str) and failure.value.surface
    assert failure.value.feature is None or isinstance(failure.value.feature, str)


def test_given_an_invalid_identifier_when_parsed_then_should_raise_the_id_error():
    with pytest.raises(ls.IdError) as failure:
        ls.Provenance(agent="bad\x00id")
    assert isinstance(failure.value, ls.InvalidError)
    assert failure.value.kind == ls.IdError.INVALID_CHAR == "invalid_char"
    assert failure.value.filter_reason is None


def stub_exceptions():
    tree = ast.parse(STUB.read_text())
    return {
        node.name: [ast.unparse(base) for base in node.bases]
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name.endswith("Error")
        and any(ast.unparse(base) != "typing.Any" for base in node.bases)
    }


def runtime_exceptions():
    def spelled(base):
        if base.__module__ == "laser_sdk":
            return base.__name__
        if base.__module__.startswith("asyncio"):
            return f"asyncio.{base.__name__}"
        return f"{base.__module__}.{base.__name__}"

    return {
        name: [spelled(base) for base in value.__bases__]
        for name, value in vars(ls).items()
        if isinstance(value, type) and issubclass(value, ls.LaserError)
    }


def test_given_the_generated_stub_when_read_then_should_list_every_exception_class_and_base():
    assert stub_exceptions() == runtime_exceptions()
