import json
import pathlib

import laser_sdk as ls
import pytest

CORPUS = json.loads(
    (
        pathlib.Path(__file__).resolve().parents[3]
        / "wire"
        / "fixtures"
        / "error_classification_cases.json"
    ).read_text()
)
SURFACES = {
    "query": ls.QueryError,
    "kv": ls.KvError,
    "fork": ls.ForkError,
    "graph": ls.GraphError,
    "session": ls.SessionError,
    "authz": ls.AuthzError,
    "filter": ls.FilterError,
    "command": ls.CommandError,
}
# A failure application code can raise is classified by its class alone, the
# same answer the SDK gives the variant it stands for.
LOCAL = {
    "no_stream": lambda: ls.NoStreamError("no default stream set"),
    "no_respond_topic": lambda: ls.NoRespondTopicError("no respond topic"),
    "config": lambda: ls.ConfigError("refused"),
    "handler_config": lambda: ls.HandlerConfigError("refused"),
    "handler": lambda: ls.HandlerError("refused"),
    "rejected": lambda: ls.RejectedError("refused"),
    "timeout": lambda: ls.TimeoutError("a reply"),
    "ambiguous_mutation": lambda: ls.AmbiguousMutationError("refused"),
    "unsupported": lambda: ls.UnsupportedError("refused"),
    "invalid": lambda: ls.InvalidError("refused"),
    "codec": lambda: ls.CodecError("refused"),
    "protocol": lambda: ls.ProtocolError("refused"),
    "state_store": lambda: ls.StateStoreError("refused"),
    "integrity": lambda: with_fields(ls.IntegrityError("refused"), reference="blob://refused"),
    "signature": lambda: ls.SignatureError("refused"),
    "policy_blocked": lambda: ls.PolicyBlockedError("refused"),
    "step_up_required": lambda: with_fields(
        ls.StepUpRequiredError("refused"), scope="storage:rotate"
    ),
    "policy_deferred": lambda: ls.PolicyDeferredError("refused"),
    "no_capable_agent": lambda: with_fields(ls.NoCapableAgentError("refused"), skill="triage"),
    "no_inbox": lambda: with_fields(ls.NoInboxError("refused"), agent="planner"),
    "presence_conflict": lambda: with_fields(
        ls.PresenceConflictError("refused"), advertised="planner", requested="writer"
    ),
    "route_principal_mismatch": lambda: with_fields(
        ls.RoutePrincipalMismatchError("refused"), agent="planner", expected=7, actual=8
    ),
    "fence_violation": lambda: with_fields(ls.FenceViolationError("refused"), held=3, current=5),
    "budget_exceeded": lambda: with_fields(ls.BudgetExceededError("refused"), ceiling=10, spent=12),
    "cancelled": lambda: with_fields(ls.CancelledError("refused"), run="run-1"),
    "quarantined": lambda: with_fields(ls.QuarantinedError("refused"), agent="planner"),
    "filter_fault": lambda: with_fields(
        ls.FilterFaultError("refused"), reason="malformed", partition_id=1, offset=7
    ),
    "filter_oversized_record": lambda: with_fields(
        ls.FilterOversizedRecordError("refused"), partition_id=1, offset=7
    ),
    "id": lambda: ls.IdError("identifier must not be empty"),
    "provenance": lambda: ls.ProvenanceError("missing required header"),
}
HAND_RAISED = [case for case in CORPUS["cases"] if case.get("hand_raised", True)]
CLASS_DEFAULTS = [
    case for case in HAND_RAISED if "local" in case["failure"] and case["failure"]["local"] in LOCAL
]


@pytest.mark.parametrize("case", HAND_RAISED, ids=[case["name"] for case in HAND_RAISED])
async def test_given_a_corpus_failure_when_it_crosses_rust_then_should_match_the_corpus(
    case,
):
    memory = ls.MemoryHandle.vector(raised_by(failure_of(case["failure"])))
    with pytest.raises(ls.LaserError) as failure:
        await memory.remember("value")
    assert classification(failure.value) == expected(case)


@pytest.mark.parametrize("case", CLASS_DEFAULTS, ids=[case["name"] for case in CLASS_DEFAULTS])
def test_given_a_hand_raised_corpus_failure_when_read_then_should_match_the_corpus(
    case,
):
    assert classification(LOCAL[case["failure"]["local"]]()) == expected(case)


def classification(error):
    return {"code": error.code} | {name: getattr(error, name) for name in CORPUS["classifiers"]}


def expected(case):
    return {"code": case["code"]} | {name: name in case["flags"] for name in CORPUS["classifiers"]}


def failure_of(failure):
    ((kind, detail),) = failure.items()
    if kind in SURFACES:
        return with_fields(SURFACES[kind]("refused"), detail=detail)
    if kind == "transport":
        return with_fields(ls.TransportError("server refused"), iggy_error_code=detail)
    if kind == "local" and detail == "link_lost":
        return with_fields(ls.TransportError("link lost"), iggy_error_code=8)
    if kind == "local":
        return LOCAL[detail]()
    raise AssertionError(f"the corpus names a failure application code cannot raise: {kind}")


def raised_by(error):
    def embed(text):
        raise error

    return embed


def with_fields(error, **fields):
    for name, value in fields.items():
        setattr(error, name, value)
    return error
