import re
from pathlib import Path

import laser_sdk as ls
import pytest
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "workflow.feature"))
scenarios(str(SCENARIOS / "workflow_local.feature"))


class Flow:
    """The workflow scenarios' state: what each answering agent received, the
    last run's outcome or error, and the local step bodies and compensations
    that ran."""

    def __init__(self):
        self.received = []
        self.outcome = None
        self.error = None
        self.run_id = None
        self.local_runs = []
        self.compensations = []


@pytest.fixture
def flow():
    return Flow()


@given(parsers.re(r'agent "(?P<name>[^"]+)" answers workflow steps on the session topic'))
def answering_agent(world, flow, name):
    spawn(world, flow, name, None, fail=False)


@given(parsers.parse('agent "{name}" fails every workflow step on the session topic'))
def failing_agent(world, flow, name):
    spawn(world, flow, name, None, fail=True)


@given(
    parsers.parse(
        'agent "{name}" advertising the skill "{skill}" answers workflow steps on the session topic'
    )
)
def advertising_agent(world, flow, name, skill):
    spawn(world, flow, name, skill, fail=False)


@when(
    parsers.parse(
        'the workflow "{name}" runs "{first}", then "{second}" after "{second_after}", '
        'then "{third}" after "{third_after}" on "{agent}"'
    )
)
def three_routed_steps(world, flow, name, first, second, second_after, third, third_after, agent):
    workflow = (
        world.laser.workflow(name, fixed_inbox=ls.AgentTopic.Sessions)
        .step(first, to=agent, build=body(first))
        .step(second, to=agent, build=body(second), after=[second_after])
        .step(third, to=agent, build=body(third), after=[third_after])
    )
    run(world, flow, workflow)


@when(
    parsers.parse(
        'the workflow "{name}" runs the step "{label}" on "{agent}" '
        "with a verifier that rejects every reply"
    )
)
def rejected_step(world, flow, name, label, agent):
    workflow = world.laser.workflow(name, fixed_inbox=ls.AgentTopic.Sessions).step(
        label, to=agent, build=body(label), verify=lambda output: False
    )
    run(world, flow, workflow)


@when(
    parsers.parse(
        'the workflow "{name}" runs "{first}" and "{second}" on "{agent}" '
        'and then "{third}" on "{breaker}", '
        "each with a compensation"
    )
)
def compensated_steps(world, flow, name, first, second, agent, third, breaker):
    workflow = (
        world.laser.workflow(name, fixed_inbox=ls.AgentTopic.Sessions)
        .step(first, to=agent, build=body(first), compensate=undo(first))
        .step(second, to=agent, build=body(second), compensate=undo(second))
        .step(third, to=breaker, build=body(third), compensate=undo(third))
    )
    run(world, flow, workflow)


@when(
    parsers.parse(
        'the workflow "{name}" runs the step "{label}" on an agent with the skill "{skill}"'
    )
)
def capable_step(world, flow, name, label, skill):
    workflow = world.laser.workflow(name).step(label, to_capable=skill, build=body(label))
    run(world, flow, workflow)


@when(
    parsers.parse(
        'the workflow "{name}" runs the step "{label}" on "{agent}" '
        "with a deadline of {deadline:d} milliseconds"
    )
)
def silent_step(world, flow, name, label, agent, deadline):
    workflow = world.laser.workflow(name, fixed_inbox=ls.AgentTopic.Sessions).step(
        label, to=agent, build=body(label), deadline_ms=deadline
    )
    run(world, flow, workflow)


@when(parsers.parse('the workflow "{name}" runs the local steps "{first}", "{second}", "{third}"'))
def local_steps(world, flow, name, first, second, third):
    run(world, flow, local_workflow(world, flow, name, [first, second, third], None, None))


@when(
    parsers.parse(
        'the workflow "{name}" runs the local steps "{first}", "{second}", "{third}" '
        'and step "{failing}" fails once'
    )
)
def local_steps_failing_once(world, flow, name, first, second, third, failing):
    flow.run_id = ls.new_conversation_id()
    run(
        world, flow, local_workflow(world, flow, name, [first, second, third], flow.run_id, failing)
    )
    assert flow.error is not None


@when(parsers.parse('the workflow "{name}" resumes the same run of local steps'))
def local_steps_resumed(world, flow, name):
    labels = ["generate", "process", "finish"]
    run(world, flow, local_workflow(world, flow, name, labels, flow.run_id, None))


@when(
    parsers.parse(
        'the workflow "{name}" runs the local steps "{first}" and "{second}" with compensations '
        'and then "{failing}" which fails'
    )
)
def local_steps_compensated(world, flow, name, first, second, failing):
    def undo_locally(label):
        def compensate(outputs):
            flow.compensations.append(f"undo {label}")
            return b""

        return compensate

    workflow = (
        world.laser.workflow(name)
        .step(first, run=local_body(flow, first, False), compensate=undo_locally(first))
        .step(second, run=local_body(flow, second, False), compensate=undo_locally(second))
        .step(failing, run=local_body(flow, failing, True))
    )
    run(world, flow, workflow)


@then(parsers.re(r'the workflow completed the steps (?P<labels>".+") in order'))
def completed_in_order(flow, labels):
    assert flow.error is None, flow.error
    assert flow.outcome.completed_steps == quoted(labels)
    assert flow.outcome.status == "completed"


@then(parsers.parse('the workflow output of step "{label}" is "{expected}"'))
def workflow_output(flow, label, expected):
    assert flow.error is None, flow.error
    assert flow.outcome.text(label) == expected


@then(parsers.parse('the workflow output text of step "{label}" is "{expected}"'))
def workflow_output_text(flow, label, expected):
    assert flow.error is None, flow.error
    assert flow.outcome.text(label) == expected


@then(parsers.parse('the workflow fails at step "{label}" with reason "{reason}"'))
def fails_at_step(flow, label, reason):
    assert isinstance(flow.error, ls.StepFailedError), flow.error
    assert flow.error.step == label
    assert flow.error.reason == reason


@then(
    parsers.parse(
        'the failure names the target "{agent}" and a deadline of {deadline:d} milliseconds'
    )
)
def failure_names_target(flow, agent, deadline):
    assert isinstance(flow.error, ls.StepFailedError), flow.error
    assert agent in flow.error.target
    assert flow.error.deadline_ms == deadline
    assert agent in str(flow.error)
    assert f"deadline {deadline} ms" in str(flow.error)


@then(parsers.parse('agent "{agent}" received the compensations "{first}" then "{second}"'))
def received_compensations(flow, agent, first, second):
    received = [
        received for by, received in flow.received if by == agent and received.startswith("undo ")
    ]
    assert received == [first, second]


@then(parsers.re(r"the local step bodies ran (?P<labels>\".+\")"))
def local_bodies_ran(flow, labels):
    assert flow.local_runs == quoted(labels)


@then(parsers.parse('the workflow fails with the local step error "{expected}"'))
def local_step_error(flow, expected):
    assert flow.error is not None
    assert expected in str(flow.error)


@then(parsers.parse('the local compensations ran "{first}" then "{second}"'))
def local_compensations(flow, first, second):
    assert flow.compensations == [first, second]


def spawn(world, flow, name, skill, fail):
    async def handle(context, message):
        received = bytes(message.body()).decode()
        flow.received.append((name, received))
        if fail:
            raise ls.RejectedError(f"{name} fails every step")
        await context.respond(f"{received} done by {name}".encode())

    async def start():
        # The agent runs on the scenario's loop, so it is spawned inside it.
        agent = world.laser.spawn_agent(
            name,
            ls.AgentTopic.Sessions,
            handle,
            respond_on=ls.AgentTopic.Sessions,
            capabilities=[skill] if skill else None,
        )
        await agent.ready()
        return agent

    agent = world.run(start)
    if not hasattr(world, "responders"):
        world.responders = []
    world.responders.append(agent)


def run(world, flow, workflow):
    try:
        flow.outcome = world.run(workflow.run)
        flow.error = None
    except Exception as error:  # noqa: BLE001 the scenario asserts the error
        flow.outcome = None
        flow.error = error


def local_workflow(world, flow, name, labels, run_id, failing):
    workflow = world.laser.workflow(name)
    if run_id is not None:
        workflow = workflow.run_id(run_id)
    for label in labels:
        workflow = workflow.step(label, run=local_body(flow, label, label == failing))
    return workflow


def local_body(flow, label, fail_once):
    state = {"fail": fail_once}

    def body_run(outputs):
        flow.local_runs.append(label)
        if state.pop("fail", False):
            raise RuntimeError(f"{label} failed")
        return f"{label} done in process"

    return body_run


def body(label):
    return lambda outputs: label


def undo(label):
    return lambda outputs: f"undo {label}"


def quoted(labels):
    return re.findall(r'"([^"]+)"', labels)
