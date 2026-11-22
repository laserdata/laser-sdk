import _thread
import asyncio
import contextvars
import hashlib
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import laser_sdk as ls
import pytest

integration = pytest.mark.integration


@integration
def test_given_three_plain_steps_when_the_workflow_is_called_then_should_return_its_value(stream):
    @ls.step
    def ingest():
        return {"satellite": "sat-7", "frames": 12}

    @ls.step
    def analyze(batch):
        return {**batch, "anomalies": 2}

    @ls.step
    def report(analysis):
        return f"{analysis['anomalies']} anomalies in {analysis['frames']} frames"

    @ls.workflow
    def pipeline():
        return report(analyze(ingest()))

    assert pipeline() == "2 anomalies in 12 frames"


@integration
def test_given_three_plain_steps_when_run_then_should_report_run_id_status_steps_and_value(stream):
    @ls.step
    def ingest():
        return 12

    @ls.step
    def analyze(frames):
        return frames / 6

    @ls.step
    def report(anomalies):
        return f"{anomalies:.0f} anomalies"

    @ls.workflow
    def pipeline():
        return {"report": report(analyze(ingest()))}

    outcome = pipeline.run()

    assert outcome.status == "completed"
    assert outcome.run_id
    assert outcome.completed_steps == ["ingest", "analyze", "report"]
    assert outcome.value == {"report": "2 anomalies"}
    assert outcome.json("ingest") == 12
    assert outcome.json("missing") is None
    assert outcome.text("report") == '"2 anomalies"'


@integration
def test_given_an_outcome_when_printed_then_should_show_run_id_status_steps_and_value(stream):
    @ls.step
    def ingest():
        return 12

    @ls.workflow
    def pipeline():
        return {"frames": ingest()}

    outcome = pipeline.run()

    assert repr(outcome) == (
        f"WorkflowOutcome(run_id='{outcome.run_id}', status='completed', "
        "completed_steps=['ingest'], value={'frames': 12})"
    )


@integration
async def test_given_an_async_workflow_and_an_async_step_when_awaited_then_should_return_its_value(
    stream,
):
    @ls.step
    async def ingest():
        await asyncio.sleep(0)
        return 12

    @ls.step
    def report(frames):
        return f"{frames} frames"

    @ls.workflow
    async def pipeline():
        return report(await ingest())

    assert await pipeline() == "12 frames"
    outcome = await pipeline.run()
    assert outcome.completed_steps == ["ingest", "report"]
    assert outcome.value == "12 frames"


@integration
def test_given_a_sync_workflow_when_it_calls_an_async_step_then_should_refuse_naming_the_fix(
    stream,
):
    @ls.step
    async def ingest():
        return 12

    @ls.workflow
    def pipeline():
        return ingest()

    with pytest.raises(ls.InvalidError, match="async def"):
        pipeline()


async def test_given_a_running_event_loop_when_a_sync_workflow_is_called_then_should_refuse_naming_the_fix():  # noqa: E501
    @ls.workflow
    def pipeline():
        return 1

    with pytest.raises(ls.InvalidError, match="async def"):
        pipeline()


def test_given_a_step_called_outside_a_workflow_then_should_refuse_naming_both_ways_out():
    @ls.step
    def ingest():
        return 12

    with pytest.raises(ls.InvalidError, match=r"workflow.*ingest\.fn"):
        ingest()
    assert ingest.fn() == 12


def test_given_a_named_step_called_outside_a_workflow_then_should_name_the_function_to_call():
    @ls.step("read_frames")
    def ingest():
        return 12

    with pytest.raises(ls.InvalidError, match=r"step `read_frames`.*ingest\.fn\(") as refused:
        ingest()
    assert "read_frames.fn" not in str(refused.value)


@integration
def test_given_a_recorded_step_when_resumed_then_should_not_call_its_function(stream):
    calls = []

    @ls.step
    def ingest():
        calls.append("ingest")
        return (12, 13)

    @ls.workflow
    def pipeline():
        return ingest()

    first = pipeline.run()
    second = pipeline.resume(first.run_id)

    assert calls == ["ingest"]
    assert first.value == [12, 13], "a fresh run reads the value as JSON holds it"
    assert second.value == [12, 13]


@integration
def test_given_a_step_called_twice_when_run_then_should_journal_the_second_under_its_own_label(
    stream,
):
    @ls.step
    def probe(value):
        return value

    @ls.workflow
    def pipeline():
        return [probe(1), probe(2)]

    outcome = pipeline.run()

    assert outcome.completed_steps == ["probe", "probe#2"]
    assert outcome.json("probe#2") == 2


@integration
def test_given_a_step_that_raises_when_run_then_should_raise_the_same_exception_object(stream):
    failure = ValueError("frames refused")

    @ls.step
    def ingest():
        raise failure

    @ls.workflow
    def pipeline():
        return ingest()

    with pytest.raises(ValueError) as raised:
        pipeline()
    assert raised.value is failure


@integration
def test_given_a_failed_run_when_resumed_then_should_run_only_the_failed_step_and_what_follows(
    stream,
):
    calls = []
    run_id = ls.new_conversation_id()

    @ls.step
    def ingest():
        calls.append("ingest")
        return 12

    @ls.step
    def analyze(frames):
        calls.append("analyze")
        if calls.count("analyze") == 1:
            raise RuntimeError("the first attempt fails")
        return frames / 6

    @ls.workflow
    def pipeline():
        return analyze(ingest())

    with pytest.raises(RuntimeError):
        pipeline.resume(run_id)
    outcome = pipeline.resume(run_id)

    assert calls == ["ingest", "analyze", "analyze"]
    assert outcome.completed_steps == ["ingest", "analyze"]


@integration
def test_given_a_workflow_running_when_the_engine_waits_then_should_let_another_thread_run_python(
    stream,
):
    ticks = []
    stop = threading.Event()

    def ticker():
        while not stop.is_set():
            ticks.append(time.monotonic())
            time.sleep(0.001)

    @ls.step
    def ingest():
        return 12

    @ls.workflow
    def pipeline():
        return ingest()

    thread = threading.Thread(target=ticker)
    thread.start()
    before = len(ticks)
    pipeline()
    during = len(ticks) - before
    stop.set()
    thread.join()

    assert during >= 3, "the interpreter lock stays free while the run waits on the server"


@integration
def test_given_two_workflows_run_at_once_when_done_then_should_each_hold_only_their_own_steps(
    stream,
):
    barrier = threading.Barrier(2)

    @ls.step
    def first_step():
        barrier.wait(timeout=10)
        return 1

    @ls.step
    def second_step():
        barrier.wait(timeout=10)
        return 2

    @ls.workflow
    def first():
        return first_step()

    @ls.workflow
    def second():
        return second_step()

    outcomes = {}
    threads = [
        threading.Thread(target=lambda: outcomes.update(first=first.run())),
        threading.Thread(target=lambda: outcomes.update(second=second.run())),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)

    assert outcomes["first"].completed_steps == ["first_step"]
    assert outcomes["second"].completed_steps == ["second_step"]


@integration
async def test_given_two_async_workflows_in_tasks_when_done_then_should_each_hold_only_their_own_steps(  # noqa: E501
    stream,
):
    @ls.step
    async def first_step():
        await asyncio.sleep(0.01)
        return 1

    @ls.step
    async def second_step():
        await asyncio.sleep(0.01)
        return 2

    @ls.workflow
    async def first():
        return await first_step()

    @ls.workflow
    async def second():
        return await second_step()

    one, two = await asyncio.gather(first.run(), second.run())

    assert one.completed_steps == ["first_step"]
    assert two.completed_steps == ["second_step"]


@integration
def test_given_a_step_returning_a_set_when_run_then_should_refuse_naming_the_step_and_the_types(
    stream,
):
    @ls.step
    def frames():
        return {1, 2}

    @ls.workflow
    def pipeline():
        return frames()

    with pytest.raises(ls.InvalidError, match=r"`frames`.*dict, list, str"):
        pipeline()


@integration
def test_given_a_step_name_holding_the_call_separator_when_called_then_should_refuse_it(stream):
    calls = []

    @ls.step("probe#2")
    def probe():
        calls.append("probe")

    @ls.workflow
    def pipeline():
        return probe()

    with pytest.raises(ls.InvalidError, match="probe#2"):
        pipeline()
    assert calls == []


@integration
def test_given_no_stream_configured_when_run_then_should_use_the_stream_named_after_the_workflow(
    iggy_endpoint, monkeypatch
):
    name = f"function-form-{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("LASER_CONNECTION_STRING", f"iggy://{iggy_endpoint}")
    monkeypatch.delenv("LASER_STREAM", raising=False)

    @ls.step
    def ingest():
        return 12

    @ls.workflow(name=name)
    def pipeline():
        return ingest()

    assert pipeline() == 12
    assert stream_exists(iggy_endpoint, name)
    delete_stream(iggy_endpoint, name)


@integration
async def test_given_a_bound_client_when_run_then_should_use_its_default_stream_and_leave_it_open(
    iggy_endpoint,
):
    name = f"function-bound-{uuid.uuid4().hex[:8]}"
    laser = await ls.Laser.connect(f"iggy://{iggy_endpoint}", stream=name)

    @ls.step
    async def ingest():
        return 12

    @ls.workflow
    async def pipeline():
        return await ingest()

    try:
        assert await pipeline.bind(laser)() == 12
        assert name in [stream.name for stream in await laser.streams()]
        await laser.stream(name).delete()
    finally:
        await laser.close()


def test_given_a_decorated_function_when_inspected_then_should_keep_its_name_doc_and_wrapped():
    def ingest():
        """Read the frames."""

    decorated_step = ls.step(ingest)
    decorated_workflow = ls.workflow(ingest)

    for decorated in (decorated_step, decorated_workflow):
        assert decorated.__name__ == "ingest"
        assert decorated.__doc__ == "Read the frames."
        assert decorated.__wrapped__ is ingest


@integration
def test_given_decorator_names_when_run_then_should_journal_under_those_names(stream):
    @ls.step("read_frames")
    def ingest():
        return 12

    @ls.workflow("frame_pipeline")
    def pipeline():
        return ingest()

    assert pipeline.run().completed_steps == ["read_frames"]


@integration
def test_given_a_keyboard_interrupt_during_a_step_when_run_then_should_leave_the_run_resumable(
    stream,
):
    run_id = ls.new_conversation_id()
    interrupted = []

    @ls.step
    def ingest():
        if not interrupted:
            interrupted.append(True)
            raise KeyboardInterrupt
        return 12

    @ls.workflow
    def pipeline():
        return ingest()

    with pytest.raises(KeyboardInterrupt):
        pipeline.resume(run_id)

    assert pipeline.resume(run_id).completed_steps == ["ingest"]


@integration
async def test_given_concurrent_branches_finishing_in_another_order_when_resumed_then_should_return_each_branch_its_own_values(  # noqa: E501
    stream,
):
    run_id = ls.new_conversation_id()
    second_finished = asyncio.Event()
    failing = [True]

    @ls.step
    async def first(branch):
        if branch == 1:
            await second_finished.wait()
        else:
            second_finished.set()
        return f"first-of-{branch}"

    @ls.step
    async def second(branch):
        return f"second-of-{branch}"

    @ls.step
    async def last():
        if failing and failing.pop():
            raise RuntimeError("the last step fails once")
        return "done"

    async def branch(number):
        return [number, await first(number), await second(number)]

    @ls.workflow
    async def pipeline():
        branches = await asyncio.gather(branch(1), branch(2))
        await last()
        return branches

    with pytest.raises(RuntimeError):
        await pipeline.resume(run_id)
    outcome = await pipeline.resume(run_id)

    assert outcome.value == [[1, "first-of-1", "second-of-1"], [2, "first-of-2", "second-of-2"]]


@integration
def test_given_a_step_retried_in_the_body_when_resumed_then_should_not_run_it_again(stream):
    run_id = ls.new_conversation_id()
    effects = []
    attempts = []
    shipping = []

    @ls.step
    def charge():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("the first attempt fails")
        effects.append("charged")
        return "charged"

    @ls.step
    def ship():
        shipping.append(1)
        if len(shipping) == 1:
            raise ValueError("shipping is down")
        return "shipped"

    @ls.workflow
    def pipeline():
        try:
            charge()
        except RuntimeError:
            charge()
        return ship()

    with pytest.raises(ValueError):
        pipeline.resume(run_id)
    outcome = pipeline.resume(run_id)

    assert effects == ["charged"]
    assert outcome.completed_steps == ["charge", "ship"]


@integration
async def test_given_a_step_called_after_the_workflow_returned_then_should_refuse_it_and_keep_the_recorded_value(  # noqa: E501
    stream,
):
    late_result = []
    finished = asyncio.Event()
    background = []

    @ls.step
    async def first(value):
        return value

    async def late():
        await finished.wait()
        try:
            await first("LATE-VALUE")
        except ls.InvalidError as error:
            late_result.append(error)

    @ls.workflow
    async def pipeline():
        background.append(asyncio.ensure_future(late()))
        return await first("ORIGINAL-VALUE")

    outcome = await pipeline.run()
    finished.set()
    await background[0]

    assert len(late_result) == 1
    assert outcome.json("first") == "ORIGINAL-VALUE"


@integration
async def test_given_a_cancelled_async_workflow_when_resumed_then_should_continue_it(stream):
    run_id = ls.new_conversation_id()
    blocked = asyncio.Event()
    calls = []

    @ls.step
    async def ingest():
        calls.append("ingest")
        return 12

    @ls.step
    async def analyze(frames):
        calls.append("analyze")
        if calls.count("analyze") == 1:
            blocked.set()
            await asyncio.Event().wait()
        return frames / 6

    @ls.workflow
    async def pipeline():
        return await analyze(await ingest())

    task = asyncio.ensure_future(pipeline.resume(run_id))
    await asyncio.wait_for(blocked.wait(), 30)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    outcome = await pipeline.resume(run_id)

    assert calls == ["ingest", "analyze", "analyze"]
    assert outcome.completed_steps == ["ingest", "analyze"]


@integration
async def test_given_a_failing_step_beside_a_running_one_when_resumed_then_should_not_repeat_the_finished_effect(  # noqa: E501
    stream,
):
    run_id = ls.new_conversation_id()
    effects = []
    failing = [True]

    @ls.step
    async def sibling():
        await asyncio.sleep(0.2)
        effects.append("sibling")
        return "sibling"

    @ls.step
    async def quick():
        if failing and failing.pop():
            raise RuntimeError("quick fails once")
        return "quick"

    @ls.workflow
    async def pipeline():
        return await asyncio.gather(sibling(), quick())

    with pytest.raises(RuntimeError):
        await pipeline.resume(run_id)
    await pipeline.resume(run_id)

    assert effects == ["sibling"]


@integration
async def test_given_a_bound_client_without_a_default_stream_when_run_then_should_use_laser_stream(
    iggy_endpoint, monkeypatch
):
    name = f"function-env-{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("LASER_STREAM", name)
    laser = await ls.Laser.connect(f"iggy://{iggy_endpoint}")

    @ls.step
    async def ingest():
        return 12

    @ls.workflow
    async def pipeline():
        return await ingest()

    try:
        await pipeline.bind(laser)()
        assert name in [stream.name for stream in await laser.streams()]
        await laser.stream(name).delete()
    finally:
        await laser.close()


@integration
def test_given_a_recorded_output_that_is_not_json_when_resumed_then_should_raise_an_sdk_error_naming_the_step(  # noqa: E501
    stream, iggy_endpoint
):
    run_id = ls.new_conversation_id()

    async def builder_run(endpoint):
        laser = await ls.Laser.connect(f"iggy://{endpoint}", stream=stream)
        try:
            await (
                laser.workflow("pipeline")
                .run_id(run_id)
                .step("ingest", run=lambda outputs: b"\xff\xfe not json")
                .run()
            )
        finally:
            await laser.close()

    on_thread(builder_run, iggy_endpoint)

    @ls.step
    def ingest():
        return 12

    @ls.workflow
    def pipeline():
        return ingest()

    with pytest.raises(ls.LaserError, match="`ingest`"):
        pipeline.resume(run_id)


@integration
def test_given_steps_on_worker_threads_that_carry_the_context_when_run_then_should_journal_them_in_the_workflow(  # noqa: E501
    stream,
):
    @ls.step
    def amplify(gain):
        return gain * 2

    @ls.workflow
    def pipeline():
        with ThreadPoolExecutor(max_workers=2) as pool:
            calls = [pool.submit(contextvars.copy_context().run, amplify, gain) for gain in (1, 2)]
            return sorted(call.result() for call in calls)

    outcome = pipeline.run()

    assert outcome.value == [2, 4]
    assert sorted(outcome.completed_steps) == ["amplify", "amplify#2"]


@integration
def test_given_a_plain_workflow_on_another_thread_when_a_step_is_called_outside_it_then_should_refuse_it(  # noqa: E501
    stream,
):
    downlinking = threading.Event()
    refused = threading.Event()
    outcomes = []

    @ls.step
    def downlink():
        downlinking.set()
        refused.wait(30)
        return 12

    @ls.step
    def uplink():
        return "sent"

    @ls.workflow
    def pipeline():
        return downlink()

    thread = threading.Thread(target=lambda: outcomes.append(pipeline.run()))
    thread.start()
    try:
        assert downlinking.wait(30)
        with pytest.raises(ls.InvalidError, match="outside a workflow"):
            uplink()
    finally:
        refused.set()
        thread.join(30)

    assert [outcome.completed_steps for outcome in outcomes] == [["downlink"]]


@integration
def test_given_a_workflow_and_a_step_on_methods_when_run_then_should_bind_the_instance(stream):
    class Satellite:
        name = "sat-7"

        @ls.step
        def ingest(self):
            return self.name

        @ls.workflow
        def pipeline(self):
            return self.ingest()

    assert Satellite().pipeline() == "sat-7"
    assert Satellite().pipeline.run().completed_steps == ["ingest"]


@integration
async def test_given_a_suspended_async_workflow_when_its_coroutine_is_closed_then_should_not_raise(
    stream,
):
    entered = asyncio.Event()

    @ls.workflow
    async def pipeline():
        entered.set()
        await asyncio.Event().wait()

    coroutine = pipeline()
    await asyncio.wait_for(step_until(coroutine, entered), 30)

    coroutine.close()


@integration
async def test_given_a_step_coroutine_that_never_starts_when_the_run_ends_then_should_not_wait_for_it(  # noqa: E501
    stream,
):
    @ls.step
    async def ingest():
        return 12

    @ls.workflow
    async def pipeline():
        ingest().close()
        return 1

    started = time.monotonic()
    await pipeline()

    assert time.monotonic() - started < 4


@integration
async def test_given_a_large_recorded_call_number_when_resumed_then_should_return_its_value(
    stream, iggy_endpoint
):
    run_id = ls.new_conversation_id()
    provenance = ls.Provenance(conversation_id=run_id)
    laser = await ls.Laser.connect(f"iggy://{iggy_endpoint}", stream=stream)
    try:
        await laser.bootstrap()
        for record in [
            {"RunStarted": {"workflow": "pipeline"}},
            {
                "StepCompleted": {
                    "label": "scan#100000000000000000000",
                    "input": hashlib.sha256(b"[]").hexdigest()[:32],
                    "output": list(b"42"),
                    "agents": [],
                }
            },
        ]:
            payload = ls.Cbor.encode(record)
            await laser.send_agent(ls.AgentTopic.WorkflowJournal, payload, provenance)
    finally:
        await laser.close()

    @ls.step
    async def scan():
        pytest.fail("the recorded step ran again")

    @ls.workflow
    async def pipeline():
        return await scan()

    outcome = await pipeline.resume(run_id)
    assert outcome.value == 42


@integration
def test_given_a_step_called_with_other_arguments_when_resumed_then_should_run_as_a_new_call(
    stream,
):
    calls = []
    run_id = ls.new_conversation_id()

    @ls.step
    def notify(attempt):
        calls.append(attempt)
        return attempt

    @ls.workflow
    def pipeline(attempt):
        return notify(attempt)

    pipeline.resume(run_id, 1)
    outcome = pipeline.resume(run_id, 2)

    assert calls == [1, 2]
    assert outcome.completed_steps == ["notify", "notify#2"]


@integration
def test_given_numbers_when_a_step_is_called_then_should_journal_the_digest_every_client_computes(
    stream, iggy_endpoint
):
    @ls.step
    def scan(*readings):
        return len(readings)

    @ls.workflow
    def pipeline():
        return scan(1.0, 1.5e-07, 1e-05, 1e21, 2.0**60, 0.1, -2.5, -0.0, 5e-324)

    pipeline()

    # SHA-256 of [1,1.5e-7,0.00001,1000000000000000000000,1152921504606846976,0.1,-2.5,0,5e-324]
    assert on_thread(journaled_inputs, iggy_endpoint, stream) == [
        "ae96dd6c224f98f4ae7b89f2f91439db"
    ]


@integration
def test_given_a_whole_float_when_resumed_with_the_equal_integer_then_should_not_run_the_step_again(
    stream,
):
    calls = []
    run_id = ls.new_conversation_id()

    @ls.step
    def uplink(gain):
        calls.append(gain)
        return gain

    @ls.workflow
    def pipeline(gain):
        return uplink(gain)

    pipeline.resume(run_id, 2.0)
    outcome = pipeline.resume(run_id, 2)

    assert calls == [2.0]
    assert outcome.completed_steps == ["uplink"]


@integration
def test_given_keyword_arguments_when_a_step_is_called_then_should_tell_them_from_a_trailing_dict(
    stream, iggy_endpoint
):
    @ls.step
    def scan(band, options=None, mode=None):
        return band

    @ls.workflow
    def pipeline():
        return [scan(1, mode="fast"), scan(1, {"mode": "fast"})]

    pipeline()

    # SHA-256 of {"args":[1],"kwargs":{"mode":"fast"}} and of [1,{"mode":"fast"}]
    assert on_thread(journaled_inputs, iggy_endpoint, stream) == [
        "1c40fb922dc62a722f6286a5f6dcba4e",
        "6c12901336b2ab6dff8defa8f7436f9f",
    ]


@integration
def test_given_keys_that_are_not_text_when_a_step_is_called_then_should_digest_them_as_json_writes_them(  # noqa: E501
    stream, iggy_endpoint
):
    @ls.step
    def scan(readings):
        return len(readings)

    @ls.workflow
    def pipeline():
        return scan({2: "c", 10: "b", 1: "a", False: "d", None: "e"})

    pipeline()

    # SHA-256 of [{"1":"a","10":"b","2":"c","false":"d","null":"e"}]
    assert on_thread(journaled_inputs, iggy_endpoint, stream) == [
        "435118b6bf4ba640b57338387af5af9f"
    ]


@integration
def test_given_arguments_json_cannot_hold_when_a_step_is_called_then_should_journal_no_digest(
    stream, iggy_endpoint
):
    @ls.step
    def scan(reading):
        return "scanned"

    @ls.workflow
    def pipeline():
        return [scan(float("nan")), scan(float("inf")), scan({1: "a", "1": "b"}), scan({1, 2})]

    pipeline()

    assert on_thread(journaled_inputs, iggy_endpoint, stream) == [None, None, None, None]


@integration
async def test_given_two_branches_retrying_a_step_when_a_failure_is_freed_late_then_should_keep_both_retries(  # noqa: E501
    stream,
):
    run_id = ls.new_conversation_id()
    attempts = {1: 0, 2: 0}
    first_failed = asyncio.Event()
    first_retrying = asyncio.Event()
    second_retrying = asyncio.Event()

    @ls.step
    async def downlink(band):
        attempts[band] += 1
        if attempts[band] == 1:
            raise RuntimeError("the first attempt fails")
        if band == 1:
            first_retrying.set()
        else:
            second_retrying.set()
            await first_retrying.wait()
        return f"frames-{band}"

    async def branch(band):
        try:
            return await downlink(band)
        except RuntimeError:
            if band == 1:
                first_failed.set()
                await second_retrying.wait()
            else:
                await first_failed.wait()
        return await downlink(band)

    @ls.workflow
    async def pipeline():
        return await asyncio.gather(branch(1), branch(2))

    outcome = await pipeline.resume(run_id)
    resumed = await pipeline.resume(run_id)

    assert outcome.value == ["frames-1", "frames-2"]
    assert sorted(outcome.completed_steps) == ["downlink", "downlink#2"]
    assert resumed.value == ["frames-1", "frames-2"]
    assert attempts == {1: 2, 2: 2}


@integration
async def test_given_a_step_called_as_the_workflow_body_returns_then_should_refuse_it(stream):
    returning = asyncio.Event()
    refusals = []
    background = []

    @ls.step
    async def downlink(band):
        return band

    async def late():
        await returning.wait()
        try:
            await downlink("late")
        except ls.InvalidError as error:
            refusals.append(str(error))

    @ls.workflow
    async def pipeline():
        background.append(asyncio.ensure_future(late()))
        try:
            return await downlink("first")
        finally:
            returning.set()

    outcome = await pipeline.run()
    await background[0]

    assert outcome.completed_steps == ["downlink"]
    assert len(refusals) == 1
    assert "has ended" in refusals[0]


@integration
def test_given_an_error_whose_text_calls_a_step_when_the_body_raises_it_then_should_refuse_the_step(
    stream,
):
    refusals = []

    @ls.step
    def downlink(band):
        return band

    @ls.workflow
    def pipeline():
        downlink("first")
        raise UplinkRefused(contextvars.copy_context(), downlink, refusals)

    with pytest.raises(UplinkRefused):
        pipeline()

    assert len(refusals) == 1
    assert "has ended" in refusals[0]


@integration
async def test_given_an_error_whose_text_calls_a_step_when_an_async_body_raises_it_then_should_refuse_the_step(  # noqa: E501
    stream,
):
    refusals = []

    @ls.step
    def downlink(band):
        return band

    @ls.workflow
    async def pipeline():
        downlink("first")
        raise UplinkRefused(contextvars.copy_context(), downlink, refusals)

    with pytest.raises(UplinkRefused):
        await pipeline()

    assert len(refusals) == 1
    assert "has ended" in refusals[0]


@integration
def test_given_a_keyboard_interrupt_while_a_failed_run_ends_then_should_raise_the_interrupt(
    stream,
):
    downlinking = threading.Event()
    interrupted = threading.Event()
    workers = []

    @ls.step
    def downlink():
        downlinking.set()
        while runs(threading.main_thread(), pipeline.__wrapped__.__code__):
            time.sleep(0.001)
        _thread.interrupt_main()
        interrupted.wait(30)
        return 12

    @ls.step
    def uplink():
        downlinking.wait(30)
        raise RuntimeError("uplink refused")

    @ls.workflow
    def pipeline():
        worker = threading.Thread(target=contextvars.copy_context().run, args=(downlink,))
        worker.start()
        workers.append(worker)
        return uplink()

    try:
        with pytest.raises(KeyboardInterrupt):
            pipeline()
    finally:
        interrupted.set()
        for worker in workers:
            worker.join(30)


@integration
def test_given_a_step_returning_text_beyond_ascii_when_run_then_should_store_the_characters(stream):
    @ls.step
    def downlink():
        return {"station": "Tromsø", "gain": "12°"}

    @ls.workflow
    def pipeline():
        return downlink()

    outcome = pipeline.run()

    assert outcome.text("downlink") == '{"station":"Tromsø","gain":"12°"}'
    assert outcome.value == {"station": "Tromsø", "gain": "12°"}


@integration
def test_given_a_step_returning_a_lone_surrogate_when_run_then_should_refuse_naming_the_step(
    stream,
):
    @ls.step
    def frames():
        return "\ud800"

    @ls.workflow
    def pipeline():
        return frames()

    with pytest.raises(ls.InvalidError, match=r"`frames`.*JSON cannot hold.*dict, list, str"):
        pipeline()


@pytest.fixture
def stream(iggy_endpoint, monkeypatch):
    name = f"function-{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("LASER_CONNECTION_STRING", f"iggy://{iggy_endpoint}")
    monkeypatch.setenv("LASER_STREAM", name)
    yield name
    delete_stream(iggy_endpoint, name)


def stream_exists(endpoint, name):
    return name in on_thread(stream_names, endpoint)


def delete_stream(endpoint, name):
    on_thread(drop_stream, endpoint, name)


# Advances a coroutine by hand until `entered` is set, so it stays suspended.
async def step_until(coroutine, entered):
    awaited = coroutine.send(None)
    while not entered.is_set():
        awaited._asyncio_future_blocking = False
        await asyncio.wait([awaited])
        awaited = coroutine.send(None)


# An error whose text calls `step` in `context`, once, and keeps the refusal.
class UplinkRefused(Exception):
    def __init__(self, context, step, refusals):
        super().__init__("uplink refused")
        self.late_call = (context, step, refusals)

    def __str__(self):
        late_call, self.late_call = self.late_call, None
        if late_call is not None:
            context, step, refusals = late_call
            try:
                context.run(step, "late")
            except ls.InvalidError as error:
                refusals.append(str(error))
        return super().__str__()


# Whether `thread` is inside the function that `code` belongs to.
def runs(thread, code):
    frame = sys._current_frames().get(thread.ident)
    while frame is not None:
        if frame.f_code is code:
            return True
        frame = frame.f_back
    return False


# A fixture's teardown can run while the test's event loop is still installed.
def on_thread(function, *args):
    results = []
    thread = threading.Thread(target=lambda: results.append(asyncio.run(function(*args))))
    thread.start()
    thread.join(30)
    return results[0]


async def stream_names(endpoint):
    laser = await ls.Laser.connect(f"iggy://{endpoint}")
    try:
        return [stream.name for stream in await laser.streams()]
    finally:
        await laser.close()


# The argument digest of every completed step the stream's journal holds.
async def journaled_inputs(endpoint, name):
    laser = await ls.Laser.connect(f"iggy://{endpoint}")
    try:
        cursor = laser.stream(name).topic("agent.workflow_journal").replay()
        inputs = []
        while batch := await cursor.poll():
            for message in batch:
                record = ls.Cbor.decode(message.payload)
                if "StepCompleted" in record:
                    inputs.append(record["StepCompleted"].get("input"))
        return inputs
    finally:
        await laser.close()


async def drop_stream(endpoint, name):
    laser = await ls.Laser.connect(f"iggy://{endpoint}")
    try:
        if name in [stream.name for stream in await laser.streams()]:
            await laser.stream(name).delete()
    finally:
        await laser.close()
