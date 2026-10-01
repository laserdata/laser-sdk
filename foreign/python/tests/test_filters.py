import asyncio
import json
from pathlib import Path

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

TOPIC = "fleet_changes"
READ_TIMEOUT = 15

SATELLITE = {
    "id": "sat-042",
    "name": "Kestrel-42",
    "mode": "safe",
    "orbit": "leo",
    "battery_pct": 61,
}
SAFE_MODE = json.dumps({"op": "u", "table": "satellites", "changed": ["mode"], "after": SATELLITE})
SAFE_MODE_VALUES = json.dumps({"op": "u", "table": "satellites", "after": SATELLITE})
DECOMMISSION = json.dumps({"op": "d", "table": "satellites", "before": {"id": "sat-042"}})
TELEMETRY = json.dumps(
    {
        "event": "satellite.telemetry_changed",
        "satellite_id": "sat-042",
        "fields": {"mode": "safe"},
    }
)
GROUND_STATION = json.dumps(
    {
        "op": "u",
        "table": "ground_stations",
        "changed": ["status"],
        "after": {"id": "svalbard", "status": "online"},
    }
)


def safe_mode_filter():
    def satellites():
        return ls.FilterExpr.pred("table", "eq", "satellites")

    return ls.ConsumerFilter.json(
        ls.FilterExpr.any(
            [
                ls.FilterExpr.all(
                    [
                        satellites(),
                        ls.FilterExpr.pred("op", "eq", "u"),
                        ls.FilterExpr.pred("changed", "contains", "mode"),
                        ls.FilterExpr.pred("after.mode", "eq", "safe"),
                    ]
                ),
                ls.FilterExpr.all([satellites(), ls.FilterExpr.pred("op", "eq", "d")]),
                ls.FilterExpr.all(
                    [
                        ls.FilterExpr.pred("event", "eq", "satellite.telemetry_changed"),
                        ls.FilterExpr.present("fields.mode"),
                    ]
                ),
            ]
        )
    )


async def publish(laser, payloads):
    producer = laser.topic(TOPIC).producer(partition=0, partitions=1)
    await producer.send_batch(payloads)


def reader(laser, consumer, **options):
    return laser.filters().reader(
        laser.default_stream, TOPIC, consumer=consumer, filter=safe_mode_filter(), **options
    )


def offsets(page):
    return [record.offset for record in page.records]


def test_given_a_filter_when_round_tripped_then_should_keep_its_digest():
    original = safe_mode_filter()

    rebuilt = ls.ConsumerFilter.from_dict(original.to_dict())

    assert rebuilt == original
    assert rebuilt.digest == original.digest
    assert len(original.digest) == 32
    assert original.codec == "json"
    assert original.fault_policy == "stop"


def test_given_an_invalid_operator_when_building_then_should_raise_invalid():
    with pytest.raises(ls.InvalidError):
        ls.FilterExpr.pred("after.mode", "resembles", "safe")


async def test_given_no_plane_when_probed_then_should_serve_native_filters_only(
    laser,
):
    capabilities = await laser.capabilities()
    if capabilities.filters_catalog:
        pytest.skip("a plane serves the filter catalog on this stack")

    assert capabilities.filters is True


async def test_given_edge_records_when_read_inline_then_should_return_matches_at_their_offsets(
    laser,
):
    await publish(laser, [SAFE_MODE, SAFE_MODE_VALUES, DECOMMISSION, TELEMETRY, GROUND_STATION])
    filtered = await reader(laser, "safe-mode-backfill", start="first")

    page = await asyncio.wait_for(filtered.next_page(), READ_TIMEOUT)

    assert offsets(page) == [0, 2, 3]
    assert page.records[1].message.payload == DECOMMISSION.encode()
    assert page.policy["digest"] == safe_mode_filter().digest
    await filtered.ack_page(page)
    await filtered.close()


async def test_given_stored_progress_when_reading_next_then_should_read_only_new_matches(
    laser,
):
    await publish(laser, [SAFE_MODE, GROUND_STATION])
    first = await reader(laser, "safe-mode-resume", start="first")
    page = await asyncio.wait_for(first.next_page(), READ_TIMEOUT)
    await first.ack_page(page)
    await first.close()

    await publish(laser, [GROUND_STATION, DECOMMISSION])
    second = await reader(laser, "safe-mode-resume")
    page = await asyncio.wait_for(second.next_page(), READ_TIMEOUT)

    assert offsets(page) == [3]
    await second.close()


async def test_given_only_non_matches_when_reading_then_should_store_the_scanned_range(
    laser,
):
    await publish(laser, [GROUND_STATION, GROUND_STATION, GROUND_STATION])
    first = await reader(laser, "safe-mode-sparse", start="first")

    assert await first.try_next_page() is None
    await first.close()

    await publish(laser, [DECOMMISSION])
    second = await reader(laser, "safe-mode-sparse")
    page = await asyncio.wait_for(second.next_page(), READ_TIMEOUT)
    assert offsets(page) == [3], "the empty page stored the range it examined"
    await second.close()


async def test_given_a_page_checkpoint_when_acknowledged_through_then_should_replay_later_records(
    laser,
):
    await publish(laser, [SAFE_MODE, SAFE_MODE, SAFE_MODE, SAFE_MODE])
    first = await reader(laser, "prefix-checkpoint", start="first", count=4)
    page = await asyncio.wait_for(first.next_page(), READ_TIMEOUT)
    assert offsets(page) == [0, 1, 2, 3]
    await first.ack_through(page.records[1])
    await first.close()
    restarted = await reader(laser, "prefix-checkpoint", start="next", count=4)
    remaining = await asyncio.wait_for(restarted.next_page(), READ_TIMEOUT)
    assert offsets(remaining) == [2, 3]
    await restarted.ack_page(remaining)
    await restarted.close()


async def test_given_out_of_order_acks_when_acked_then_should_store_only_the_completed_prefix(
    laser,
):
    await publish(laser, [SAFE_MODE, DECOMMISSION])
    first = await reader(laser, "safe-mode-ordered", start="first", count=1)
    earlier = await asyncio.wait_for(first.next_record(), READ_TIMEOUT)
    later = await asyncio.wait_for(first.next_record(), READ_TIMEOUT)

    await first.ack(later)
    await first.close()

    second = await reader(laser, "safe-mode-ordered")
    record = await asyncio.wait_for(second.next_record(), READ_TIMEOUT)
    assert (earlier.offset, later.offset) == (0, 1)
    assert record.offset == 0, "the earlier record was never completed"
    await second.close()


async def test_given_a_malformed_record_when_reading_then_should_deliver_matches_then_the_fault(
    laser,
):
    await publish(laser, [SAFE_MODE, "not json", DECOMMISSION])
    filtered = await reader(laser, "safe-mode-fault", start="first")

    page = await asyncio.wait_for(filtered.next_page(), READ_TIMEOUT)
    assert offsets(page) == [0]
    await filtered.ack_page(page)
    with pytest.raises(ls.FilterError) as fault:
        await filtered.next_page()

    assert fault.value.reason == "fault"
    assert fault.value.fault_reason == "malformed"
    assert (fault.value.partition_id, fault.value.offset) == (0, 1)
    await filtered.close()


async def test_given_a_local_guard_when_reading_then_should_agree_with_the_server(laser):
    await publish(laser, [GROUND_STATION, TELEMETRY])
    filtered = await reader(laser, "safe-mode-guarded", start="first", local_guard=True)

    page = await asyncio.wait_for(filtered.next_page(), READ_TIMEOUT)

    assert offsets(page) == [1]
    await filtered.close()


async def test_given_a_local_reader_when_acknowledging_then_should_refuse(laser):
    await publish(laser, [DECOMMISSION])
    filtered = await reader(laser, "safe-mode-local", start="first", read_mode="local")
    page = await asyncio.wait_for(filtered.next_page(), READ_TIMEOUT)

    with pytest.raises(ls.InvalidError):
        await filtered.ack_page(page)
    await filtered.close()


async def test_given_stored_records_when_previewed_then_should_judge_them_without_progress(
    laser,
):
    await publish(laser, [SAFE_MODE, SAFE_MODE_VALUES, DECOMMISSION])

    preview = await laser.filters().preview(
        laser.default_stream, TOPIC, 0, filter=safe_mode_filter(), max_records=10, explain=True
    )

    assert preview["examined"] == 3
    assert preview["matched"] == 2
    verdicts = {(record["offset"], record["verdict"]) for record in preview["records"]}
    assert (1, "rejected") in verdicts
    assert (2, "selected") in verdicts


async def test_given_a_sample_when_tested_and_validated_then_should_explain_and_digest(laser):
    tested = await laser.filters().test(SAFE_MODE_VALUES, filter=safe_mode_filter())
    validation = await laser.filters().validate(safe_mode_filter())

    assert tested["explanation"]["verdict"] == "rejected"
    assert validation["digest"] == safe_mode_filter().digest
    assert validation["reads_payload"] is True


async def test_given_no_catalog_when_listing_saved_filters_then_should_refuse_as_unsupported(laser):
    if (await laser.capabilities()).filters_catalog:
        pytest.skip("a plane serves the filter catalog on this stack")
    with pytest.raises(ls.UnsupportedError):
        await laser.filters().list()


async def test_given_two_readers_when_acknowledging_another_readers_page_then_should_reject(laser):
    await publish(laser, [SAFE_MODE])
    first = await reader(laser, "owner-one", start="first")
    second = await reader(laser, "owner-two", start="first")
    first_page = await asyncio.wait_for(first.next_page(), READ_TIMEOUT)
    second_page = await asyncio.wait_for(second.next_page(), READ_TIMEOUT)
    with pytest.raises(ls.InvalidError):
        await second.ack_page(first_page)
    with pytest.raises(ls.InvalidError):
        await second.ack(first_page.records[0])
    await second.ack_page(second_page)
    await first.close()
    await second.close()


async def test_given_buffered_records_when_reader_is_closed_then_should_not_yield_more(laser):
    await publish(laser, [SAFE_MODE, DECOMMISSION])
    filtered = await reader(laser, "closed-reader", start="first")
    await asyncio.wait_for(filtered.next_record(), READ_TIMEOUT)
    await filtered.close()
    with pytest.raises(ls.ConfigError):
        await filtered.next_record()


@pytest.mark.parametrize("method", ["next_record", "next_page", "try_next_page"])
async def test_given_a_completed_read_when_cancelled_before_delivery_then_should_return_it_again(
    laser, monkeypatch, method
):
    await publish(laser, [SAFE_MODE, DECOMMISSION])
    filtered = await reader(laser, "cancelled-read", start="first", count=1)
    loop = asyncio.get_running_loop()
    callbacks = []

    def hold_delivery(callback, *args, **kwargs):
        callbacks.append((callback, args))

    try:
        with monkeypatch.context() as patch:
            patch.setattr(loop, "call_soon_threadsafe", hold_delivery)
            pending = asyncio.ensure_future(getattr(filtered, method)())
            async with asyncio.timeout(READ_TIMEOUT):
                while not callbacks:
                    await asyncio.sleep(0.001)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            await asyncio.sleep(0)
        for callback, args in callbacks:
            callback(*args)
        result = await asyncio.wait_for(getattr(filtered, method)(), READ_TIMEOUT)
        if method == "next_record":
            assert result.offset == 0
            await filtered.ack(result)
        else:
            assert offsets(result) == [0]
            await filtered.ack_page(result)
    finally:
        await filtered.close()


@pytest.mark.parametrize("method", ["next_record", "next_page", "try_next_page"])
async def test_given_a_delivered_read_when_cancelled_before_resuming_then_should_return_it_again(
    laser, monkeypatch, method
):
    await publish(laser, [SAFE_MODE, DECOMMISSION])
    filtered = await reader(laser, "cancelled-after-delivery", start="first", count=1)
    loop = asyncio.get_running_loop()
    callbacks = []

    def hold_delivery(callback, *args, **kwargs):
        callbacks.append((callback, args))

    try:
        with monkeypatch.context() as patch:
            patch.setattr(loop, "call_soon_threadsafe", hold_delivery)
            pending = asyncio.ensure_future(getattr(filtered, method)())
            async with asyncio.timeout(READ_TIMEOUT):
                while not callbacks:
                    await asyncio.sleep(0.001)
        for callback, args in callbacks:
            callback(*args)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        result = await asyncio.wait_for(getattr(filtered, method)(), READ_TIMEOUT)
        if method == "next_record":
            assert result.offset == 0, "the delivered record was given back"
            await filtered.ack(result)
        else:
            assert offsets(result) == [0], "the delivered page was given back"
            await filtered.ack_page(result)
    finally:
        await filtered.close()


async def test_given_typed_custom_headers_when_filtered_then_should_preserve_types_and_payload(
    laser,
):
    producer = laser.topic(TOPIC).producer(partition=0, partitions=1)
    for priority in [("uint8", 1), ("uint8", 2), "2"]:
        await producer.send(
            b"\xff\x00",
            headers={
                "routing.priority": priority,
                "armed": True,
                "temperature": ("float32", -1.5),
                "sequence": ("uint64", 2**64 - 1),
            },
        )
    filter = ls.ConsumerFilter.headers_only(
        ls.FilterExpr.all(
            [
                ls.FilterExpr.header("routing.priority", "eq", 2),
                ls.FilterExpr.header("armed", "eq", True),
                ls.FilterExpr.header("temperature", "lt", 0),
                ls.FilterExpr.header("sequence", "gt", 2**63 - 1),
            ]
        )
    )
    filtered = await laser.filters().reader(
        laser.default_stream,
        TOPIC,
        consumer="typed-headers",
        filter=filter,
        start="first",
        local_guard=True,
    )
    try:
        record = await asyncio.wait_for(filtered.next_record(), READ_TIMEOUT)
        assert record.offset == 1
        assert record.message.payload == b"\xff\x00"
        assert record.message.header_kinds["routing.priority"] == "uint8"
        await filtered.ack(record)
        assert await filtered.try_next_page() is None
    finally:
        await filtered.close()


async def test_given_a_fresh_filtered_reader_when_reading_next_then_should_start_at_zero(laser):
    await publish(laser, [SAFE_MODE, DECOMMISSION])
    first = await reader(laser, "fresh-next", count=1)
    zero = await asyncio.wait_for(first.next_record(), READ_TIMEOUT)
    assert zero.offset == 0
    await first.ack(zero)
    await first.close()
    resumed = await reader(laser, "fresh-next", count=1)
    assert (await asyncio.wait_for(resumed.next_record(), READ_TIMEOUT)).offset == 1
    await resumed.close()


@pytest.mark.parametrize("method", ["next_record", "next_page", "try_next_page"])
async def test_given_a_read_when_cancelled_before_start_then_should_not_consume_it(laser, method):
    await publish(laser, [SAFE_MODE])
    filtered = await reader(laser, f"cancel-before-start-{method}", start="first")
    try:
        pending = asyncio.ensure_future(getattr(filtered, method)())
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await asyncio.sleep(0.05)
        result = await asyncio.wait_for(getattr(filtered, method)(), READ_TIMEOUT)
        if method == "next_record":
            assert result.offset == 0
            await filtered.ack(result)
        else:
            assert offsets(result) == [0]
            await filtered.ack_page(result)
    finally:
        await filtered.close()


def test_given_shared_codec_cases_when_evaluated_locally_then_should_match_rust_and_typescript():
    corpus = json.loads(
        (
            Path(__file__).resolve().parents[3] / "wire" / "fixtures" / "filter_codec_cases.json"
        ).read_text()
    )
    for case in corpus["cases"]:
        headers = {}
        for header in case["headers"]:
            value = header["value"]
            headers[header["key"]] = (
                ("uint64", value["value"]) if value["kind"] == "uint" else value["value"]
            )
        filter = ls.ConsumerFilter.from_dict(case["filter"])
        assert (
            filter.evaluate(
                bytes.fromhex(case["payload_hex"]),
                headers,
                schemas=corpus["schemas"],
                max_payload_bytes=case["max_payload_bytes"],
                max_depth=case["max_depth"],
            )
            == case["expected"]
        ), case["name"]


@pytest.mark.parametrize("codec", ["cbor", "avro", "protobuf"])
async def test_given_a_saved_binary_filter_when_bound_then_should_preview_guard_pause_and_ack(
    laser, codec
):
    if not (await laser.capabilities()).filters_catalog:
        pytest.skip("requires a managed plane")
    corpus = json.loads(
        (Path(__file__).resolve().parents[3] / "wire/fixtures/filter_codec_cases.json").read_text()
    )
    case = next(case for case in corpus["cases"] if case["name"] == f"{codec} exact 42")
    definition = case["filter"]
    schema_id = None
    binding = None
    saved = None
    filtered = None
    try:
        if definition["schema_refs"]:
            source = next(
                schema["source"]
                for schema in corpus["schemas"]
                if schema["id"] == definition["schema_refs"][0]
            )
            schema_id = await laser.register_schema(source)

            async def visible():
                while await laser.get_schema(schema_id) is None:
                    await asyncio.sleep(0.05)

            await asyncio.wait_for(visible(), READ_TIMEOUT)
            definition["schema_refs"] = [schema_id]
        headers = {} if schema_id is None else {"agdx.sid": ("uint32", schema_id)}
        payload = bytes.fromhex(case["payload_hex"])
        filter = ls.ConsumerFilter.from_dict(definition)
        filters = laser.filters()
        sample = await filters.test(payload, filter=filter, headers=headers)
        assert sample["explanation"]["verdict"] == "selected"
        await laser.topic(TOPIC).producer(partition=0, partitions=1).send(payload, headers=headers)
        preview = await filters.preview(laser.default_stream, TOPIC, 0, filter=filter)
        assert preview["matched"] == 1
        saved = await filters.register(f"{laser.default_stream}-{codec}", filter)
        binding = await filters.create_consumer_group(
            laser.default_stream, TOPIC, f"{codec}-readers", saved["filter_id"], saved["revision"]
        )
        filtered = await filters.reader(
            laser.default_stream,
            TOPIC,
            group_id=binding["identity"]["group_id"],
            start="first",
            local_guard=True,
        )
        record = await asyncio.wait_for(filtered.next_record(), READ_TIMEOUT)
        assert record.message.payload == payload
        assert record.offset == 0
        await filters.set_revision_enabled(saved["filter_id"], saved["revision"], False)
        await filtered.ack(record)
        with pytest.raises(ls.FilterError) as stopped:
            await filtered.try_next_page()
        assert stopped.value.reason == "revision_disabled"
        await filters.set_revision_enabled(saved["filter_id"], saved["revision"], True)
    finally:
        if filtered is not None:
            await filtered.close()
        if binding is not None:
            await laser.filters().unbind_binding(binding)
        if saved is not None:
            await laser.filters().archive(saved["filter_id"])
            await laser.filters().delete(saved["filter_id"])
        if schema_id is not None:
            await laser.drop_schema(schema_id)


async def test_given_pass_faults_when_guarded_then_should_verify_server_bounds(laser):
    await publish(laser, ["broken JSON", SAFE_MODE])
    definition = ls.ConsumerFilter.json(ls.FilterExpr.present("table"), fault_policy="pass")
    filtered = await laser.filters().reader(
        laser.default_stream,
        TOPIC,
        consumer="guarded-pass",
        filter=definition,
        start="first",
        local_guard=True,
        count=2,
    )
    page = await asyncio.wait_for(filtered.next_page(), READ_TIMEOUT)
    assert [(record.offset, record.evaluated) for record in page.records] == [(0, False), (1, True)]
    await filtered.ack_page(page)
    await filtered.close()


async def test_given_a_raised_page_bound_when_80_pages_are_outstanding_then_should_ack_all(laser):
    await publish(laser, [SAFE_MODE.encode()] * 80)
    filtered = await reader(laser, "large-window", count=1, max_unacked_pages=80)
    pages = []
    try:
        for offset in range(80):
            page = await asyncio.wait_for(filtered.next_page(), READ_TIMEOUT)
            assert [record.offset for record in page.records] == [offset]
            pages.append(page)
        await filtered.ack_through(pages[-1].records[-1])
    finally:
        await filtered.close()
