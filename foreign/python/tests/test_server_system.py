import asyncio
import http.client
import json
import uuid

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

UNKNOWN_METHOD = {"jsonrpc": "2.0", "id": 1, "method": "nope"}


def _with_credentials(endpoint, credentials):
    return f"{credentials}@{endpoint.split('@', 1)[1]}"


def _call(server, method, path, headers=None, body=None):
    connection = http.client.HTTPConnection(server.host, server.port, timeout=10)
    try:
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        return response.status, response.read().decode()
    finally:
        connection.close()


async def _rpc(server, message, headers=None):
    return await asyncio.to_thread(
        _call,
        server,
        "POST",
        "/",
        {"content-type": "application/json", **(headers or {})},
        json.dumps(message),
    )


async def test_given_a_server_when_topic_options_are_described_then_should_list_their_rules(
    laser,
):
    options = await laser.describe_options("topic")
    segment_size = next(option for option in options if option.key == "segment_size")
    assert segment_size.kind == "uint64"
    assert segment_size.default_value is not None
    assert segment_size.description


async def test_given_an_unknown_scope_when_options_are_described_then_should_raise_invalid(laser):
    with pytest.raises(ls.InvalidError):
        await laser.describe_options("cluster")


async def test_given_a_new_stream_when_changes_are_read_then_should_report_its_creation(
    laser,
):
    latest = await laser.metadata_changes(ls.MetadataChangeStart.latest())
    assert latest.status == "ok"
    assert latest.next is not None
    name = f"changes-{uuid.uuid4().hex[:12]}"
    await laser.stream(name).ensure()
    try:
        page = await laser.metadata_changes(
            ls.MetadataChangeStart.after(latest.next), groups=["streams"]
        )
        created = next(record for record in page.records if record.subject.name == name)
        assert created.kind == "stream_created"
        assert created.subject.stream_id is not None
        assert created.timestamp_micros > 0
    finally:
        await laser.stream(name).delete()


async def test_given_a_malformed_start_when_changes_are_read_then_should_raise_invalid(
    laser,
):
    with pytest.raises(ls.InvalidError):
        await laser.metadata_changes(ls.MetadataChangeStart.after("mc1_nope"))
    with pytest.raises(ls.InvalidError):
        await laser.metadata_changes(groups=["nodes"])
    with pytest.raises(ls.InvalidError):
        await laser.metadata_changes(limit=0)


async def test_given_no_server_grants_when_changes_are_read_then_should_be_refused(
    laser, iggy_endpoint
):
    name = f"reader-{uuid.uuid4().hex[:12]}"
    await laser.user(name).create("secret-1")
    try:
        reader = await ls.Laser.connect(_with_credentials(iggy_endpoint, f"{name}:secret-1"))
        try:
            with pytest.raises(ls.TransportError) as refused:
                await reader.metadata_changes()
            assert refused.value.iggy_error_code == 41
        finally:
            await reader.close()
    finally:
        await laser.user(name).delete()


PING_CODE = 1
GET_STATS_CODE = 10
LOGIN_USER_CODE = 38
INVALID_COMMAND = 3
OPERATION_NOT_RETAINED = 14008


async def test_given_raw_commands_when_sent_then_should_return_the_reply_of_the_node_or_the_replica(
    laser,
):
    assert await laser.send_command(PING_CODE, b"") == b""
    replica = (await laser.cluster()).nodes[0].replica_id
    assert await laser.send_command_to_replica(GET_STATS_CODE, b"", replica)
    with pytest.raises(ls.TransportError) as refused:
        await laser.send_command(LOGIN_USER_CODE, b"")
    assert refused.value.iggy_error_code == INVALID_COMMAND


async def test_given_an_operation_the_client_does_not_retain_when_settled_then_should_name_it(
    laser,
):
    operation = 2**64 - 8
    for settle in (
        lambda: laser.resolve_operation(operation, 5_000),
        lambda: laser.abandon_operation(operation),
    ):
        with pytest.raises(ls.TransportError) as refused:
            await settle()
        assert refused.value.iggy_error_code == OPERATION_NOT_RETAINED
        assert refused.value.uncertain_operation == operation


async def test_given_a_negative_timeout_when_an_operation_is_resolved_then_should_raise_invalid(
    laser,
):
    with pytest.raises(ls.InvalidError):
        await laser.resolve_operation(7, -1)


async def test_given_an_a2a_server_when_origins_call_it_then_should_refuse_foreign_ones(
    laser,
):
    bridge = ls.A2aBridge(
        laser,
        "a2a-edge",
        "agent.sessions",
        "agent.sessions",
        allowed_origins=["https://app.example.com"],
    )
    server = await bridge.serve()
    try:
        status, body = await _rpc(server, UNKNOWN_METHOD)
        assert status == 200
        assert json.loads(body)["error"]["code"] == -32601
        assert (await _rpc(server, UNKNOWN_METHOD, {"origin": "https://app.example.com"}))[0] == 200
        assert (await _rpc(server, UNKNOWN_METHOD, {"origin": "https://evil.example"}))[0] == 403
        assert (await _rpc(server, UNKNOWN_METHOD, {"origin": server.url}))[0] == 200
        assert (await asyncio.to_thread(_call, server, "GET", "/"))[0] == 405
        assert (await asyncio.to_thread(_call, server, "GET", "/.well-known/agent-card.json"))[
            0
        ] == 404
    finally:
        await server.close()


async def test_given_a_public_url_when_the_a2a_card_is_fetched_then_should_serve_it(
    laser,
):
    bridge = ls.A2aBridge(
        laser,
        "a2a-edge",
        "agent.sessions",
        "agent.sessions",
        public_url="https://bridge.example/a2a",
    )
    server = await bridge.serve()
    try:
        status, body = await asyncio.to_thread(
            _call,
            server,
            "GET",
            "/.well-known/agent-card.json",
            {"origin": "https://evil.example"},
        )
        assert status == 200
        assert json.loads(body)["supportedInterfaces"][0]["url"] == "https://bridge.example/a2a"
    finally:
        await server.close()


async def test_given_a_caller_hook_when_an_a2a_request_arrives_then_should_name_the_caller(
    laser,
):
    seen = []

    async def caller(headers):
        token = headers.get("x-caller")
        if token == "bad":
            raise PermissionError("refused")
        seen.append(token)
        return token

    bridge = ls.A2aBridge(laser, "a2a-edge", "agent.sessions", "agent.sessions")
    server = await bridge.serve(caller=caller)
    try:
        assert (await _rpc(server, UNKNOWN_METHOD, {"X-Caller": "alice"}))[0] == 200
        assert seen == ["alice"]
        assert (await _rpc(server, UNKNOWN_METHOD, {"x-caller": "bad"}))[0] == 401
    finally:
        await server.close()


async def test_given_an_mcp_server_when_messages_arrive_then_should_answer_or_accept_them(
    laser,
):
    bridge = ls.McpBridge(
        laser,
        "mcp-edge",
        "agent.sessions",
        "agent.sessions",
        "tools",
        allowed_origins=["https://app.example.com"],
    )
    server = await bridge.serve()
    try:
        status, body = await _rpc(server, {"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        assert status == 200
        assert "result" in json.loads(body)
        status, body = await _rpc(server, {"jsonrpc": "2.0", "method": "notifications/initialized"})
        assert (status, body) == (202, "")
        ping = {"jsonrpc": "2.0", "id": 2, "method": "ping"}
        assert (await _rpc(server, ping, {"mcp-protocol-version": "1999-01-01"}))[0] == 400
        assert (await _rpc(server, ping, {"mcp-protocol-version": "2025-06-18"}))[0] == 200
        assert (await _rpc(server, ping, {"origin": "https://evil.example"}))[0] == 403
        assert (await _rpc(server, ping, {"origin": "https://app.example.com"}))[0] == 200
    finally:
        await server.close()
