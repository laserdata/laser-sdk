import ast
import pathlib

import laser_sdk as ls
import pytest

STUB = pathlib.Path(__file__).resolve().parents[1] / "laser_sdk.pyi"
AWAITED_FACTORIES = (
    ls.Laser.connect,
    ls.Laser.connect_env,
    ls.Laser.local,
    ls.Laser.connect_with_stream,
    ls.Laser.memory_topic,
)


def test_given_the_package_when_its_doc_is_read_then_should_open_with_the_three_task_programs():
    doc = ls.__doc__
    for line in (
        "async with await ls.Laser.connect_env() as root:",
        'async with root.scratch_stream("demo") as scratch:',
        '.step("analyze", after=["ingest"], run=analyze)',
        'memory = laser.memory("assistant")',
        "await memory.supersede(city,",
        "sessions = laser.sessions()",
        "https://context7.com/laserdata/laser-sdk",
    ):
        assert line in doc


def test_given_the_package_doc_when_read_then_should_end_with_the_install_and_summary_command():
    assert ls.__doc__.rstrip().endswith(
        'python -m pip install laser-sdk && python -c "import laser_sdk; print(laser_sdk.__doc__)"'
    )


def test_given_the_stub_when_parsed_then_should_carry_the_package_doc():
    assert ast.get_docstring(ast.parse(STUB.read_text())) == ls.__doc__.strip()


def test_given_an_awaitable_factory_when_its_doc_is_read_then_should_say_await_it_first():
    for factory in AWAITED_FACTORIES:
        assert factory.__doc__.startswith("Await it"), factory.__qualname__


async def test_given_connect_env_not_awaited_when_used_as_a_client_then_should_name_the_await(
    monkeypatch,
):
    monkeypatch.delenv("LASER_CONNECTION_STRING", raising=False)
    pending = ls.Laser.connect_env()
    with pytest.raises(AttributeError, match=r"laser = await Laser\.connect_env\(\)"):
        pending.workflow("jobs")
    assert not hasattr(pending, "memory")
    assert "await Laser.connect_env()" in repr(pending)
    with pytest.raises(ls.ConfigError):
        await pending


async def test_given_connect_env_not_awaited_when_entered_then_should_name_the_await(monkeypatch):
    monkeypatch.delenv("LASER_CONNECTION_STRING", raising=False)
    pending = ls.Laser.connect_env()
    with pytest.raises(TypeError, match=r"async with await Laser\.connect_env\(\) as laser:"):
        async with pending:
            pass
    with pytest.raises(ls.ConfigError):
        await pending


async def test_given_no_connection_string_when_connecting_from_env_with_a_stream_then_should_raise_config_error(  # noqa: E501
    monkeypatch,
):
    monkeypatch.delenv("LASER_CONNECTION_STRING", raising=False)
    with pytest.raises(ls.ConfigError, match="LASER_CONNECTION_STRING is not set"):
        await ls.Laser.connect_env(stream="picked")


async def test_given_an_empty_connection_string_when_connecting_from_env_then_should_raise_config_error(  # noqa: E501
    monkeypatch,
):
    monkeypatch.setenv("LASER_CONNECTION_STRING", "")
    with pytest.raises(ls.ConfigError, match="LASER_CONNECTION_STRING is not set"):
        await ls.Laser.connect_env()


@pytest.mark.integration
async def test_given_an_empty_stream_when_connecting_from_env_then_should_leave_no_default_stream(
    iggy_endpoint, monkeypatch
):
    monkeypatch.setenv("LASER_CONNECTION_STRING", iggy_endpoint)
    monkeypatch.setenv("LASER_STREAM", "")
    async with await ls.Laser.connect_env() as laser:
        assert laser.default_stream is None


@pytest.mark.integration
async def test_given_a_stream_when_connecting_from_env_then_should_pin_it_over_laser_stream(
    iggy_endpoint, monkeypatch
):
    monkeypatch.setenv("LASER_CONNECTION_STRING", iggy_endpoint)
    monkeypatch.setenv("LASER_STREAM", "from-env")
    async with (
        await ls.Laser.connect_env(stream="picked") as picked,
        await ls.Laser.connect_env() as from_env,
    ):
        assert [picked.default_stream, from_env.default_stream] == ["picked", "from-env"]


@pytest.mark.integration
async def test_given_memory_topic_not_awaited_when_remembering_then_should_name_the_await(laser):
    pending = laser.memory_topic("agent.memory")
    with pytest.raises(AttributeError, match=r"memory = await laser\.memory_topic\(\.\.\.\)"):
        pending.remember("Sam lives in Berlin", user="sam")
    memory = await pending
    assert await memory.remember("Sam lives in Berlin", user="sam")
