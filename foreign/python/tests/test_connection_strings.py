import socket

import laser_sdk as ls
import pytest

SECRET = "sup3rs3cr3t"


def _leak(error):
    for text in (str(error), repr(error), *map(str, error.args)):
        if SECRET in text:
            return text
    return None


async def test_given_http_or_an_unknown_scheme_when_connecting_then_should_refuse_naming_the_binary_transports():  # noqa: E501
    with pytest.raises(ls.ConfigError, match="HTTP has no binary request path"):
        await ls.Laser.connect("iggy+http://iggy:iggy@127.0.0.1:3000")
    with pytest.raises(ls.ConfigError, match="iggy\\+quic://"):
        await ls.Laser.connect("tcp://iggy:iggy@127.0.0.1:8090")


@pytest.mark.parametrize(
    "value",
    [
        "iggy+quic://iggy:iggy@127.0.0.1?tls=true",
        "iggy+quic://iggy:iggy@127.0.0.1?validate_certificate=yes",
        "iggy+ws://iggy:iggy@127.0.0.1?nodelay=true",
        "iggy+ws://iggy:iggy@127.0.0.1?tls=true&tls_validate_certificate=false&tls_ca_file=/ca.pem",
    ],
)
async def test_given_an_option_the_transport_does_not_read_when_connecting_then_should_refuse_it(
    value,
):
    with pytest.raises(ls.ConfigError):
        await ls.Laser.connect(value)


async def test_given_a_refused_or_failed_connection_when_reported_then_should_never_echo_the_secret():  # noqa: E501
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        closed = listener.getsockname()[1]
    for value in [
        f"iggy+http://user:{SECRET}@host",
        f"iggy+quic://user:{SECRET}@host?tls=true",
        f"iggy+ws://{SECRET}@host?tls=yes",
        f"iggy+tcp://user:{SECRET}@host:99999",
        f"user:{SECRET}@host?foo=bar",
        f"iggy+tcp://user:{SECRET}@127.0.0.1:{closed}",
        f"iggy+quic://user:{SECRET}@127.0.0.1:{closed}",
        f"iggy+ws://user:{SECRET}@127.0.0.1:{closed}",
    ]:
        with pytest.raises(ls.LaserError) as failure:
            await ls.Laser.connect(value, connect_timeout_ms=300)
        assert _leak(failure.value) is None
