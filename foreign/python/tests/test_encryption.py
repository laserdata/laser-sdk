import asyncio

import laser_sdk as ls
import pytest

KEY = b"laser-encryption-test-key-32byte"
PLAINTEXT = b'{"host":"node-7","cpu":82}'
# Sealed with KEY by Apache Iggy's Rust encryptor and by Node's AES-256-GCM.
IGGY_CIPHERTEXT = (
    "2cf9899e78c2d7e11900d208ccc590f5ca112bc7f67b1d5180fba3d30354613f"
    "a3c954477cc01d73545c7105dce1ac30680fe7a79699"
)
NODE_CIPHERTEXT = (
    "bfafee07d0742f433ed130c340127691377c4f54776eda6e40a841af10d5b4c9"
    "50612150a76700ff3b017a802971514632695045f015"
)


def test_given_the_shared_vectors_when_decrypting_then_should_read_the_plaintext():
    encryption = ls.Encryption.aes256_gcm(KEY)
    for vector in (IGGY_CIPHERTEXT, NODE_CIPHERTEXT):
        assert encryption.decrypt(bytes.fromhex(vector)) == PLAINTEXT


def test_given_a_key_when_encrypting_then_should_decrypt_with_a_fresh_nonce_each_time():
    encryption = ls.Encryption.aes256_gcm(KEY)
    first = encryption.encrypt(PLAINTEXT)
    second = encryption.encrypt(PLAINTEXT)
    assert len(first) == 12 + len(PLAINTEXT) + 16
    assert first != second
    assert encryption.decrypt(first) == PLAINTEXT


def test_given_another_key_when_decrypting_then_should_raise_iggy_error_72():
    sealed = ls.Encryption.aes256_gcm(KEY).encrypt(PLAINTEXT)
    with pytest.raises(ls.TransportError) as raised:
        ls.Encryption.aes256_gcm(bytes(32)).decrypt(sealed)
    assert raised.value.iggy_error_code == 72


def test_given_a_short_key_when_building_then_should_raise_invalid_error():
    with pytest.raises(ls.InvalidError):
        ls.Encryption.aes256_gcm(bytes(16))


def test_given_an_encryption_when_printed_then_should_not_show_the_key():
    assert repr(ls.Encryption.aes256_gcm(KEY)) == "Encryption(Aes256Gcm)"


@pytest.mark.integration
async def test_given_an_encrypted_producer_when_read_with_and_without_the_key_then_should_store_only_ciphertext(  # noqa: E501
    laser,
):
    topic = laser.topic("sealed")
    encryption = ls.Encryption.aes256_gcm(KEY)
    producer = topic.producer(partition=0, partitions=1, encryption=encryption)
    await producer.send(PLAINTEXT, headers={"source": "node-7"})

    plain = topic.consumer("without-key", partition=0, polling="first")
    try:
        stored = await asyncio.wait_for(plain.next(), 10)
    finally:
        await plain.shutdown()
    assert bytes(stored.payload) != PLAINTEXT
    assert encryption.decrypt(stored.payload) == PLAINTEXT
    assert stored.headers == {}

    opened = topic.consumer("with-key", partition=0, polling="first", encryption=encryption)
    try:
        record = await asyncio.wait_for(opened.next(), 10)
    finally:
        await opened.shutdown()
    assert bytes(record.payload) == PLAINTEXT
    assert record.headers == {"source": "node-7"}


@pytest.mark.integration
async def test_given_an_encrypted_group_consumer_when_reading_then_should_see_the_plaintext(laser):
    topic = laser.topic("sealed-group")
    encryption = ls.Encryption.aes256_gcm(KEY)
    await topic.producer(partitions=1, encryption=encryption).send(PLAINTEXT)
    consumer = topic.consumer_group("sealed-readers").consumer(
        polling="first", encryption=encryption
    )
    try:
        record = await asyncio.wait_for(consumer.next(), 10)
    finally:
        await consumer.shutdown()
    assert bytes(record.payload) == PLAINTEXT


@pytest.mark.integration
async def test_given_a_record_that_does_not_decrypt_when_read_then_should_report_it_once_and_read_past(  # noqa: E501
    laser,
):
    topic = laser.topic("sealed-mixed")
    encryption = ls.Encryption.aes256_gcm(KEY)
    await topic.producer(partition=0, partitions=1).send(b"not sealed")
    await topic.producer(partition=0, partitions=1, encryption=encryption).send(PLAINTEXT)
    consumer = topic.consumer("mixed", partition=0, polling="first", encryption=encryption)
    try:
        with pytest.raises(ls.TransportError) as raised:
            await asyncio.wait_for(consumer.next(), 10)
        assert raised.value.iggy_error_code == 72
        record = await asyncio.wait_for(consumer.next(), 10)
    finally:
        await consumer.shutdown()
    assert bytes(record.payload) == PLAINTEXT
    assert record.position.offset == 1


@pytest.mark.integration
async def test_given_an_encrypted_consumer_when_it_commits_on_polling_then_should_refuse_it(laser):
    consumer = laser.topic("sealed-polling").consumer(
        "polling",
        partition=0,
        auto_commit="polling",
        encryption=ls.Encryption.aes256_gcm(KEY),
    )
    with pytest.raises(ls.InvalidError):
        await consumer.init()


@pytest.mark.integration
async def test_given_a_sharding_callable_when_sending_in_background_then_should_route_each_send_through_it(  # noqa: E501
    laser,
):
    topic = laser.topic("sharded")
    seen = []

    def sharding(request):
        seen.append((request.shards, request.stream, request.topic, request.records))
        return request.records % request.shards

    producer = topic.producer(
        partition=0,
        partitions=1,
        background=ls.BackgroundConfig(num_shards=4),
        sharding=sharding,
    )
    await producer.send_batch([b"one", b"two", b"three"])
    await producer.shutdown()
    assert seen == [(4, laser.default_stream, "sharded", 3)]

    consumer = topic.consumer("sharded-reader", partition=0, polling="first")
    try:
        payloads = [bytes((await asyncio.wait_for(consumer.next(), 10)).payload) for _ in range(3)]
    finally:
        await consumer.shutdown()
    assert payloads == [b"one", b"two", b"three"]


@pytest.mark.integration
async def test_given_a_sharding_callable_when_it_raises_then_should_fail_the_send(laser):
    def sharding(_request):
        raise ValueError("no shard for this send")

    producer = laser.topic("sharded-raises").producer(
        partitions=1, background=ls.BackgroundConfig(num_shards=2), sharding=sharding
    )
    with pytest.raises(ls.LaserError, match="no shard for this send"):
        await producer.send(b"reading")
    await producer.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize(
    ("name", "shard"),
    [("past-last", 2), ("negative", -1), ("bool", False), ("float", 1.0)],
)
async def test_given_a_sharding_callable_when_it_returns_no_shard_index_then_should_fail_the_send_and_queue_nothing(  # noqa: E501
    laser, name, shard
):
    topic = laser.topic(f"sharded-refused-{name}")
    picks = iter([shard, 1])
    producer = topic.producer(
        partition=0,
        partitions=1,
        background=ls.BackgroundConfig(num_shards=2),
        sharding=lambda _request: next(picks),
    )
    with pytest.raises(ls.InvalidError):
        await producer.send(b"refused")
    await producer.send(b"accepted")
    await producer.shutdown()

    consumer = topic.consumer(f"sharded-refused-{name}-reader", partition=0, polling="first")
    try:
        record = await asyncio.wait_for(consumer.next(), 10)
    finally:
        await consumer.shutdown()
    assert bytes(record.payload) == b"accepted"


@pytest.mark.integration
async def test_given_a_sharding_callable_without_background_mode_when_building_then_should_refuse_it(  # noqa: E501
    laser,
):
    producer = laser.topic("sharded-direct").producer(sharding=lambda _request: 0)
    with pytest.raises(ls.InvalidError):
        await producer.init()


@pytest.mark.integration
async def test_given_a_sharding_value_that_is_not_callable_when_building_then_should_refuse_it(
    laser,
):
    with pytest.raises(ls.InvalidError):
        laser.topic("sharded-value").producer(
            background=ls.BackgroundConfig(num_shards=2), sharding=3
        )
