import asyncio
import contextvars

import laser_sdk as ls
import pytest

REQUEST = contextvars.ContextVar("request")
RELEASED = bytes.fromhex("a1624f6ba16852656c6561736564f5")
RELEASE = {"v": 1, "namespace": "coord", "key": b"run", "lease_token": 7, "holder_id": "worker"}


async def test_given_a_context_variable_when_hooks_run_then_should_see_the_caller_value():
    seen = []

    def sync_embed(text):
        seen.append(("sync", REQUEST.get(None)))
        return [1.0]

    async def async_embed(text):
        seen.append(("async", REQUEST.get(None)))
        return [1.0]

    REQUEST.set("caller")
    await ls.MemoryHandle.vector(sync_embed).remember("fact")
    await ls.MemoryHandle.vector(async_embed).remember("fact")
    assert seen == [("sync", "caller"), ("async", "caller")]


async def test_given_an_embedder_object_when_remembering_then_should_call_its_embed_method():
    class Embedder:
        def __init__(self):
            self.texts = []

        async def embed(self, text):
            self.texts.append(text)
            return [1.0]

    embedder = Embedder()
    memory = ls.MemoryHandle.vector(embedder)
    await memory.remember("fact")
    assert embedder.texts == ["fact"]


def test_given_a_value_without_the_hook_method_when_building_memory_then_should_refuse_it():
    with pytest.raises(ls.InvalidError, match="embed"):
        ls.MemoryHandle.vector(7)
    with pytest.raises(ls.InvalidError, match="rerank"):
        ls.MemoryHandle.vector(lambda _: [1.0]).reranker(object())


async def test_given_a_pending_async_embedder_when_the_caller_is_cancelled_then_should_cancel_it():
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def embed(text):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    pending = asyncio.ensure_future(ls.MemoryHandle.vector(embed).remember("fact"))
    await asyncio.wait_for(started.wait(), 5)
    pending.cancel()
    await asyncio.wait_for(cancelled.wait(), 5)


@pytest.mark.parametrize("shape", ["sync", "object"])
async def test_given_a_sync_or_object_handler_when_wrapped_then_should_run_and_remember(shape):
    memory = ls.MemoryHandle.vector(lambda _: [1.0])
    conversation = ls.new_conversation_id()
    message = ls.agent_message(b"handled", ls.Provenance(conversation_id=conversation))
    handled = []

    def handle(context, incoming):
        handled.append(incoming.payload)

    class Handler:
        def handle(self, context, incoming):
            handled.append(incoming.payload)

    handler = handle if shape == "sync" else Handler()
    await ls.MemoryHandler(handler, memory).auto_remember("message")(None, message)
    assert handled == [b"handled"]
    items = await memory.recall(conversation=conversation)
    assert [item.text() for item in items] == ["handled"]


async def test_given_a_pending_handler_when_the_wrapper_is_cancelled_then_should_cancel_it():
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handle(context, incoming):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    wrapped = ls.MemoryHandler(handle, ls.MemoryHandle.vector(lambda _: [1.0]))
    pending = asyncio.ensure_future(wrapped(None, ls.agent_message(b"x", ls.Provenance())))
    await asyncio.wait_for(started.wait(), 5)
    pending.cancel()
    await asyncio.wait_for(cancelled.wait(), 5)


async def test_given_a_sync_transport_when_mutating_then_should_use_its_replies():
    class Transport:
        def __init__(self):
            self.frames = []

        def send(self, code, frame):
            self.frames.append(frame)
            return RELEASED

        def reset(self):
            pass

    transport = Transport()
    client = ls.FencedLeaseClient(transport)
    assert await client.release(client.prepare_release(RELEASE))
    assert len(transport.frames) == 1


async def test_given_a_stalled_send_when_its_attempt_times_out_then_should_cancel_it_before_reset():
    class Stalled:
        def __init__(self):
            self.cancelled = asyncio.Event()
            self.cancelled_before_reset = None

        async def send(self, code, frame):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                raise

        async def reset(self):
            self.cancelled_before_reset = self.cancelled.is_set()

    transport = Stalled()
    client = ls.FencedLeaseClient(transport).with_attempt_timeout(0.005)
    with pytest.raises(ls.LaserError) as failure:
        await client.release(client.prepare_release(RELEASE))
    assert failure.value.ambiguous_mutation
    await asyncio.wait_for(transport.cancelled.wait(), 5)
    assert transport.cancelled_before_reset


async def test_given_a_sync_blob_store_when_claim_checking_then_should_round_trip_the_body():
    class Store:
        def __init__(self):
            self.values = {}

        def put(self, payload):
            self.values["blob:1"] = bytes(payload)
            return "blob:1"

        def get(self, reference):
            return self.values[reference]

    store = Store()
    capsule, content_type = await ls.check_in(store, 1, b"body")
    assert content_type == "ref"
    assert await ls.resolve_body(store, capsule) == b"body"


async def test_given_a_blob_store_error_when_resolving_then_should_keep_its_sdk_class():
    class Store:
        async def put(self, payload):
            return "blob:1"

        async def get(self, reference):
            raise ls.UnsupportedError("store offline")

    store = Store()
    capsule, _ = await ls.check_in(store, 1, b"body")
    with pytest.raises(ls.UnsupportedError, match="store offline"):
        await ls.resolve_body(store, capsule)
