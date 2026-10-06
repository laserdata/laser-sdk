"""The same fleet reading in three binary payload formats."""

import asyncio
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path

import _common
import cbor2
import laser_sdk as ls
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

ASSETS = Path(__file__).resolve().parents[1] / "shared"


@dataclass(frozen=True)
class Reading:
    satellite: str
    mode: str
    battery: int


async def wait_for_schema(laser: ls.Laser, schema_id: int) -> None:
    while await laser.schemas().get(schema_id) is None:
        await asyncio.sleep(0.05)


async def run_codecs(laser: ls.Laser) -> None:
    _common.phase("select typed records inside CBOR, Avro, and Protobuf payloads")
    registered = []
    try:
        for codec in ("cbor", "avro", "protobuf"):
            source = None
            message_type = None
            if codec == "avro":
                source = {"kind": "avro", "schema": (ASSETS / "fleet-reading.avsc").read_text()}
            elif codec == "protobuf":
                descriptor = (ASSETS / "fleet-reading.desc").read_bytes()
                source = {
                    "kind": "protobuf",
                    "descriptor_set": list(descriptor),
                    "message_type": "fleet.Reading",
                }
                descriptors = descriptor_pb2.FileDescriptorSet.FromString(descriptor)
                pool = descriptor_pool.DescriptorPool()
                for file in descriptors.file:
                    pool.Add(file)
                message_type = message_factory.GetMessageClass(
                    pool.FindMessageTypeByName("fleet.Reading")
                )
            schema_id = None
            compiled = None
            if source is not None:
                schema_id = await laser.schemas().register(source, name=f"fleet-{codec}")
                registered.append(schema_id)
                await asyncio.wait_for(wait_for_schema(laser, schema_id), 15)
                compiled = ls.CompiledSchema.compile(source, id=schema_id)
            topic_name = f"fleet_{codec}"
            topic = laser.topic(topic_name)
            await topic.ensure(1)
            for mode, battery in (("nominal", 80), ("safe", 20), ("nominal", 60)):
                value = asdict(Reading("sat-042", mode, battery))
                if codec == "cbor":
                    payload = cbor2.dumps(value)
                elif codec == "avro":
                    payload = compiled.encode_avro(value)
                else:
                    payload = message_type(**value).SerializeToString()
                publish = topic.publish().raw_bytes(payload, codec)
                if schema_id is not None:
                    publish.schema_id(schema_id)
                await publish.send()
            expr = ls.FilterExpr.pred("mode", "eq", "safe")
            filter = (
                ls.ConsumerFilter.cbor(expr)
                if codec == "cbor"
                else getattr(ls.ConsumerFilter, codec)(expr, [schema_id])
            )
            group = topic.consumer_group(f"safe-{codec}")
            await group.create(filter=filter)
            reader = await group.reader(start="first", local_guard=True)
            try:
                record = await asyncio.wait_for(reader.next_record(), 15)
                value = (
                    cbor2.loads(record.message.payload)
                    if compiled is None
                    else compiled.decode(record.message.payload)
                )
                selected = Reading(**value)
                print(
                    f"  {codec}: {selected.satellite} entered {selected.mode} mode "
                    f"at {selected.battery}% battery, 1 of 3 records delivered"
                )
                await reader.ack(record)
            finally:
                await reader.close()
                await group.filter().delete()
    except BaseException:
        for schema_id in registered:
            with suppress(Exception):
                await laser.schemas().drop(schema_id)
        raise
    first_error: Exception | None = None
    for schema_id in registered:
        try:
            await laser.schemas().drop(schema_id)
        except Exception as error:
            first_error = first_error or error
    if first_error is not None:
        raise first_error
