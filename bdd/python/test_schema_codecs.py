import json
from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

scenarios(str(Path(__file__).parent.parent / "scenarios/schema_codecs.feature"))


@given(parsers.parse('a "{codec}" writer schema'))
def writer_schema(bench, codec):
    source = {
        "avro_long": {"kind": "avro", "schema": '"long"'},
        "json_string": {"kind": "json_schema", "schema": '{"type":"string"}'},
    }[codec]
    bench.writer_schema = ls.CompiledSchema.compile(source)


@when(parsers.parse('I decode schema payload "{payload}"'))
def decode_payload(bench, payload):
    try:
        bench.writer_schema.decode(bytes(json.loads(payload)))
        bench.writer_payload_rejected = False
    except ls.CodecError:
        bench.writer_payload_rejected = True


@then(parsers.parse("the schema payload is {outcome}"))
def payload_outcome(bench, outcome):
    assert bench.writer_payload_rejected == (outcome == "rejected")


@when("I decode every case of the shared writer schema corpus")
def decode_writer_corpus(bench):
    corpus = json.loads(
        (Path(__file__).parent.parent.parent / "wire/fixtures/writer_schema_cases.json").read_text()
    )
    schemas = {schema["id"]: schema["source"] for schema in corpus["schemas"]}
    failures = []
    for case in corpus["cases"]:
        try:
            compiled = ls.CompiledSchema.compile(schemas[case["schema"]], id=case["schema"])
        except ls.InvalidError:
            if not case.get("refused"):
                failures.append(case["name"])
            continue
        try:
            value = compiled.decode(bytes.fromhex(case.get("payload_hex", "")))
            matches = not case.get("refused") and value == case.get("expected")
        except ls.CodecError:
            matches = not case.get("refused") and "expected" not in case
        if not matches:
            failures.append(case["name"])
    bench.writer_corpus_failures = failures


@then("every corpus case matches its expected value")
def writer_corpus_matches(bench):
    assert bench.writer_corpus_failures == []
