import json
from pathlib import Path

import laser_sdk as ls

CORPUS = json.loads(
    (
        Path(__file__).resolve().parents[3] / "wire" / "fixtures" / "writer_schema_cases.json"
    ).read_text()
)


def test_given_the_writer_schema_corpus_when_decoded_then_should_match_every_expected_value():
    schemas = {schema["id"]: schema["source"] for schema in CORPUS["schemas"]}
    failures = []
    for case in CORPUS["cases"]:
        try:
            compiled = ls.CompiledSchema.compile(schemas[case["schema"]], id=case["schema"])
        except ls.InvalidError as error:
            if not case.get("refused"):
                failures.append(f"{case['name']}: {error}")
            continue
        if case.get("refused"):
            failures.append(f"{case['name']}: compiled")
            continue
        try:
            value = compiled.decode(bytes.fromhex(case.get("payload_hex", "")))
        except ls.CodecError as error:
            if "expected" in case:
                failures.append(f"{case['name']}: {error}")
            continue
        if value != case.get("expected"):
            failures.append(f"{case['name']}: {value!r}")
    assert failures == []


def test_given_a_protobuf_reading_when_decoded_then_should_keep_exact_64_bit_integers():
    source = next(schema["source"] for schema in CORPUS["schemas"] if schema["id"] == 9)
    case = next(case for case in CORPUS["cases"] if case["name"] == "protobuf common types")
    value = ls.CompiledSchema.compile(source, id=9).decode(bytes.fromhex(case["payload_hex"]))
    assert value["cpu_usage"] == -9007199254740993
    assert value["total"] == 18446744073709551615
    assert value["mode"] == 2
    assert value["digest"] == [0, 255]
