import asyncio
import json
import os
import sys
from pathlib import Path

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

EXAMPLES = Path(__file__).parents[3] / "examples" / "python"
PROGRAM_TIMEOUT_SECONDS = 60


async def test_given_the_standalone_workflow_when_run_then_should_print_three_completed_local_steps(
    iggy_endpoint,
):
    report = await printed(iggy_endpoint, "workflow.py")
    async with await ls.Laser.connect(iggy_endpoint) as laser:
        await laser.stream("pipeline").delete()

    assert report == {
        "status": "completed",
        "steps": ["ingest", "analyze", "report"],
        "report": "report on analyzed frames from sat-7",
    }


async def test_given_the_standalone_memory_correction_when_run_then_should_recall_the_correction_first(  # noqa: E501
    iggy_endpoint,
):
    assert await printed(iggy_endpoint, "memory_correction.py") == {
        "user": "user-42",
        "query": "Where does the user live, and what should I know before booking dinner?",
        "facts": ["Lives in Lisbon", "Prefers a window seat", "Allergic to peanuts"],
    }


async def printed(endpoint: str, program: str) -> dict:
    environment = {key: value for key, value in os.environ.items() if key != "LASER_STREAM"}
    environment["LASER_CONNECTION_STRING"] = endpoint
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        str(EXAMPLES / program),
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), PROGRAM_TIMEOUT_SECONDS)
    assert process.returncode == 0, f"{program} exited {process.returncode}:\n{stderr.decode()}"
    return json.loads(stdout)
