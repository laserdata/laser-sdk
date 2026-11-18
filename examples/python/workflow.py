# python -m pip install laser-sdk
# Needs only LASER_CONNECTION_STRING, which connect_env() reads. No server to start when it is set.
# Check: echo "${LASER_CONNECTION_STRING:+configured}"
import asyncio
import json

import laser_sdk as ls


async def analyze(outputs):
    await asyncio.sleep(1)
    return b"analyzed " + outputs["ingest"]


async def main():
    async with await ls.Laser.connect_env() as root:
        async with root.scratch_stream("workflow") as scratch:
            outcome = await (
                scratch.laser.workflow("pipeline")
                .step("ingest", run=lambda outputs: "frames from sat-7")
                .step("analyze", after=["ingest"], run=analyze)
                .step(
                    "report",
                    after=["analyze"],
                    run=lambda outputs: b"report on " + outputs["analyze"],
                )
                .run()
            )

    print(
        json.dumps(
            {
                "run_id": outcome.run_id,
                "status": outcome.status,
                "completed_steps": outcome.completed_steps,
                "report": outcome.text("report"),
            }
        )
    )


asyncio.run(main())
