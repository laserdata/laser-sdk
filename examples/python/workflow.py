"""workflow: three in-process steps, journaled and resumable, printed as JSON.

Each step's body runs in this process and gets the outputs of the steps before
it. The run creates its own topics, so the program needs no bootstrap, agent or
handler. It copies alone: it imports only the SDK.

Run it:
    LASER_CONNECTION_STRING='<connection string>' python3 workflow.py

Docs: https://docs.laserdata.com/laser-sdk/workflow
"""

import asyncio
import json

from laser_sdk import Laser


async def main() -> None:
    async with await Laser.connect_env() as laser:
        async with laser.scratch_stream("workflow-demo") as scratch:
            outcome = await (
                scratch.laser.workflow("job-runner")
                .step("create", run=lambda outputs: "job-42")
                .step(
                    "process",
                    run=lambda outputs: "processed " + outputs["create"].decode(),
                    after=["create"],
                )
                .step("finish", run=lambda outputs: "completed", after=["process"])
                .run()
            )
    report = {
        "run_id": outcome.run_id,
        "status": outcome.status,
        "completed_steps": outcome.completed_steps,
        "outputs": {label: outcome.text(label) for label in outcome.completed_steps},
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
