# python -m pip install laser-sdk
# Needs only LASER_CONNECTION_STRING. No server to start when it is set.
import json
import time

import laser_sdk as ls


@ls.step
def ingest():
    return "frames from sat-7"


@ls.step
def analyze(frames):
    time.sleep(1)
    return f"analyzed {frames}"


@ls.step
def report(analysis):
    return f"report on {analysis}"


@ls.workflow
def pipeline():
    return report(analyze(ingest()))


run = pipeline.run()
print(json.dumps({"status": run.status, "steps": run.completed_steps, "report": run.value}))
