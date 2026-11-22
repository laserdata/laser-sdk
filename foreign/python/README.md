# LaserData - Laser SDK

Durable workflows and long-term memory for AI agents in Python, with sessions, key-value state, queries, and typed streaming on Apache Iggy. [LaserData, Inc.](https://laserdata.com) maintains it under Apache-2.0. Guides: [docs.laserdata.com/laser-sdk](https://docs.laserdata.com/laser-sdk).

## Install

```bash
python -m pip install laser-sdk
```

## A three-step workflow

```python
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
```

It prints `{"status": "completed", "steps": ["ingest", "analyze", "report"], "report": "report on analyzed frames from sat-7"}`. `pipeline.run()` returns a `WorkflowOutcome` with `run_id`, `status`, `completed_steps` and `value`, and calling `pipeline()` returns the function's value.

## A memory correction

```python
# python -m pip install laser-sdk
# Needs only LASER_CONNECTION_STRING, which connect_env() reads. No server to start when it is set.
import asyncio
import json

import laser_sdk as ls

USER = "user-42"
QUERY = "Where does the user live, and what should I know before booking dinner?"


async def main():
    async with await ls.Laser.connect_env() as root:
        async with root.scratch_stream("memory") as scratch:
            memory = scratch.laser.memory("assistant")

            # Conversation 1: the user shares three facts.
            first = ls.new_conversation_id()
            city = await memory.remember("Lives in Gdansk", conversation=first, user=USER)
            await memory.remember("Allergic to peanuts", conversation=first, user=USER)
            await memory.remember("Prefers a window seat", conversation=first, user=USER)

            # Conversation 2: the user moved. supersede stores the new fact and forgets the old one.
            second = ls.new_conversation_id()
            await memory.supersede(city, "Lives in Lisbon", conversation=second, user=USER)

            # Recall for the user across every conversation.
            facts = await memory.recall(user=USER, semantic=QUERY, limit=10)
            print(
                json.dumps(
                    {"user": USER, "query": QUERY, "facts": [fact.text() for fact in facts]},
                    indent=2,
                )
            )


asyncio.run(main())
```

It prints `{"user": "user-42", "query": "Where does the user live, and what should I know before booking dinner?", "facts": ["Lives in Lisbon", "Prefers a window seat", "Allergic to peanuts"]}`. Gdansk is gone.

## How a workflow runs

The `WorkflowOutcome` also carries `run_id`, `value` (what the workflow function returned), `outputs`, each step's output by label as bytes, `json(label)`, and `text(label)`. A step that raises ends the run as failed and the same exception reaches the caller, with no outcome to read the run ID from, so a caller that wants to resume a failed run names the run ID first with `pipeline.resume(run_id)`. The completed steps stay journaled, so that resume runs the failed step and what follows it.

`@ls.step` makes a plain function a step, `@ls.workflow` makes a plain function a workflow, and the workflow calls its steps in ordinary code. `pipeline.run()` connects, runs the function, closes the connection, and returns the `WorkflowOutcome`. Calling `pipeline()` returns the function's value instead. A step takes and returns what JSON holds: dict, list, str, int, float, bool, or None. Each completed step is journaled, so `pipeline.resume(run_id)` returns the recorded value without calling that step again. A call is known by its step name and its arguments, and calls with equal arguments take their records in call order, so a resumed workflow calls the same steps as before. A step called on another thread belongs to the workflow only through its context: use `asyncio.to_thread`, or `contextvars.copy_context().run`. A step name holding `#` is refused, because the journal numbers repeated calls with it. An `async def` workflow is awaited, and its steps can be `async def` too. For steps routed to agents, verifiers, compensation and budgets, use the builder, `Laser.workflow(name).step(...)`.

## How memory recall works

A first conversation stores three facts about the user `user-42`. A later conversation corrects the city with `supersede`, which remembers the new fact first and then forgets the old one in every conversation. A recall for the user reads every conversation and returns a list of `MemoryItem`, ranked by the words each fact shares with the query, newest first among equals, and drops none. It waits for the writes this connection made, so the correction shows at once. On LaserData Cloud and Laser Stack recall reads the managed memory view. Apache Iggy serves no view, so there recall folds the memory topic in process, and the same program runs on every server.

On a client `memory(...)`, `workflow(...)`, the builder's `step(...)` and `sessions(...)` return at once, and `scratch_stream(prefix)` is entered with `async with`. `Laser.connect_env()`, `memory_topic(...)` and every call that reads or writes (`remember`, `supersede`, `recall`, `forget`, the builder's `run()`, `bootstrap`) return an awaitable. Both programs are in [`examples/python`](https://github.com/laserdata/laser-sdk/blob/main/examples/python/README.md) as `workflow.py` and `memory_correction.py`.

## Streams and the connection string

Each program runs as written and reads the connection string from `LASER_CONNECTION_STRING`. `python -c "import laser_sdk; print(laser_sdk.__doc__)"` prints the package's own summary of the workflow, memory, and session calls, with what each one takes and returns. The workflow runs on the stream named by `LASER_STREAM`, or on a stream named after the workflow, and keeps that stream after the run so the run can be resumed. The memory program creates a uniquely named scratch stream and deletes it when the `async with` block ends, also when the block raised. With `LASER_CONNECTION_STRING` set, a program needs no Docker and no local server. Then there is no server to start. A LaserData API key is not a connection string. Only without the variable, copy the connection string from the deployment's Credentials tab in the [LaserData Cloud](https://laserdata.cloud) Console, or run `./scripts/up` in a [Laser Stack](https://github.com/laserdata/laser-stack) checkout and use the `LASER_CONNECTION_STRING` it prints. Never print or log it. To keep what the memory program writes, set `LASER_STREAM`, connect with `Laser.connect_env(stream=name)`, or call `laser.with_default_stream(name)` instead of opening a scratch stream. A workflow run and the first memory write create a missing stream. The guides are at [docs.laserdata.com/laser-sdk](https://docs.laserdata.com/laser-sdk) and on [Context7](https://context7.com/laserdata/laser-sdk).

> This source tree is the `0.11.0` release. The wire contract and public API use semantic versioning. Before `1.0.0`, minor releases can contain breaking changes. Wheels ship for Linux (x86_64, aarch64), macOS (one universal2 wheel for Intel and Apple Silicon), and Windows (x64), for Python 3.10 and later.

## Reference

Every other call is in the [reference](https://github.com/laserdata/laser-sdk/blob/main/foreign/python/REFERENCE.md): connecting, publishing and consuming, typed topics, queries, key-value state, consumer filters, the knowledge graph, agents, sessions, the change feed, edge interop, and errors. The guides at [docs.laserdata.com/laser-sdk](https://docs.laserdata.com/laser-sdk) show each feature in TypeScript, Rust, and Python.

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.
