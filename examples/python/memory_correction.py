"""memory_correction: replace a fact for one user, then recall the current facts.

The first conversation stores two facts about a user. A later conversation
corrects one of them with `supersede`, which remembers the new fact and forgets
the old one in every conversation. Recall for the user reads every conversation
and returns the correction at once. It copies alone: it imports only the SDK.

Recall reads the managed memory view on LaserData Cloud and Laser Stack, and
folds the memory topic in process on Apache Iggy, which serves no view.

Run it:
    LASER_CONNECTION_STRING='<connection string>' python3 memory_correction.py

Docs: https://docs.laserdata.com/laser-sdk/memory
"""

import asyncio
import json

from laser_sdk import Laser, new_conversation_id


async def main() -> None:
    async with await Laser.connect_env() as laser:
        async with laser.scratch_stream("memory-demo") as scratch:
            memory = scratch.laser.memory("assistant")
            first = new_conversation_id()
            home = await memory.remember("Sam lives in Berlin", user="sam", conversation=first)
            await memory.remember("Sam is allergic to peanuts", user="sam", conversation=first)
            await memory.supersede(
                home, "Sam lives in Amsterdam", user="sam", conversation=new_conversation_id()
            )
            facts = await memory.recall(user="sam", semantic="where does Sam live")
    print(json.dumps([item.text() for item in facts], indent=2))


if __name__ == "__main__":
    asyncio.run(main())
