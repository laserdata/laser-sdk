# python -m pip install laser-sdk
# Needs only LASER_CONNECTION_STRING, which connect_env() reads. No server to start when it is set.
# Check: echo "${LASER_CONNECTION_STRING:+configured}"
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
