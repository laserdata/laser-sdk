// npm install @laserdata/laser-sdk tsx
// Run with: npx tsx main.mts
// Needs only LASER_CONNECTION_STRING, which connectEnv() reads. No server to start when it is set.
// Check: echo "${LASER_CONNECTION_STRING:+configured}"
import { ConversationId, Laser, memoryItemText } from "@laserdata/laser-sdk"

const user = "user-42"
const query = "Where does the user live, and what should I know before booking dinner?"

await using root = await Laser.connectEnv()
await using scratch = await root.scratchStream("memory")
const memory = scratch.laser.memory("assistant")

// Conversation 1: the user shares three facts.
const first = ConversationId.new()
const city = await memory.remember("Lives in Gdansk").scope(first).user(user).send()
await memory.remember("Allergic to peanuts").scope(first).user(user).send()
await memory.remember("Prefers a window seat").scope(first).user(user).send()

// Conversation 2: the user moved. supersede stores the new fact and forgets the old one.
const second = ConversationId.new()
await memory.supersede(city, "Lives in Lisbon").scope(second).user(user).send()

// Recall for the user across every conversation.
const facts = await memory.recall().user(user).semantic(query).limit(10).fetch()
console.log(JSON.stringify({ user, query, facts: facts.map(memoryItemText) }, null, 2))
