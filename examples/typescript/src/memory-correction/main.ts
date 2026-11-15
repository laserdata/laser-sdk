import { ConversationId, Laser, memoryItemText } from "@laserdata/laser-sdk"

await using laser = await Laser.connectEnv()
await using scratch = await laser.scratchStream("memory-demo")
const memory = scratch.laser.memory("assistant")
const first = ConversationId.new()
const home = await memory.remember("Sam lives in Berlin").scope(first).user("sam").send()
await memory.remember("Sam is allergic to peanuts").scope(first).user("sam").send()
await memory
  .supersede(home, "Sam lives in Amsterdam")
  .scope(ConversationId.new())
  .user("sam")
  .send()
const facts = await memory.recall().user("sam").semantic("where does Sam live").fetch()
console.log(JSON.stringify(facts.map(memoryItemText), null, 2))
