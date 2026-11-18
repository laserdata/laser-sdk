import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import path from "node:path"
import { test } from "node:test"

void test("given_the_package_when_its_doc_is_read_then_should_open_with_the_three_task_programs", () => {
  const source = readFileSync(path.resolve(process.cwd(), "src/index.ts"), "utf8")
  const doc = source.slice(0, source.indexOf("@packageDocumentation"))
  for (const line of [
    "await using root = await Laser.connectEnv()",
    'await using scratch = await root.scratchStream("demo")',
    '.stepLocal("analyze", async ({ outputs }) => {',
    'const memory = laser.memory("assistant")',
    "await memory.supersede(city,",
    "const sessions = laser.sessions()",
    "https://context7.com/laserdata/laser-sdk",
    "npm install @laserdata/laser-sdk tsx"
  ])
    assert.ok(doc.includes(line), line)
})
