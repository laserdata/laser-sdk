import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { readFile } from "node:fs/promises"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"
import { Laser } from "@laserdata/laser-sdk"

const execute = promisify(execFile)
const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

// Runs one standalone program as a user would and parses what it printed.
async function printed(program: string): Promise<unknown> {
  const { stdout } = await execute(
    process.execPath,
    [fileURLToPath(new URL(`../src/${program}/main.js`, import.meta.url))],
    { env: { ...process.env, LASER_CONNECTION_STRING: CONNECTION_STRING }, timeout: 60_000 }
  )
  return JSON.parse(stdout)
}

void test(
  "given_the_standalone_workflow_when_run_then_should_print_three_completed_local_steps",
  { concurrency: false, timeout: 70_000 },
  async () => {
    const report = await printed("workflow")
    const laser = await Laser.connect(CONNECTION_STRING)
    try {
      await laser.stream("pipeline").delete()
    } finally {
      await laser.close()
    }

    assert.deepEqual(report, {
      status: "completed",
      steps: ["ingest", "analyze", "report"],
      report: "report on analyzed frames from sat-7"
    })
  }
)

void test(
  "given_the_standalone_memory_correction_when_run_then_should_recall_the_correction_first",
  { concurrency: false, timeout: 70_000 },
  async () => {
    assert.deepEqual(await printed("memory-correction"), {
      user: "user-42",
      query: "Where does the user live, and what should I know before booking dinner?",
      facts: ["Lives in Lisbon", "Prefers a window seat", "Allergic to peanuts"]
    })
  }
)

void test("given_an_existing_project_when_setting_up_an_example_then_should_keep_its_module_type", async () => {
  for (const program of ["workflow", "memory-correction"]) {
    const source = await readFile(new URL(`../../src/${program}/main.ts`, import.meta.url), "utf8")
    assert.deepEqual(source.split("\n").slice(0, 2), [
      "// npm install @laserdata/laser-sdk tsx",
      "// Run with: npx tsx main.mts"
    ])
  }
})

void test("given_the_package_readme_when_read_then_should_show_both_standalone_programs_verbatim", async () => {
  const readme = await readFile(
    new URL("../../../../foreign/typescript/README.md", import.meta.url),
    "utf8"
  )
  for (const program of ["workflow", "memory-correction"]) {
    const source = await readFile(new URL(`../../src/${program}/main.ts`, import.meta.url), "utf8")
    assert.ok(readme.includes("```ts\n" + source), `the README does not show ${program} verbatim`)
  }
})
