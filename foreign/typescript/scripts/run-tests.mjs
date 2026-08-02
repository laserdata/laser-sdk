import { spawn } from "node:child_process"
import { readdir } from "node:fs/promises"
import path from "node:path"

const SUITE_ROOTS = {
  unit: "dist-test/test/unit",
  wire: "dist-test/test/wire",
  robustness: "dist-test/test/robustness",
  integration: "dist-test/test/integration",
  e2e: "dist-test/test/e2e"
}

async function collectTestFiles(root) {
  const files = []
  let entries
  try {
    entries = await readdir(root, { withFileTypes: true })
  } catch {
    return files
  }
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    const full = path.join(root, entry.name)
    if (entry.isDirectory()) {
      files.push(...(await collectTestFiles(full)))
    } else if (entry.name.endsWith(".test.js")) {
      files.push(full)
    }
  }
  return files
}

const requestedSuites = process.argv.slice(2)
if (requestedSuites.length === 0) {
  console.error("usage: run-tests.mjs <suite...>")
  process.exit(1)
}

const files = []
for (const suite of requestedSuites) {
  const root = SUITE_ROOTS[suite]
  if (!root) {
    console.error(`unknown test suite: ${suite}`)
    process.exit(1)
  }
  const suiteFiles = await collectTestFiles(root)
  if (suiteFiles.length === 0) {
    console.error(`suite "${suite}" matched no test files under ${root}`)
    process.exit(1)
  }
  files.push(...suiteFiles)
}

const child = spawn(process.execPath, ["--test", ...files], {
  stdio: "inherit",
  shell: false
})

child.on("exit", (code) => {
  process.exit(code ?? 1)
})
