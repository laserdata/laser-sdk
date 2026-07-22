import { readdir, readFile } from "node:fs/promises"
import path from "node:path"

const SRC = path.resolve(import.meta.dirname, "../dist")
const IMPORT = /(?:import|export)\s+(?:[^"']*?\s+from\s+)?["'](\.[^"']+)["']/g

async function sources(directory) {
  const files = []
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name)
    if (entry.isDirectory()) files.push(...(await sources(full)))
    else if (entry.name.endsWith(".js")) files.push(full)
  }
  return files
}

function targetOf(file, specifier) {
  const target = path.resolve(path.dirname(file), specifier)
  return target.startsWith(SRC) ? target : undefined
}

const files = await sources(SRC)
const graph = new Map()
const failures = []

for (const file of files) {
  const text = await readFile(file, "utf8")
  const targets = []
  for (const match of text.matchAll(IMPORT)) {
    const target = targetOf(file, match[1])
    if (target !== undefined) targets.push(target)
  }
  graph.set(file, targets)

  const relative = path.relative(SRC, file)
  if (relative.startsWith(`wire${path.sep}`)) {
    for (const target of targets) {
      const dependency = path.relative(SRC, target)
      if (/^(agent|bridges|managed|memory|runtime|stream)[\\/]/.test(dependency)) {
        failures.push(`${relative} imports higher layer ${dependency}`)
      }
    }
  }
}

const visiting = new Set()
const visited = new Set()

function visit(file, trail) {
  if (visiting.has(file)) {
    const start = trail.indexOf(file)
    failures.push(
      `dependency cycle: ${trail
        .slice(start)
        .concat(file)
        .map((item) => path.relative(SRC, item))
        .join(" -> ")}`
    )
    return
  }
  if (visited.has(file)) return
  visiting.add(file)
  for (const target of graph.get(file) ?? []) visit(target, trail.concat(file))
  visiting.delete(file)
  visited.add(file)
}

for (const file of files) visit(file, [])

if (failures.length > 0) {
  for (const failure of [...new Set(failures)].sort()) console.error(failure)
  process.exit(1)
}

console.log(`boundary-check ok, ${String(files.length)} emitted modules, no cycles`)
