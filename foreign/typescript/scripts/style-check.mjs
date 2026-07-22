import { readdir, readFile } from "node:fs/promises"
import path from "node:path"

const ROOT = path.resolve(import.meta.dirname, "..")

const SCAN_EXTENSIONS = new Set([".ts", ".mjs", ".md", ".json", ".yml", ".yaml"])
const EXCLUDE_DIRS = new Set(["node_modules", "dist", "dist-test", "coverage", ".git"])
const SELF_PATH = path.resolve(import.meta.dirname, "style-check.mjs")

const FORBIDDEN_PHRASES = [
  "successfully processed",
  "something went wrong",
  "as an ai",
  "i apologize",
  "please try again later"
]

async function collectFiles(dir) {
  const files = []
  const entries = await readdir(dir, { withFileTypes: true })
  for (const entry of entries) {
    if (EXCLUDE_DIRS.has(entry.name)) continue
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) {
      files.push(...(await collectFiles(full)))
    } else if (SCAN_EXTENSIONS.has(path.extname(entry.name))) {
      files.push(full)
    }
  }
  return files
}

function checkFile(file, text) {
  const violations = []
  if (text.includes("—")) {
    violations.push("em dash character")
  }
  if (file.endsWith(".ts") && /;\s*$/m.test(text.replace(/;\s*\/\/.*$/gm, ""))) {
    violations.push("semicolon in TypeScript source")
  }
  const lower = text.toLowerCase()
  for (const phrase of FORBIDDEN_PHRASES) {
    if (lower.includes(phrase)) {
      violations.push(`forbidden phrase: ${phrase}`)
    }
  }
  return violations
}

const files = await collectFiles(ROOT)
let failed = false

for (const file of files) {
  if (file === SELF_PATH) continue
  const text = await readFile(file, "utf8")
  const violations = checkFile(file, text)
  if (violations.length > 0) {
    failed = true
    console.error(`${path.relative(ROOT, file)}: ${violations.join(", ")}`)
  }
}

if (failed) {
  process.exit(1)
}
