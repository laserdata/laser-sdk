import { readdir, readFile } from "node:fs/promises"
import path from "node:path"

const ROOT =
  process.argv[2] === undefined
    ? path.resolve(import.meta.dirname, "..")
    : path.resolve(process.cwd(), process.argv[2])

const SCAN_EXTENSIONS = new Set([".ts", ".mjs", ".md", ".json", ".yml", ".yaml"])
const EXCLUDE_DIRS = new Set([
  "node_modules",
  "dist",
  "dist-test",
  "coverage",
  "target",
  ".git",
  ".venv",
  ".pytest_cache",
  ".mypy_cache"
])
const SELF_PATH = path.resolve(import.meta.dirname, "style-check.mjs")
const EM_DASH = String.fromCodePoint(0x2014)

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
  if (text.includes(EM_DASH)) {
    violations.push("em dash character")
  }
  if (file.endsWith(".ts") && /;\s*$/m.test(text.replace(/;\s*\/\/.*$/gm, ""))) {
    violations.push("semicolon in TypeScript source")
  }
  if (file.endsWith(".ts") && /^\s*(?:\/\/|\/\*\*?|\*)[^\n;]*;/m.test(text)) {
    violations.push("semicolon in TypeScript comment")
  }
  if (file.endsWith(".md")) {
    const prose = text.replace(/^---\r?\n[\s\S]*?\r?\n---\r?\n/, "").replace(/```[\s\S]*?```/g, "")
    if (/;/.test(prose.replace(/TL;DR/g, ""))) {
      violations.push("semicolon in Markdown prose")
    }
    const lines = prose.split(/\r?\n/)
    const plain = (line) =>
      line.trim().length > 0 && !/^\s*(?:#|>|[-*+] |\d+\. |\||<|\[.+\]:| {4})/.test(line)
    if (lines.some((line, index) => index > 0 && plain(line) && plain(lines[index - 1]))) {
      violations.push("hard-wrapped Markdown paragraph")
    }
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
