import { execFileSync } from "node:child_process"
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"

const packageRoot = fileURLToPath(new URL("..", import.meta.url))
const workspace = await mkdtemp(path.join(tmpdir(), "laser-sdk-pack-"))
const npmEnvironment = {
  ...process.env,
  npm_config_cache: path.join(workspace, "npm-cache")
}

function run(command, args, cwd) {
  execFileSync(command, args, { cwd, env: npmEnvironment, stdio: "inherit" })
}

async function writeConsumer(directory, type) {
  await mkdir(directory, { recursive: true })
  await writeFile(
    path.join(directory, "package.json"),
    `${JSON.stringify({ private: true, type }, null, 2)}\n`
  )
}

async function verifyEsm(directory, tarball) {
  await writeConsumer(directory, "module")
  run(
    "npm",
    ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false", tarball],
    directory
  )
  await writeFile(
    path.join(directory, "smoke.mjs"),
    `const entries = [
  "@laserdata/laser-sdk",
  "@laserdata/laser-sdk/full",
  "@laserdata/laser-sdk/testing",
  "@laserdata/laser-sdk/opentelemetry"
]
for (const entry of entries) {
  const module = await import(entry)
  if (Object.keys(module).length === 0) throw new Error(\`empty export: \${entry}\`)
}
`
  )
  run(process.execPath, ["smoke.mjs"], directory)
  await writeFile(
    path.join(directory, "index.ts"),
    `import { Laser } from "@laserdata/laser-sdk"
import { wire } from "@laserdata/laser-sdk/full"
import { TestClock } from "@laserdata/laser-sdk/testing"
import { OpenTelemetryObserver } from "@laserdata/laser-sdk/opentelemetry"

void [Laser, wire, TestClock, OpenTelemetryObserver]
`
  )
  await writeFile(
    path.join(directory, "tsconfig.json"),
    `${JSON.stringify(
      {
        compilerOptions: {
          module: "NodeNext",
          moduleResolution: "NodeNext",
          strict: true,
          noEmit: true,
          skipLibCheck: false
        },
        files: ["index.ts"]
      },
      null,
      2
    )}\n`
  )
  run(
    process.execPath,
    [path.join(packageRoot, "node_modules", "@typescript", "native", "bin", "tsc"), "-p", "."],
    directory
  )
}

async function verifyCommonJs(directory, tarball) {
  await writeConsumer(directory, "commonjs")
  run(
    "npm",
    ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--package-lock=false", tarball],
    directory
  )
  await writeFile(
    path.join(directory, "smoke.cjs"),
    `const entries = [
  "@laserdata/laser-sdk",
  "@laserdata/laser-sdk/full",
  "@laserdata/laser-sdk/testing",
  "@laserdata/laser-sdk/opentelemetry"
]
Promise.all(entries.map((entry) => import(entry))).then((modules) => {
  if (modules.some((module) => Object.keys(module).length === 0)) {
    throw new Error("a package entry point exported no symbols")
  }
})
`
  )
  run(process.execPath, ["smoke.cjs"], directory)
}

try {
  let tarball = process.argv[2]
  if (tarball === undefined) {
    const output = execFileSync("npm", ["pack", "--json", "--pack-destination", workspace], {
      cwd: packageRoot,
      encoding: "utf8",
      env: npmEnvironment
    })
    const [entry] = JSON.parse(output)
    if (entry?.filename === undefined) throw new Error("npm pack returned no tarball")
    tarball = path.join(workspace, entry.filename)
  } else {
    tarball = path.resolve(tarball)
  }
  await verifyEsm(path.join(workspace, "esm"), tarball)
  await verifyCommonJs(path.join(workspace, "commonjs"), tarball)
  console.log("pack:consumer ok, ESM, CommonJS dynamic import, and declarations")
} finally {
  await rm(workspace, { recursive: true, force: true })
}
