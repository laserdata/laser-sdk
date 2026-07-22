import { execFileSync } from "node:child_process"
import { mkdirSync, writeFileSync } from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"

const packageRoot = fileURLToPath(new URL("..", import.meta.url))
const output = path.resolve(process.argv[2] ?? path.join(packageRoot, "artifacts", "sbom.cdx.json"))
const sbom = execFileSync("npm", ["sbom", "--sbom-format", "cyclonedx"], {
  cwd: packageRoot,
  encoding: "utf8"
})
mkdirSync(path.dirname(output), { recursive: true })
writeFileSync(output, sbom)
console.log(`wrote CycloneDX SBOM to ${output}`)
