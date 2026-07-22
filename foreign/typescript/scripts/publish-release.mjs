import { execFileSync } from "node:child_process"

const [tarball, tag] = process.argv.slice(2)
if (tarball === undefined || tag === undefined) {
  console.error("usage: publish-release.mjs <tarball> <ts-vTAG>")
  process.exit(2)
}

const match =
  /^ts-v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(alpha|beta|rc)\.(0|[1-9]\d*))?$/.exec(tag)
if (match === null) {
  console.error(`unsupported release tag: ${tag}`)
  process.exit(1)
}

const prerelease = match[4]
const distTag = prerelease === undefined ? "latest" : prerelease === "rc" ? "next" : prerelease
execFileSync("npm", ["publish", tarball, "--access", "public", "--tag", distTag, "--provenance"], {
  stdio: "inherit"
})
