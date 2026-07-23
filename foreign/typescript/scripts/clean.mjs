import { rm } from "node:fs/promises"

for (const dir of ["dist", "dist-test", "coverage"]) {
  await rm(dir, { recursive: true, force: true })
}
