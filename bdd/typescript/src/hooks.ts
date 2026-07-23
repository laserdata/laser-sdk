import { After, setDefaultTimeout } from "@cucumber/cucumber"
import type { LaserWorld } from "./world.js"

setDefaultTimeout(30_000)

After(async function (this: LaserWorld) {
  await this.close()
})
