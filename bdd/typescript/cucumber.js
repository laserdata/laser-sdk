// `LASER_BDD_PLANE=1` marks a stack with a managed plane, which skips the
// `@no_plane` scenarios.
export default {
  paths: ["../scenarios/**/*.feature"],
  tags: process.env.LASER_BDD_PLANE ? "not @no_plane" : "not @plane",
  import: ["dist/**/*.js"],
  strict: true,
  parallel: 1,
  format: ["progress"],
  publishQuiet: true
}
