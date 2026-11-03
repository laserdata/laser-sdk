// `LASER_BDD_MANAGED=1` marks a stack with a managed backend, which skips the
// `@unmanaged` scenarios.
export default {
  paths: ["../scenarios/**/*.feature"],
  tags: process.env.LASER_BDD_MANAGED ? "not @unmanaged" : "not @managed",
  import: ["dist/**/*.js"],
  strict: true,
  parallel: 1,
  format: ["progress"],
  publishQuiet: true
}
