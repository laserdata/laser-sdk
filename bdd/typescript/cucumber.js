export default {
  paths: ["../scenarios/**/*.feature"],
  import: ["dist/**/*.js"],
  strict: true,
  parallel: 1,
  format: ["progress"],
  publishQuiet: true
}
