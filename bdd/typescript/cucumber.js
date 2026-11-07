// `LASER_BDD_MANAGED=1` marks a stack with a managed backend, which skips the
// `@unmanaged` scenarios. `LASER_BDD_SQL_BACKEND` names a declared SQL backend
// and enables the `@remote_sql` scenarios.
export default {
  paths: ["../scenarios/**/*.feature"],
  tags: [
    process.env.LASER_BDD_MANAGED ? "not @unmanaged" : "not @managed",
    process.env.LASER_BDD_SQL_BACKEND ? undefined : "not @remote_sql"
  ]
    .filter(Boolean)
    .join(" and "),
  import: ["dist/**/*.js"],
  strict: true,
  parallel: 1,
  format: ["progress"],
  publishQuiet: true
}
