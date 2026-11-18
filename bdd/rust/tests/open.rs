// The Rust reference runner for the cross-SDK BDD scenarios. It loads the
// shared Gherkin under `bdd/scenarios/` and runs every scenario against a real
// Apache Iggy. Set `LASER_BDD_URL` to a connection string or `LASER_BDD_ADDR=host:port` to use an existing server, and
// `LASER_BDD_MANAGED=1` when that server has a managed backend, which skips
// the `@unmanaged` scenarios. Without it the `@managed` scenarios are skipped,
// whether the tag sits on the scenario or on its feature. `@remote_sql`
// scenarios run only when `LASER_BDD_SQL_BACKEND` names the declared SQL
// backend (`postgres` or `velodb`).

mod common;
mod steps;

use common::world::LaserWorld;
use cucumber::World;

#[tokio::main]
async fn main() {
    let recovering_catalog = std::env::var_os("LASER_BDD_RECOVERING_CATALOG").is_some();
    let managed = std::env::var_os("LASER_BDD_MANAGED").is_some();
    let remote_sql = std::env::var_os("LASER_BDD_SQL_BACKEND").is_some();
    LaserWorld::cucumber()
        .max_concurrent_scenarios(1)
        .after(|_, _, _, _, world| {
            Box::pin(async move {
                if let Some(world) = world {
                    world.managed.cleanup().await;
                }
            })
        })
        .filter_run_and_exit("../scenarios", move |feature, _, scenario| {
            // A tag on the feature applies to every scenario in it.
            let tagged = |name: &str| {
                feature
                    .tags
                    .iter()
                    .chain(&scenario.tags)
                    .any(|tag| tag == name)
            };
            if recovering_catalog || tagged("recovering_catalog") {
                return recovering_catalog && tagged("recovering_catalog");
            }
            !(managed && tagged("unmanaged"))
                && !(!managed && tagged("managed"))
                && !(!remote_sql && tagged("remote_sql"))
        })
        .await;
}
