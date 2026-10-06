// The Rust reference runner for the cross-SDK BDD scenarios. It loads the
// shared Gherkin under `bdd/scenarios/` and runs every scenario against a real
// Apache Iggy. Set `LASER_BDD_URL` to a connection string or `LASER_BDD_ADDR=host:port` to use an existing server, and
// `LASER_BDD_PLANE=1` when that server has a managed plane, which skips the
// `@no_plane` scenarios.

mod common;
mod steps;

use common::world::LaserWorld;
use cucumber::World;

#[tokio::main]
async fn main() {
    let plane = std::env::var_os("LASER_BDD_PLANE").is_some();
    LaserWorld::cucumber()
        .max_concurrent_scenarios(1)
        .filter_run_and_exit("../scenarios", move |_, _, scenario| {
            !(plane && scenario.tags.iter().any(|tag| tag == "no_plane"))
                && !(!plane && scenario.tags.iter().any(|tag| tag == "plane"))
        })
        .await;
}
