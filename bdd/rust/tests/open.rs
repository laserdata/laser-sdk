// The Rust reference runner for the cross-SDK BDD scenarios. It loads the
// shared Gherkin under `bdd/scenarios/` and runs every scenario against a real
// Apache Iggy. Set `LASER_BDD_ADDR=host:port` to use an existing server.

mod common;
mod steps;

use common::world::LaserWorld;
use cucumber::World;

#[tokio::main]
async fn main() {
    LaserWorld::cucumber()
        .max_concurrent_scenarios(1)
        .run_and_exit("../scenarios")
        .await;
}
