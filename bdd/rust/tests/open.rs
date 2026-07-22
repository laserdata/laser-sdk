// The Rust reference runner for the cross-SDK BDD scenarios. It loads the
// shared Gherkin under `bdd/scenarios/` and runs every scenario against a real
// Apache Iggy testcontainer. By default it manages its own container, set
// `LASER_BDD_ADDR=host:3000` to run against an already-running server (the path
// other language runners share via docker-compose).

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
