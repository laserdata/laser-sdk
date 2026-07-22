use crate::common::world::LaserWorld;
use cucumber::{then, when};

#[then("the run registry is unavailable")]
async fn run_registry_unavailable(world: &mut LaserWorld) {
    let capabilities = world.laser().capabilities().await;
    assert!(
        !capabilities.agent_workflow,
        "the run registry should not be advertised on open Apache Iggy"
    );
}

#[when(regex = r#"^I submit a run to agent "([^"]+)"$"#)]
async fn submit_run(world: &mut LaserWorld, agent: String) {
    let result = world.laser().runs().submit(&agent, b"input").await;
    world.last_code = result.as_ref().err().map(|error| error.code());
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[when(regex = r#"^I read the status of run "([^"]+)"$"#)]
async fn run_status(world: &mut LaserWorld, run_id: String) {
    let result = world.laser().runs().status(&run_id).await;
    world.last_code = result.as_ref().err().map(|error| error.code());
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[when(regex = r#"^I cancel run "([^"]+)"$"#)]
async fn cancel_run(world: &mut LaserWorld, run_id: String) {
    let result = world.laser().runs().cancel(&run_id).await;
    world.last_code = result.as_ref().err().map(|error| error.code());
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[when("I list runs")]
async fn list_runs(world: &mut LaserWorld) {
    let result = world.laser().runs().list().fetch().await;
    world.last_code = result.as_ref().err().map(|error| error.code());
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}
