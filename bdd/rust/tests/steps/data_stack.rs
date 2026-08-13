use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_bdd::data_stack::{DataStackModel, ModelError};

fn model(world: &mut LaserWorld) -> &mut DataStackModel {
    world.data_stack.as_mut().expect("a data stack model")
}

#[given("a fresh data stack contract model")]
async fn fresh_model(world: &mut LaserWorld) {
    world.data_stack = Some(DataStackModel::default());
}

#[when(regex = r#"^I register destination \"([^\"]+)\" at global revision (\d+)$"#)]
async fn register_destination(world: &mut LaserWorld, name: String, revision: u64) {
    match model(world).register(&name, revision) {
        Ok(operation) => world.data_stack_operation = Some(operation),
        Err(error) => world.data_stack_error = Some(error),
    }
}

#[when(
    regex = r#"^I enable destination \"([^\"]+)\" at global revision (\d+) and definition revision (\d+)$"#
)]
async fn enable_destination(
    world: &mut LaserWorld,
    name: String,
    global_revision: u64,
    definition_revision: u64,
) {
    match model(world).enable(&name, global_revision, definition_revision) {
        Ok(operation) => world.data_stack_operation = Some(operation),
        Err(error) => world.data_stack_error = Some(error),
    }
}

#[when(regex = r#"^I record a retention gap from required offset (\d+) to retained offset (\d+)$"#)]
async fn record_gap(world: &mut LaserWorld, required: u64, retained: u64) {
    world.data_stack_error = model(world)
        .record_retention_gap("orders-lakehouse", required, retained)
        .err();
}

#[when(
    regex = r#"^I accept the retention gap at next offset (\d+) and checkpoint revision (\d+)$"#
)]
async fn accept_gap(world: &mut LaserWorld, next_offset: u64, checkpoint_revision: u64) {
    world.data_stack_error = model(world)
        .accept_retention_gap("orders-lakehouse", next_offset, checkpoint_revision)
        .err();
}

#[then("the operation is accepted with a stable identity")]
async fn operation_accepted(world: &mut LaserWorld) {
    assert!(
        world
            .data_stack_operation
            .as_ref()
            .is_some_and(|operation| !operation.id.is_empty())
    );
}

#[then(regex = r#"^destination \"([^\"]+)\" is disabled at definition revision (\d+)$"#)]
async fn destination_disabled(world: &mut LaserWorld, name: String, revision: u64) {
    let destination = model(world).destination(&name).expect("destination");
    assert_eq!(destination.effective_state, "disabled");
    assert_eq!(destination.definition_revision, revision);
}

#[then(regex = r#"^destination \"([^\"]+)\" is running at definition revision (\d+)$"#)]
async fn destination_running(world: &mut LaserWorld, name: String, revision: u64) {
    let destination = model(world).destination(&name).expect("destination");
    assert_eq!(destination.effective_state, "running");
    assert_eq!(destination.definition_revision, revision);
}

#[then(regex = r#"^the destination mutation conflicts with global revision (\d+)$"#)]
async fn mutation_conflicts(world: &mut LaserWorld, revision: u64) {
    assert_eq!(
        world.data_stack_error,
        Some(ModelError::Conflict {
            observed_revision: revision
        })
    );
}

#[then(
    regex = r#"^destination \"([^\"]+)\" is blocked by the retention gap at checkpoint revision (\d+)$"#
)]
async fn destination_blocked(world: &mut LaserWorld, name: String, revision: u64) {
    let destination = model(world).destination(&name).expect("destination");
    assert_eq!(destination.effective_state, "blocked");
    assert_eq!(destination.checkpoint_revision, revision);
}

#[then(regex = r#"^destination \"([^\"]+)\" is running at next offset (\d+)$"#)]
async fn destination_at_offset(world: &mut LaserWorld, name: String, next_offset: u64) {
    let destination = model(world).destination(&name).expect("destination");
    assert_eq!(destination.effective_state, "running");
    assert_eq!(destination.next_offset, next_offset);
}

#[given(regex = r#"^a query execution with rows \"([^\"]+)\"$"#)]
async fn seed_query(world: &mut LaserWorld, rows: String) {
    model(world).seed_query(rows.split(", ").map(str::to_owned).collect());
}

#[when(regex = r#"^I read a query page with limit (\d+)$"#)]
async fn read_page(world: &mut LaserWorld, limit: usize) {
    match model(world).page(limit) {
        Ok(page) => world.data_stack_page = Some(page),
        Err(error) => world.data_stack_error = Some(error),
    }
}

#[then(regex = r#"^the query page contains \"([^\"]+)\" and a continuation cursor$"#)]
async fn page_has_cursor(world: &mut LaserWorld, rows: String) {
    let page = world.data_stack_page.as_ref().expect("query page");
    assert_eq!(page.0, rows.split(", ").collect::<Vec<_>>());
    assert!(page.1.is_some());
}

#[when("I cancel the query execution")]
async fn cancel_query(world: &mut LaserWorld) {
    model(world).cancel_query();
}

#[then("the query execution status is cancelled")]
async fn query_is_cancelled(world: &mut LaserWorld) {
    assert!(model(world).query_cancelled());
}

#[then("the continuation page is rejected as cancelled")]
async fn cancelled_page_rejected(world: &mut LaserWorld) {
    assert_eq!(model(world).page(2), Err(ModelError::Cancelled));
}
