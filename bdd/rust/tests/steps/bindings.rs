use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::query::ProjectionBinding;
use laser_sdk::wire::control::{InvalidRecordPolicy, OversizePolicy};

#[given(regex = r#"^a binding from stream "([^"]+)" topic "([^"]+)"$"#)]
async fn binding_from(world: &mut LaserWorld, stream: String, topic: String) {
    world.binding_builder = Some(ProjectionBinding::builder().source(stream, topic));
}

#[when("the binding lets the backend skip rejected records")]
async fn invalid_records(world: &mut LaserWorld) {
    let builder = world.binding_builder.take().expect("a binding builder");
    world.binding_builder = Some(builder.invalid_records(InvalidRecordPolicy::DeadLetter));
}

#[when("I build the binding")]
async fn build_binding(world: &mut LaserWorld) {
    let binding = world
        .binding_builder
        .take()
        .expect("a binding builder")
        .build();
    world.binding_wire = Some(serde_json::to_value(&binding).expect("the binding encodes"));
}

#[then("the binding's wire form omits invalid_records")]
async fn omits_invalid_records(world: &mut LaserWorld) {
    let wire = world.binding_wire.as_ref().expect("a built binding");
    assert!(wire.get("invalid_records").is_none(), "{wire}");
}

#[then("the binding's wire form carries invalid_records dead_letter")]
async fn carries_invalid_records(world: &mut LaserWorld) {
    let wire = world.binding_wire.as_ref().expect("a built binding");
    assert_eq!(wire["invalid_records"], "dead_letter", "{wire}");
}

#[when("the binding keeps references for oversized records")]
async fn reference_oversize(world: &mut LaserWorld) {
    let builder = world.binding_builder.take().expect("a binding builder");
    world.binding_builder = Some(builder.oversize(OversizePolicy::Reference));
}

#[then("the binding's wire form carries oversize reference")]
async fn carries_oversize_reference(world: &mut LaserWorld) {
    let wire = world.binding_wire.as_ref().expect("a built binding");
    assert_eq!(wire["oversize"], "reference", "{wire}");
}
