use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::query::ProjectionBinding;

#[given(regex = r#"^a binding from stream "([^"]+)" topic "([^"]+)"$"#)]
async fn binding_from(world: &mut LaserWorld, stream: String, topic: String) {
    world.binding_builder = Some(ProjectionBinding::builder().source(stream, topic));
}

#[when("the binding lets the plane skip rejected records")]
async fn skip_rejected(world: &mut LaserWorld) {
    let builder = world.binding_builder.take().expect("a binding builder");
    world.binding_builder = Some(builder.skip_rejected());
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

#[then("the binding's wire form omits skip_rejected")]
async fn omits_skip_rejected(world: &mut LaserWorld) {
    let wire = world.binding_wire.as_ref().expect("a built binding");
    assert!(wire.get("skip_rejected").is_none(), "{wire}");
}

#[then("the binding's wire form carries skip_rejected true")]
async fn carries_skip_rejected(world: &mut LaserWorld) {
    let wire = world.binding_wire.as_ref().expect("a built binding");
    assert_eq!(wire["skip_rejected"], true, "{wire}");
}
