use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::stream::ScratchStream;

#[when(regex = r#"^I open a scratch stream with the prefix "([^"]+)"$"#)]
async fn open_scratch(world: &mut LaserWorld, prefix: String) {
    let scratch = world
        .laser()
        .scratch_stream(prefix)
        .await
        .expect("the scratch stream opens");
    world.scratch_streams = vec![scratch];
}

#[then(regex = r#"^the scratch stream is named "([^"]+)" and 8 lowercase hex characters$"#)]
async fn scratch_named(world: &mut LaserWorld, prefix: String) {
    let name = scratch(world).name();
    let suffix = name
        .strip_prefix(&prefix)
        .expect("the name starts with the prefix");
    assert_eq!(suffix.len(), 8, "{name}");
    assert!(
        suffix
            .chars()
            .all(|character| character.is_ascii_digit() || ('a'..='f').contains(&character)),
        "{name}"
    );
}

#[then("the scratch stream exists")]
async fn scratch_exists(world: &mut LaserWorld) {
    let name = scratch(world).name().to_owned();
    assert!(
        world
            .laser()
            .stream(name)
            .exists()
            .await
            .expect("the stream reads")
    );
}

#[then("the scratch handle's default stream is the scratch stream")]
async fn scratch_default_stream(world: &mut LaserWorld) {
    let scratch = scratch(world);
    assert_eq!(scratch.laser().default_stream(), Some(scratch.name()));
}

#[when("I delete the scratch stream")]
async fn delete_scratch(world: &mut LaserWorld) {
    scratch(world)
        .delete()
        .await
        .expect("the scratch stream deletes");
}

#[then("the scratch stream no longer exists")]
async fn scratch_gone(world: &mut LaserWorld) {
    let name = scratch(world).name().to_owned();
    assert!(
        !world
            .laser()
            .stream(name)
            .exists()
            .await
            .expect("the stream reads")
    );
}

#[then("deleting the scratch stream again succeeds")]
async fn delete_scratch_again(world: &mut LaserWorld) {
    scratch(world)
        .delete()
        .await
        .expect("a second delete succeeds");
}

#[when(regex = r#"^I open two scratch streams with the prefix "([^"]+)"$"#)]
async fn open_two_scratch(world: &mut LaserWorld, prefix: String) {
    let mut opened = Vec::new();
    for _ in 0..2 {
        opened.push(
            world
                .laser()
                .scratch_stream(&prefix)
                .await
                .expect("a scratch stream opens"),
        );
    }
    world.scratch_streams = opened;
}

#[then("the two scratch streams have different names")]
async fn different_names(world: &mut LaserWorld) {
    let [first, second] = world.scratch_streams.as_slice() else {
        panic!("two scratch streams are open");
    };
    assert_ne!(first.name(), second.name());
    for scratch in &world.scratch_streams {
        scratch.delete().await.expect("the scratch stream deletes");
    }
}

fn scratch(world: &LaserWorld) -> &ScratchStream {
    world
        .scratch_streams
        .first()
        .expect("a scratch stream is open")
}
