use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::prelude::full::*;
use std::time::Duration;

async fn eventually<T>(mut read: impl AsyncFnMut() -> Option<T>) -> T {
    for _ in 0..200 {
        if let Some(value) = read().await {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the session read did not converge within 10s");
}

fn labelled(turn: &SessionTurn) -> String {
    format!("{}:{}", turn.kind, turn.text())
}

#[when(regex = r#"^I open the session "([^"]+)"$"#)]
async fn open_session(world: &mut LaserWorld, id: String) {
    world.session = Some(world.laser().sessions().create(id));
}

#[when(regex = r#"^I append an? "([^"]+)" turn "([^"]+)" to the session$"#)]
async fn append_turn(world: &mut LaserWorld, kind: String, text: String) {
    let kind: SessionTurnKind = kind.parse().expect("a known session turn kind");
    world
        .session()
        .append(kind, text.into_bytes())
        .await
        .expect("the turn should be appended");
}

#[when(regex = r"^I take a session checkpoint after (\d+) turns?$")]
async fn take_checkpoint(world: &mut LaserWorld, turns: usize) {
    let session = world.session().clone();
    let checkpoint = eventually(async || {
        let checkpoint = session
            .checkpoint()
            .await
            .expect("a checkpoint should read");
        let seen = session
            .turns_at(checkpoint.clone())
            .await
            .expect("turns up to the checkpoint should read");
        (seen.len() == turns).then_some(checkpoint)
    })
    .await;
    world.checkpoint = Some(checkpoint);
}

#[then(regex = r#"^the session context is "([^"]+)", "([^"]+)", "([^"]+)" in order$"#)]
async fn context_is(world: &mut LaserWorld, first: String, second: String, third: String) {
    let session = world.session().clone();
    let turns = eventually(async || {
        let turns = session
            .context()
            .await
            .expect("the context should assemble");
        (turns.len() == 3).then_some(turns)
    })
    .await;
    let labels: Vec<String> = turns.iter().map(labelled).collect();
    assert_eq!(labels, vec![first, second, third]);
}

#[then(regex = r#"^opening the session "([^"]+)" again reaches the same conversation$"#)]
async fn same_conversation(world: &mut LaserWorld, id: String) {
    assert_eq!(
        world.laser().sessions().create(id).conversation(),
        world.session().conversation()
    );
}

#[then(regex = r#"^opening the session "([^"]+)" reaches a different conversation$"#)]
async fn different_conversation(world: &mut LaserWorld, id: String) {
    assert_ne!(
        world.laser().sessions().create(id).conversation(),
        world.session().conversation()
    );
}

#[then(regex = r#"^the turns since the checkpoint are "([^"]+)"$"#)]
async fn turns_since(world: &mut LaserWorld, text: String) {
    let session = world.session().clone();
    let checkpoint = world.checkpoint.clone().expect("a checkpoint was taken");
    let turns = eventually(async || {
        let turns = session
            .turns_since(checkpoint.clone())
            .await
            .expect("turns since the checkpoint should read");
        (turns.len() == 1).then_some(turns)
    })
    .await;
    assert_eq!(turns[0].text(), text);
}

#[then(regex = r#"^the turns at the checkpoint are "([^"]+)"$"#)]
async fn turns_at(world: &mut LaserWorld, text: String) {
    let checkpoint = world.checkpoint.clone().expect("a checkpoint was taken");
    let turns = world
        .session()
        .turns_at(checkpoint)
        .await
        .expect("turns at the checkpoint should read");
    let texts: Vec<String> = turns.iter().map(SessionTurn::text).collect();
    assert_eq!(texts, vec![text]);
}
