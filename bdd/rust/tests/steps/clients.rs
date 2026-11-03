use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::prelude::PrincipalId;

#[when("I list every connected client")]
async fn list_every_client(world: &mut LaserWorld) {
    world.clients = world
        .laser()
        .client_metadata()
        .all()
        .await
        .expect("every node answers its client listing");
}

#[then("the listing holds an authenticated connection")]
fn listing_holds_an_authenticated_connection(world: &mut LaserWorld) {
    let user = world
        .clients
        .iter()
        .find_map(|client| client.user_id)
        .expect("a listed connection is authenticated");
    world.client_user = Some(user);
}

#[when("I list every connected client of that user")]
async fn list_every_client_of_that_user(world: &mut LaserWorld) {
    let user = world.client_user.expect("a user was listed");
    world.clients = world
        .laser()
        .client_metadata()
        .principal(PrincipalId::new(user))
        .all()
        .await
        .expect("every node answers the user's client listing");
}

#[then("every listed connection belongs to that user")]
fn every_listed_connection_belongs_to_that_user(world: &mut LaserWorld) {
    let user = world.client_user.expect("a user was listed");
    assert!(
        !world.clients.is_empty(),
        "the user's own connection is listed"
    );
    assert!(
        world
            .clients
            .iter()
            .all(|client| client.user_id == Some(user)),
        "{:?} all belong to user {user}",
        world.clients
    );
}
