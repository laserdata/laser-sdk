// cargo add laser-sdk
// cargo add tokio --features macros,rt-multi-thread
// cargo add serde --features derive
// cargo add serde_json
// Needs only LASER_CONNECTION_STRING, which connect_env() reads. No server to start when it is set.
use laser_sdk::prelude::*;
use serde::Serialize;

const USER: &str = "user-42";
const QUERY: &str = "Where does the user live, and what should I know before booking dinner?";

#[derive(Serialize)]
struct Recall {
    user: &'static str,
    query: &'static str,
    facts: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Laser::connect_env().await?;
    let scratch = root.scratch_stream("memory").await?;
    let result = correct_and_recall(scratch.laser()).await;
    scratch.delete().await?;
    println!("{}", serde_json::to_string_pretty(&result?)?);
    Ok(())
}

async fn correct_and_recall(laser: &Laser) -> Result<Recall, LaserError> {
    let memory = laser.memory("assistant");

    // Conversation 1: the user shares three facts.
    let first = ConversationId::new();
    let city = memory
        .remember("Lives in Gdansk")
        .scope(first)
        .user(USER)
        .send()
        .await?;
    memory
        .remember("Allergic to peanuts")
        .scope(first)
        .user(USER)
        .send()
        .await?;
    memory
        .remember("Prefers a window seat")
        .scope(first)
        .user(USER)
        .send()
        .await?;

    // Conversation 2: the user moved. supersede stores the new fact and forgets the old one.
    let second = ConversationId::new();
    memory
        .supersede(city, "Lives in Lisbon")
        .scope(second)
        .user(USER)
        .send()
        .await?;

    // Recall for the user across every conversation.
    let facts = memory
        .recall(None)
        .user(USER)
        .semantic(QUERY)
        .limit(10)
        .fetch()
        .await?;
    Ok(Recall {
        user: USER,
        query: QUERY,
        facts: facts.iter().map(MemoryItem::text).collect(),
    })
}
