use laser_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let laser = Laser::connect_env().await?;
    let scratch = laser.scratch_stream("memory-demo").await?;
    let facts = run(scratch.laser()).await;
    scratch.delete().await?;
    println!("{}", serde_json::to_string_pretty(&facts?)?);
    Ok(())
}

async fn run(laser: &Laser) -> Result<Vec<String>, LaserError> {
    let memory = laser.memory("assistant");
    let first = ConversationId::new();
    let home = memory
        .remember("Sam lives in Berlin")
        .scope(first)
        .user("sam")
        .send()
        .await?;
    memory
        .remember("Sam is allergic to peanuts")
        .scope(first)
        .user("sam")
        .send()
        .await?;
    memory
        .supersede(home, "Sam lives in Amsterdam")
        .scope(ConversationId::new())
        .user("sam")
        .send()
        .await?;
    let facts = memory
        .recall(None)
        .user("sam")
        .semantic("where does Sam live")
        .fetch()
        .await?;
    Ok(facts.iter().map(MemoryItem::text).collect())
}
