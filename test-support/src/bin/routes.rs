use iggy::prelude::{Client, Identifier, IggyClient, IggyError};
use iggy_binary_protocol::codes::GET_PARTITION_ROUTE_CODE;
use iggy_binary_protocol::requests::messages::GetPartitionRouteRequest;
use iggy_binary_protocol::responses::messages::PartitionRouteResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::wire_conversions::identifier_to_wire;
use std::num::ParseIntError;
use std::process::ExitCode;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const USAGE: &str = "usage: laser-test-routes <connection-string> <stream> <topic> <partitions>";
// A view change leaves a route unknown for a moment, so each one is asked again until then.
const ROUTE_DEADLINE: Duration = Duration::from_secs(30);
const ROUTE_RETRY: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
enum RouteError {
    #[error("{USAGE}")]
    Usage,
    #[error("partitions must be a count: {0}")]
    Partitions(#[from] ParseIntError),
    #[error(transparent)]
    Iggy(#[from] IggyError),
    #[error("partition {partition} named no primary within {ROUTE_DEADLINE:?}")]
    Unanswered { partition: u32 },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match primaries(std::env::args().skip(1).collect()).await {
        Ok(primaries) => {
            let listed: Vec<String> = primaries.iter().map(u8::to_string).collect();
            println!("[{}]", listed.join(","));
            ExitCode::SUCCESS
        }
        Err(RouteError::Usage) => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn primaries(arguments: Vec<String>) -> Result<Vec<u8>, RouteError> {
    let [connection_string, stream, topic, partitions] =
        <[String; 4]>::try_from(arguments).map_err(|_| RouteError::Usage)?;
    let partitions: u32 = partitions.parse()?;
    let client = IggyClient::from_connection_string(&connection_string)?;
    client.connect().await?;
    let stream_id = identifier_to_wire(&Identifier::named(&stream)?)?;
    let topic_id = identifier_to_wire(&Identifier::named(&topic)?)?;
    let mut primaries = Vec::new();
    for partition_id in 0..partitions {
        let request = GetPartitionRouteRequest {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            partition_id,
        };
        let deadline = Instant::now() + ROUTE_DEADLINE;
        let primary = loop {
            let route = client
                .send_binary_request(GET_PARTITION_ROUTE_CODE, request.to_bytes())
                .await
                .ok()
                .and_then(|reply| PartitionRouteResponse::decode_from(&reply).ok());
            if let Some(route) = route {
                break route.primary_replica;
            }
            if Instant::now() >= deadline {
                return Err(RouteError::Unanswered {
                    partition: partition_id,
                });
            }
            sleep(ROUTE_RETRY).await;
        };
        primaries.push(primary);
    }
    Ok(primaries)
}
