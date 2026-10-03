use laser_examples::phase;
use laser_sdk::filters::{ConsumerFilter, FilterExpr, FilteredStart};
use laser_sdk::prelude::full::*;
use laser_sdk::query::{CmpOp, SchemaDef, SchemaSource};
use laser_sdk::schema_codecs::CompiledSchema;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize, Message)]
struct Reading {
    #[prost(string, tag = "1")]
    satellite: String,
    #[prost(string, tag = "2")]
    mode: String,
    #[prost(int32, tag = "3")]
    battery: i32,
}

pub async fn run(laser: &Laser, stream: &str) -> Result<(), LaserError> {
    phase("select typed records inside CBOR, Avro, and Protobuf payloads");
    let mut registered = Vec::new();
    let result = async {
        for codec in ["cbor", "avro", "protobuf"] {
            let source = match codec {
                "avro" => Some(SchemaSource::Avro {
                    schema: include_str!("../../../shared/fleet-reading.avsc").to_owned(),
                }),
                "protobuf" => Some(SchemaSource::Protobuf {
                    descriptor_set: include_bytes!("../../../shared/fleet-reading.desc").to_vec(),
                    message_type: "fleet.Reading".to_owned(),
                }),
                _ => None,
            };
            let schema = if let Some(source) = source {
                let id = laser
                    .schemas()
                    .register(source.clone())
                    .name(format!("fleet-{codec}"))
                    .send()
                    .await?;
                registered.push(id);
                tokio::time::timeout(Duration::from_secs(15), async {
                    while laser.schemas().get(id).await?.is_none() {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Ok::<(), LaserError>(())
                })
                .await
                .map_err(|_| LaserError::Timeout("writer schema visibility"))??;
                Some((
                    id,
                    CompiledSchema::compile(&SchemaDef {
                        id,
                        source,
                        name: None,
                        version: None,
                    })?,
                ))
            } else {
                None
            };
            let topic_name = format!("fleet_{codec}");
            let topic = laser.topic(&topic_name);
            topic.ensure(1).await?;
            for (mode, battery) in [("nominal", 80), ("safe", 20), ("nominal", 60)] {
                let reading = Reading {
                    satellite: "sat-042".to_owned(),
                    mode: mode.to_owned(),
                    battery,
                };
                let mut payload = Vec::new();
                match codec {
                    "avro" => {
                        payload = schema
                            .as_ref()
                            .expect("registered Avro schema")
                            .1
                            .encode_avro(&reading)?
                    }
                    "protobuf" => payload = reading.encode_to_vec(),
                    _ => ciborium::into_writer(&reading, &mut payload)
                        .map_err(|error| LaserError::Codec(error.to_string()))?,
                }
                let content_type = match codec {
                    "avro" => ContentType::Avro,
                    "protobuf" => ContentType::Protobuf,
                    _ => ContentType::Cbor,
                };
                let mut publish = topic.publish().raw_bytes(payload, content_type);
                if let Some((id, _)) = &schema {
                    publish = publish.schema_id(*id);
                }
                publish.send().await?;
            }
            let expr = FilterExpr::pred("mode", CmpOp::Eq, "safe");
            let filter = match codec {
                "avro" => ConsumerFilter::avro(expr, [schema.as_ref().expect("schema").0]),
                "protobuf" => ConsumerFilter::protobuf(expr, [schema.as_ref().expect("schema").0]),
                _ => ConsumerFilter::cbor(expr),
            };
            let group = laser
                .stream(stream)
                .topic(&topic_name)
                .consumer_group(format!("safe-{codec}"));
            group.create().filter(filter).build().await?;
            let mut reader = group
                .reader()?
                .start(FilteredStart::First)
                .local_guard(true)
                .build()
                .await?;
            let outcome = async {
                let record = tokio::time::timeout(Duration::from_secs(15), reader.next_record())
                    .await
                    .map_err(|_| LaserError::Timeout("filtered codec record"))??;
                let reading: Reading = match &schema {
                    Some((_, schema)) => {
                        serde_json::from_value(schema.decode(&record.message.payload)?)
                            .map_err(|error| LaserError::Codec(error.to_string()))?
                    }
                    None => ciborium::from_reader(record.message.payload.as_ref())
                        .map_err(|error| LaserError::Codec(error.to_string()))?,
                };
                println!(
                    "  {codec}: {} entered {} mode at {}% battery, 1 of 3 records delivered",
                    reading.satellite, reading.mode, reading.battery
                );
                reader.ack(&record).await
            }
            .await;
            let closed = reader.close().await;
            let deleted = group.filter().delete().await.map(|_| ());
            outcome.and(closed).and(deleted)?;
        }
        Ok(())
    }
    .await;
    let mut cleanup = Ok(());
    for id in registered {
        cleanup = cleanup.and(laser.schemas().drop(id).await);
    }
    result.and(cleanup)
}
