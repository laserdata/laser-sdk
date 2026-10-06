use crate::harness;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Sample {
    host: String,
    cpu_millis: i64,
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_typed_publishes_when_read_back_then_should_yield_values_and_skip_the_poison() {
    let laser = harness::laser().await;
    laser
        .topic("samples")
        .ensure(1)
        .await
        .expect("topic exists");
    let samples = laser.topic("samples").json::<Sample>();

    for (host, cpu_millis) in [("node-1", 21350), ("node-2", 42010)] {
        samples
            .publish(&Sample {
                host: host.to_owned(),
                cpu_millis,
            })
            .expect("body encodes")
            .send()
            .await
            .expect("publish succeeds");
    }
    // A record that is not a Sample rides the same topic: the typed reader must
    // yield its position-carrying error and keep going, never wedge.
    laser
        .topic("samples")
        .send(&b"not json"[..], Default::default(), None)
        .await
        .expect("raw publish succeeds");

    let mut records = samples.records("typed-it").expect("reader opens");
    let mut values = Vec::new();
    let mut poison = Vec::new();
    while let Some(next) = records.next().await {
        match next {
            Ok(record) => values.push(record.value),
            Err(error) => poison.push(error),
        }
    }
    assert_eq!(
        values,
        vec![
            Sample {
                host: "node-1".to_owned(),
                cpu_millis: 21350,
            },
            Sample {
                host: "node-2".to_owned(),
                cpu_millis: 42010,
            },
        ]
    );
    assert_eq!(poison.len(), 1, "exactly the raw record fails to decode");
    let position = poison[0].position.expect("a positioned decode failure");
    assert_eq!(position.offset, 2, "the poison sits after the two samples");

    // Caught up: the reader answers None and its offsets resume a later read
    // past everything already yielded.
    let offsets = records.offsets().to_vec();
    samples
        .publish(&Sample {
            host: "node-3".to_owned(),
            cpu_millis: 121_540,
        })
        .expect("body encodes")
        .send()
        .await
        .expect("publish succeeds");
    let mut resumed = samples
        .records("typed-it")
        .expect("reader opens")
        .from_offsets(offsets);
    let record = resumed
        .next()
        .await
        .expect("the new fill is there")
        .expect("it decodes");
    assert_eq!(record.value.host, "node-3");
    assert!(resumed.next().await.is_none(), "then caught up again");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_the_cbor_form_when_round_tripped_then_should_decode_with_positions() {
    let laser = harness::laser().await;
    laser.topic("ticks").ensure(1).await.expect("topic exists");
    let ticks = laser.topic("ticks").cbor::<Sample>();
    ticks
        .publish(&Sample {
            host: "node-4".to_owned(),
            cpu_millis: 17890,
        })
        .expect("body encodes")
        .send()
        .await
        .expect("publish succeeds");

    let mut records = ticks.records("typed-cbor-it").expect("reader opens");
    let record = records
        .next()
        .await
        .expect("the tick is there")
        .expect("it decodes");
    assert_eq!(record.value.cpu_millis, 17890);
    assert_eq!(record.position.offset, 0);
    assert!(records.next().await.is_none());
}
