use crate::error::LaserError;
use crate::laser::Laser;
use bytes::Bytes;
use laser_wire::codes::CONTROL_OP_VERSION;
use laser_wire::control::{ControlCommand, ControlEnvelope};
use laser_wire::framing::encode_named;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

// Co-locate every control command on one partition so a `RegisterProjection` is
// applied before the `ApplyBinding` that references it, and a run-source
// register lands in order with the rest.
pub(crate) const CONTROL_PARTITION_KEY: &str = "control";

impl Laser {
    /// Publish one durable control command to `<ops>/control.commands`. The
    /// shared write path for the projection registry and the run-source
    /// registry, so it is compiled whenever either surface is enabled.
    pub(crate) async fn publish_control(&self, command: ControlCommand) -> Result<(), LaserError> {
        let timestamp_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                LaserError::Invalid(format!("system clock is before the unix epoch: {error}"))
            })?
            .as_micros() as u64;
        let envelope = ControlEnvelope {
            v: CONTROL_OP_VERSION,
            timestamp_micros,
            command,
        };
        let payload = Bytes::from(
            encode_named(&envelope)
                .map_err(|error| LaserError::Codec(format!("encode control command: {error}")))?,
        );
        self.send_with_headers_on(
            self.ops_stream(),
            self.control_topic(),
            payload,
            BTreeMap::new(),
            Some(CONTROL_PARTITION_KEY),
        )
        .await
    }
}
