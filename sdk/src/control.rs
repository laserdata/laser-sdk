use crate::error::LaserError;
use crate::laser::Laser;
use laser_wire::codes::{AGDX_CONTROL_PUBLISH_CODE, CONTROL_OP_VERSION};
use laser_wire::control::{ControlCommand, ControlEnvelope};
use laser_wire::framing::encode_named;
use laser_wire::result::{CommandError, ResultCode};

impl Laser {
    /// Submit one control command through the authenticated managed endpoint.
    #[cfg(feature = "agent")]
    pub(crate) async fn publish_control(&self, command: ControlCommand) -> Result<(), LaserError> {
        self.publish_control_in(command, None).await
    }

    /// Publish one control command scoped to `stream`, the stream its
    /// resource names belong to. The shared write path for the projection
    /// registry and session source registration.
    pub(crate) async fn publish_control_in(
        &self,
        command: ControlCommand,
        stream: Option<&str>,
    ) -> Result<(), LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.managed {
            return Err(LaserError::unsupported(
                "control",
                "control commands require a managed backend",
            ));
        }
        if let Some(versions) = capabilities.versions
            && versions.control != CONTROL_OP_VERSION
        {
            return Err(LaserError::Invalid(format!(
                "control wire version must be {CONTROL_OP_VERSION}, got {}",
                versions.control
            )));
        }
        let envelope = ControlEnvelope {
            v: CONTROL_OP_VERSION,
            timestamp_micros: 0,
            command,
            stream: stream.map(str::to_owned),
            stamp: None,
        };
        let payload = encode_named(&envelope)
            .map_err(|error| LaserError::Codec(format!("encode control command: {error}")))?;
        let reply = self
            .send_raw_with_response(AGDX_CONTROL_PUBLISH_CODE, payload)
            .await?;
        let reply: CommandError = crate::error::decode_managed_reply(&reply)?;
        match reply.code {
            ResultCode::Ok => Ok(()),
            _ => Err(reply.into()),
        }
    }
}
