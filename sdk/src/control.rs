use crate::error::LaserError;
use crate::laser::Laser;
use crate::nodes::Target;
use laser_wire::codes::{AGDX_CONTROL_PUBLISH_CODE, CONTROL_OP_VERSION};
use laser_wire::control::{ControlCommand, ControlEnvelope};
use laser_wire::framing::encode_named;
use laser_wire::result::{CommandError, ResultCode};
use laser_wire::validate::Validate;

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
        self.publish_control_with_operation(command, stream, u128::from(ulid::Ulid::generate()))
            .await
    }

    pub(crate) async fn publish_control_with_operation(
        &self,
        command: ControlCommand,
        stream: Option<&str>,
        operation_id: u128,
    ) -> Result<(), LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.managed {
            return Err(LaserError::unsupported(
                "control",
                "control commands require a managed backend",
            ));
        }
        // The same refusal a server of another control version answers.
        if let Some(versions) = capabilities.versions
            && versions.control != CONTROL_OP_VERSION
        {
            return Err(LaserError::from(CommandError::new(
                ResultCode::VersionSkew,
                format!(
                    "control wire version must be {CONTROL_OP_VERSION}, got {}",
                    versions.control
                ),
            )));
        }
        command.validate()?;
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
            .send_managed_with_operation(
                AGDX_CONTROL_PUBLISH_CODE,
                payload,
                operation_id,
                Target::of_code(AGDX_CONTROL_PUBLISH_CODE),
            )
            .await?
            .0;
        let reply: CommandError = crate::error::decode_managed_reply(&reply)?;
        match reply.code {
            ResultCode::Ok => Ok(()),
            _ => Err(reply.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::{Capabilities, OpVersions};
    use iggy::prelude::IggyClient;

    #[tokio::test]
    async fn given_another_control_op_version_when_publishing_then_should_report_version_skew() {
        let laser = Laser::from_client(IggyClient::default()).with_capabilities(Capabilities {
            managed: true,
            versions: Some(OpVersions::new(1, CONTROL_OP_VERSION + 1, 1, 1)),
            ..Capabilities::OPEN
        });
        let error = laser
            .publish_control_in(ControlCommand::DropProjection("orders".to_owned()), None)
            .await
            .expect_err("a skewed control version refuses before sending");
        assert_eq!(error.code(), ResultCode::VersionSkew);
        assert!(error.is_version_skew(), "{error}");
    }

    #[tokio::test]
    async fn given_open_iggy_when_publishing_an_invalid_command_then_should_refuse_it_as_unsupported()
     {
        let command = ControlCommand::DropProjection(String::new());
        assert!(
            command.validate().is_err(),
            "the case needs an invalid command"
        );
        let laser = Laser::from_client(IggyClient::default()).with_capabilities(Capabilities::OPEN);
        let error = laser
            .publish_control_in(command, None)
            .await
            .expect_err("open Apache Iggy serves no control command");
        assert!(error.is_unsupported(), "{error}");
    }
}
