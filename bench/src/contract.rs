use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ContractFingerprint {
    pub sdk_agent_op_version: u32,
    pub wire_agent_op_version: u32,
}

#[must_use]
pub fn fingerprint() -> ContractFingerprint {
    ContractFingerprint {
        sdk_agent_op_version: laser_sdk::wire::codes::AGENT_OP_VERSION,
        wire_agent_op_version: laser_wire::codes::AGENT_OP_VERSION,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_sdk_and_wire_dependencies_when_fingerprinted_then_should_share_agent_version() {
        let fingerprint = fingerprint();
        assert_eq!(fingerprint.sdk_agent_op_version, 1);
        assert_eq!(
            fingerprint.sdk_agent_op_version,
            fingerprint.wire_agent_op_version
        );
    }
}
