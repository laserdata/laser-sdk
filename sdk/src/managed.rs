use crate::error::LaserError;
use crate::laser::Laser;
use laser_wire::batch::{BatchItem, BatchReply, BatchRequest};
use laser_wire::codes::{
    AGDX_BATCH_CODE, AGDX_KV_CAS_FENCED_CODE, AGDX_KV_GET_CODE, AGDX_KV_LEASE_CODE,
    AGDX_KV_LEASE_RENEW_CODE, AGDX_KV_RELEASE_CODE, BATCH_OP_VERSION,
};
use laser_wire::framing::decode_named;
use laser_wire::kv::KvGet;
use laser_wire::validate::Validate;

impl Laser {
    /// Execute up to [`MAX_BATCH_OPS`](laser_wire::limits::MAX_BATCH_OPS)
    /// independent managed commands in one round trip. Input order is preserved
    /// and each returned slot contains that operation's typed reply bytes.
    /// Batching amortizes transport cost. It is not a transaction.
    pub async fn execute_batch(&self, ops: Vec<BatchItem>) -> Result<Vec<Vec<u8>>, LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.managed {
            return Err(LaserError::unsupported(
                "batch",
                "the managed command band is not served by this deployment",
            ));
        }
        if !capabilities.kv.fenced_leases && ops.iter().any(requires_fenced_leases) {
            return Err(LaserError::unsupported_feature(
                "batch",
                "kv_fenced_leases",
                "the batch contains a fenced-lease request that this deployment must not decode under the old contract",
            ));
        }
        let request = BatchRequest {
            v: BATCH_OP_VERSION,
            ops,
        };
        request.validate()?;
        let payload = laser_wire::framing::encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode batch: {error}")))?;
        let payload = self
            .send_raw_with_response(AGDX_BATCH_CODE, payload)
            .await?;
        let reply: BatchReply = crate::error::decode_managed_reply(&payload)?;
        Ok(reply.results)
    }
}

fn requires_fenced_leases(item: &BatchItem) -> bool {
    match item.code {
        AGDX_KV_LEASE_CODE
        | AGDX_KV_LEASE_RENEW_CODE
        | AGDX_KV_RELEASE_CODE
        | AGDX_KV_CAS_FENCED_CODE => true,
        AGDX_KV_GET_CODE => {
            decode_named::<KvGet>(&item.payload).is_ok_and(|request| request.min_position.is_some())
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capabilities::Capabilities;
    use laser_wire::codes::KV_LEASE_OP_VERSION;
    use laser_wire::kv::{CasExpect, KvCasFenced};

    #[tokio::test]
    async fn given_a_fenced_item_without_the_capability_when_batched_then_should_fail_before_send()
    {
        let laser = Laser::from_client(crate::iggy::clients::client::IggyClient::default())
            .with_capabilities(Capabilities::OPEN.with_managed(true).with_kv(true));
        let request = KvCasFenced {
            v: KV_LEASE_OP_VERSION,
            namespace: "state".to_owned(),
            key: b"value".to_vec(),
            value: b"next".to_vec(),
            expires_at_micros: None,
            expect: CasExpect::Absent,
            fence_namespace: "coordination".to_owned(),
            fence_key: b"owner".to_vec(),
            fence_token: 1,
        };
        let outcome = laser
            .execute_batch(vec![BatchItem {
                code: AGDX_KV_CAS_FENCED_CODE,
                payload: laser_wire::framing::encode_named(&request).expect("request encodes"),
            }])
            .await;
        assert!(matches!(outcome, Err(LaserError::Unsupported { .. })));
    }
}
