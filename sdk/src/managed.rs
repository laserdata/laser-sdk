use crate::error::LaserError;
use crate::laser::Laser;
use laser_wire::batch::{BatchItem, BatchReply, BatchRequest};
use laser_wire::codes::{AGDX_BATCH_CODE, BATCH_OP_VERSION};
use laser_wire::validate::Validate;

impl Laser {
    /// Execute up to [`MAX_BATCH_OPS`](laser_wire::limits::MAX_BATCH_OPS)
    /// independent managed commands in one round trip. Input order is preserved
    /// and each returned slot contains that operation's typed reply bytes.
    /// Batching amortizes transport cost. It is not a transaction.
    pub async fn execute_batch(&self, ops: Vec<BatchItem>) -> Result<Vec<Vec<u8>>, LaserError> {
        if !self.capabilities().await.managed {
            return Err(LaserError::unsupported(
                "batch",
                "the managed command band is not served by this deployment",
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
