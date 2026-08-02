use serde::{Deserialize, Serialize};

/// The mixed-operation batch request
/// ([`AGDX_BATCH_CODE`](crate::codes::AGDX_BATCH_CODE)): up to
/// [`MAX_BATCH_OPS`](crate::limits::MAX_BATCH_OPS) managed requests in one
/// round trip, each carrying its own command code and encoded payload. The
/// batch amortizes the round trip and nothing else: items execute
/// independently in order, each yields its own result, and a failed item
/// fails alone (explicitly NOT a transaction). A nested batch (an item whose
/// code is the batch code) is rejected.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchRequest {
    /// Op version, [`BATCH_OP_VERSION`](crate::codes::BATCH_OP_VERSION).
    pub v: u32,
    /// The managed requests, executed in order.
    pub ops: Vec<BatchItem>,
}

/// One managed request inside a [`BatchRequest`]: the command code it would
/// have been sent under on its own, and its encoded request bytes verbatim.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchItem {
    /// The managed command code (e.g. `AGDX_KV_GET_CODE`).
    pub code: u32,
    /// The op's own encoded request frame, exactly what a standalone send
    /// would carry.
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
}

/// The batch reply: each op's own reply bytes, in request order, exactly what
/// a standalone round trip would have returned (including a typed error
/// reply for an item that failed, so the caller decodes each slot with the
/// item's own reply type).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchReply {
    /// Per-op reply frames, index-aligned with the request's `ops`.
    #[serde(with = "crate::encoding::vec_bin_bytes")]
    pub results: Vec<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_batch_when_round_tripped_then_should_preserve_items_and_results() {
        let request = BatchRequest {
            v: crate::codes::BATCH_OP_VERSION,
            ops: vec![
                BatchItem {
                    code: crate::codes::AGDX_KV_GET_CODE,
                    payload: b"\xa1av\x01".to_vec(),
                },
                BatchItem {
                    code: crate::codes::AGDX_QUERY_CODE,
                    payload: Vec::new(),
                },
            ],
        };
        let decoded: BatchRequest =
            decode_named(&encode_named(&request).expect("encodes")).expect("decodes");
        assert_eq!(decoded.ops.len(), 2);
        assert_eq!(decoded.ops[0].code, crate::codes::AGDX_KV_GET_CODE);
        assert_eq!(decoded.ops[0].payload, request.ops[0].payload);

        let reply = BatchReply {
            results: vec![b"ok".to_vec(), Vec::new()],
        };
        let decoded: BatchReply =
            decode_named(&encode_named(&reply).expect("encodes")).expect("decodes");
        assert_eq!(decoded.results, reply.results);
    }
}
