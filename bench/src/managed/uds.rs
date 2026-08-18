use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use laser_sdk::wire::codes::{AGDX_KV_GET_CODE, KV_OP_VERSION};
use laser_sdk::wire::forward::ForwardedCommand;
use laser_sdk::wire::framing::{decode_named, encode_named, frame_encode};
use laser_sdk::wire::kv::{KvGet, KvOutcome, KvReply};
use laser_sdk::wire::limits::MAX_FRAME_BYTES;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

use super::{
    ManagedArmSummary, ManagedCase, ManagedProcessMeasurement, capture_processes, finish_processes,
    run_load, summarize, validate_case, warmup,
};
use crate::BenchError;
use crate::engine::{LoadResult, Operation};
use crate::process::PlaneProfile;

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_uds_arm
)]
pub enum UdsArm {
    KvGetMiss,
}

fn invalid_uds_arm(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported UDS arm `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct UdsSummary {
    pub operation: ManagedArmSummary,
    pub backend_profile: PlaneProfile,
    pub configuration: serde_json::Value,
}

pub struct UdsEvidence {
    pub summary: UdsSummary,
    pub load: LoadResult,
    pub processes: Vec<ManagedProcessMeasurement>,
}

#[derive(Clone)]
struct UdsContext {
    connections: Arc<Vec<Mutex<UnixStream>>>,
    request: Arc<Vec<u8>>,
}

/// Run a benchmark-only plane UDS diagnostic without the Iggy forwarding hop.
///
/// # Errors
///
/// Returns an error when the socket cannot be opened, framing fails, or plane returns an unexpected reply.
pub async fn run_uds_evidence(
    socket_path: &Path,
    case: &ManagedCase,
    arm: UdsArm,
    profile: PlaneProfile,
    monitored_processes: &[(String, u32)],
) -> Result<UdsEvidence, BenchError> {
    validate_case(case)?;
    let request = request(arm)?;
    let connections = connect(socket_path, case.concurrency).await?;
    let context = UdsContext {
        connections: Arc::new(connections),
        request: Arc::new(request),
    };
    let timeout = Duration::from_millis(case.timeout_millis);
    warmup(case, timeout, context.clone().operation()).await?;
    let before = capture_processes(monitored_processes)?;
    let load = run_load(case, timeout, context.operation()).await?;
    let processes = finish_processes(before, "measurement")?;
    let operation = summarize(arm.into(), &load, case, 1);
    Ok(UdsEvidence {
        summary: UdsSummary {
            operation,
            backend_profile: profile,
            configuration: serde_json::json!({
                "path": "direct_plane_uds_diagnostic",
                "latency_boundary": "uds_frame_write_to_reply_frame",
                "connections": case.concurrency,
                "identity": "synthetic_trusted_user_1",
                "public_sdk_path": false,
                "diagnostic_only": true,
            }),
        },
        load,
        processes,
    })
}

fn request(arm: UdsArm) -> Result<Vec<u8>, BenchError> {
    match arm {
        UdsArm::KvGetMiss => {
            let payload = encode_named(&KvGet {
                v: KV_OP_VERSION,
                namespace: "laser_bench_uds".to_owned(),
                key: b"missing".to_vec(),
                if_none_match: None,
                min_position: None,
            })
            .map_err(|error| BenchError::Invalid(format!("encode UDS KV request: {error}")))?;
            let forwarded = encode_named(&ForwardedCommand {
                user_id: 1,
                client_id: 0,
                correlation: None,
                operation_id: None,
                read_all: false,
                command_code: AGDX_KV_GET_CODE,
                payload,
                grants: Vec::new(),
            })
            .map_err(|error| {
                BenchError::Invalid(format!("encode UDS forwarded command: {error}"))
            })?;
            frame_encode(&forwarded)
                .map_err(|error| BenchError::Invalid(format!("frame UDS command: {error}")))
        }
    }
}

async fn connect(path: &Path, count: usize) -> Result<Vec<Mutex<UnixStream>>, BenchError> {
    let mut connections = Vec::with_capacity(count);
    for _ in 0..count {
        let stream = UnixStream::connect(path).await.map_err(|error| {
            BenchError::Invalid(format!("connect plane UDS `{}`: {error}", path.display()))
        })?;
        connections.push(Mutex::new(stream));
    }
    Ok(connections)
}

impl UdsContext {
    fn operation(self) -> Operation {
        Arc::new(move |sequence| {
            let context = self.clone();
            Box::pin(async move {
                let connection = usize::try_from(sequence)
                    .map_err(|_| "UDS sequence exceeds usize".to_owned())?
                    % context.connections.len();
                let mut stream = context.connections[connection].lock().await;
                stream
                    .write_all(&context.request)
                    .await
                    .map_err(|error| error.to_string())?;
                stream.flush().await.map_err(|error| error.to_string())?;
                let response = read_frame(&mut stream).await?;
                match decode_named::<KvReply>(&response).map_err(|error| error.to_string())? {
                    KvReply::Ok(KvOutcome::Value(None)) => Ok(()),
                    KvReply::Ok(_) => {
                        Err("direct UDS KV miss returned an unexpected outcome".to_owned())
                    }
                    KvReply::Err(error) => Err(format!("direct UDS KV miss failed: {error:?}")),
                    _ => Err("direct UDS KV miss returned an unknown reply".to_owned()),
                }
            })
        })
    }
}

async fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>, String> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|error| error.to_string())?;
    let length = u32::from_le_bytes(prefix) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(format!("plane UDS reply exceeds {MAX_FRAME_BYTES} bytes"));
    }
    let mut payload = vec![0; length];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|error| error.to_string())?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_uds_arm_when_request_is_built_then_should_use_shared_forwarded_frame() {
        let framed = request(UdsArm::KvGetMiss).expect("request should encode");
        let (payload, consumed) = laser_sdk::wire::framing::frame_decode(&framed)
            .expect("frame should decode")
            .expect("frame should be complete");
        let forwarded: ForwardedCommand =
            decode_named(payload).expect("forwarded command should decode");
        assert_eq!(consumed, framed.len());
        assert_eq!(forwarded.command_code, AGDX_KV_GET_CODE);
        assert_eq!(forwarded.user_id, 1);
    }

    #[test]
    fn given_uds_arm_names_when_parsed_then_should_use_snake_case() {
        assert_eq!(
            "kv_get_miss"
                .parse::<UdsArm>()
                .expect("UDS arm should parse"),
            UdsArm::KvGetMiss
        );
        assert!("kv-get-miss".parse::<UdsArm>().is_err());
    }
}
