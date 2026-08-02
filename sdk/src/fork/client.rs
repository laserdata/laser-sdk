use crate::error::LaserError;
use crate::fork::{
    ForkCreate, ForkDelete, ForkError, ForkInfo, ForkKind, ForkList, ForkOutcome, ForkPromote,
    ForkPut, ForkReply,
};
use crate::laser::Laser;
use crate::query::{
    AGDX_FORK_CREATE_CODE, AGDX_FORK_DELETE_CODE, AGDX_FORK_LIST_CODE, AGDX_FORK_PROMOTE_CODE,
    AGDX_FORK_PUT_CODE, FORK_OP_VERSION,
};
use laser_wire::framing::encode_named;
#[cfg(test)]
use laser_wire::limits::MAX_FORK_ID_BYTES;
use serde::Serialize;
use std::collections::BTreeMap;

impl Laser {
    /// A handle to one fork by id. Cheap to create, since it borrows the connection.
    /// Open the fork with [`ForkHandle::create`], then write rows, query it
    /// (`query(...).fork(id)`), and finally [`promote`](ForkHandle::promote) or
    /// [`squash`](ForkHandle::squash) it.
    pub fn fork<'a>(&'a self, fork_id: impl Into<String>) -> ForkHandle<'a> {
        ForkHandle {
            laser: self,
            fork_id: fork_id.into(),
        }
    }

    /// Every open fork for the authenticated user.
    pub async fn forks(&self) -> Result<Vec<ForkInfo>, LaserError> {
        match self
            .execute_fork(AGDX_FORK_LIST_CODE, &ForkList { v: FORK_OP_VERSION })
            .await?
        {
            ForkOutcome::List(forks) => Ok(forks),
            other => Err(unexpected("list", &other)),
        }
    }

    // Send one fork command over the binary connection and decode the reply.
    // Gated on `forks` (set by the connect-time probe). Without it, `Unsupported`.
    pub(crate) async fn execute_fork(
        &self,
        code: u32,
        request: &impl Serialize,
    ) -> Result<ForkOutcome, LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.forks {
            return Err(LaserError::unsupported(
                "fork",
                "forks are not served by this deployment",
            ));
        }
        // Fail fast on advertised version skew (see `execute_query`): a server
        // that advertised its accepted fork op version spares the round-trip.
        if let Some(versions) = capabilities.versions
            && versions.fork != FORK_OP_VERSION
        {
            return Err(ForkError::Version {
                expected: versions.fork,
                got: FORK_OP_VERSION,
            }
            .into());
        }
        let payload = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode fork request: {error}")))?;
        let payload = self.send_raw_with_response(code, payload).await?;
        match crate::error::decode_managed_reply::<ForkReply>(&payload)? {
            ForkReply::Ok(outcome) => Ok(outcome),
            ForkReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "fork: unknown reply variant".to_owned(),
            )),
        }
    }
}

/// A handle to one fork. Build it with [`Laser::fork`].
pub struct ForkHandle<'a> {
    laser: &'a Laser,
    fork_id: String,
}

impl<'a> ForkHandle<'a> {
    /// The fork id this handle is bound to.
    pub fn id(&self) -> &str {
        &self.fork_id
    }

    /// Start opening this fork. Choose `.severed()` or `.continuous()` (default
    /// continuous), optionally narrow the snapshot with `.tables([...])`, then
    /// `.send().await` for the fork's metadata.
    pub fn create(&self) -> ForkCreateRequest<'a> {
        ForkCreateRequest {
            laser: self.laser,
            fork_id: self.fork_id.clone(),
            parent: None,
            kind: ForkKind::Continuous,
            tables: Vec::new(),
        }
    }

    /// Promote this fork: splice its speculative rows onto the trunk (and apply
    /// its tombstones), then squash it. Returns the number of rows applied.
    pub async fn promote(&self) -> Result<usize, LaserError> {
        match self
            .laser
            .execute_fork(
                AGDX_FORK_PROMOTE_CODE,
                &ForkPromote {
                    v: FORK_OP_VERSION,
                    fork_id: self.fork_id.clone(),
                },
            )
            .await?
        {
            ForkOutcome::Promoted { rows } => Ok(rows),
            other => Err(unexpected("promote", &other)),
        }
    }

    /// Squash this fork: discard its speculative rows. Returns `true` when an open
    /// fork was removed, `false` when none existed.
    pub async fn squash(&self) -> Result<bool, LaserError> {
        match self
            .laser
            .execute_fork(
                AGDX_FORK_DELETE_CODE,
                &ForkDelete {
                    v: FORK_OP_VERSION,
                    fork_id: self.fork_id.clone(),
                },
            )
            .await?
        {
            ForkOutcome::Deleted(existed) => Ok(existed),
            other => Err(unexpected("squash", &other)),
        }
    }

    /// Write one speculative row into this fork at `(table, partition_id, offset)`.
    /// Add indexed fields with `.field`, an opaque body with `.payload`, an
    /// embedding with `.embedding`, or mark `.tombstone()` to hide the trunk row
    /// at that coordinate from the fork's view. Finish with `.send().await`.
    pub fn put_row(
        &self,
        table: impl Into<String>,
        partition_id: u32,
        offset: u64,
    ) -> ForkPutRequest<'a> {
        ForkPutRequest {
            laser: self.laser,
            fork_id: self.fork_id.clone(),
            table: table.into(),
            partition_id,
            offset,
            projection_id: String::new(),
            projection_version: 0,
            fields: BTreeMap::new(),
            metadata: BTreeMap::new(),
            payload: None,
            embedding: None,
            tombstone: false,
        }
    }
}

/// Fluent builder for [`ForkHandle::create`].
#[must_use = "call .send().await to create the fork"]
pub struct ForkCreateRequest<'a> {
    laser: &'a Laser,
    fork_id: String,
    parent: Option<String>,
    kind: ForkKind,
    tables: Vec<String>,
}

impl<'a> ForkCreateRequest<'a> {
    /// Make a frozen snapshot at the trunk's current offsets (later appends hidden).
    pub fn severed(mut self) -> Self {
        self.kind = ForkKind::Severed;
        self
    }

    /// Make a live branch that keeps seeing new trunk appends (the default).
    pub fn continuous(mut self) -> Self {
        self.kind = ForkKind::Continuous;
        self
    }

    /// Record this fork's parent (audit only, the fork still branches off the trunk).
    pub fn parent(mut self, parent: impl Into<String>) -> Self {
        self.parent = Some(parent.into());
        self
    }

    /// Narrow a severed snapshot to these tables. Empty captures every table.
    pub fn tables(mut self, tables: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tables = tables.into_iter().map(Into::into).collect();
        self
    }

    /// Open the fork. Returns its metadata.
    pub async fn send(self) -> Result<ForkInfo, LaserError> {
        validated_fork_id(&self.fork_id)?;
        let request = ForkCreate {
            v: FORK_OP_VERSION,
            fork_id: self.fork_id,
            parent: self.parent,
            kind: self.kind,
            tables: self.tables,
        };
        match self
            .laser
            .execute_fork(AGDX_FORK_CREATE_CODE, &request)
            .await?
        {
            ForkOutcome::Created(info) => Ok(info),
            other => Err(unexpected("create", &other)),
        }
    }
}

/// Fluent builder for [`ForkHandle::put_row`].
#[must_use = "call .send().await to write the speculative row"]
pub struct ForkPutRequest<'a> {
    laser: &'a Laser,
    fork_id: String,
    table: String,
    partition_id: u32,
    offset: u64,
    projection_id: String,
    projection_version: u32,
    fields: BTreeMap<String, String>,
    metadata: BTreeMap<String, String>,
    payload: Option<Vec<u8>>,
    embedding: Option<String>,
    tombstone: bool,
}

impl<'a> ForkPutRequest<'a> {
    /// Set the projection id/version this speculative row belongs to.
    pub fn projection(mut self, id: impl Into<String>, version: u32) -> Self {
        self.projection_id = id.into();
        self.projection_version = version;
        self
    }

    /// Add one indexed field (the columns queries filter and order on).
    pub fn field(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(name.into(), value.into());
        self
    }

    /// Add one metadata header (non-indexed).
    pub fn metadata(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(name.into(), value.into());
        self
    }

    /// Attach an opaque payload body.
    pub fn payload(mut self, payload: impl AsRef<[u8]>) -> Self {
        self.payload = Some(payload.as_ref().to_vec());
        self
    }

    /// Attach an embedding as a JSON array literal (e.g. `[0.1,0.2,...]`).
    pub fn embedding(mut self, embedding: impl Into<String>) -> Self {
        self.embedding = Some(embedding.into());
        self
    }

    /// Mark this as a tombstone: hide the trunk row at this coordinate from the
    /// fork's view instead of writing a value.
    pub fn tombstone(mut self) -> Self {
        self.tombstone = true;
        self
    }

    /// Write the speculative row.
    pub async fn send(self) -> Result<(), LaserError> {
        let request = ForkPut {
            v: FORK_OP_VERSION,
            fork_id: self.fork_id,
            table: self.table,
            partition_id: self.partition_id,
            offset: self.offset,
            projection_id: self.projection_id,
            projection_version: self.projection_version,
            fields: self.fields,
            metadata: self.metadata,
            payload: self.payload,
            embedding: self.embedding,
            tombstone: self.tombstone,
        };
        match self
            .laser
            .execute_fork(AGDX_FORK_PUT_CODE, &request)
            .await?
        {
            ForkOutcome::Written => Ok(()),
            other => Err(unexpected("put_row", &other)),
        }
    }
}

fn unexpected(op: &str, outcome: &ForkOutcome) -> LaserError {
    LaserError::Protocol(format!("fork {op}: unexpected outcome {outcome:?}"))
}

// Reject a malformed fork id before the round-trip, using the wire contract's
// canonical `validate_fork_id` (non-empty, within `MAX_FORK_ID_BYTES`, and the
// shared charset safelist) so a client fails fast on exactly what the managed
// side would reject, and the anti-injection charset rule lives in one place.
fn validated_fork_id(fork_id: &str) -> Result<(), LaserError> {
    laser_wire::fork::validate_fork_id(fork_id).map_err(|error| LaserError::Invalid(error.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_an_over_long_or_empty_fork_id_when_validated_then_should_reject() {
        assert!(validated_fork_id("").is_err());
        assert!(validated_fork_id("experiment-2026-q2").is_ok());
        let too_long = "f".repeat(MAX_FORK_ID_BYTES + 1);
        assert!(validated_fork_id(&too_long).is_err());
        let at_cap = "f".repeat(MAX_FORK_ID_BYTES);
        assert!(validated_fork_id(&at_cap).is_ok());
    }

    #[test]
    fn given_fork_error_when_mapped_then_should_nest_the_typed_error() {
        let error: LaserError = ForkError::NotFound("f9".to_owned()).into();
        assert!(matches!(error, LaserError::Fork(ForkError::NotFound(_))));
        assert!(error.is_not_found());
        let unsupported: LaserError = ForkError::Unsupported("x".to_owned()).into();
        assert!(matches!(
            unsupported,
            LaserError::Fork(ForkError::Unsupported(_))
        ));
        assert!(unsupported.is_unsupported());
    }
}
