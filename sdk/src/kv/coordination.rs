use crate::error::{LaserError, driver_never_wrote, driver_wrote};
use crate::iggy::prelude::{Client, IggyError};
use crate::kv::{
    AGDX_KV_CAS_FENCED_CODE, AGDX_KV_GET_CODE, AGDX_KV_LEASE_CODE, AGDX_KV_LEASE_RENEW_CODE,
    AGDX_KV_RELEASE_CODE, KvCasFenced, KvEntry, KvGet, KvLease, KvLeaseRenew, KvRelease, Lease,
};
use crate::laser::{Laser, unknown_outcome};
use laser_wire::framing::encode_named;
use laser_wire::kv::{KvOutcome, KvReply};
use laser_wire::mutation::{MANAGED_REQUEST_VERSION, ManagedRequestEnvelope};
use laser_wire::validate::Validate;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Per-attempt bound before an in-flight coordination request is declared
/// ambiguous and transport retirement begins. Retirement still waits for any
/// cancellation-safe Iggy request task to stop before recovery proceeds. It
/// sits above the managed backend's 10 second fold wait, so a mutation the fold
/// has not applied yet comes back as a `Pending` reply rather than a timeout.
pub const DEFAULT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);

/// The first pause before an ambiguous acquisition or renewal is repeated
/// under its prepared identity. It doubles up to [`MAX_REPEAT_DELAY`].
const FIRST_REPEAT_DELAY: Duration = Duration::from_millis(100);
const MAX_REPEAT_DELAY: Duration = Duration::from_secs(2);
/// How often a reused dedicated connection may probe again for the
/// fenced-lease capability it lacked.
const CAPABILITY_REPROBE_INTERVAL: Duration = Duration::from_secs(1);

/// The transport seam under [`FencedLeaseClient`]: send one managed command
/// frame and return the raw reply bytes, or discard connection state after a
/// cancellation or ambiguous response. Injectable, so the client's envelope,
/// retry, and decode behavior unit-tests without a server.
#[trait_variant::make(ManagedKvTransport: Send)]
pub trait LocalManagedKvTransport {
    /// Establish the transport and perform deterministic capability checks
    /// before a request enters the ambiguity window. Connection refusal,
    /// authentication failure, and unsupported deployments surface unchanged.
    fn ready(&self) -> impl Future<Output = Result<(), LaserError>> {
        async { Ok(()) }
    }
    /// Send one framed managed command and return the raw reply bytes.
    async fn send(&self, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError>;
    /// Actively close and discard any connection state. Called after a timed-out
    /// or failed attempt whose request may still be in flight. This method must
    /// stop that work before it returns, so its remaining lifetime cannot outlive
    /// an acquisition recovery wait and a lockstep transport cannot read a stale
    /// reply as the answer to the next request.
    async fn reset(&self);

    /// The connection [`send_on`](Self::send_on) would use now, read when a
    /// call starts. A transport without a replaceable connection answers 0.
    fn generation(&self) -> u64 {
        0
    }

    /// Send on the connection `generation` names. A transport that has
    /// replaced it refuses with a retryable error before sending. The default
    /// sends on the current connection.
    fn send_on(
        &self,
        _generation: u64,
        code: u32,
        frame: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, LaserError>> {
        self.send(code, frame)
    }

    /// [`reset`](Self::reset) only while the connection is still
    /// `generation`, so a late failure on a retired connection never ends the
    /// one that replaced it. The default resets.
    fn reset_generation(&self, _generation: u64) -> impl Future<Output = ()> {
        self.reset()
    }

    /// Close the transport permanently when supported. The default retires its connection.
    fn close(&self) -> impl Future<Output = ()>
    where
        Self: Sync,
    {
        async { self.reset().await }
    }
}

/// The object-safe counterpart of [`ManagedKvTransport`], for a consumer that
/// must hold its transport behind a pointer instead of as a generic parameter:
/// a state-provider seam reached through a `#[non_exhaustive]` enum cannot grow
/// a type parameter without leaking it through every construction and call
/// site. [`ManagedKvTransport`]'s methods return an anonymous future per
/// implementation, so that trait has no vtable. This one boxes the future to
/// get one.
///
/// Do not implement this by hand. Every [`ManagedKvTransport`] already
/// implements it, and [`SharedKvTransport`] implements [`ManagedKvTransport`]
/// back, so a runtime-selected transport drops straight into
/// [`FencedLeaseClient`] with the same envelope, retry, and decode behavior:
///
/// ```no_run
/// # use std::sync::Arc;
/// # use laser_sdk::kv::{DedicatedKvTransport, FencedLeaseClient, SharedKvTransport};
/// let transport: SharedKvTransport = Arc::new(DedicatedKvTransport::new("iggy+tcp://..."));
/// let client = FencedLeaseClient::new(transport);
/// ```
///
/// The boxing costs one allocation per request, against a network round trip
/// under an in-flight request timeout.
#[async_trait::async_trait]
pub trait DynManagedKvTransport: Send + Sync {
    /// See [`ManagedKvTransport::ready`].
    async fn ready(&self) -> Result<(), LaserError>;
    /// See [`ManagedKvTransport::send`].
    async fn send(&self, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError>;
    /// See [`ManagedKvTransport::reset`]. The same contract holds: in-flight
    /// work must stop before this returns.
    async fn reset(&self);

    /// See [`ManagedKvTransport::generation`].
    fn generation(&self) -> u64 {
        0
    }

    /// See [`ManagedKvTransport::send_on`].
    async fn send_on(
        &self,
        _generation: u64,
        code: u32,
        frame: Vec<u8>,
    ) -> Result<Vec<u8>, LaserError> {
        self.send(code, frame).await
    }

    /// See [`ManagedKvTransport::reset_generation`].
    async fn reset_generation(&self, _generation: u64) {
        self.reset().await;
    }

    async fn close(&self) {
        self.reset().await;
    }
}

/// A [`ManagedKvTransport`] chosen at runtime: the shape a consumer stores when
/// the concrete transport (dedicated connection, stub, in-process fake) is a
/// configuration decision rather than a type-level one.
pub type SharedKvTransport = Arc<dyn DynManagedKvTransport>;

#[async_trait::async_trait]
impl<T: ManagedKvTransport + Send + Sync> DynManagedKvTransport for T {
    async fn ready(&self) -> Result<(), LaserError> {
        ManagedKvTransport::ready(self).await
    }

    async fn send(&self, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
        ManagedKvTransport::send(self, code, frame).await
    }

    async fn reset(&self) {
        ManagedKvTransport::reset(self).await;
    }

    fn generation(&self) -> u64 {
        ManagedKvTransport::generation(self)
    }

    async fn send_on(
        &self,
        generation: u64,
        code: u32,
        frame: Vec<u8>,
    ) -> Result<Vec<u8>, LaserError> {
        ManagedKvTransport::send_on(self, generation, code, frame).await
    }

    async fn reset_generation(&self, generation: u64) {
        ManagedKvTransport::reset_generation(self, generation).await;
    }

    async fn close(&self) {
        ManagedKvTransport::close(self).await;
    }
}

// The other direction, so boxing a transport does not cost the typed client:
// dispatch goes through the vtable to the concrete transport's own
// implementation, never back into this one.
impl ManagedKvTransport for SharedKvTransport {
    async fn ready(&self) -> Result<(), LaserError> {
        DynManagedKvTransport::ready(&**self).await
    }

    async fn send(&self, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
        DynManagedKvTransport::send(&**self, code, frame).await
    }

    async fn reset(&self) {
        DynManagedKvTransport::reset(&**self).await;
    }

    fn generation(&self) -> u64 {
        DynManagedKvTransport::generation(&**self)
    }

    async fn send_on(
        &self,
        generation: u64,
        code: u32,
        frame: Vec<u8>,
    ) -> Result<Vec<u8>, LaserError> {
        DynManagedKvTransport::send_on(&**self, generation, code, frame).await
    }

    async fn reset_generation(&self, generation: u64) {
        DynManagedKvTransport::reset_generation(&**self, generation).await;
    }

    async fn close(&self) {
        DynManagedKvTransport::close(&**self).await;
    }
}

/// A coordination transport over its own dedicated connection, built lazily
/// from a connection string and discarded whole on [`reset`]. Dedicated
/// because a raw managed request holds a lockstep client for its full bound:
/// borrowing a shared producer client would let one slow coordination call
/// stall unrelated traffic (and the reverse). The first readiness check after
/// a reset reconnects and re-verifies the `kv_fenced_leases` capability, failing
/// closed against a pre-fencing deployment. That deployment keeps its one
/// connection, so each later call answers `Unsupported` without a new login,
/// and probes the capability again at most once a second.
///
/// [`reset`]: ManagedKvTransport::reset
pub struct DedicatedKvTransport {
    connection_string: String,
    slot: Mutex<Option<Laser>>,
    // Moves on every retirement, under the slot lock, so it names the
    // connection the slot holds.
    generation: AtomicU64,
    probed_at: std::sync::Mutex<Option<Instant>>,
    closed: AtomicBool,
    retirement: Mutex<()>,
}

impl DedicatedKvTransport {
    /// A transport that will connect to `connection_string` on first use.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            slot: Mutex::new(None),
            generation: AtomicU64::new(0),
            probed_at: std::sync::Mutex::new(None),
            closed: AtomicBool::new(false),
            retirement: Mutex::new(()),
        }
    }

    /// Close this dedicated transport. Reset retires a connection and permits later reuse.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        ManagedKvTransport::reset(self).await;
    }

    async fn client(&self) -> Result<Laser, LaserError> {
        let _retirement = self.retirement.lock().await;
        let mut slot = self.slot.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::iggy::prelude::IggyError::ClientShutdown.into());
        }
        let (laser, reused) = match slot.as_ref() {
            Some(laser) => (laser.clone(), true),
            None => {
                let laser = Laser::connect(&self.connection_string).await?;
                *slot = Some(laser.clone());
                *self
                    .probed_at
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Instant::now());
                (laser, false)
            }
        };
        let mut capabilities = laser.capabilities().await;
        if reused && !capabilities.kv.fenced_leases && self.reprobe_due() {
            capabilities = laser.refresh_capabilities().await;
        }
        if !capabilities.kv.fenced_leases {
            return Err(LaserError::unsupported_feature(
                "kv",
                "coordination",
                "the fenced-lease contract is not advertised by this deployment",
            ));
        }
        Ok(laser)
    }

    // A connection that lacks the capability answers every later call from its
    // last probe until a second has passed, so a refused call costs no round trip.
    fn reprobe_due(&self) -> bool {
        let mut probed_at = self
            .probed_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let due = probed_at.is_none_or(|at| at.elapsed() >= CAPABILITY_REPROBE_INTERVAL);
        if due {
            *probed_at = Some(Instant::now());
        }
        due
    }

    // Retires the connection the slot holds, or only while it is still
    // `generation` when a failing call names the one it used.
    async fn retire(&self, generation: Option<u64>) {
        let _retirement = self.retirement.lock().await;
        let retired = {
            let mut slot = self.slot.lock().await;
            if generation.is_some_and(|used| used != self.generation.load(Ordering::Acquire)) {
                return;
            }
            self.generation.fetch_add(1, Ordering::AcqRel);
            slot.take()
        };
        if let Some(laser) = retired {
            let _ = laser.client().shutdown().await;
        }
    }

    // Sends one frame, keeping the unknown outcome of a frame any attempt wrote.
    async fn send_frame(laser: &Laser, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
        let written = AtomicBool::new(false);
        laser
            .send_raw_preframed(code, frame, &written)
            .await
            .map_err(|error| written_outcome(error, written.load(Ordering::Acquire)).into())
    }
}

impl ManagedKvTransport for DedicatedKvTransport {
    async fn ready(&self) -> Result<(), LaserError> {
        self.client().await.map(|_| ())
    }

    async fn send(&self, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
        let laser = self.slot.lock().await.as_ref().cloned().ok_or_else(|| {
            LaserError::HandlerConfig("coordination transport is not ready".to_owned())
        })?;
        Self::send_frame(&laser, code, frame).await
    }

    async fn reset(&self) {
        self.retire(None).await;
    }

    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    async fn send_on(
        &self,
        generation: u64,
        code: u32,
        frame: Vec<u8>,
    ) -> Result<Vec<u8>, LaserError> {
        let laser = {
            let slot = self.slot.lock().await;
            // The connection this call readied was retired, so nothing is sent
            // on its replacement.
            if self.generation.load(Ordering::Acquire) != generation {
                return Err(IggyError::NotConnected.into());
            }
            slot.as_ref().cloned()
        }
        .ok_or_else(|| {
            LaserError::HandlerConfig("coordination transport is not ready".to_owned())
        })?;
        Self::send_frame(&laser, code, frame).await
    }

    async fn reset_generation(&self, generation: u64) {
        self.retire(Some(generation)).await;
    }

    async fn close(&self) {
        DedicatedKvTransport::close(self).await;
    }
}

/// One prepared coordination mutation: the command code and exact framed
/// bytes, `ManagedRequestEnvelope` included, minted once and bound to the
/// [`FencedLeaseClient`] that prepared it. It is deliberately not `Clone`, so
/// one operation identity cannot be fanned out across transports or
/// deployments.
#[derive(Debug)]
pub struct PreparedMutation {
    client_id: u128,
    code: u32,
    operation_id: u128,
    frame: Vec<u8>,
    ambiguous_recovery: AmbiguousMutationRecovery,
    first_sent: OnceLock<Instant>,
}

/// The recovery a caller must use when execution of a prepared mutation
/// returns [`LaserError::AmbiguousMutation`]. This keeps operation-specific
/// recovery inspectable instead of asking a generic retry classifier to guess.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AmbiguousMutationRecovery {
    /// Do not acquire under a new identity until this duration has elapsed
    /// after the ambiguous result. A grant never outlives its requested TTL.
    /// Repeating this exact [`PreparedMutation`] first is safe: the backend
    /// answers a repeated identity with its recorded outcome.
    WaitForLeaseExpiry(Duration),
    /// Repeat this exact [`PreparedMutation`], preserving its operation id.
    RepeatPrepared,
    /// Read the target and reconcile through the fenced CAS precondition.
    ReconcileTargetPrecondition,
}

impl PreparedMutation {
    /// The stable operation identity this mutation carries across retries.
    pub fn operation_id(&self) -> u128 {
        self.operation_id
    }

    /// The operation-specific action required after an ambiguous result.
    pub fn ambiguous_recovery(&self) -> AmbiguousMutationRecovery {
        self.ambiguous_recovery
    }

    // A grant or renewal can apply no earlier than the first send, so a
    // validity measured from it never outlasts the backend's own expiry.
    fn valid_until(&self, granted_ttl: Duration) -> Instant {
        let sent = *self.first_sent.get_or_init(Instant::now);
        sent.checked_add(granted_ttl).unwrap_or(sent)
    }

    fn build(
        client_id: u128,
        code: u32,
        request: &impl serde::Serialize,
        ambiguous_recovery: AmbiguousMutationRecovery,
    ) -> Result<Self, LaserError> {
        let payload = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        let operation_id = u128::from(ulid::Ulid::generate());
        let frame = encode_named(&ManagedRequestEnvelope {
            v: MANAGED_REQUEST_VERSION,
            operation_id,
            payload,
        })
        .map_err(|error| LaserError::Codec(format!("encode envelope: {error}")))?;
        Ok(Self {
            client_id,
            code,
            operation_id,
            frame,
            ambiguous_recovery,
            first_sent: OnceLock::new(),
        })
    }
}

/// The typed client for the fenced-lease coordination contract: acquire,
/// renew, and release a revocable lease, apply a fenced compare-and-swap, and
/// run the barriered read, over an injectable [`ManagedKvTransport`].
///
/// The mutation flow is two-phase by design. `prepare_*` validates the request
/// and mints the operation id once. The matching execute method sends the
/// exact frame under a per-attempt timeout. A timeout or transport failure
/// resets the transport and surfaces as [`LaserError::AmbiguousMutation`]. It
/// is not generically retryable: [`PreparedMutation::ambiguous_recovery`]
/// provides the machine-readable recovery action. Renew and release may repeat
/// the same prepared request, a fenced CAS reconciles through its target
/// precondition, while an acquire whose reply was lost may repeat the same
/// prepared request and must otherwise wait through its requested maximum TTL
/// before a fresh acquisition, because a grant that landed without its reply
/// holds the key until it expires. A granted [`Lease`] is valid locally until
/// its first send plus the granted TTL. The managed
/// backend answers a repeated identity with its recorded outcome, and a reply
/// that names no outcome yet (`Pending`, `BackendUnavailable`, `Unavailable`)
/// is ambiguous like a lost one. Unexpected reply outcomes are rejected
/// fail-closed, never coerced.
pub struct FencedLeaseClient<T> {
    client_id: u128,
    transport: T,
    attempt_timeout: Duration,
    closed: AtomicBool,
    retirement: Mutex<()>,
}

impl FencedLeaseClient<DedicatedKvTransport> {
    /// A client over its own dedicated connection to `connection_string`,
    /// connected lazily on first use.
    pub fn connect_dedicated(connection_string: impl Into<String>) -> Self {
        Self::new(DedicatedKvTransport::new(connection_string))
    }
}

impl<T: ManagedKvTransport + Sync> FencedLeaseClient<T> {
    /// A client over `transport` with the default per-attempt timeout.
    pub fn new(transport: T) -> Self {
        Self {
            client_id: u128::from(ulid::Ulid::generate()),
            transport,
            attempt_timeout: DEFAULT_ATTEMPT_TIMEOUT,
            closed: AtomicBool::new(false),
            retirement: Mutex::new(()),
        }
    }

    /// Returns the client with a caller-chosen per-attempt timeout. A zero
    /// duration is rejected before connection or send as `LaserError::Invalid`.
    #[must_use]
    pub fn with_attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = timeout;
        self
    }

    /// Close the client and retire its transport. Later executions fail before readiness or send.
    pub async fn close(&self) {
        let _retirement = self.retirement.lock().await;
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.transport.close().await;
        }
    }

    /// Prepare a lease acquisition. Validation happens here, once, so a retry
    /// can never diverge from what was first sent.
    pub fn prepare_acquire(&self, request: &KvLease) -> Result<PreparedMutation, LaserError> {
        request.validate()?;
        PreparedMutation::build(
            self.client_id,
            AGDX_KV_LEASE_CODE,
            request,
            AmbiguousMutationRecovery::WaitForLeaseExpiry(Duration::from_micros(
                request.lease_ttl_micros,
            )),
        )
    }

    /// Prepare a lease renewal.
    pub fn prepare_renew(&self, request: &KvLeaseRenew) -> Result<PreparedMutation, LaserError> {
        request.validate()?;
        PreparedMutation::build(
            self.client_id,
            AGDX_KV_LEASE_RENEW_CODE,
            request,
            AmbiguousMutationRecovery::RepeatPrepared,
        )
    }

    /// Prepare a lease release.
    pub fn prepare_release(&self, request: &KvRelease) -> Result<PreparedMutation, LaserError> {
        request.validate()?;
        PreparedMutation::build(
            self.client_id,
            AGDX_KV_RELEASE_CODE,
            request,
            AmbiguousMutationRecovery::RepeatPrepared,
        )
    }

    /// Prepare a fenced compare-and-swap.
    pub fn prepare_cas_fenced(
        &self,
        request: &KvCasFenced,
    ) -> Result<PreparedMutation, LaserError> {
        request.validate()?;
        PreparedMutation::build(
            self.client_id,
            AGDX_KV_CAS_FENCED_CODE,
            request,
            AmbiguousMutationRecovery::ReconcileTargetPrecondition,
        )
    }

    /// Execute a prepared acquisition, returning the granted [`Lease`].
    pub async fn acquire(&self, op: &PreparedMutation) -> Result<Lease, LaserError> {
        match self
            .execute_with_timeout(op, AGDX_KV_LEASE_CODE, "acquire", self.attempt_timeout)
            .await?
        {
            KvOutcome::Leased {
                lease_token,
                granted_ttl_micros,
                position,
            } => Ok(Lease {
                token: lease_token,
                granted_ttl: Duration::from_micros(granted_ttl_micros),
                position,
                valid_until: op.valid_until(Duration::from_micros(granted_ttl_micros)),
            }),
            other => Err(unexpected("acquire", &other)),
        }
    }

    /// Execute a prepared renewal, returning the extended [`Lease`] (same
    /// token, fresh TTL and position).
    pub async fn renew(&self, op: &PreparedMutation) -> Result<Lease, LaserError> {
        self.renew_with_timeout(op, self.attempt_timeout).await
    }

    pub(crate) async fn renew_with_timeout(
        &self,
        op: &PreparedMutation,
        timeout: Duration,
    ) -> Result<Lease, LaserError> {
        match self
            .execute_with_timeout(op, AGDX_KV_LEASE_RENEW_CODE, "renew", timeout)
            .await?
        {
            KvOutcome::Renewed {
                lease_token,
                granted_ttl_micros,
                position,
            } => Ok(Lease {
                token: lease_token,
                granted_ttl: Duration::from_micros(granted_ttl_micros),
                position,
                valid_until: op.valid_until(Duration::from_micros(granted_ttl_micros)),
            }),
            other => Err(unexpected("renew", &other)),
        }
    }

    /// Execute a prepared acquisition and resolve an ambiguous result by
    /// repeating the same identity while the requested TTL since the first send
    /// is open. A grant that comes back already past its
    /// [`valid_until`](Lease::valid_until) is [`KvError::LeaseLost`].
    ///
    /// [`KvError::LeaseLost`]: laser_wire::kv::KvError::LeaseLost
    pub(crate) async fn acquire_resolving(
        &self,
        op: &PreparedMutation,
    ) -> Result<Lease, LaserError> {
        let window = match op.ambiguous_recovery {
            AmbiguousMutationRecovery::WaitForLeaseExpiry(ttl) => ttl,
            _ => return self.acquire(op).await,
        };
        let mut delay = FIRST_REPEAT_DELAY;
        loop {
            match self.acquire(op).await {
                Err(error @ LaserError::AmbiguousMutation { .. }) => {
                    let elapsed = op.first_sent.get().map_or(window, Instant::elapsed);
                    let remaining = window.saturating_sub(elapsed);
                    if remaining.is_zero() {
                        return Err(error);
                    }
                    tokio::time::sleep(delay.min(remaining)).await;
                    delay = delay.saturating_mul(2).min(MAX_REPEAT_DELAY);
                }
                Ok(lease) if lease.valid_until <= Instant::now() => {
                    return Err(laser_wire::kv::KvError::LeaseLost.into());
                }
                result => return result,
            }
        }
    }

    /// Execute a prepared renewal, repeating it under the same identity after
    /// an ambiguous result until `deadline`, the instant the held lease stops
    /// being valid locally.
    #[cfg(any(feature = "agent", test))]
    pub(crate) async fn renew_resolving(
        &self,
        op: &PreparedMutation,
        deadline: Instant,
    ) -> Result<Lease, LaserError> {
        let mut delay = FIRST_REPEAT_DELAY;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(laser_wire::kv::KvError::LeaseLost.into());
            }
            match self
                .renew_with_timeout(op, remaining.min(self.attempt_timeout))
                .await
            {
                Err(LaserError::AmbiguousMutation { .. }) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    tokio::time::sleep(delay.min(remaining)).await;
                    delay = delay.saturating_mul(2).min(MAX_REPEAT_DELAY);
                }
                result => return result,
            }
        }
    }

    /// Execute a prepared release. `true` when a held lease was released,
    /// `false` when none was held (idempotent).
    pub async fn release(&self, op: &PreparedMutation) -> Result<bool, LaserError> {
        match self
            .execute_with_timeout(op, AGDX_KV_RELEASE_CODE, "release", self.attempt_timeout)
            .await?
        {
            KvOutcome::Released(released) => Ok(released),
            other => Err(unexpected("release", &other)),
        }
    }

    /// Execute a prepared fenced compare-and-swap, returning the entry's new
    /// version.
    pub async fn cas_fenced(&self, op: &PreparedMutation) -> Result<u64, LaserError> {
        match self
            .execute_with_timeout(
                op,
                AGDX_KV_CAS_FENCED_CODE,
                "cas_fenced",
                self.attempt_timeout,
            )
            .await?
        {
            KvOutcome::Committed { version } => Ok(version),
            other => Err(unexpected("cas_fenced", &other)),
        }
    }

    /// The (optionally barriered) read. A read is not a managed mutation, so
    /// it rides unframed with no operation id, and a timeout is a plain
    /// retryable [`LaserError::Timeout`], never ambiguous. A barrier the fold
    /// cannot reach surfaces as the typed
    /// [`KvError::Stale`](laser_wire::kv::KvError::Stale), which the caller
    /// must treat as a retryable orchestration failure, never as an absent
    /// value.
    pub async fn get(&self, request: &KvGet) -> Result<Option<KvEntry>, LaserError> {
        Self::ensure_attempt_timeout(self.attempt_timeout)?;
        self.ready().await?;
        let generation = self.transport.generation();
        let frame = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        let send = self.transport.send_on(generation, AGDX_KV_GET_CODE, frame);
        let reply = match tokio::time::timeout(self.attempt_timeout, send).await {
            Err(_elapsed) => {
                self.transport.reset_generation(generation).await;
                return Err(LaserError::Timeout("kv coordination get"));
            }
            Ok(Err(error)) => {
                self.transport.reset_generation(generation).await;
                return Err(error);
            }
            Ok(Ok(bytes)) => bytes,
        };
        match decode_reply(&reply)? {
            KvOutcome::Value(entry) => Ok(entry),
            other => Err(unexpected("get", &other)),
        }
    }

    async fn execute_with_timeout(
        &self,
        op: &PreparedMutation,
        expected_code: u32,
        what: &'static str,
        attempt_timeout: Duration,
    ) -> Result<KvOutcome, LaserError> {
        Self::ensure_attempt_timeout(attempt_timeout)?;
        if op.client_id != self.client_id {
            return Err(LaserError::Invalid(
                "prepared mutation belongs to a different fenced-lease client".to_owned(),
            ));
        }
        // A prepared release replayed through `acquire` would coerce outcomes
        // across operations. The pairing is checked, not assumed.
        if op.code != expected_code {
            return Err(LaserError::Invalid(format!(
                "prepared mutation carries code {} but was executed as {what}",
                op.code
            )));
        }
        self.ready_with_timeout(attempt_timeout).await?;
        // A failure retires only the connection this call used, never one a
        // later call has opened since.
        let generation = self.transport.generation();
        op.first_sent.get_or_init(Instant::now);
        let send = self
            .transport
            .send_on(generation, op.code, op.frame.clone());
        let reply = match tokio::time::timeout(attempt_timeout, send).await {
            Err(_elapsed) => {
                self.transport.reset_generation(generation).await;
                return Err(LaserError::AmbiguousMutation {
                    operation_id: Some(op.operation_id),
                    message: format!(
                        "{what} attempt exceeded {attempt_timeout:?} and requires operation-specific recovery"
                    ),
                });
            }
            Ok(Err(error)) => {
                self.transport.reset_generation(generation).await;
                // A frame never written cannot have applied. One written without
                // a reply may have.
                let may_have_written = match FrameWrite::from(&error) {
                    FrameWrite::Written => true,
                    FrameWrite::NotWritten => false,
                    FrameWrite::Unknown => error.is_retryable() && !error.is_permission_denied(),
                };
                if !may_have_written {
                    return Err(error);
                }
                return Err(LaserError::AmbiguousMutation {
                    operation_id: Some(op.operation_id),
                    message: format!("{what} transport failed mid-request: {error}"),
                });
            }
            Ok(Ok(bytes)) => bytes,
        };
        // The backend accepted the frame but names no outcome yet, or the node
        // could not reach its backend: the mutation may still apply. The reply
        // arrived whole, so the connection stays in step and other requests on
        // it keep running.
        if let Some(unknown) = unknown_outcome(&reply) {
            return Err(LaserError::AmbiguousMutation {
                operation_id: Some(op.operation_id),
                message: format!(
                    "{what} operation {:032x} outcome is unknown ({:?}: {}) and requires operation-specific recovery",
                    op.operation_id, unknown.code, unknown.message
                ),
            });
        }
        decode_reply(&reply)
    }

    fn ensure_attempt_timeout(timeout: Duration) -> Result<(), LaserError> {
        if timeout.is_zero() {
            Err(LaserError::Invalid(
                "coordination attempt timeout must be greater than zero".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    async fn ready(&self) -> Result<(), LaserError> {
        Self::ensure_attempt_timeout(self.attempt_timeout)?;
        self.ready_with_timeout(self.attempt_timeout).await
    }

    async fn ready_with_timeout(&self, timeout: Duration) -> Result<(), LaserError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::iggy::prelude::IggyError::ClientShutdown.into());
        }
        let generation = self.transport.generation();
        match tokio::time::timeout(timeout, self.transport.ready()).await {
            Err(_elapsed) => {
                self.transport.reset_generation(generation).await;
                Err(LaserError::Timeout("kv coordination connect"))
            }
            // A deployment without the capability stays connected, so the next call
            // answers the same way instead of reconnecting and logging in again.
            Ok(Err(error @ LaserError::Unsupported { .. })) => Err(error),
            Ok(Err(error)) => {
                self.transport.reset_generation(generation).await;
                Err(error)
            }
            Ok(Ok(())) => Ok(()),
        }
    }
}

// What a failed send did with its frame, as the driver error that ended it
// tells.
enum FrameWrite {
    Written,
    NotWritten,
    Unknown,
}

impl From<&LaserError> for FrameWrite {
    fn from(error: &LaserError) -> Self {
        match error {
            LaserError::AmbiguousMutation { .. } => Self::Written,
            LaserError::Transport(transport) if driver_wrote(transport.as_iggy()) => Self::Written,
            LaserError::Transport(transport) if driver_never_wrote(transport.as_iggy()) => {
                Self::NotWritten
            }
            _ => Self::Unknown,
        }
    }
}

// A frame an earlier attempt wrote keeps its unknown outcome, whatever a later
// attempt answered.
fn written_outcome(error: IggyError, written: bool) -> IggyError {
    if written && !driver_wrote(&error) {
        IggyError::TransientNotCommitted
    } else {
        error
    }
}

fn decode_reply(payload: &[u8]) -> Result<KvOutcome, LaserError> {
    match crate::error::decode_managed_reply::<KvReply>(payload)? {
        KvReply::Ok(outcome) => Ok(outcome),
        KvReply::Err(error) => Err(error.into()),
        // The reply enum is non_exhaustive: a newer server's unknown variant
        // is a protocol surprise, not an outcome.
        _ => Err(LaserError::Protocol(
            "kv coordination: unknown reply variant".to_owned(),
        )),
    }
}

fn unexpected(what: &str, outcome: &KvOutcome) -> LaserError {
    LaserError::Protocol(format!(
        "kv coordination {what}: unexpected outcome {outcome:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::TransportError;
    use crate::iggy::prelude::IggyClient;
    use crate::kv::{CasExpect, KV_LEASE_OP_VERSION, KV_OP_VERSION, KvError};
    use laser_wire::framing::decode_named;
    use laser_wire::mutation::MutationPosition;
    use laser_wire::result::{CommandError, ResultCode};
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

    struct StubTransport {
        replies: StdMutex<Vec<Result<Vec<u8>, LaserError>>>,
        sent: StdMutex<Vec<(u32, Vec<u8>)>>,
        resets: AtomicUsize,
        delay: Option<Duration>,
    }

    impl StubTransport {
        fn answering(replies: Vec<Result<Vec<u8>, LaserError>>) -> Self {
            Self {
                replies: StdMutex::new(replies),
                sent: StdMutex::new(Vec::new()),
                resets: AtomicUsize::new(0),
                delay: None,
            }
        }

        fn hanging() -> Self {
            Self {
                replies: StdMutex::new(Vec::new()),
                sent: StdMutex::new(Vec::new()),
                resets: AtomicUsize::new(0),
                delay: Some(Duration::from_secs(60)),
            }
        }
    }

    impl ManagedKvTransport for &StubTransport {
        async fn send(&self, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
            self.sent.lock().expect("sent lock").push((code, frame));
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            self.replies.lock().expect("replies lock").remove(0)
        }

        async fn reset(&self) {
            self.resets.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct UnreadyTransport {
        resets: AtomicUsize,
    }

    impl ManagedKvTransport for &UnreadyTransport {
        async fn ready(&self) -> Result<(), LaserError> {
            Err(LaserError::unsupported_feature(
                "kv",
                "coordination",
                "fenced leases are unavailable",
            ))
        }

        async fn send(&self, _code: u32, _frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
            panic!("a permanent setup failure must stop before send")
        }

        async fn reset(&self) {
            self.resets.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct SlowResetTransport {
        reset_finished: AtomicBool,
    }

    impl ManagedKvTransport for &SlowResetTransport {
        async fn send(&self, _code: u32, _frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
            std::future::pending().await
        }

        async fn reset(&self) {
            tokio::time::sleep(Duration::from_millis(25)).await;
            self.reset_finished.store(true, Ordering::SeqCst);
        }
    }

    struct SlowReadyTransport {
        ready_delay: Duration,
        send_delay: Duration,
    }

    impl ManagedKvTransport for &SlowReadyTransport {
        async fn ready(&self) -> Result<(), LaserError> {
            tokio::time::sleep(self.ready_delay).await;
            Ok(())
        }

        async fn send(&self, _code: u32, _frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
            tokio::time::sleep(self.send_delay).await;
            ok_reply(KvOutcome::Leased {
                lease_token: 9,
                granted_ttl_micros: 30_000_000,
                position: position(),
            })
        }

        async fn reset(&self) {}
    }

    // A transport whose connection another call replaces while a send is out.
    struct ReplacedConnectionTransport {
        generation: AtomicU64,
        sent_on: StdMutex<Vec<u64>>,
        retired: StdMutex<Vec<u64>>,
        resets: AtomicUsize,
    }

    impl ManagedKvTransport for &ReplacedConnectionTransport {
        async fn send(&self, _code: u32, _frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
            Err(LaserError::Invalid(
                "a send must name its connection".to_owned(),
            ))
        }

        fn generation(&self) -> u64 {
            self.generation.load(Ordering::SeqCst)
        }

        async fn send_on(
            &self,
            generation: u64,
            _code: u32,
            _frame: Vec<u8>,
        ) -> Result<Vec<u8>, LaserError> {
            self.sent_on.lock().expect("sent lock").push(generation);
            self.generation.fetch_add(1, Ordering::SeqCst);
            Err(IggyError::Disconnected.into())
        }

        async fn reset(&self) {
            self.resets.fetch_add(1, Ordering::SeqCst);
        }

        async fn reset_generation(&self, generation: u64) {
            self.retired.lock().expect("retired lock").push(generation);
        }
    }

    fn ok_reply(outcome: KvOutcome) -> Result<Vec<u8>, LaserError> {
        Ok(encode_named(&KvReply::Ok(outcome)).expect("reply encodes"))
    }

    fn err_reply(error: KvError) -> Result<Vec<u8>, LaserError> {
        Ok(encode_named(&KvReply::Err(error)).expect("reply encodes"))
    }

    fn lease_request() -> KvLease {
        KvLease {
            v: KV_LEASE_OP_VERSION,
            namespace: "connectors.coordination".to_owned(),
            key: b"source-owner".to_vec(),
            lease_ttl_micros: 30_000_000,
            holder_id: "node-1".to_owned(),
            subject_user_id: Some(42),
        }
    }

    fn renew_request() -> KvLeaseRenew {
        KvLeaseRenew {
            v: KV_LEASE_OP_VERSION,
            namespace: "connectors.coordination".to_owned(),
            key: b"source-owner".to_vec(),
            holder_id: "node-1".to_owned(),
            subject_user_id: Some(42),
            lease_token: 7,
            lease_ttl_micros: 30_000_000,
        }
    }

    fn release_request() -> KvRelease {
        KvRelease {
            v: KV_LEASE_OP_VERSION,
            namespace: "connectors.coordination".to_owned(),
            key: b"source-owner".to_vec(),
            lease_token: 7,
            holder_id: "node-1".to_owned(),
        }
    }

    fn position() -> MutationPosition {
        MutationPosition {
            topic_generation: 1,
            partition: 0,
            offset: 512,
        }
    }

    fn unknown_outcome_reply(code: ResultCode) -> Result<Vec<u8>, LaserError> {
        Ok(
            encode_named(&CommandError::new(code, "the outcome is not known yet"))
                .expect("reply encodes"),
        )
    }

    #[tokio::test]
    async fn given_an_unknown_outcome_reply_when_executing_then_should_keep_the_transport_and_report_ambiguity_with_the_identity()
     {
        for code in [
            ResultCode::Pending,
            ResultCode::BackendUnavailable,
            ResultCode::Unavailable,
        ] {
            let transport = StubTransport::answering(vec![unknown_outcome_reply(code)]);
            let client = FencedLeaseClient::new(&transport);
            let op = client
                .prepare_acquire(&lease_request())
                .expect("the request prepares");

            let outcome = client.acquire(&op).await;

            let Err(LaserError::AmbiguousMutation { operation_id, .. }) = outcome else {
                panic!("{code:?} must be ambiguous, got {outcome:?}");
            };
            assert_eq!(
                operation_id,
                Some(op.operation_id()),
                "the ambiguity carries the prepared identity to repeat"
            );
            assert_eq!(
                transport.resets.load(Ordering::SeqCst),
                0,
                "a whole reply leaves the connection in step for other requests"
            );
            assert_eq!(
                op.ambiguous_recovery(),
                AmbiguousMutationRecovery::WaitForLeaseExpiry(Duration::from_secs(30))
            );
        }
    }

    #[tokio::test]
    async fn given_a_prepared_renewal_when_retried_then_should_reuse_the_exact_frame_and_id() {
        let stub = StubTransport::answering(vec![
            Err(LaserError::Timeout("boom")),
            ok_reply(KvOutcome::Renewed {
                lease_token: 7,
                granted_ttl_micros: 30_000_000,
                position: position(),
            }),
        ]);
        let client = FencedLeaseClient::new(&stub);
        let request = KvLeaseRenew {
            v: KV_LEASE_OP_VERSION,
            namespace: "connectors.coordination".to_owned(),
            key: b"source-owner".to_vec(),
            holder_id: "node-1".to_owned(),
            subject_user_id: Some(42),
            lease_token: 7,
            lease_ttl_micros: 30_000_000,
        };
        let op = client.prepare_renew(&request).expect("prepares");
        assert_eq!(
            op.ambiguous_recovery(),
            AmbiguousMutationRecovery::RepeatPrepared
        );

        let first = client.renew(&op).await;
        assert!(
            matches!(first, Err(LaserError::AmbiguousMutation { .. })),
            "a transport failure is ambiguous, got {first:?}"
        );
        let lease = client.renew(&op).await.expect("retry succeeds");
        assert_eq!(lease.token, 7);
        assert_eq!(lease.position, position());

        let sent = stub.sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0], sent[1], "the retry reuses the exact frame");
        let envelope: ManagedRequestEnvelope =
            decode_named(&sent[0].1).expect("the frame is a managed envelope");
        assert_eq!(envelope.operation_id, op.operation_id());
        assert_ne!(envelope.operation_id, 0);
        let inner: KvLeaseRenew =
            decode_named(&envelope.payload).expect("the payload is the renewal");
        assert_eq!(inner.holder_id, "node-1");
        assert_eq!(stub.resets.load(Ordering::SeqCst), 1, "reset after failure");
    }

    // The seam a consumer behind a `#[non_exhaustive]` enum needs: the transport
    // is a runtime choice, so it must survive the trip through a trait object
    // with the client's envelope and retry behavior intact.
    #[tokio::test]
    async fn given_a_boxed_transport_when_used_then_should_drive_the_typed_client() {
        struct OwnedStub {
            sent: StdMutex<Vec<(u32, Vec<u8>)>>,
        }

        impl ManagedKvTransport for OwnedStub {
            async fn send(&self, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
                self.sent.lock().expect("sent lock").push((code, frame));
                ok_reply(KvOutcome::Leased {
                    lease_token: 9,
                    granted_ttl_micros: 30_000_000,
                    position: position(),
                })
            }

            async fn reset(&self) {}
        }

        let stub = Arc::new(OwnedStub {
            sent: StdMutex::new(Vec::new()),
        });
        let transport: SharedKvTransport = Arc::clone(&stub) as SharedKvTransport;
        let client = FencedLeaseClient::new(transport);
        let op = client.prepare_acquire(&lease_request()).expect("prepares");
        let lease = client.acquire(&op).await.expect("acquires");

        assert_eq!(lease.token, 9);
        assert_eq!(lease.granted_ttl, Duration::from_micros(30_000_000));
        // The boxed hop preserves the exact framing: still one managed envelope
        // under the prepared operation id.
        let sent = stub.sent.lock().expect("sent lock");
        assert_eq!(sent[0].0, AGDX_KV_LEASE_CODE);
        let envelope: ManagedRequestEnvelope =
            decode_named(&sent[0].1).expect("the frame is a managed envelope");
        assert_eq!(envelope.operation_id, op.operation_id());
    }

    #[tokio::test]
    async fn given_two_prepared_mutations_when_built_then_should_mint_distinct_operation_ids() {
        let stub = StubTransport::answering(Vec::new());
        let client = FencedLeaseClient::new(&stub);
        let first = client.prepare_acquire(&lease_request()).expect("prepares");
        let second = client.prepare_acquire(&lease_request()).expect("prepares");
        assert_ne!(first.operation_id(), second.operation_id());
    }

    #[tokio::test]
    async fn given_a_hung_transport_when_executing_then_should_reset_and_report_ambiguous() {
        let stub = StubTransport::hanging();
        let client = FencedLeaseClient::new(&stub).with_attempt_timeout(Duration::from_millis(100));
        let op = client.prepare_acquire(&lease_request()).expect("prepares");
        assert_eq!(
            op.ambiguous_recovery(),
            AmbiguousMutationRecovery::WaitForLeaseExpiry(Duration::from_secs(30))
        );
        let outcome = client.acquire(&op).await;
        assert!(matches!(outcome, Err(LaserError::AmbiguousMutation { .. })));
        assert_eq!(stub.resets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn given_slow_readiness_when_send_fits_the_bound_then_should_not_report_ambiguous() {
        let transport = SlowReadyTransport {
            ready_delay: Duration::from_millis(125),
            send_delay: Duration::from_millis(125),
        };
        let client =
            FencedLeaseClient::new(&transport).with_attempt_timeout(Duration::from_millis(200));
        let op = client.prepare_acquire(&lease_request()).expect("prepares");

        let lease = client.acquire(&op).await.expect("acquires after readiness");

        assert_eq!(lease.token, 9);
    }

    #[test]
    fn given_default_client_when_built_then_should_allow_attempts_beyond_the_server_fold_wait() {
        let stub = StubTransport::answering(Vec::new());
        let client = FencedLeaseClient::new(&stub);

        assert_eq!(client.attempt_timeout, Duration::from_secs(15));
    }

    #[tokio::test]
    async fn given_an_ambiguous_acquisition_when_the_repeat_is_answered_then_should_return_the_grant_under_the_same_identity()
     {
        let stub = StubTransport::answering(vec![
            unknown_outcome_reply(ResultCode::Pending),
            ok_reply(KvOutcome::Leased {
                lease_token: 4,
                granted_ttl_micros: 30_000_000,
                position: position(),
            }),
        ]);
        let client = FencedLeaseClient::new(&stub);
        let op = client.prepare_acquire(&lease_request()).expect("prepares");

        let lease = client
            .acquire_resolving(&op)
            .await
            .expect("the repeat answers the recorded grant");

        assert_eq!(lease.token, 4);
        let sent = stub.sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 2, "one repeat after the unknown outcome");
        assert_eq!(sent[0], sent[1], "the repeat carries the same identity");
        assert_eq!(stub.resets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn given_an_acquisition_that_stays_unknown_when_its_ttl_window_passes_then_should_report_ambiguity_without_a_further_wait()
     {
        let stub = StubTransport::answering(
            (0..64)
                .map(|_| unknown_outcome_reply(ResultCode::Pending))
                .collect(),
        );
        let client = FencedLeaseClient::new(&stub);
        let request = KvLease {
            lease_ttl_micros: 1_000_000,
            ..lease_request()
        };
        let op = client.prepare_acquire(&request).expect("prepares");
        let started = Instant::now();

        let outcome = client.acquire_resolving(&op).await;

        assert!(
            matches!(outcome, Err(LaserError::AmbiguousMutation { .. })),
            "an outcome still unknown at the window's end stays ambiguous, got {outcome:?}"
        );
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_secs(1), "repeats fill the window");
        assert!(
            elapsed < Duration::from_secs(2),
            "no TTL sleep follows the window, took {elapsed:?}"
        );
        assert!(stub.sent.lock().expect("sent lock").len() > 2);
    }

    #[tokio::test]
    async fn given_a_grant_answered_late_when_acquired_then_should_be_valid_from_the_first_send() {
        let transport = SlowReadyTransport {
            ready_delay: Duration::ZERO,
            send_delay: Duration::from_millis(200),
        };
        let client = FencedLeaseClient::new(&transport);
        let op = client.prepare_acquire(&lease_request()).expect("prepares");
        let before = Instant::now();

        let lease = client.acquire(&op).await.expect("acquires");

        let after = Instant::now();
        assert!(lease.valid_until >= before + lease.granted_ttl);
        assert!(
            lease.valid_until + Duration::from_millis(200) <= after + lease.granted_ttl,
            "the reply delay is spent from the grant, not added to it"
        );
    }

    #[tokio::test]
    async fn given_a_grant_that_arrives_after_its_validity_when_resolved_then_should_report_the_lease_lost()
     {
        struct LateGrant;

        impl ManagedKvTransport for LateGrant {
            async fn send(&self, _code: u32, _frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
                tokio::time::sleep(Duration::from_millis(1_100)).await;
                ok_reply(KvOutcome::Leased {
                    lease_token: 9,
                    granted_ttl_micros: 1_000_000,
                    position: position(),
                })
            }

            async fn reset(&self) {}
        }

        let client = FencedLeaseClient::new(LateGrant);
        let request = KvLease {
            lease_ttl_micros: 1_000_000,
            ..lease_request()
        };
        let op = client.prepare_acquire(&request).expect("prepares");

        let outcome = client.acquire_resolving(&op).await;

        assert!(
            matches!(outcome, Err(LaserError::Kv(KvError::LeaseLost))),
            "a grant dead on arrival is never handed out as held, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn given_an_ambiguous_renewal_when_resolved_then_should_repeat_the_same_identity_until_answered()
     {
        let stub = StubTransport::answering(vec![
            unknown_outcome_reply(ResultCode::Unavailable),
            ok_reply(KvOutcome::Renewed {
                lease_token: 7,
                granted_ttl_micros: 30_000_000,
                position: position(),
            }),
        ]);
        let client = FencedLeaseClient::new(&stub);
        let op = client.prepare_renew(&renew_request()).expect("prepares");

        let lease = client
            .renew_resolving(&op, Instant::now() + Duration::from_secs(5))
            .await
            .expect("the repeat answers the renewal");

        assert_eq!(lease.token, 7);
        let sent = stub.sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0], sent[1]);
    }

    #[tokio::test]
    async fn given_an_ambiguous_renewal_when_the_lease_runs_out_then_should_report_the_lease_lost()
    {
        let stub = StubTransport::answering(
            (0..64)
                .map(|_| unknown_outcome_reply(ResultCode::Pending))
                .collect(),
        );
        let client = FencedLeaseClient::new(&stub);
        let op = client.prepare_renew(&renew_request()).expect("prepares");

        let outcome = client
            .renew_resolving(&op, Instant::now() + Duration::from_millis(300))
            .await;

        assert!(
            matches!(outcome, Err(LaserError::Kv(KvError::LeaseLost))),
            "work stops once the held lease is no longer valid, got {outcome:?}"
        );
    }

    #[test]
    fn given_a_reused_connection_without_the_capability_when_checked_twice_within_a_second_then_should_probe_once()
     {
        let transport = DedicatedKvTransport::new("iggy://localhost:8090");

        assert!(transport.reprobe_due(), "a connection never probed is due");
        assert!(
            !transport.reprobe_due(),
            "a refused call within the interval answers from the last probe"
        );
    }

    #[tokio::test]
    async fn given_a_timed_out_mutation_when_reset_is_slow_then_should_wait_for_retirement() {
        let transport = SlowResetTransport {
            reset_finished: AtomicBool::new(false),
        };
        let client =
            FencedLeaseClient::new(&transport).with_attempt_timeout(Duration::from_millis(1));
        let op = client.prepare_acquire(&lease_request()).expect("prepares");
        let started = tokio::time::Instant::now();

        let outcome = client.acquire(&op).await;

        assert!(matches!(outcome, Err(LaserError::AmbiguousMutation { .. })));
        assert!(transport.reset_finished.load(Ordering::SeqCst));
        assert!(started.elapsed() >= Duration::from_millis(25));
    }

    #[tokio::test]
    async fn given_a_closed_coordination_client_when_readied_then_should_not_reconnect() {
        let client = FencedLeaseClient::connect_dedicated("not a connection string");
        client.close().await;
        client.close().await;
        ManagedKvTransport::reset(&client.transport).await;
        assert!(matches!(
            ManagedKvTransport::ready(&client.transport).await,
            Err(LaserError::Transport(TransportError(
                crate::iggy::prelude::IggyError::ClientShutdown
            )))
        ));
    }

    #[tokio::test]
    async fn given_a_send_whose_connection_was_replaced_when_it_fails_then_should_retire_only_the_connection_it_used()
     {
        let transport = ReplacedConnectionTransport {
            generation: AtomicU64::new(1),
            sent_on: StdMutex::new(Vec::new()),
            retired: StdMutex::new(Vec::new()),
            resets: AtomicUsize::new(0),
        };
        let client = FencedLeaseClient::new(&transport);
        let op = client.prepare_renew(&renew_request()).expect("prepares");

        let outcome = client.renew(&op).await;

        assert!(matches!(outcome, Err(LaserError::AmbiguousMutation { .. })));
        assert_eq!(*transport.sent_on.lock().expect("sent lock"), vec![1]);
        assert_eq!(*transport.retired.lock().expect("retired lock"), vec![1]);
        assert_eq!(
            transport.resets.load(Ordering::SeqCst),
            0,
            "a plain reset would retire the replacement connection"
        );
    }

    #[tokio::test]
    async fn given_a_late_failure_from_a_retired_connection_when_reset_then_should_keep_the_newer_connection()
     {
        let transport = DedicatedKvTransport::new("iggy://localhost:8090");
        *transport.slot.lock().await = Some(Laser::from_client(IggyClient::default()));
        let first = ManagedKvTransport::generation(&transport);

        ManagedKvTransport::reset_generation(&transport, first).await;
        assert!(
            transport.slot.lock().await.is_none(),
            "the failing call retires its own connection"
        );
        *transport.slot.lock().await = Some(Laser::from_client(IggyClient::default()));
        ManagedKvTransport::reset_generation(&transport, first).await;

        assert!(
            transport.slot.lock().await.is_some(),
            "a late failure from the old connection leaves the new one running"
        );
        let refused =
            ManagedKvTransport::send_on(&transport, first, AGDX_KV_RELEASE_CODE, Vec::new()).await;
        assert!(
            refused.is_err_and(|error| error.is_retryable()),
            "a send readied on the old connection is refused, not sent on the new one"
        );
    }

    #[tokio::test]
    async fn given_a_permanent_setup_failure_when_executing_then_should_preserve_the_error() {
        let unready = UnreadyTransport::default();
        let client = FencedLeaseClient::new(&unready);
        let op = client.prepare_acquire(&lease_request()).expect("prepares");
        let outcome = client.acquire(&op).await;
        assert!(matches!(outcome, Err(LaserError::Unsupported { .. })));
    }

    #[tokio::test]
    async fn given_an_unsupported_deployment_when_called_repeatedly_then_should_keep_the_connection()
     {
        let unready = UnreadyTransport::default();
        let client = FencedLeaseClient::new(&unready);
        for _ in 0..3 {
            let op = client.prepare_acquire(&lease_request()).expect("prepares");
            assert!(matches!(
                client.acquire(&op).await,
                Err(LaserError::Unsupported { .. })
            ));
        }
        assert_eq!(unready.resets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn given_a_permanent_send_failure_when_executing_then_should_preserve_the_error() {
        let stub = StubTransport::answering(vec![Err(LaserError::unsupported_feature(
            "kv",
            "coordination",
            "fenced leases are unavailable",
        ))]);
        let client = FencedLeaseClient::new(&stub);
        let op = client.prepare_acquire(&lease_request()).expect("prepares");
        let outcome = client.acquire(&op).await;
        assert!(matches!(outcome, Err(LaserError::Unsupported { .. })));
        assert_eq!(stub.resets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn given_an_authorization_send_failure_when_executing_then_should_preserve_the_error() {
        let stub = StubTransport::answering(vec![Err(LaserError::from(
            crate::iggy::prelude::IggyError::Unauthorized,
        ))]);
        let client = FencedLeaseClient::new(&stub);
        let op = client.prepare_acquire(&lease_request()).expect("prepares");
        let outcome = client.acquire(&op).await;
        assert!(matches!(
            outcome,
            Err(LaserError::Transport(TransportError(
                crate::iggy::prelude::IggyError::Unauthorized
            )))
        ));
        assert_eq!(stub.resets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn given_a_driver_that_wrote_the_frame_and_lost_its_reply_when_executed_then_should_report_ambiguity_with_the_identity()
     {
        let stub = StubTransport::answering(vec![Err(IggyError::OperationUncertain(9).into())]);
        let client = FencedLeaseClient::new(&stub);
        let op = client
            .prepare_release(&release_request())
            .expect("prepares");

        let outcome = client.release(&op).await;

        let Err(LaserError::AmbiguousMutation { operation_id, .. }) = outcome else {
            panic!("a written frame without a reply is ambiguous, got {outcome:?}");
        };
        assert_eq!(operation_id, Some(op.operation_id));
        assert_eq!(stub.resets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn given_a_driver_that_stopped_before_writing_the_frame_when_executed_then_should_report_a_definite_failure()
     {
        let stub = StubTransport::answering(vec![Err(IggyError::NotConnected.into())]);
        let client = FencedLeaseClient::new(&stub);
        let op = client
            .prepare_release(&release_request())
            .expect("prepares");

        let outcome = client.release(&op).await;

        assert!(
            matches!(
                outcome,
                Err(LaserError::Transport(TransportError(
                    IggyError::NotConnected
                )))
            ),
            "a frame never written cannot have applied, got {outcome:?}"
        );
        assert_eq!(stub.resets.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn given_a_frame_an_earlier_attempt_wrote_when_a_later_attempt_stops_before_writing_then_should_keep_the_unknown_outcome()
     {
        assert_eq!(
            written_outcome(IggyError::ClientShutdown, true),
            IggyError::TransientNotCommitted
        );
        assert_eq!(
            written_outcome(IggyError::ClientShutdown, false),
            IggyError::ClientShutdown
        );
        assert_eq!(
            written_outcome(IggyError::OperationUncertain(4), true),
            IggyError::OperationUncertain(4)
        );
    }

    #[tokio::test]
    async fn given_a_zero_attempt_timeout_when_executing_then_should_reject_without_sending() {
        let stub = StubTransport::answering(Vec::new());
        let client = FencedLeaseClient::new(&stub).with_attempt_timeout(Duration::ZERO);
        let op = client.prepare_acquire(&lease_request()).expect("prepares");
        let outcome = client.acquire(&op).await;
        assert!(matches!(outcome, Err(LaserError::Invalid(_))));
        assert!(stub.sent.lock().expect("sent lock").is_empty());
    }

    #[tokio::test]
    async fn given_a_prepared_mutation_from_another_client_when_executed_then_should_reject() {
        let first_stub = StubTransport::answering(Vec::new());
        let second_stub = StubTransport::answering(Vec::new());
        let first = FencedLeaseClient::new(&first_stub);
        let second = FencedLeaseClient::new(&second_stub);
        let op = first.prepare_acquire(&lease_request()).expect("prepares");
        let outcome = second.acquire(&op).await;
        assert!(matches!(outcome, Err(LaserError::Invalid(_))));
        assert!(second_stub.sent.lock().expect("sent lock").is_empty());
    }

    #[tokio::test]
    async fn given_an_unexpected_outcome_when_executing_then_should_fail_closed() {
        // A `Leased` answer to a release must never be read as success.
        let stub = StubTransport::answering(vec![ok_reply(KvOutcome::Leased {
            lease_token: 7,
            granted_ttl_micros: 30_000_000,
            position: position(),
        })]);
        let client = FencedLeaseClient::new(&stub);
        let release = KvRelease {
            v: KV_LEASE_OP_VERSION,
            namespace: "connectors.coordination".to_owned(),
            key: b"source-owner".to_vec(),
            lease_token: 7,
            holder_id: "node-1".to_owned(),
        };
        let op = client.prepare_release(&release).expect("prepares");
        let outcome = client.release(&op).await;
        assert!(matches!(outcome, Err(LaserError::Protocol(_))));
    }

    #[tokio::test]
    async fn given_a_mismatched_prepared_mutation_when_executed_then_should_reject() {
        let stub = StubTransport::answering(Vec::new());
        let client = FencedLeaseClient::new(&stub);
        let op = client.prepare_acquire(&lease_request()).expect("prepares");
        let outcome = client.release(&op).await;
        assert!(matches!(outcome, Err(LaserError::Invalid(_))));
        assert!(stub.sent.lock().expect("sent lock").is_empty());
    }

    #[tokio::test]
    async fn given_a_renewal_when_executed_then_should_return_the_same_token() {
        let stub = StubTransport::answering(vec![ok_reply(KvOutcome::Renewed {
            lease_token: 7,
            granted_ttl_micros: 25_000_000,
            position: position(),
        })]);
        let client = FencedLeaseClient::new(&stub);
        let renew = KvLeaseRenew {
            v: KV_LEASE_OP_VERSION,
            namespace: "connectors.coordination".to_owned(),
            key: b"source-owner".to_vec(),
            holder_id: "node-1".to_owned(),
            subject_user_id: Some(42),
            lease_token: 7,
            lease_ttl_micros: 30_000_000,
        };
        let op = client.prepare_renew(&renew).expect("prepares");
        let lease = client.renew(&op).await.expect("renews");
        assert_eq!(lease.token, 7);
        assert_eq!(lease.granted_ttl, Duration::from_micros(25_000_000));
    }

    #[tokio::test]
    async fn given_a_lease_lost_reply_when_executing_then_should_surface_the_typed_error() {
        let stub = StubTransport::answering(vec![err_reply(KvError::LeaseLost)]);
        let client = FencedLeaseClient::new(&stub);
        let cas = KvCasFenced {
            v: KV_LEASE_OP_VERSION,
            namespace: "connectors.state".to_owned(),
            session: None,
            key: b"source_state/pg".to_vec(),
            value: b"cursor".to_vec(),
            expires_at_micros: None,
            ttl_micros: None,
            expect: CasExpect::Match(4),
            fence_namespace: "connectors.coordination".to_owned(),
            fence_key: b"source-owner".to_vec(),
            fence_token: 7,
        };
        let op = client.prepare_cas_fenced(&cas).expect("prepares");
        let outcome = client.cas_fenced(&op).await;
        assert!(matches!(outcome, Err(LaserError::Kv(KvError::LeaseLost))));
        assert_eq!(
            stub.resets.load(Ordering::SeqCst),
            0,
            "a typed reply is not ambiguous"
        );
    }

    #[tokio::test]
    async fn given_an_invalid_request_when_preparing_then_should_reject_before_any_send() {
        let stub = StubTransport::answering(Vec::new());
        let client = FencedLeaseClient::new(&stub);
        let stale_version = KvLease {
            v: KV_LEASE_OP_VERSION + 1,
            ..lease_request()
        };
        assert!(client.prepare_acquire(&stale_version).is_err());
        let no_holder = KvLease {
            holder_id: String::new(),
            ..lease_request()
        };
        assert!(client.prepare_acquire(&no_holder).is_err());
        assert!(stub.sent.lock().expect("sent lock").is_empty());
    }

    #[tokio::test]
    async fn given_a_barriered_get_when_answered_stale_then_should_surface_stale_not_absent() {
        let stub = StubTransport::answering(vec![err_reply(KvError::Stale {
            required: position(),
        })]);
        let client = FencedLeaseClient::new(&stub);
        let request = KvGet {
            v: KV_OP_VERSION,
            namespace: "connectors.state".to_owned(),
            key: b"source_state/pg".to_vec(),
            if_none_match: None,
            min_position: Some(position()),
        };
        let outcome = client.get(&request).await;
        assert!(matches!(
            outcome,
            Err(LaserError::Kv(KvError::Stale { .. }))
        ));
        // The read rides unframed: no managed envelope, no operation id.
        let sent = stub.sent.lock().expect("sent lock");
        assert_eq!(sent[0].0, AGDX_KV_GET_CODE);
        let direct: KvGet = decode_named(&sent[0].1).expect("the frame is the bare get");
        assert_eq!(direct.min_position, Some(position()));
    }
}
