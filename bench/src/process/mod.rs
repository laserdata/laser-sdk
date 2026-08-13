use std::env;
use std::fs::{self, OpenOptions};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::{Duration, Instant};

use laser_sdk::laser::Laser;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::time::sleep;

use crate::BenchError;
use crate::binary::BinaryManifest;
use crate::host::pin_process;
use crate::manifest::Environment;

const IGGY_USERNAME: &str = "iggy";
const IGGY_PASSWORD: &str = "iggy";
const PLANE_HEALTH_PATH: &str = "/health";
const PLANE_READY_PATH: &str = "/ready";
const COMPOSE_IGGY_SERVICE: &str = "iggy";
const COMPOSE_PLANE_SERVICE: &str = "plane";
const COMPOSE_IGGY_PORT: &str = "8090";

pub struct ComposeServices {
    compose_file: PathBuf,
    project: String,
    logs_directory: PathBuf,
    stopped: bool,
}

impl ComposeServices {
    /// Start an isolated non-authoritative Compose stack and resolve its host endpoints.
    ///
    /// # Errors
    ///
    /// Returns an error when Compose startup, service discovery, credential discovery, or PID discovery fails.
    pub fn start(
        compose_file: &Path,
        logs_directory: &Path,
        requires_plane: bool,
        profile: PlaneProfile,
    ) -> Result<(Self, NativeIggy, Option<NativePlane>), BenchError> {
        let compose_file = compose_file.canonicalize().map_err(|error| {
            BenchError::Invalid(format!(
                "failed to resolve Compose file `{}`: {error}",
                compose_file.display()
            ))
        })?;
        let project = compose_project(logs_directory);
        let mut services = Self {
            compose_file,
            project,
            logs_directory: logs_directory.to_path_buf(),
            stopped: false,
        };
        let mut args = vec!["up", "-d", "--wait", COMPOSE_IGGY_SERVICE];
        if requires_plane {
            args.push(COMPOSE_PLANE_SERVICE);
        }
        services.run_checked(&args)?;
        match services.resolve(requires_plane, profile) {
            Ok((iggy, plane)) => Ok((services, iggy, plane)),
            Err(error) => {
                let _ = services.finish();
                Err(error)
            }
        }
    }

    /// Capture service logs and stop the isolated project.
    ///
    /// # Errors
    ///
    /// Returns an error when logs cannot be written or Compose teardown fails.
    pub fn shutdown(mut self) -> Result<(), BenchError> {
        self.finish()
    }

    fn resolve(
        &self,
        requires_plane: bool,
        profile: PlaneProfile,
    ) -> Result<(NativeIggy, Option<NativePlane>), BenchError> {
        let port = self.output_checked(&["port", COMPOSE_IGGY_SERVICE, COMPOSE_IGGY_PORT])?;
        let port = parse_compose_port(&port)?;
        let username = self.service_environment(COMPOSE_IGGY_SERVICE, "IGGY_ROOT_USERNAME")?;
        let password = self.service_environment(COMPOSE_IGGY_SERVICE, "IGGY_ROOT_PASSWORD")?;
        let iggy_pid = self.service_pid(COMPOSE_IGGY_SERVICE)?;
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let iggy = NativeIggy::external(
            address,
            format!("{username}:{password}@{address}"),
            iggy_pid,
        );
        let plane = requires_plane
            .then(|| self.service_pid(COMPOSE_PLANE_SERVICE))
            .transpose()?
            .map(|pid| NativePlane::external(pid, profile));
        Ok((iggy, plane))
    }

    fn service_environment(&self, service: &str, key: &str) -> Result<String, BenchError> {
        let value = self.output_checked(&["exec", "-T", service, "printenv", key])?;
        let value = value.trim();
        if value.is_empty() {
            return Err(BenchError::Invalid(format!(
                "Compose service `{service}` has no `{key}` value"
            )));
        }
        Ok(value.to_owned())
    }

    fn service_pid(&self, service: &str) -> Result<u32, BenchError> {
        let container = self.output_checked(&["ps", "-q", service])?;
        let container = container.trim();
        if container.is_empty() {
            return Err(BenchError::Invalid(format!(
                "Compose service `{service}` has no running container"
            )));
        }
        let output = StdCommand::new("docker")
            .args(["inspect", "--format", "{{.State.Pid}}", container])
            .output()
            .map_err(|error| {
                BenchError::Invalid(format!("failed to inspect `{service}`: {error}"))
            })?;
        checked_output(output, &format!("inspect Compose service `{service}`"))?
            .trim()
            .parse::<u32>()
            .map_err(|error| {
                BenchError::Invalid(format!("invalid host PID for `{service}`: {error}"))
            })
    }

    fn output_checked(&self, args: &[&str]) -> Result<String, BenchError> {
        let output = self.command().args(args).output().map_err(|error| {
            BenchError::Invalid(format!("failed to run Docker Compose: {error}"))
        })?;
        checked_output(output, "run Docker Compose")
    }

    fn run_checked(&self, args: &[&str]) -> Result<(), BenchError> {
        self.output_checked(args).map(|_| ())
    }

    fn command(&self) -> StdCommand {
        let mut command = StdCommand::new("docker");
        command
            .args(["compose", "--project-name"])
            .arg(&self.project)
            .arg("--file")
            .arg(&self.compose_file);
        command
    }

    fn finish(&mut self) -> Result<(), BenchError> {
        if self.stopped {
            return Ok(());
        }
        let logs_result = self
            .command()
            .args(["logs", "--no-color", "--timestamps"])
            .output()
            .map_err(|error| {
                BenchError::Invalid(format!("failed to capture Compose logs: {error}"))
            })
            .and_then(|logs| {
                let path = self.logs_directory.join("compose.log");
                let mut combined = logs.stdout;
                combined.extend_from_slice(&logs.stderr);
                fs::write(&path, combined).map_err(|source| BenchError::Write { path, source })
            });
        let shutdown_result =
            self.run_checked(&["down", "--volumes", "--remove-orphans", "--timeout", "10"]);
        if shutdown_result.is_ok() {
            self.stopped = true;
        }
        logs_result.and(shutdown_result)
    }
}

impl Drop for ComposeServices {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
    Serialize,
    PartialEq,
    Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_plane_profile
)]
pub enum PlaneProfile {
    #[default]
    Embedded,
    Postgres,
}

impl PlaneProfile {
    #[must_use]
    pub fn label(self) -> &'static str {
        self.into()
    }

    #[must_use]
    pub fn projection_backend(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Embedded => "embedded",
        }
    }
}

fn invalid_plane_profile(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported plane profile `{value}`"))
}

pub struct NativeIggy {
    child: Option<Child>,
    external_pid: Option<u32>,
    run_directory: PathBuf,
    plane_socket: Option<PathBuf>,
    pub address: SocketAddr,
    pub connection_string: String,
    pub system_path: PathBuf,
}

pub struct StoppedIggy {
    run_directory: PathBuf,
    plane_socket: Option<PathBuf>,
    address: SocketAddr,
    system_path: PathBuf,
}

impl NativeIggy {
    /// Operating-system process identifier for resource accounting.
    #[must_use]
    pub fn pid(&self) -> Option<u32> {
        self.child
            .as_ref()
            .and_then(Child::id)
            .or(self.external_pid)
    }

    /// Represent an externally managed Iggy process after its VSR endpoint and host PID were resolved.
    #[must_use]
    pub fn external(address: SocketAddr, connection_string: String, pid: u32) -> Self {
        Self {
            child: None,
            external_pid: Some(pid),
            run_directory: PathBuf::new(),
            plane_socket: None,
            address,
            connection_string,
            system_path: PathBuf::new(),
        }
    }

    /// Start the resolved `iggy-server` binary with TCP VSR as its only public transport.
    ///
    /// # Errors
    ///
    /// Returns an error when its evidence changed, directories or logs cannot be created, the process cannot start, or readiness times out.
    pub async fn start(
        manifest: &BinaryManifest,
        run_directory: &Path,
        plane_socket: Option<&Path>,
        environment: &Environment,
    ) -> Result<Self, BenchError> {
        if manifest.name != "iggy-server" {
            return Err(BenchError::Invalid(format!(
                "only iggy-server is supported, resolved `{}`",
                manifest.name
            )));
        }
        manifest.verify()?;
        let address = available_address()?;
        let system_path = run_directory.join("iggy-system");
        fs::create_dir_all(&system_path).map_err(|source| BenchError::Write {
            path: system_path.clone(),
            source,
        })?;
        Self::spawn(
            manifest,
            run_directory,
            plane_socket.map(Path::to_path_buf),
            address,
            system_path,
            environment,
            "iggy",
        )
        .await
    }

    async fn spawn(
        manifest: &BinaryManifest,
        run_directory: &Path,
        plane_socket: Option<PathBuf>,
        address: SocketAddr,
        system_path: PathBuf,
        environment: &Environment,
        log_name: &str,
    ) -> Result<Self, BenchError> {
        manifest.verify()?;
        let stdout = create_log(&run_directory.join(format!("{log_name}.stdout.log")))?;
        let stderr = create_log(&run_directory.join(format!("{log_name}.stderr.log")))?;
        let mut command = Command::new(&manifest.path);
        command
            .env("IGGY_ROOT_USERNAME", IGGY_USERNAME)
            .env("IGGY_ROOT_PASSWORD", IGGY_PASSWORD)
            .env("IGGY_SYSTEM_PATH", &system_path)
            .env("IGGY_TCP_ENABLED", "true")
            .env("IGGY_TCP_ADDRESS", address.to_string())
            .env("IGGY_HTTP_ENABLED", "false")
            .env("IGGY_QUIC_ENABLED", "false")
            .env("IGGY_WEBSOCKET_ENABLED", "false")
            .env("IGGY_PLANE_ENABLED", plane_socket.is_some().to_string())
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        if let Some(socket) = plane_socket.as_ref() {
            command
                .env("IGGY_PLANE_SOCKET_PATH", socket)
                .env("IGGY_PLANE_REQUEST_TIMEOUT", "5s");
        }
        let child = command.spawn().map_err(|error| {
            BenchError::Invalid(format!("failed to start iggy-server: {error}"))
        })?;
        if let Some(host) = environment.host.as_ref() {
            pin_process(
                child
                    .id()
                    .ok_or_else(|| BenchError::Invalid("Iggy process has no PID".to_owned()))?,
                &host.iggy_cpus,
            )?;
        }
        let mut server = Self {
            child: Some(child),
            external_pid: None,
            run_directory: run_directory.to_path_buf(),
            plane_socket,
            address,
            connection_string: format!("{IGGY_USERNAME}:{IGGY_PASSWORD}@{address}"),
            system_path,
        };
        server.wait_ready(Duration::from_secs(30)).await?;
        Ok(server)
    }

    /// Stop the server while retaining its benchmark-owned state for a restart.
    ///
    /// # Errors
    ///
    /// Returns an error when process termination or waiting fails.
    pub async fn stop_for_restart(mut self) -> Result<StoppedIggy, BenchError> {
        let stopped = StoppedIggy {
            run_directory: self.run_directory.clone(),
            plane_socket: self.plane_socket.clone(),
            address: self.address,
            system_path: self.system_path.clone(),
        };
        let child = self.child.as_mut().ok_or_else(|| {
            BenchError::Invalid(
                "externally managed Iggy cannot be restarted by the harness".to_owned(),
            )
        })?;
        let pid = child
            .id()
            .ok_or_else(|| BenchError::Invalid("Iggy process has no PID".to_owned()))?;
        let result = unsafe {
            libc::kill(
                i32::try_from(pid).map_err(|error| {
                    BenchError::Invalid(format!("invalid Iggy process ID: {error}"))
                })?,
                libc::SIGTERM,
            )
        };
        if result != 0 {
            return Err(BenchError::Invalid(format!(
                "failed to signal Iggy: {}",
                std::io::Error::last_os_error()
            )));
        }
        if let Ok(result) = tokio::time::timeout(Duration::from_secs(10), child.wait()).await {
            result.map_err(|error| {
                BenchError::Invalid(format!("failed to wait for Iggy: {error}"))
            })?;
        } else {
            child
                .kill()
                .await
                .map_err(|error| BenchError::Invalid(format!("failed to stop Iggy: {error}")))?;
            child.wait().await.map_err(|error| {
                BenchError::Invalid(format!("failed to wait for Iggy: {error}"))
            })?;
        }
        Ok(stopped)
    }

    /// Prove that the SDK's mandatory VSR client completes a connection to this server.
    ///
    /// # Errors
    ///
    /// Returns an error when the VSR client cannot authenticate and connect.
    pub async fn probe_vsr(&self) -> Result<Laser, BenchError> {
        Laser::connect(&self.connection_string)
            .await
            .map_err(|error| BenchError::Invalid(format!("live VSR probe failed: {error}")))
    }

    /// Stop the server and wait for process exit.
    ///
    /// # Errors
    ///
    /// Returns an error when termination or waiting fails.
    pub async fn shutdown(mut self) -> Result<(), BenchError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        child
            .kill()
            .await
            .map_err(|error| BenchError::Invalid(format!("failed to stop Iggy: {error}")))?;
        child
            .wait()
            .await
            .map_err(|error| BenchError::Invalid(format!("failed to wait for Iggy: {error}")))?;
        Ok(())
    }

    async fn wait_ready(&mut self, timeout: Duration) -> Result<(), BenchError> {
        let deadline = Instant::now() + timeout;
        loop {
            if TcpStream::connect(self.address).await.is_ok() {
                return Ok(());
            }
            if let Some(status) = self
                .child
                .as_mut()
                .ok_or_else(|| {
                    BenchError::Invalid("external Iggy readiness is coordinator-owned".to_owned())
                })?
                .try_wait()
                .map_err(|error| BenchError::Invalid(format!("failed to inspect Iggy: {error}")))?
            {
                return Err(BenchError::Invalid(format!(
                    "iggy-server exited before readiness with {status}"
                )));
            }
            if Instant::now() >= deadline {
                return Err(BenchError::Invalid(format!(
                    "iggy-server did not listen on {} within {timeout:?}",
                    self.address
                )));
            }
            sleep(Duration::from_millis(20)).await;
        }
    }
}

impl StoppedIggy {
    /// Restart Iggy on the same address and persisted system path.
    ///
    /// # Errors
    ///
    /// Returns an error when binary verification, process start, affinity, or readiness fails.
    pub async fn restart(
        self,
        manifest: &BinaryManifest,
        environment: &Environment,
    ) -> Result<NativeIggy, BenchError> {
        NativeIggy::spawn(
            manifest,
            &self.run_directory,
            self.plane_socket,
            self.address,
            self.system_path,
            environment,
            "iggy-restart-001",
        )
        .await
    }
}

impl Drop for NativeIggy {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

pub struct NativePlane {
    child: Option<Child>,
    external_pid: Option<u32>,
    run_directory: PathBuf,
    pub health_address: Option<SocketAddr>,
    pub socket_path: PathBuf,
    pub db_path: PathBuf,
    pub profile: PlaneProfile,
}

pub struct StoppedPlane {
    run_directory: PathBuf,
    socket_path: PathBuf,
    db_path: PathBuf,
}

impl NativePlane {
    /// Operating-system process identifier for resource accounting.
    #[must_use]
    pub fn pid(&self) -> Option<u32> {
        self.child
            .as_ref()
            .and_then(Child::id)
            .or(self.external_pid)
    }

    /// Represent an externally managed plane process whose container health was already checked.
    #[must_use]
    pub fn external(pid: u32, profile: PlaneProfile) -> Self {
        Self {
            child: None,
            external_pid: Some(pid),
            run_directory: PathBuf::new(),
            health_address: None,
            socket_path: PathBuf::from("/run/laserdata/plane.sock"),
            db_path: PathBuf::from("/var/lib/laser-plane/index.db"),
            profile,
        }
    }

    /// Start the resolved plane binary against the provided Iggy server and query socket.
    ///
    /// # Errors
    ///
    /// Returns an error when its evidence changed, directories or logs cannot be created, the process cannot start, or readiness does not converge.
    pub async fn start(
        manifest: &BinaryManifest,
        run_directory: &Path,
        iggy: &NativeIggy,
        socket_path: PathBuf,
        environment: &Environment,
    ) -> Result<Self, BenchError> {
        if manifest.name != "plane" {
            return Err(BenchError::Invalid(format!(
                "only plane is supported, resolved `{}`",
                manifest.name
            )));
        }
        manifest.verify()?;
        let data_directory = run_directory.join("plane-data");
        fs::create_dir_all(&data_directory).map_err(|source| BenchError::Write {
            path: data_directory.clone(),
            source,
        })?;
        let db_path = data_directory.join("index.db");
        Self::spawn(
            manifest,
            run_directory,
            iggy,
            socket_path,
            db_path,
            environment,
            "plane",
        )
        .await
    }

    async fn spawn(
        manifest: &BinaryManifest,
        run_directory: &Path,
        iggy: &NativeIggy,
        socket_path: PathBuf,
        db_path: PathBuf,
        environment: &Environment,
        log_name: &str,
    ) -> Result<Self, BenchError> {
        let health_address = available_address()?;
        let stdout = create_log(&run_directory.join(format!("{log_name}.stdout.log")))?;
        let stderr = create_log(&run_directory.join(format!("{log_name}.stderr.log")))?;
        let profile = environment.plane_profile;
        let mut command = Command::new(&manifest.path);
        command
            .env("LD_PLANE_IGGY_URL", format!("tcp://{}", iggy.address))
            .env("LD_PLANE_IGGY_USERNAME", IGGY_USERNAME)
            .env("LD_PLANE_IGGY_PASSWORD", IGGY_PASSWORD)
            .env("LD_PLANE_IGGY_TLS_DISABLED", "true")
            .env(
                "LD_PLANE_IGGY_PAT_FILE",
                "/nonexistent/laser-bench-plane.pat",
            )
            .env("LD_PLANE_DB_PATH", &db_path)
            .env("LD_PLANE_QUERY_SOCKET", &socket_path)
            .env("LD_PLANE_HEALTH_ADDR", health_address.to_string())
            .env("RUST_LOG", "plane=info,iggy=warn")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        match profile {
            PlaneProfile::Embedded => {}
            PlaneProfile::Postgres => configure_postgres(&mut command, environment)?,
        }
        let child = command
            .spawn()
            .map_err(|error| BenchError::Invalid(format!("failed to start plane: {error}")))?;
        if let Some(host) = environment.host.as_ref() {
            pin_process(
                child
                    .id()
                    .ok_or_else(|| BenchError::Invalid("plane process has no PID".to_owned()))?,
                &host.plane_cpus,
            )?;
        }
        let mut plane = Self {
            child: Some(child),
            external_pid: None,
            run_directory: run_directory.to_path_buf(),
            health_address: Some(health_address),
            socket_path,
            db_path,
            profile,
        };
        plane.wait_available(Duration::from_secs(30)).await?;
        Ok(plane)
    }

    /// Restart plane against the same database and Iggy socket contract.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown, stale-socket cleanup, process start, or readiness fails.
    pub async fn restart(
        self,
        manifest: &BinaryManifest,
        iggy: &NativeIggy,
        environment: &Environment,
        restart: u32,
    ) -> Result<Self, BenchError> {
        self.stop_for_restart()
            .await?
            .start(manifest, iggy, environment, restart)
            .await
    }

    /// Stop plane while retaining the state required for an in-place restart.
    ///
    /// # Errors
    ///
    /// Returns an error when plane cannot be stopped or its stale socket cannot be removed.
    pub async fn stop_for_restart(self) -> Result<StoppedPlane, BenchError> {
        let stopped = StoppedPlane {
            run_directory: self.run_directory.clone(),
            socket_path: self.socket_path.clone(),
            db_path: self.db_path.clone(),
        };
        self.shutdown().await?;
        if stopped.socket_path.exists() {
            fs::remove_file(&stopped.socket_path).map_err(|source| BenchError::Write {
                path: stopped.socket_path.clone(),
                source,
            })?;
        }
        Ok(stopped)
    }

    /// Stop the plane process and wait for process exit.
    ///
    /// # Errors
    ///
    /// Returns an error when termination or waiting fails.
    pub async fn shutdown(mut self) -> Result<(), BenchError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        child
            .kill()
            .await
            .map_err(|error| BenchError::Invalid(format!("failed to stop plane: {error}")))?;
        child
            .wait()
            .await
            .map_err(|error| BenchError::Invalid(format!("failed to wait for plane: {error}")))?;
        if self.socket_path.exists() {
            fs::remove_file(&self.socket_path).map_err(|source| BenchError::Write {
                path: self.socket_path.clone(),
                source,
            })?;
        }
        Ok(())
    }

    /// Wait for plane to complete its backend handshake with Iggy.
    ///
    /// # Errors
    ///
    /// Returns an error when readiness does not converge or plane exits.
    pub async fn wait_ready(&mut self, timeout: Duration) -> Result<(), BenchError> {
        self.wait_for_endpoint(PLANE_READY_PATH, timeout, "become ready")
            .await
    }

    async fn wait_available(&mut self, timeout: Duration) -> Result<(), BenchError> {
        self.wait_for_endpoint(PLANE_HEALTH_PATH, timeout, "become available")
            .await
    }

    async fn wait_for_endpoint(
        &mut self,
        path: &str,
        timeout: Duration,
        expectation: &str,
    ) -> Result<(), BenchError> {
        let Some(health_address) = self.health_address else {
            return Ok(());
        };
        let deadline = Instant::now() + timeout;
        loop {
            if self.socket_path.exists() && matches!(health_ready(health_address, path), Ok(true)) {
                return Ok(());
            }
            if let Some(status) = self
                .child
                .as_mut()
                .ok_or_else(|| {
                    BenchError::Invalid("external plane readiness is coordinator-owned".to_owned())
                })?
                .try_wait()
                .map_err(|error| BenchError::Invalid(format!("failed to inspect plane: {error}")))?
            {
                let detail = plane_exit_detail(&self.run_directory);
                return Err(BenchError::Invalid(format!(
                    "plane exited before readiness with {status}{detail}"
                )));
            }
            if Instant::now() >= deadline {
                return Err(BenchError::Invalid(format!(
                    "plane did not {expectation} on {health_address} within {timeout:?}"
                )));
            }
            sleep(Duration::from_millis(20)).await;
        }
    }
}

fn plane_exit_detail(run_directory: &Path) -> String {
    let iggy_log = run_directory.join("iggy.stdout.log");
    if fs::read_to_string(&iggy_log)
        .is_ok_and(|log| log.contains("rejecting login: incompatible protocol version"))
    {
        return format!(
            "; Iggy rejected plane's client protocol, rebuild or replace plane; logs: {}",
            run_directory.display()
        );
    }
    format!("; logs: {}", run_directory.display())
}

impl StoppedPlane {
    /// Start plane from retained database state after a controlled outage.
    ///
    /// # Errors
    ///
    /// Returns an error when binary verification, process start, or availability fails.
    pub async fn start(
        self,
        manifest: &BinaryManifest,
        iggy: &NativeIggy,
        environment: &Environment,
        restart: u32,
    ) -> Result<NativePlane, BenchError> {
        manifest.verify()?;
        NativePlane::spawn(
            manifest,
            &self.run_directory,
            iggy,
            self.socket_path,
            self.db_path,
            environment,
            &format!("plane.restart-{restart:03}"),
        )
        .await
    }
}

impl Drop for NativePlane {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

fn configure_postgres(command: &mut Command, environment: &Environment) -> Result<(), BenchError> {
    let dsn_env = environment
        .postgres_dsn_env
        .as_deref()
        .ok_or_else(|| BenchError::Invalid("postgres_dsn_env is required".to_owned()))?;
    let dsn = env::var(dsn_env).map_err(|_| {
        BenchError::Invalid(format!(
            "Postgres DSN environment variable `{dsn_env}` is not set"
        ))
    })?;
    let declaration = serde_json::json!([{
        "id": "postgres",
        "kind": "postgres",
        "dsn": dsn,
        "ssl_mode": environment.postgres_ssl_mode,
        "pool_connections": environment.postgres_pool_connections,
        "schema": environment.postgres_schema,
        "timeout": environment.postgres_timeout,
        "serves": "kv,graph,runs,folds,forks",
    }]);
    command.env("LD_PLANE_BACKENDS", serde_json::to_string(&declaration)?);
    Ok(())
}

fn compose_project(logs_directory: &Path) -> String {
    let mut hasher = DefaultHasher::new();
    logs_directory.hash(&mut hasher);
    format!("laser-bench-{}-{:x}", std::process::id(), hasher.finish())
}

fn parse_compose_port(value: &str) -> Result<u16, BenchError> {
    value
        .trim()
        .rsplit_once(':')
        .map_or(value.trim(), |(_, port)| port)
        .parse::<u16>()
        .map_err(|error| {
            BenchError::Invalid(format!("invalid Compose Iggy port `{value}`: {error}"))
        })
}

fn checked_output(output: std::process::Output, action: &str) -> Result<String, BenchError> {
    if !output.status.success() {
        return Err(BenchError::Invalid(format!(
            "failed to {action}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8(output.stdout).map_err(|error| {
        BenchError::Invalid(format!(
            "invalid UTF-8 while attempting to {action}: {error}"
        ))
    })
}

fn available_address() -> Result<SocketAddr, BenchError> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .map_err(|error| BenchError::Invalid(format!("failed to reserve a TCP port: {error}")))?;
    let port = listener
        .local_addr()
        .map_err(|error| BenchError::Invalid(format!("failed to inspect a TCP port: {error}")))?
        .port();
    Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
}

fn create_log(path: &Path) -> Result<std::fs::File, BenchError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| BenchError::Write {
            path: path.to_path_buf(),
            source,
        })
}

fn health_ready(address: SocketAddr, path: &str) -> Result<bool, std::io::Error> {
    let mut stream = std::net::TcpStream::connect_timeout(&address, Duration::from_millis(200))?;
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    stream.set_write_timeout(Some(Duration::from_millis(200)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )?;
    let mut status_line = String::new();
    BufReader::new(stream).read_line(&mut status_line)?;
    Ok(status_line.starts_with("HTTP/1.1 200") || status_line.starts_with("HTTP/1.0 200"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_local_host_when_reserving_address_then_should_return_tcp_endpoint() {
        let address = available_address().expect("loopback address should be available");
        assert!(address.ip().is_loopback());
        assert_ne!(address.port(), 0);
    }

    #[test]
    fn given_embedded_profile_when_rendering_label_then_should_use_manifest_name() {
        assert_eq!(PlaneProfile::Embedded.label(), "embedded");
    }

    #[test]
    fn given_plane_profiles_when_parsed_then_should_route_projections_to_selected_backend() {
        let postgres = "postgres"
            .parse::<PlaneProfile>()
            .expect("Postgres profile should parse");
        let embedded = "embedded"
            .parse::<PlaneProfile>()
            .expect("embedded profile should parse");
        assert_eq!(postgres.projection_backend(), "postgres");
        assert_eq!(embedded.projection_backend(), "embedded");
        assert!("local".parse::<PlaneProfile>().is_err());
    }

    #[test]
    fn given_compose_port_output_when_parsed_then_should_return_host_port() {
        assert_eq!(
            parse_compose_port("127.0.0.1:18090\n").expect("IPv4 port should parse"),
            18_090
        );
        assert_eq!(
            parse_compose_port("[::1]:18091\n").expect("IPv6 port should parse"),
            18_091
        );
        assert!(parse_compose_port("missing").is_err());
    }

    #[test]
    fn given_protocol_rejection_when_plane_exits_then_should_report_rebuild_action() {
        let directory = tempfile::tempdir().expect("temporary directory should be available");
        fs::write(
            directory.path().join("iggy.stdout.log"),
            "rejecting login: incompatible protocol version",
        )
        .expect("Iggy log should be written");

        let detail = plane_exit_detail(directory.path());

        assert!(detail.contains("rebuild or replace plane"));
        assert!(detail.contains(&directory.path().display().to_string()));
    }
}
