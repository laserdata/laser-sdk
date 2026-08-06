use std::path::{Path, PathBuf};

mod execution;
mod execution_lock;
mod ui;

use clap::{Parser, Subcommand};
use execution::{
    analyze_path, bundle_suite, execute_deterministic_gates, execute_scenario, execute_suite,
    inspect_histogram, prepare_output, resolve_stack, verify_bundle, write_json,
};
use execution_lock::ExecutionLock;
use laser_bench::binary::sha256_file;
use laser_bench::claims::ClaimsRegister;
use laser_bench::doctor;
use laser_bench::manifest::SuiteManifest;
use laser_bench::schema::{SUITE_SCHEMA, validate_json};
use laser_bench::{BenchError, contract};

#[derive(Debug, Parser)]
#[command(
    name = "laser-bench",
    version,
    about = "Reproducible Laser SDK benchmark harness"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Doctor {
        #[arg(long, default_value = "claims.toml")]
        claims: PathBuf,
        #[arg(long)]
        suite: PathBuf,
        #[arg(long)]
        provision: bool,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    Run {
        scenario: String,
        #[arg(long)]
        suite: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        plan_only: bool,
    },
    Suite {
        manifest: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Analyze {
        suite_dir: PathBuf,
    },
    Histogram {
        sidecar: PathBuf,
    },
    Bundle {
        suite_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    VerifyBundle {
        bundle_dir: PathBuf,
    },
}

fn main() {
    ui::init();
    ui::banner();
    let cli = Cli::parse();
    let runtime = build_runtime(&cli);
    if let Err(error) = runtime.block_on(Box::pin(execute(cli))) {
        ui::failure(&error.to_string());
        std::process::exit(1);
    }
}

/// Size the runtime to the pinned client CPU set when the suite declares host
/// controls, so Tokio does not oversubscribe a machine-wide worker pool onto
/// a small pinned core set during timed measurements.
fn build_runtime(cli: &Cli) -> tokio::runtime::Runtime {
    let manifest = match &cli.command {
        Command::Doctor { suite, .. } | Command::Run { suite, .. } => Some(suite),
        Command::Suite { manifest, .. } => Some(manifest),
        _ => None,
    };
    let client_cpus = manifest
        .and_then(|path| SuiteManifest::load(path).ok())
        .and_then(|manifest| manifest.environment.host.map(|host| host.client_cpus.len()));
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(workers) = client_cpus.filter(|count| *count > 0) {
        builder.worker_threads(workers);
    }
    builder
        .enable_all()
        .build()
        .expect("the benchmark runtime should build")
}

async fn execute(cli: Cli) -> Result<(), BenchError> {
    match cli.command {
        Command::Doctor {
            claims,
            suite,
            provision: should_provision,
            output,
        } => {
            ClaimsRegister::load(&claims)?;
            let manifest = SuiteManifest::load(&suite)?;
            validate_json(SUITE_SCHEMA, &serde_json::to_value(&manifest)?)?;
            let mut report = doctor::inspect(&manifest)?;
            if should_provision {
                let _lock = ExecutionLock::acquire()?;
                let output = output.ok_or_else(|| {
                    BenchError::Invalid("live doctor requires --output".to_owned())
                })?;
                prepare_output(&output)?;
                let benchmark_root = Path::new(env!("CARGO_MANIFEST_DIR"));
                let stack = resolve_stack(&manifest, benchmark_root)?;
                write_json(
                    &output.join("resolved-stack.json"),
                    &serde_json::to_value(&stack)?,
                )?;
                report = doctor::inspect_live(report, &stack, &manifest, &output).await?;
                write_json(
                    &output.join("doctor-report.json"),
                    &serde_json::to_value(&report)?,
                )?;
            }
            let fingerprint = contract::fingerprint();
            ui::host(&report.host);
            ui::doctor(&report);
            ui::success(&format!(
                "configuration valid, TCP VSR {}, live VSR {}, AGDX agent op version {}",
                report.tcp_vsr_only,
                report.live_vsr.is_some_and(|valid| valid),
                fingerprint.sdk_agent_op_version,
            ));
            Ok(())
        }
        Command::Run {
            scenario,
            suite,
            output,
            plan_only,
        } => {
            let suite_digest = sha256_file(&suite)?;
            let manifest = SuiteManifest::load(&suite)?;
            let selected = manifest
                .scenarios
                .iter()
                .find(|item| item.name == scenario)
                .ok_or_else(|| {
                    BenchError::Invalid(format!(
                        "scenario `{scenario}` is not declared by the suite"
                    ))
                })?;
            if !selected.offered_rates.is_empty() {
                return Err(BenchError::Invalid(
                    "a scenario with offered_rates must run through `suite`".to_owned(),
                ));
            }
            prepare_output(&output)?;
            write_json(
                &output.join("run-plan.json"),
                &serde_json::json!({
                    "schema_version": 1,
                    "suite": manifest.name,
                    "scenario": selected,
                    "contract": contract::fingerprint()
                }),
            )?;
            if plan_only {
                ui::success(&format!("run plan written to {}", output.display()));
                return Ok(());
            }
            let _lock = ExecutionLock::acquire()?;
            execute_deterministic_gates(&output)?;
            Box::pin(execute_scenario(
                &manifest,
                selected,
                &suite_digest,
                &output,
            ))
            .await
        }
        Command::Suite { manifest, output } => Box::pin(execute_suite(&manifest, &output)).await,
        Command::Analyze { suite_dir } => analyze_path(&suite_dir),
        Command::Histogram { sidecar } => inspect_histogram(&sidecar),
        Command::Bundle { suite_dir, output } => bundle_suite(&suite_dir, &output),
        Command::VerifyBundle { bundle_dir } => verify_bundle(&bundle_dir),
    }
}
