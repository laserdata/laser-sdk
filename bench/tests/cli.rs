use std::fs;
use std::process::Command;

#[test]
fn given_valid_contract_when_doctor_runs_then_should_accept_it() {
    let output = Command::new(env!("CARGO_BIN_EXE_laser-bench"))
        .args([
            "doctor",
            "--claims",
            "claims.toml",
            "--suite",
            "fixtures/suite-minimal.toml",
        ])
        .output()
        .expect("doctor should start");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("AGDX agent op version 1"));
}

#[test]
fn given_declared_scenario_when_run_is_planned_then_should_write_immutable_plan() {
    let temp = tempfile::tempdir().expect("temporary directory should be created");
    let output_dir = temp.path().join("run");
    let first = Command::new(env!("CARGO_BIN_EXE_laser-bench"))
        .args([
            "run",
            "stream_direct",
            "--suite",
            "fixtures/suite-minimal.toml",
            "--output",
        ])
        .arg(&output_dir)
        .arg("--plan-only")
        .output()
        .expect("run planner should start");
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(output_dir.join("run-plan.json").is_file());

    let second = Command::new(env!("CARGO_BIN_EXE_laser-bench"))
        .args([
            "run",
            "stream_direct",
            "--suite",
            "fixtures/suite-minimal.toml",
            "--output",
        ])
        .arg(&output_dir)
        .arg("--plan-only")
        .output()
        .expect("second run planner should start");
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("already exists"));
}

#[test]
fn given_report_directory_when_analyzed_then_should_validate_evidence() {
    let temp = tempfile::tempdir().expect("temporary directory should be created");
    fs::copy(
        "fixtures/report-minimal.json",
        temp.path().join("report.json"),
    )
    .expect("report fixture should be copied");
    let output = Command::new(env!("CARGO_BIN_EXE_laser-bench"))
        .arg("analyze")
        .arg(temp.path())
        .output()
        .expect("analyzer should start");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("report evidence valid"));
}
