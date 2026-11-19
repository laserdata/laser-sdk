use crate::harness;
use laser_sdk::agent::StepCall;
use laser_sdk::prelude::full::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn given_three_steps_in_a_body_when_run_then_should_journal_each_and_return_the_value() {
    let laser = fresh_laser("three").await;

    let outcome = laser
        .workflow("pipeline")
        .run_with(async |run| {
            let frames: u32 = run.step("ingest", async || Ok(12)).await?;
            let anomalies: u32 = run.step("analyze", async || Ok(frames / 6)).await?;
            run.step("report", async || {
                Ok(format!("{anomalies} anomalies in {frames} frames"))
            })
            .await
        })
        .await
        .expect("the run completes");

    assert_eq!(outcome.completed_steps, ["ingest", "analyze", "report"]);
    assert_eq!(outcome.status, SessionStatus::Completed);
    assert_eq!(
        outcome.value.as_deref(),
        Some(br#""2 anomalies in 12 frames""#.as_slice())
    );
    assert_eq!(outcome.json::<u32>("ingest").expect("decodes"), Some(12));
    assert_eq!(outcome.json::<u32>("missing").expect("decodes"), None);
    let reread = laser
        .workflow("pipeline")
        .run_id(outcome.run_id)
        .run_with(async |_run| Ok(()))
        .await
        .expect("the run reopens");
    assert_eq!(
        reread.completed_steps,
        ["ingest", "analyze", "report"],
        "the workflow's own value is not a journaled step"
    );
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_recorded_step_when_the_run_is_resumed_then_should_return_its_value_without_running_the_body()
 {
    let laser = fresh_laser("resume").await;
    let runs = AtomicUsize::new(0);
    let run_id = ConversationId::new();
    let body = async |run: &WorkflowRun| {
        run.step("ingest", async || {
            Ok(runs.fetch_add(1, Ordering::SeqCst) as u32 + 41)
        })
        .await
    };

    let first = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(body)
        .await
        .expect("the first run completes");
    let second = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(body)
        .await
        .expect("the resumed run completes");

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(first.value.as_deref(), Some(b"41".as_slice()));
    assert_eq!(second.value.as_deref(), Some(b"41".as_slice()));
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_step_called_twice_when_run_then_should_journal_the_second_call_under_its_own_label()
 {
    let laser = fresh_laser("twice").await;

    let outcome = laser
        .workflow("pipeline")
        .run_with(async |run| {
            let first: u32 = run.step("probe", async || Ok(1)).await?;
            let second: u32 = run.step("probe", async || Ok(2)).await?;
            let third: u32 = run.step("probe", async || Ok(3)).await?;
            Ok(first + second + third)
        })
        .await
        .expect("the run completes");

    assert_eq!(outcome.completed_steps, ["probe", "probe#2", "probe#3"]);
    assert_eq!(outcome.json::<u32>("probe#3").expect("decodes"), Some(3));
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_steps_started_concurrently_when_run_then_should_label_them_in_call_order() {
    let laser = fresh_laser("concurrent").await;

    let outcome = laser
        .workflow("pipeline")
        .run_with(async |run| {
            let (first, second) = tokio::join!(
                run.step("probe", async || Ok(1_u32)),
                run.step("probe", async || Ok(2_u32))
            );
            Ok(first? + second?)
        })
        .await
        .expect("the run completes");

    assert_eq!(outcome.json::<u32>("probe").expect("decodes"), Some(1));
    assert_eq!(outcome.json::<u32>("probe#2").expect("decodes"), Some(2));
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_body_that_fails_after_one_step_when_run_then_should_fail_the_run_and_keep_the_completed_step()
 {
    let laser = fresh_laser("failed").await;
    let run_id = ConversationId::new();

    let failed = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(async |run| {
            let frames: u32 = run.step("ingest", async || Ok(12)).await?;
            run.step("analyze", async || -> Result<u32, LaserError> {
                Err(LaserError::Handler(format!("{frames} frames refused")))
            })
            .await
        })
        .await;

    assert!(
        matches!(&failed, Err(LaserError::Handler(message)) if message == "12 frames refused"),
        "the step's own error reaches the caller: {failed:?}"
    );
    let reread = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(async |_run| Ok(()))
        .await
        .expect("a failed run reopens");
    assert_eq!(reread.completed_steps, ["ingest"]);
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_failed_run_when_resumed_then_should_run_only_the_failed_step_and_what_follows() {
    let laser = fresh_laser("retry").await;
    let run_id = ConversationId::new();
    let ingests = AtomicUsize::new(0);
    let analyzes = AtomicUsize::new(0);
    let body = async |run: &WorkflowRun| {
        let frames: u32 = run
            .step("ingest", async || {
                ingests.fetch_add(1, Ordering::SeqCst);
                Ok(12)
            })
            .await?;
        let anomalies: u32 = run
            .step("analyze", async || {
                if analyzes.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(LaserError::Handler("the first attempt fails".to_owned()));
                }
                Ok(frames / 6)
            })
            .await?;
        run.step("report", async || Ok(anomalies)).await
    };

    let first = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(body)
        .await;
    let second = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(body)
        .await
        .expect("the resumed run completes");

    assert!(first.is_err());
    assert_eq!(ingests.load(Ordering::SeqCst), 1);
    assert_eq!(analyzes.load(Ordering::SeqCst), 2);
    assert_eq!(second.completed_steps, ["ingest", "analyze", "report"]);
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_no_default_stream_when_run_with_a_body_then_should_use_the_stream_named_after_the_workflow()
 {
    let client = harness::connected_laser().await;
    let name = format!("function_form_{}", ConversationId::new());
    let laser = Laser::from_client(harness::client().await);
    assert_eq!(laser.default_stream(), None);

    let outcome = laser
        .workflow(&name)
        .run_with(async |run| run.step("ingest", async || Ok(12_u32)).await)
        .await
        .expect("the run creates the stream named after the workflow");

    assert_eq!(outcome.completed_steps, ["ingest"]);
    client
        .stream(name)
        .delete()
        .await
        .expect("the workflow's stream exists and deletes");
}

#[tokio::test]
async fn given_a_workflow_name_that_cannot_name_a_stream_when_run_then_should_fail_naming_laser_stream()
 {
    let laser = Laser::from_client(harness::client().await);

    let refused = laser
        .workflow(&"w".repeat(256))
        .run_with(async |_run| Ok(()))
        .await;

    assert!(
        matches!(&refused, Err(LaserError::Invalid(message)) if message.contains("LASER_STREAM")),
        "the refusal names the way out: {refused:?}"
    );
}

#[tokio::test]
async fn given_a_cancelled_run_when_the_next_step_starts_then_should_stop_before_its_body() {
    let laser = harness::laser().await;
    laser
        .topic(AgentTopic::Control.topic_string())
        .ensure(4, None)
        .await
        .expect("provisioning creates the control topic");
    let run_id = ConversationId::new();
    laser
        .sessions()
        .control(laser.default_stream().expect("stream"), run_id)
        .as_operator("operator".parse::<AgentId>().expect("valid agent id"))
        .cancel()
        .await
        .expect("the cancel request is sent");
    let bodies = AtomicUsize::new(0);

    let result = harness::eventually(|| async {
        let result = laser
            .workflow("cancelled_pipeline")
            .run_id(run_id)
            .run_with(async |run| {
                run.step("never", async || {
                    bodies.fetch_add(1, Ordering::SeqCst);
                    Ok(1_u32)
                })
                .await
            })
            .await;
        matches!(result, Err(LaserError::Cancelled { .. })).then_some(result)
    })
    .await;

    assert!(matches!(result, Err(LaserError::Cancelled { .. })));
    assert_eq!(bodies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn given_a_value_json_cannot_hold_when_a_step_returns_it_then_should_fail_naming_the_step() {
    let laser = fresh_laser("unencodable").await;

    let refused = laser
        .workflow("pipeline")
        .run_with(async |run| {
            run.step("matrix", async || {
                Ok(BTreeMap::from([((1_u8, 2_u8), 3_u8)]))
            })
            .await
        })
        .await;

    assert!(
        matches!(&refused, Err(LaserError::Codec(message)) if message.contains("`matrix`")),
        "the refusal names the step: {refused:?}"
    );
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_number_json_cannot_hold_when_a_step_returns_it_then_should_fail_naming_the_step() {
    let laser = fresh_laser("not_a_number").await;

    let refused = laser
        .workflow("pipeline")
        .run_with(async |run| run.step("ratio", async || Ok(f64::NAN)).await)
        .await;

    assert!(
        matches!(&refused, Err(LaserError::Codec(message)) if message.contains("`ratio`")),
        "the refusal names the step: {refused:?}"
    );
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_step_name_holding_the_call_separator_when_called_then_should_refuse_it() {
    let laser = fresh_laser("separator").await;
    let bodies = AtomicUsize::new(0);

    let refused = laser
        .workflow("pipeline")
        .run_with(async |run| {
            run.step("probe#2", async || {
                bodies.fetch_add(1, Ordering::SeqCst);
                Ok(1_u32)
            })
            .await
        })
        .await;

    assert!(
        matches!(&refused, Err(LaserError::Invalid(message)) if message.contains("`probe#2`")),
        "the refusal names the step: {refused:?}"
    );
    assert_eq!(bodies.load(Ordering::SeqCst), 0);
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_builder_run_when_its_outcome_is_read_then_should_carry_no_value() {
    let laser = fresh_laser("builder").await;

    let outcome = laser
        .workflow("pipeline")
        .step_local("ingest", |_: &StepContext<'_>| b"12".to_vec())
        .run()
        .await
        .expect("the builder run completes");

    assert_eq!(outcome.value, None);
    assert_eq!(outcome.json::<u32>("ingest").expect("decodes"), Some(12));
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_run_the_caller_drives_when_finished_twice_then_should_refuse_the_second_finish() {
    let laser = fresh_laser("driven").await;
    let run = laser
        .workflow("pipeline")
        .start()
        .await
        .expect("the run opens");
    let Ok(StepCall::Due(label)) = run.begin_step("ingest", None) else {
        panic!("a fresh run has no recorded step");
    };
    run.checkpoint().await.expect("the run is not cancelled");
    run.complete_step(&label, b"12".to_vec())
        .await
        .expect("the step is journaled");

    let outcome = run.finish(Ok(None)).await.expect("the run ends");
    let again = run.finish(Ok(None)).await;

    assert_eq!(outcome.completed_steps, ["ingest"]);
    assert!(matches!(again, Err(LaserError::Invalid(_))));
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_step_retried_in_the_body_when_resumed_then_should_return_the_recorded_retry() {
    let laser = fresh_laser("retried").await;
    let run_id = ConversationId::new();
    let effects = AtomicUsize::new(0);
    let attempts = AtomicUsize::new(0);
    let fail_last = AtomicUsize::new(0);
    let body = async |run: &WorkflowRun| {
        let charge = async || {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(LaserError::Handler("the first attempt fails".to_owned()));
            }
            effects.fetch_add(1, Ordering::SeqCst);
            Ok(7_u32)
        };
        let charged = match run.step("charge", charge).await {
            Ok(charged) => charged,
            Err(_) => run.step("charge", charge).await?,
        };
        run.step("ship", async || {
            if fail_last.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(LaserError::Handler("shipping is down".to_owned()));
            }
            Ok(charged)
        })
        .await
    };

    let first = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(body)
        .await;
    let second = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(body)
        .await
        .expect("the resumed run completes");

    assert!(first.is_err());
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(second.completed_steps, ["charge", "ship"]);
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_failed_call_beside_a_recorded_one_when_resumed_then_should_run_only_the_failed_call()
 {
    let laser = fresh_laser("joined").await;
    let run_id = ConversationId::new();
    let failures = AtomicUsize::new(0);
    let body = async |run: &WorkflowRun| {
        let (first, second) = tokio::join!(
            run.step("downlink", async || {
                if failures.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(LaserError::Handler("the first downlink fails".to_owned()));
                }
                Ok("frame-1".to_owned())
            }),
            run.step("downlink", async || Ok("frame-2".to_owned()))
        );
        Ok([first?, second?])
    };

    let first = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(body)
        .await;
    let second = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(body)
        .await
        .expect("the resumed run completes");

    assert!(first.is_err());
    assert_eq!(
        second.value.as_deref(),
        Some(br#"["frame-1","frame-2"]"#.as_slice())
    );
    assert_eq!(second.completed_steps, ["downlink#2", "downlink"]);
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_concurrent_branches_in_another_order_when_resumed_then_should_answer_each_by_its_input()
 {
    let laser = fresh_laser("branches").await;
    let run_id = ConversationId::new();
    let run = laser
        .workflow("pipeline")
        .run_id(run_id)
        .start()
        .await
        .expect("the run opens");
    for (branch, output) in [(b"1", b"11"), (b"2", b"22")] {
        let Ok(StepCall::Due(label)) = run.begin_step("analyze", Some(branch)) else {
            panic!("a fresh run has no recorded step");
        };
        run.complete_step(&label, output.to_vec())
            .await
            .expect("the step is journaled");
    }
    run.finish(Ok(None)).await.expect("the run ends");

    let resumed = laser
        .workflow("pipeline")
        .run_id(run_id)
        .start()
        .await
        .expect("the run reopens");

    assert_eq!(
        resumed.begin_step("analyze", Some(b"2")).expect("a call"),
        StepCall::Recorded(b"22".to_vec())
    );
    assert_eq!(
        resumed.begin_step("analyze", Some(b"1")).expect("a call"),
        StepCall::Recorded(b"11".to_vec())
    );
    assert_eq!(
        resumed.begin_step("analyze", Some(b"3")).expect("a call"),
        StepCall::Due("analyze#3".to_owned())
    );
    resumed.abandon_step("analyze#3");
    resumed.finish(Ok(None)).await.expect("the run ends");
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_step_called_after_the_run_ended_when_resumed_then_should_keep_the_recorded_value()
{
    let laser = fresh_laser("late").await;
    let run_id = ConversationId::new();
    let run = laser
        .workflow("pipeline")
        .run_id(run_id)
        .start()
        .await
        .expect("the run opens");
    let Ok(StepCall::Due(label)) = run.begin_step("ingest", None) else {
        panic!("a fresh run has no recorded step");
    };
    run.complete_step(&label, b"1".to_vec())
        .await
        .expect("the step is journaled");
    run.finish(Ok(None)).await.expect("the run ends");

    let late = run.begin_step("ingest", None);
    let unrecorded = run.complete_step("ingest", b"2".to_vec()).await;

    assert!(matches!(late, Err(LaserError::Invalid(_))));
    assert!(matches!(unrecorded, Err(LaserError::Invalid(_))));
    let reread = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(async |_run| Ok(()))
        .await
        .expect("the run reopens");
    assert_eq!(reread.json::<u32>("ingest").expect("decodes"), Some(1));
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_run_that_refuses_new_steps_when_a_step_is_called_then_should_refuse_it_and_record_the_one_in_flight()
 {
    let laser = fresh_laser("refusing").await;
    let run = laser
        .workflow("pipeline")
        .start()
        .await
        .expect("the run opens");
    let Ok(StepCall::Due(label)) = run.begin_step("downlink", None) else {
        panic!("a fresh run has no recorded step");
    };

    run.refuse_new_steps();

    let late = run.begin_step("uplink", None);
    run.complete_step(&label, b"1".to_vec())
        .await
        .expect("a step in flight still records");
    let outcome = run.finish(Ok(None)).await.expect("the run ends");

    assert!(
        matches!(&late, Err(LaserError::Invalid(message)) if message.contains("`uplink`")),
        "the refusal names the step: {late:?}"
    );
    assert_eq!(outcome.completed_steps, ["downlink"]);
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_step_in_flight_when_the_run_finishes_then_should_wait_for_its_record() {
    let laser = fresh_laser("settle").await;
    let run = std::sync::Arc::new(
        laser
            .workflow("pipeline")
            .start()
            .await
            .expect("the run opens"),
    );
    let Ok(StepCall::Due(label)) = run.begin_step("slow", None) else {
        panic!("a fresh run has no recorded step");
    };
    let slow = std::sync::Arc::clone(&run);
    let recording = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        slow.complete_step(&label, b"1".to_vec()).await
    });

    let outcome = run.finish(Ok(None)).await.expect("the run ends");

    recording
        .await
        .expect("the step task joins")
        .expect("the step records inside the grace");
    assert_eq!(outcome.completed_steps, ["slow"]);
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_step_that_never_settles_when_the_run_finishes_then_should_end_within_the_grace() {
    let laser = fresh_laser("unsettled").await;
    let run = laser
        .workflow("pipeline")
        .start()
        .await
        .expect("the run opens");
    let Ok(StepCall::Due(label)) = run.begin_step("never", None) else {
        panic!("a fresh run has no recorded step");
    };
    let started = std::time::Instant::now();

    let outcome = run.finish(Ok(None)).await.expect("the run ends");
    let late = run.complete_step(&label, b"1".to_vec()).await;

    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    assert!(outcome.completed_steps.is_empty());
    assert!(matches!(late, Err(LaserError::Invalid(_))));
    delete_stream(&laser).await;
}

#[tokio::test]
async fn given_a_released_run_when_resumed_then_should_continue_it() {
    let laser = fresh_laser("released").await;
    let run_id = ConversationId::new();
    let run = laser
        .workflow("pipeline")
        .run_id(run_id)
        .start()
        .await
        .expect("the run opens");
    let Ok(StepCall::Due(label)) = run.begin_step("ingest", None) else {
        panic!("a fresh run has no recorded step");
    };
    run.complete_step(&label, b"1".to_vec())
        .await
        .expect("the step is journaled");

    run.release();

    let resumed = laser
        .workflow("pipeline")
        .run_id(run_id)
        .run_with(async |run| run.step("ingest", async || Ok(2_u32)).await)
        .await
        .expect("a released run resumes");
    assert_eq!(resumed.value.as_deref(), Some(b"1".as_slice()));
    delete_stream(&laser).await;
}

// A laser on a fresh stream that nothing bootstrapped.
async fn fresh_laser(suffix: &str) -> Laser {
    let laser = harness::connected_laser().await;
    let stream = format!(
        "{}_{suffix}",
        laser
            .default_stream()
            .expect("the test laser names its stream")
    );
    laser.with_default_stream(stream)
}

async fn delete_stream(laser: &Laser) {
    laser
        .stream(laser.default_stream().expect("a default stream"))
        .delete()
        .await
        .expect("the stream deletes");
}
