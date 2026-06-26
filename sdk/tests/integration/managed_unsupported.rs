// The managed coordination surface against raw Apache Iggy (no managed backend):
// the connect-time `AGDX_HELLO` probe gets rejected, so the per-feature
// capabilities stay off and every coordination call returns a clean
// `Unsupported` classified as `ResultCode::Unsupported`. This pins
// the no-fallback boundary end to end through the real connect path,
// the contract every language SDK must honor.

use crate::harness::laser;
use laser_sdk::prelude::Capabilities;
use laser_sdk::query::{Consistency, ResultCode};

#[tokio::test]
async fn given_open_iggy_when_capabilities_read_then_should_report_coordination_features_off() {
    let laser = laser().await;
    let caps = laser.capabilities().await;
    assert!(!caps.managed, "open Apache Iggy is not a managed host");
    assert!(!caps.kv.cas, "compare-and-swap must not be advertised");
    assert!(
        !caps.serves_consistency(Consistency::ReadYourWrites),
        "read-your-writes must not be advertised"
    );
    assert!(
        !caps.serves_consistency(Consistency::Strong),
        "strong consistency must not be advertised"
    );
}

#[tokio::test]
async fn given_open_iggy_when_compare_and_swap_then_should_be_unsupported() {
    let laser = laser().await;
    let error = laser
        .kv("coordination")
        .set("lock")
        .bytes(b"held")
        .expect_absent()
        .commit()
        .await
        .expect_err("compare-and-swap must be unsupported on open Apache Iggy");
    assert!(
        error.is_unsupported(),
        "expected Unsupported, got {error:?}"
    );
    assert_eq!(error.code(), ResultCode::Unsupported);
}

#[tokio::test]
async fn given_open_iggy_when_compare_and_swap_with_version_then_should_be_unsupported() {
    let laser = laser().await;
    let error = laser
        .kv("coordination")
        .set("counter")
        .bytes(b"1")
        .expect_version(7)
        .commit()
        .await
        .expect_err("compare-and-swap must be unsupported on open Apache Iggy");
    assert!(
        error.is_unsupported(),
        "expected Unsupported, got {error:?}"
    );
    assert_eq!(error.code(), ResultCode::Unsupported);
}

#[tokio::test]
async fn given_open_iggy_when_read_your_writes_query_then_should_be_unsupported() {
    let laser = laser().await;
    let error = laser
        .query("events")
        .read_your_writes()
        .fetch()
        .await
        .expect_err("read-your-writes must be unsupported on open Apache Iggy");
    assert!(
        error.is_unsupported(),
        "expected Unsupported, got {error:?}"
    );
    assert_eq!(error.code(), ResultCode::Unsupported);
}

#[tokio::test]
async fn given_open_iggy_when_strong_consistency_query_then_should_be_unsupported() {
    let laser = laser().await;
    let error = laser
        .query("events")
        .consistency(Consistency::Strong)
        .fetch()
        .await
        .expect_err("strong consistency must be unsupported on open Apache Iggy");
    assert!(
        error.is_unsupported(),
        "expected Unsupported, got {error:?}"
    );
    assert_eq!(error.code(), ResultCode::Unsupported);
}

// `commit()` requires a precondition (`expect_version`/`expect_absent`). Without
// one it is a programmer error surfaced as a typed `Invalid`, never a panic and
// never a round-trip. The check fires before the capability gate, so it holds
// even on open Iggy.
#[tokio::test]
async fn given_a_set_without_a_precondition_when_committed_then_should_be_invalid() {
    let laser = laser().await;
    let error = laser
        .kv("coordination")
        .set("k")
        .bytes(b"v")
        .commit()
        .await
        .expect_err("a commit without a precondition must be Invalid");
    assert_eq!(error.code(), ResultCode::InvalidArgument);
    assert!(
        !error.is_unsupported(),
        "missing precondition is not Unsupported"
    );
}

// The pre-gate in isolation: a connection where the base query surface IS
// available but the consistency level is NOT advertised must refuse the level
// locally, before any send. The open-Iggy tests above cannot reach this case
// (there `managed_query` is off, so the query fails at the managed gate first),
// yet it is the one that matters, since without the pre-gate an unaware backend
// would silently drop the additive `consistency` field and serve a stale read.
#[tokio::test]
async fn given_managed_query_without_read_your_writes_when_querying_then_should_refuse_the_level() {
    let laser = laser()
        .await
        .with_capabilities(Capabilities::OPEN.with_query(true));
    let error = laser
        .query("events")
        .read_your_writes()
        .fetch()
        .await
        .expect_err("an unadvertised read-your-writes level must be refused locally");
    assert!(
        error.is_unsupported(),
        "expected Unsupported, got {error:?}"
    );
    assert_eq!(error.code(), ResultCode::Unsupported);
}

#[tokio::test]
async fn given_read_your_writes_only_when_querying_strong_then_should_refuse_the_level() {
    // Read-your-writes does not imply strong, so a connection that advertises
    // only the weaker level still refuses a strong query locally.
    let laser = laser().await.with_capabilities(
        Capabilities::OPEN
            .with_query(true)
            .with_query_consistency(Consistency::ReadYourWrites),
    );
    let error = laser
        .query("events")
        .consistency(Consistency::Strong)
        .fetch()
        .await
        .expect_err("strong must be refused when only read-your-writes is advertised");
    assert!(
        error.is_unsupported(),
        "expected Unsupported, got {error:?}"
    );
    assert_eq!(error.code(), ResultCode::Unsupported);
}

// The other side of the gate: an advertised level passes the local check
// and is actually sent. Open Iggy has no managed backend, so the send fails,
// but crucially NOT with the pre-gate's "not served by this deployment"
// message, proving the gate let an advertised level through
// rather than blocking it.
#[tokio::test]
async fn given_advertised_read_your_writes_when_querying_then_should_pass_the_local_gate() {
    let laser = laser().await.with_capabilities(
        Capabilities::OPEN
            .with_query(true)
            .with_query_consistency(Consistency::ReadYourWrites),
    );
    let error = laser
        .query("events")
        .read_your_writes()
        .fetch()
        .await
        .expect_err("open Iggy has no managed backend, so the send fails");
    assert!(
        !error.to_string().contains("not served by this deployment"),
        "an advertised level must pass the pre-gate, got: {error}"
    );
}
