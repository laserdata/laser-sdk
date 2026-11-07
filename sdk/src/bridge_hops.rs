use crate::error::LaserError;
use laser_wire::query::Value;
use serde_json::Value as JsonValue;

/// The key an inbound request carries its `bridge_hops` path under: in MCP
/// `params._meta`, in A2A `params.metadata`. A bridge that forwards a call to
/// another bridge's HTTP endpoint puts the path of the work it handles there,
/// so the next bridge can refuse a loop.
pub const BRIDGE_HOPS_META_KEY: &str = "laserdata.com/bridge_hops";

/// Append this bridge to the ordered loop-guard path.
///
/// A bridge must reject a message when its own id is already present. The
/// returned list is ready to store under `bridge_hops` metadata.
pub fn enter_bridge(bridge: &str, previous: &[String]) -> Result<Vec<String>, LaserError> {
    if bridge.is_empty() {
        return Err(LaserError::Invalid(
            "bridge id must not be empty".to_owned(),
        ));
    }
    if previous.iter().any(|hop| hop == bridge) {
        return Err(LaserError::Invalid(format!(
            "bridge loop detected at `{bridge}`"
        )));
    }
    let mut hops = previous.to_vec();
    hops.push(bridge.to_owned());
    Ok(hops)
}

// The `bridge_hops` metadata value a bridge stamps on what it publishes: the
// hop path as a list of strings.
pub(crate) fn hops_metadata(hops: &[String]) -> Value {
    Value::List(hops.iter().map(Value::from).collect())
}

// The path one call carries: the bridge's configured path with the call's
// inbound hops before this bridge, refused when either already holds it.
pub(crate) fn call_hops(
    configured: &[String],
    inbound: &[String],
) -> Result<Vec<String>, LaserError> {
    let Some((bridge, previous)) = configured.split_last() else {
        return Err(LaserError::Invalid(
            "bridge id must not be empty".to_owned(),
        ));
    };
    if inbound.is_empty() {
        return Ok(configured.to_vec());
    }
    let mut path = previous.to_vec();
    path.extend(
        inbound
            .iter()
            .filter(|hop| !previous.contains(hop))
            .cloned(),
    );
    enter_bridge(bridge, &path)
}

// The inbound hop path under `BRIDGE_HOPS_META_KEY` of a request's metadata
// object, empty when absent. Anything but a list of strings is refused.
pub(crate) fn inbound_hops(metadata: Option<&JsonValue>) -> Result<Vec<String>, LaserError> {
    let invalid =
        || LaserError::Invalid(format!("{BRIDGE_HOPS_META_KEY} must be a list of strings"));
    let Some(hops) = metadata.and_then(|metadata| metadata.get(BRIDGE_HOPS_META_KEY)) else {
        return Ok(Vec::new());
    };
    hops.as_array()
        .ok_or_else(invalid)?
        .iter()
        .map(|hop| hop.as_str().map(str::to_owned).ok_or_else(invalid))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn given_a_hop_path_when_stamped_then_should_be_a_list_of_strings() {
        let hops = enter_bridge("mcp-edge", &["a2a-edge".to_owned()]).expect("no loop");
        assert_eq!(
            hops_metadata(&hops),
            Value::List(vec![
                Value::Str("a2a-edge".to_owned()),
                Value::Str("mcp-edge".to_owned()),
            ])
        );
    }

    #[test]
    fn given_a_path_holding_the_bridge_when_entered_then_should_reject_the_loop() {
        let error = enter_bridge("edge", &["edge".to_owned()]).expect_err("loop");
        assert!(matches!(error, LaserError::Invalid(_)));
    }

    #[test]
    fn given_inbound_hops_when_a_call_enters_the_bridge_then_should_extend_or_refuse_the_path() {
        let configured = vec!["upstream".to_owned(), "a2a-edge".to_owned()];
        assert_eq!(
            call_hops(&configured, &[]).expect("no inbound hops"),
            configured
        );
        assert_eq!(
            call_hops(&configured, &["upstream".to_owned(), "mcp-edge".to_owned()])
                .expect("no loop"),
            ["upstream", "mcp-edge", "a2a-edge"]
        );
        assert!(matches!(
            call_hops(&configured, &["a2a-edge".to_owned(), "mcp-edge".to_owned()]),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_request_metadata_when_read_then_should_return_the_hops_or_refuse_a_bad_shape() {
        assert!(inbound_hops(None).expect("absent").is_empty());
        assert!(
            inbound_hops(Some(&json!({"trace": "x"})))
                .expect("no key")
                .is_empty()
        );
        assert_eq!(
            inbound_hops(Some(&json!({BRIDGE_HOPS_META_KEY: ["a2a-edge"]}))).expect("a list"),
            ["a2a-edge"]
        );
        for bad in [
            json!({BRIDGE_HOPS_META_KEY: "a2a-edge"}),
            json!({BRIDGE_HOPS_META_KEY: [1]}),
        ] {
            assert!(matches!(
                inbound_hops(Some(&bad)),
                Err(LaserError::Invalid(_))
            ));
        }
    }
}
