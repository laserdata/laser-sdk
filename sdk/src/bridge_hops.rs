use crate::error::LaserError;
use laser_wire::query::Value;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
