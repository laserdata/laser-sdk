use crate::agent::wire_id;
use crate::error::InvalidError;
use crate::validate::Validate;
use serde::{Deserialize, Serialize};

wire_id!(
    /// Stable identity of one operator-owned backend resource.
    BackendResourceId
);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendBinding {
    pub resource_id: BackendResourceId,
    pub generation: u64,
}

impl Validate for BackendBinding {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.resource_id.as_u128() == 0 {
            return Err(InvalidError::new("backend resource id must be nonzero"));
        }
        if self.generation == 0 {
            return Err(InvalidError::new("backend generation must be nonzero"));
        }
        Ok(())
    }
}
