use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedOperation {
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationState {
    pub definition_revision: u64,
    pub checkpoint_revision: u64,
    pub effective_state: &'static str,
    pub next_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelError {
    Conflict { observed_revision: u64 },
    Cancelled,
    NotFound,
}

#[derive(Default)]
pub struct DataStackModel {
    global_revision: u64,
    destinations: BTreeMap<String, DestinationState>,
    query_rows: Vec<String>,
    query_cursor: usize,
    query_cancelled: bool,
}

impl DataStackModel {
    pub fn register(
        &mut self,
        name: &str,
        expected_global_revision: u64,
    ) -> Result<AcceptedOperation, ModelError> {
        self.require_global_revision(expected_global_revision)?;
        self.global_revision += 1;
        self.destinations.insert(
            name.to_owned(),
            DestinationState {
                definition_revision: 1,
                checkpoint_revision: 0,
                effective_state: "disabled",
                next_offset: 0,
            },
        );
        Ok(AcceptedOperation {
            id: format!("operation-{}", self.global_revision),
        })
    }

    pub fn enable(
        &mut self,
        name: &str,
        expected_global_revision: u64,
        expected_definition_revision: u64,
    ) -> Result<AcceptedOperation, ModelError> {
        self.require_global_revision(expected_global_revision)?;
        let destination = self
            .destinations
            .get_mut(name)
            .ok_or(ModelError::NotFound)?;
        if destination.definition_revision != expected_definition_revision {
            return Err(ModelError::Conflict {
                observed_revision: self.global_revision,
            });
        }
        self.global_revision += 1;
        destination.definition_revision += 1;
        destination.checkpoint_revision += 1;
        destination.effective_state = "running";
        Ok(AcceptedOperation {
            id: format!("operation-{}", self.global_revision),
        })
    }

    pub fn record_retention_gap(
        &mut self,
        name: &str,
        _required_offset: u64,
        _retained_offset: u64,
    ) -> Result<(), ModelError> {
        let destination = self
            .destinations
            .get_mut(name)
            .ok_or(ModelError::NotFound)?;
        self.global_revision += 1;
        destination.checkpoint_revision += 1;
        destination.effective_state = "blocked";
        Ok(())
    }

    pub fn accept_retention_gap(
        &mut self,
        name: &str,
        next_offset: u64,
        expected_checkpoint_revision: u64,
    ) -> Result<(), ModelError> {
        let destination = self
            .destinations
            .get_mut(name)
            .ok_or(ModelError::NotFound)?;
        if destination.checkpoint_revision != expected_checkpoint_revision {
            return Err(ModelError::Conflict {
                observed_revision: self.global_revision,
            });
        }
        self.global_revision += 1;
        destination.checkpoint_revision += 1;
        destination.effective_state = "running";
        destination.next_offset = next_offset;
        Ok(())
    }

    pub fn destination(&self, name: &str) -> Option<&DestinationState> {
        self.destinations.get(name)
    }

    pub fn seed_query(&mut self, rows: Vec<String>) {
        self.query_rows = rows;
        self.query_cursor = 0;
        self.query_cancelled = false;
    }

    pub fn page(&mut self, limit: usize) -> Result<(Vec<String>, Option<usize>), ModelError> {
        if self.query_cancelled {
            return Err(ModelError::Cancelled);
        }
        let start = self.query_cursor;
        let end = start.saturating_add(limit).min(self.query_rows.len());
        self.query_cursor = end;
        let cursor = (end < self.query_rows.len()).then_some(end);
        Ok((self.query_rows[start..end].to_vec(), cursor))
    }

    pub fn cancel_query(&mut self) {
        self.query_cancelled = true;
    }

    pub fn query_cancelled(&self) -> bool {
        self.query_cancelled
    }

    fn require_global_revision(&self, expected: u64) -> Result<(), ModelError> {
        if self.global_revision == expected {
            Ok(())
        } else {
            Err(ModelError::Conflict {
                observed_revision: self.global_revision,
            })
        }
    }
}
