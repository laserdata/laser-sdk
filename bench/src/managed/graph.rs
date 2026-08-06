use std::sync::Arc;
use std::time::{Duration, Instant};

use laser_sdk::laser::Laser;
use laser_sdk::wire::graph::{EdgeDir, GraphEdge, GraphNode};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use strum::{Display, EnumString, IntoStaticStr};

use super::{
    ManagedArmSummary, ManagedCase, ManagedProcessMeasurement, capture_processes, finish_processes,
    run_load, summarize, validate_case, warmup_count,
};
use crate::BenchError;
use crate::engine::{LoadResult, Operation};
use crate::process::PlaneProfile;

const RELATION: &str = "bench_edge";

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_graph_arm
)]
pub enum GraphArm {
    EdgeUpsert,
    NeighborRead,
    NodeUpsert,
    Traversal,
    VectorStart,
}

fn invalid_graph_arm(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported graph arm `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct GraphSummary {
    pub operation: ManagedArmSummary,
    pub backend_profile: PlaneProfile,
    pub graph: String,
    pub fan_out: usize,
    pub depth: u32,
    pub vector_dimensions: Option<usize>,
    pub corpus_entries: Option<u64>,
    pub configuration: serde_json::Value,
}

pub struct GraphEvidence {
    pub summary: GraphSummary,
    pub load: LoadResult,
    pub processes: Vec<ManagedProcessMeasurement>,
}

#[derive(Clone)]
struct GraphOperationContext {
    laser: Laser,
    arm: GraphArm,
    case: ManagedCase,
    graph: String,
    root: GraphNode,
    expected_traversal_nodes: usize,
    vector_dimensions: usize,
}

/// Run one knowledge-graph operation through the public managed SDK surface.
///
/// # Errors
///
/// Returns an error when capability discovery, graph setup, execution, or final-state validation fails.
pub async fn run_graph_evidence(
    laser: &Laser,
    case: &ManagedCase,
    arm: GraphArm,
    profile: PlaneProfile,
    scenario: &str,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<GraphEvidence, BenchError> {
    validate_case(case)?;
    validate_graph_case(case, arm)?;
    wait_for_graph(laser, Duration::from_secs(30)).await?;
    let graph = graph_name(scenario, seed);
    let root = GraphNode::entity("bench_root", &graph);
    let expected_traversal_nodes = if arm == GraphArm::Traversal {
        traversal_node_count(case.batch_size, case.partitions)?
    } else {
        0
    };
    let vector_dimensions = case.payload_bytes / size_of::<f32>();
    let context = GraphOperationContext {
        laser: laser.clone(),
        arm,
        case: case.clone(),
        graph,
        root,
        expected_traversal_nodes,
        vector_dimensions,
    };
    prepare_graph(&context).await?;
    let timeout = Duration::from_millis(case.timeout_millis);
    let warmup_operations = case.warmup_seconds.max(1);
    warmup_count(
        warmup_operations,
        case,
        timeout,
        context.clone().operation(0),
    )
    .await?;
    let before = capture_processes(monitored_processes)?;
    let load = run_load(case, timeout, context.clone().operation(warmup_operations)).await?;
    let processes = finish_processes(before, "measurement")?;
    validate_graph(&context, &load).await?;
    let elements = elements_per_operation(arm, case, expected_traversal_nodes);
    let operation = summarize(arm.into(), &load, case, elements);
    Ok(GraphEvidence {
        summary: GraphSummary {
            operation,
            backend_profile: profile,
            graph: context.graph,
            fan_out: case.batch_size,
            depth: case.partitions,
            vector_dimensions: (arm == GraphArm::VectorStart).then_some(vector_dimensions),
            corpus_entries: (arm == GraphArm::VectorStart)
                .then_some(case.corpus_entries.unwrap_or_default()),
            configuration: serde_json::json!({
                "path": "laser_graph_through_iggy_and_plane",
                "backend_profile": profile,
                "setup_timed": false,
                "validation_timed": false,
                "batch_size_role": "fan_out_or_top_k",
                "partitions_role": "traversal_depth",
            }),
        },
        load,
        processes,
    })
}

async fn wait_for_graph(laser: &Laser, timeout: Duration) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;
    loop {
        if laser.refresh_capabilities().await.graph {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "plane did not advertise graph within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn prepare_graph(context: &GraphOperationContext) -> Result<(), BenchError> {
    match context.arm {
        GraphArm::NodeUpsert => Ok(()),
        GraphArm::EdgeUpsert => {
            let mut nodes = Vec::new();
            for operation in 0..context.case.operations {
                nodes.push(GraphOperationContext::edge_root(operation));
                for target in 0..context.case.batch_size {
                    nodes.push(GraphOperationContext::edge_target(operation, target));
                }
            }
            context.upsert(nodes, Vec::new()).await
        }
        GraphArm::NeighborRead => {
            let (nodes, edges) = star(&context.root, context.case.batch_size, "neighbor");
            context.upsert(nodes, edges).await
        }
        GraphArm::Traversal => {
            let (nodes, edges) = tree(
                &context.root,
                context.case.batch_size,
                context.case.partitions,
            )?;
            context.upsert(nodes, edges).await
        }
        GraphArm::VectorStart => {
            let entries = context.case.corpus_entries.unwrap_or_default();
            let nodes = (0..entries)
                .map(|id| vector_node(id, context.vector_dimensions))
                .collect();
            context.upsert(nodes, Vec::new()).await
        }
    }
}

impl GraphOperationContext {
    fn operation(self, offset: u64) -> Operation {
        Arc::new(move |sequence| {
            let context = self.clone();
            Box::pin(async move {
                let operation = offset
                    .checked_add(sequence)
                    .ok_or_else(|| "graph operation ID overflowed".to_owned())?;
                context.execute(operation).await
            })
        })
    }

    async fn execute(&self, operation: u64) -> Result<(), String> {
        let operation = if self.arm == GraphArm::EdgeUpsert {
            operation % self.case.operations
        } else {
            operation
        };
        match self.arm {
            GraphArm::NodeUpsert => {
                let nodes = (0..self.case.batch_size)
                    .map(|item| Self::operation_node(operation, item))
                    .collect();
                self.upsert(nodes, Vec::new())
                    .await
                    .map_err(|error| error.to_string())
            }
            GraphArm::EdgeUpsert => {
                let root = Self::edge_root(operation);
                let edges = (0..self.case.batch_size)
                    .map(|item| {
                        GraphEdge::relate(&root, RELATION, &Self::edge_target(operation, item))
                    })
                    .collect();
                self.upsert(Vec::new(), edges)
                    .await
                    .map_err(|error| error.to_string())
            }
            GraphArm::NeighborRead => {
                let result = self
                    .laser
                    .graph(&self.graph)
                    .limit(star_element_count(self.case.batch_size))
                    .neighbors(self.root.id, EdgeDir::Out, Some(RELATION.to_owned()), 1)
                    .await
                    .map_err(|error| error.to_string())?;
                expect_nodes(&result.nodes, self.case.batch_size.saturating_add(1))
            }
            GraphArm::Traversal => {
                let mut traversal = self
                    .laser
                    .graph(&self.graph)
                    .start_ids(vec![self.root.id])
                    .limit(tree_element_count(self.expected_traversal_nodes));
                for _ in 0..self.case.partitions {
                    traversal = traversal.out(RELATION);
                }
                let result = traversal.fetch().await.map_err(|error| error.to_string())?;
                expect_nodes(&result.nodes, self.expected_traversal_nodes)
            }
            GraphArm::VectorStart => {
                let result = self
                    .laser
                    .graph(&self.graph)
                    .start_nearest(vec![0.0; self.vector_dimensions], self.case.batch_size)
                    .limit(self.case.batch_size)
                    .fetch()
                    .await
                    .map_err(|error| error.to_string())?;
                expect_nodes(&result.nodes, self.case.batch_size)
            }
        }
    }

    async fn upsert(&self, nodes: Vec<GraphNode>, edges: Vec<GraphEdge>) -> Result<(), BenchError> {
        self.laser
            .graph(&self.graph)
            .upsert(nodes, edges)
            .await
            .map_err(|error| BenchError::Invalid(format!("graph upsert: {error}")))
    }

    fn operation_node(operation: u64, item: usize) -> GraphNode {
        GraphNode::entity("bench_operation", format!("{operation:016x}_{item:08x}"))
    }

    fn edge_root(operation: u64) -> GraphNode {
        GraphNode::entity("bench_edge_root", format!("{operation:016x}"))
    }

    fn edge_target(operation: u64, item: usize) -> GraphNode {
        GraphNode::entity("bench_edge_target", format!("{operation:016x}_{item:08x}"))
    }
}

async fn validate_graph(
    context: &GraphOperationContext,
    load: &LoadResult,
) -> Result<(), BenchError> {
    if context.arm == GraphArm::NodeUpsert && load.outcomes.successful != 0 {
        let operation = context
            .case
            .warmup_seconds
            .max(1)
            .saturating_add(load.outcomes.offered.saturating_sub(1));
        let ids = (0..context.case.batch_size)
            .map(|item| GraphOperationContext::operation_node(operation, item).id)
            .collect();
        let result = context
            .laser
            .graph(&context.graph)
            .start_ids(ids)
            .limit(context.case.batch_size)
            .fetch()
            .await
            .map_err(|error| BenchError::Invalid(format!("validate graph nodes: {error}")))?;
        expect_nodes(&result.nodes, context.case.batch_size).map_err(BenchError::Invalid)?;
    }
    if context.arm == GraphArm::EdgeUpsert && load.outcomes.successful != 0 {
        let operation = context
            .case
            .warmup_seconds
            .max(1)
            .saturating_add(load.outcomes.offered.saturating_sub(1))
            % context.case.operations;
        let result = context
            .laser
            .graph(&context.graph)
            .limit(star_element_count(context.case.batch_size))
            .neighbors(
                GraphOperationContext::edge_root(operation).id,
                EdgeDir::Out,
                Some(RELATION.to_owned()),
                1,
            )
            .await
            .map_err(|error| BenchError::Invalid(format!("validate graph edges: {error}")))?;
        expect_nodes(&result.nodes, context.case.batch_size.saturating_add(1))
            .map_err(BenchError::Invalid)?;
    }
    Ok(())
}

fn star(root: &GraphNode, fan_out: usize, label: &str) -> (Vec<GraphNode>, Vec<GraphEdge>) {
    let mut nodes = vec![root.clone()];
    let mut edges = Vec::with_capacity(fan_out);
    for item in 0..fan_out {
        let target = GraphNode::entity(label, format!("{item:08x}"));
        edges.push(GraphEdge::relate(root, RELATION, &target));
        nodes.push(target);
    }
    (nodes, edges)
}

fn tree(
    root: &GraphNode,
    fan_out: usize,
    depth: u32,
) -> Result<(Vec<GraphNode>, Vec<GraphEdge>), BenchError> {
    let mut nodes = vec![root.clone()];
    let mut edges = Vec::new();
    let mut frontier = vec![root.clone()];
    for level in 0..depth {
        let mut next = Vec::new();
        for (parent_index, parent) in frontier.iter().enumerate() {
            for child in 0..fan_out {
                let node = GraphNode::entity(
                    "bench_tree",
                    format!("{level:02x}_{parent_index:08x}_{child:08x}"),
                );
                edges.push(GraphEdge::relate(parent, RELATION, &node));
                nodes.push(node.clone());
                next.push(node);
            }
        }
        frontier = next;
    }
    if nodes.len() > laser_sdk::wire::limits::MAX_GRAPH_RESULT_ELEMENTS {
        return Err(BenchError::Invalid(
            "graph tree exceeds result cap".to_owned(),
        ));
    }
    Ok((nodes, edges))
}

fn vector_node(id: u64, dimensions: usize) -> GraphNode {
    let mut node = GraphNode::entity("bench_vector", format!("{id:016x}"));
    let coordinate = f32::from(u16::try_from(id % 1_024).expect("coordinate is bounded"));
    node.embedding = Some(vec![coordinate; dimensions]);
    node
}

fn expect_nodes(nodes: &[GraphNode], expected: usize) -> Result<(), String> {
    if nodes.len() == expected {
        Ok(())
    } else {
        Err(format!(
            "graph returned {} nodes, expected {expected}",
            nodes.len()
        ))
    }
}

fn traversal_node_count(fan_out: usize, depth: u32) -> Result<usize, BenchError> {
    let mut level = 1usize;
    let mut total = 1usize;
    for _ in 0..depth {
        level = level
            .checked_mul(fan_out)
            .ok_or_else(|| BenchError::Invalid("graph traversal size overflowed".to_owned()))?;
        total = total
            .checked_add(level)
            .ok_or_else(|| BenchError::Invalid("graph traversal size overflowed".to_owned()))?;
    }
    Ok(total)
}

fn star_element_count(fan_out: usize) -> usize {
    fan_out.saturating_mul(2).saturating_add(1)
}

fn tree_element_count(nodes: usize) -> usize {
    nodes.saturating_mul(2).saturating_sub(1)
}

fn elements_per_operation(arm: GraphArm, case: &ManagedCase, traversal: usize) -> u64 {
    let elements = match arm {
        GraphArm::Traversal => traversal,
        GraphArm::EdgeUpsert
        | GraphArm::NeighborRead
        | GraphArm::NodeUpsert
        | GraphArm::VectorStart => case.batch_size,
    };
    u64::try_from(elements).unwrap_or(u64::MAX)
}

fn graph_name(scenario: &str, seed: u64) -> String {
    let digest = Sha256::digest(scenario.as_bytes());
    let scenario = u64::from_be_bytes(
        digest[..size_of::<u64>()]
            .try_into()
            .expect("SHA-256 prefix has a fixed length"),
    );
    format!("bench_graph_{scenario:016x}_{seed:016x}")
}

fn validate_graph_case(case: &ManagedCase, arm: GraphArm) -> Result<(), BenchError> {
    if arm == GraphArm::Traversal {
        if case.partitions > laser_sdk::wire::limits::MAX_GRAPH_TRAVERSE_DEPTH {
            return Err(BenchError::Invalid(format!(
                "graph depth exceeds {}",
                laser_sdk::wire::limits::MAX_GRAPH_TRAVERSE_DEPTH
            )));
        }
        let nodes = traversal_node_count(case.batch_size, case.partitions)?;
        if tree_element_count(nodes) > laser_sdk::wire::limits::MAX_GRAPH_RESULT_ELEMENTS {
            return Err(BenchError::Invalid(
                "graph traversal workload exceeds result cap".to_owned(),
            ));
        }
    }
    if arm == GraphArm::NeighborRead
        && star_element_count(case.batch_size) > laser_sdk::wire::limits::MAX_GRAPH_RESULT_ELEMENTS
    {
        return Err(BenchError::Invalid(
            "graph neighbor workload exceeds result cap".to_owned(),
        ));
    }
    if arm == GraphArm::VectorStart {
        let entries = case.corpus_entries.unwrap_or_default();
        if entries < u64::try_from(case.batch_size).unwrap_or(u64::MAX)
            || case.payload_bytes < size_of::<f32>()
            || !case.payload_bytes.is_multiple_of(size_of::<f32>())
        {
            return Err(BenchError::Invalid(
                "vector_start requires corpus_entries >= batch_size and payload_bytes divisible by four"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_fan_out_and_depth_when_counted_then_should_include_every_level() {
        assert_eq!(traversal_node_count(2, 3).expect("tree should fit"), 15);
    }

    #[test]
    fn given_tree_when_generated_then_should_match_expected_node_and_edge_counts() {
        let root = GraphNode::entity("root", "one");
        let (nodes, edges) = tree(&root, 2, 2).expect("tree should build");
        assert_eq!(nodes.len(), 7);
        assert_eq!(edges.len(), 6);
    }
}
