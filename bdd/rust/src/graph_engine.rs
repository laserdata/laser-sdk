// The reference knowledge-graph engine: the executable specification of the graph
// semantics every backend and every SDK port must reproduce. Pure and transport-
// free, so the shared Gherkin pins one cross-language contract: content-addressed
// node convergence (the same entity from two messages is one node) and bounded
// directional traversal. A node id is the value's stable hash, so the Rust and
// Python ports converge identically.

use std::collections::{BTreeSet, HashMap};

/// Which way a hop follows edges from the current frontier.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Dir {
    Out,
    In,
}

struct Edge {
    from: u64,
    edge_type: String,
    to: u64,
}

/// An in-memory graph of labelled nodes and typed edges. Nodes are keyed by the
/// stable hash of their value, so re-adding the same value is the same node.
#[derive(Default)]
pub struct GraphEngine {
    // node id -> display value
    nodes: HashMap<u64, String>,
    edges: Vec<Edge>,
}

impl GraphEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add (or find) the node for `value`, returning its content-addressed id.
    pub fn upsert_node(&mut self, value: &str) -> u64 {
        let id = node_id(value);
        self.nodes.entry(id).or_insert_with(|| value.to_owned());
        id
    }

    /// Add a typed edge between two nodes.
    pub fn add_edge(&mut self, from: u64, edge_type: &str, to: u64) {
        self.edges.push(Edge {
            from,
            edge_type: edge_type.to_owned(),
            to,
        });
    }

    /// The number of distinct nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Traverse from `start` following `hops` (each an edge type and direction).
    /// Returns the display values of the reachable frontier after the last hop,
    /// sorted for a deterministic assertion.
    pub fn traverse(&self, start: &str, hops: &[(String, Dir)]) -> Vec<String> {
        let mut frontier: BTreeSet<u64> = BTreeSet::new();
        frontier.insert(node_id(start));
        for (edge_type, dir) in hops {
            let mut next: BTreeSet<u64> = BTreeSet::new();
            for &node in &frontier {
                for edge in &self.edges {
                    if &edge.edge_type != edge_type {
                        continue;
                    }
                    match dir {
                        Dir::Out if edge.from == node => {
                            next.insert(edge.to);
                        }
                        Dir::In if edge.to == node => {
                            next.insert(edge.from);
                        }
                        _ => {}
                    }
                }
            }
            frontier = next;
        }
        let mut values: Vec<String> = frontier
            .into_iter()
            .filter_map(|id| self.nodes.get(&id).cloned())
            .collect();
        values.sort();
        values
    }
}

// A node's content-addressed id: a salted FNV-1a over its value, the same hash
// family the memory id uses, so the same entity always lands on one node.
fn node_id(value: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for &byte in std::iter::once(&0x6eu8).chain(value.as_bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observe(graph: &mut GraphEngine, from: &str, edge_type: &str, to: &str) {
        let from_id = graph.upsert_node(from);
        let to_id = graph.upsert_node(to);
        graph.add_edge(from_id, edge_type, to_id);
    }

    #[test]
    fn given_one_shared_entity_when_observed_twice_then_should_converge_on_one_node() {
        let mut graph = GraphEngine::new();
        observe(&mut graph, "Alice", "works_at", "Acme");
        observe(&mut graph, "Bob", "works_at", "Acme");
        assert_eq!(graph.node_count(), 3, "Acme is one shared node");
    }

    #[test]
    fn given_a_two_hop_path_when_traversed_then_should_reach_the_far_node() {
        let mut graph = GraphEngine::new();
        observe(&mut graph, "Alice", "works_at", "Acme");
        observe(&mut graph, "Acme", "located_in", "Berlin");
        let reached = graph.traverse(
            "Alice",
            &[
                ("works_at".to_owned(), Dir::Out),
                ("located_in".to_owned(), Dir::Out),
            ],
        );
        assert_eq!(reached, vec!["Berlin"]);
    }

    #[test]
    fn given_an_edge_type_when_traversed_then_should_exclude_other_edges() {
        let mut graph = GraphEngine::new();
        observe(&mut graph, "Alice", "works_at", "Acme");
        observe(&mut graph, "Alice", "lives_in", "Berlin");
        let reached = graph.traverse("Alice", &[("works_at".to_owned(), Dir::Out)]);
        assert_eq!(reached, vec!["Acme"]);
    }

    #[test]
    fn given_an_incoming_traversal_when_walked_then_should_reach_the_source() {
        let mut graph = GraphEngine::new();
        observe(&mut graph, "Alice", "works_at", "Acme");
        let reached = graph.traverse("Acme", &[("works_at".to_owned(), Dir::In)]);
        assert_eq!(reached, vec!["Alice"]);
    }
}
