use crate::iggy::prelude::{Client, ClusterClient, IggyClient};
use iggy_common::{ClusterMetadata, ClusterNode, ClusterNodeRole, ClusterNodeStatus};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Pinned {
    pub(crate) node: String,
    pub(crate) client: Arc<IggyClient>,
}

#[derive(Default)]
pub(crate) struct ManagedRoute {
    pinned: tokio::sync::Mutex<Option<Pinned>>,
}

impl ManagedRoute {
    /// `None` while managed requests use the shared connection.
    pub(crate) async fn pinned(&self) -> Option<Pinned> {
        self.pinned.lock().await.clone()
    }

    /// `failed` is `None` for the shared connection. Only a move away from `failed`
    /// takes effect, so callers that fail together move once.
    pub(crate) async fn repin(
        &self,
        connection_string: &str,
        shared: &IggyClient,
        failed: Option<&str>,
        versions: Option<laser_wire::hello::OpVersions>,
    ) -> Option<String> {
        let mut pinned = self.pinned.lock().await;
        let current = pinned.as_ref().map(|pinned| pinned.node.as_str());
        if current != failed {
            // Another caller already moved off the failed node.
            return current.map(str::to_owned);
        }
        let metadata = match &*pinned {
            Some(old) => match old.client.get_cluster_metadata().await {
                Ok(metadata) => metadata,
                Err(_) => shared.get_cluster_metadata().await.ok()?,
            },
            None => shared.get_cluster_metadata().await.ok()?,
        };
        for node in candidates(&metadata, failed) {
            let Some(endpoint) = crate::filters::route::node_endpoint(connection_string, node)
            else {
                continue;
            };
            let Ok(client) =
                crate::filters::route::dial(connection_string, &endpoint, ("managed", "failover"))
                    .await
            else {
                continue;
            };
            if !serves_managed(&client, versions).await {
                let _ = client.disconnect().await;
                continue;
            }
            if let Some(old) = pinned.take() {
                let _ = old.client.disconnect().await;
            }
            tracing::info!(
                target: "laser",
                node = %node.name,
                "managed requests moved to another node after its backend failed"
            );
            *pinned = Some(Pinned {
                node: node.name.clone(),
                client: Arc::new(client),
            });
            return Some(node.name.clone());
        }
        None
    }

    /// No-op unless still pinned to `node`.
    pub(crate) async fn unpin(&self, node: &str) {
        let mut pinned = self.pinned.lock().await;
        if pinned.as_ref().is_some_and(|pinned| pinned.node == node)
            && let Some(old) = pinned.take()
        {
            let _ = old.client.disconnect().await;
        }
    }

    pub(crate) async fn close(&self) {
        if let Some(old) = self.pinned.lock().await.take() {
            let _ = old.client.disconnect().await;
        }
    }
}

/// With no pinned node the shared connection was on the leader, so the leader is skipped.
pub(crate) fn candidates<'a>(
    metadata: &'a ClusterMetadata,
    failed: Option<&str>,
) -> Vec<&'a ClusterNode> {
    metadata
        .nodes
        .iter()
        .filter(|node| match failed {
            Some(name) => node.name != name,
            None => node.role != ClusterNodeRole::Leader,
        })
        .filter(|node| {
            !matches!(
                node.status,
                ClusterNodeStatus::Stopping | ClusterNodeStatus::Unreachable
            )
        })
        .collect()
}

/// A node on other op versions would decode the same frame differently, so it is rejected.
async fn serves_managed(
    client: &IggyClient,
    versions: Option<laser_wire::hello::OpVersions>,
) -> bool {
    let Ok(reply) = client
        .send_binary_request(laser_wire::codes::AGDX_HELLO_CODE, bytes::Bytes::new())
        .await
    else {
        return false;
    };
    let Ok(announce) =
        laser_wire::framing::decode_named::<laser_wire::hello::BackendAnnounce>(&reply)
    else {
        return false;
    };
    announce.ready && versions.is_none_or(|versions| versions == announce.versions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::TransportEndpoints;

    fn node(name: &str, role: ClusterNodeRole, status: ClusterNodeStatus) -> ClusterNode {
        ClusterNode {
            name: name.to_owned(),
            ip: "10.0.0.1".to_owned(),
            endpoints: TransportEndpoints::new(8090, 8080, 8070, 8060),
            role,
            status,
        }
    }

    fn roster() -> ClusterMetadata {
        ClusterMetadata {
            name: "cluster".to_owned(),
            nodes: vec![
                node("a", ClusterNodeRole::Leader, ClusterNodeStatus::Healthy),
                node("b", ClusterNodeRole::Follower, ClusterNodeStatus::Healthy),
                node(
                    "c",
                    ClusterNodeRole::Follower,
                    ClusterNodeStatus::Unreachable,
                ),
                node("d", ClusterNodeRole::Follower, ClusterNodeStatus::Healthy),
            ],
        }
    }

    fn names(nodes: Vec<&ClusterNode>) -> Vec<&str> {
        nodes.into_iter().map(|node| node.name.as_str()).collect()
    }

    #[test]
    fn given_shared_connection_failed_when_choosing_then_should_skip_the_leader_and_unreachable_nodes()
     {
        assert_eq!(names(candidates(&roster(), None)), vec!["b", "d"]);
    }

    #[test]
    fn given_pinned_node_failed_when_choosing_then_should_skip_it_and_allow_the_leader() {
        assert_eq!(names(candidates(&roster(), Some("b"))), vec!["a", "d"]);
    }
}
