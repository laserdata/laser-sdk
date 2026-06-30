use crate::agent::{AgentRegistry, RegisteredCard};
use crate::error::LaserError;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::AgentId;

/// Where a message is addressed: one specific agent, every listener on the topic,
/// or resolved by capability against the card registry (one capable agent, or all
/// of them for fan-out).
#[derive(Debug, Clone)]
pub enum Router {
    To(AgentId),
    Broadcast,
    /// Route to the one capable agent the policy picks.
    ToCapable(CapabilitySelector),
    /// Route to every capable agent (fan-out: a diagnostic pool, a verifier panel).
    AllCapable(CapabilitySelector),
}

/// A capability route: which skill to resolve and how to pick among the agents
/// that advertise it.
#[derive(Debug, Clone)]
pub struct CapabilitySelector {
    pub skill: String,
    pub policy: RoutePolicy,
}

impl CapabilitySelector {
    /// A selector for `skill`, picking by `policy`.
    pub fn new(skill: impl Into<String>, policy: RoutePolicy) -> Self {
        Self {
            skill: skill.into(),
            policy,
        }
    }
}

/// How a resolved target agent is turned into the topic its work is sent on. The
/// substrate has no single well-known inbox, on purpose: a real deployment scopes
/// each tenant to its own stream and each workflow to its own topic, and a topic
/// may be created for one process and dropped when done. So routing resolves a
/// target to where that agent actually consumes, never a shared name baked into
/// the SDK that breaks the moment two tenants share a server.
///
/// **Stream scope.** A route resolves a *topic*, addressed within the caller's
/// current stream (`laser.with_stream`). That stream is the tenant boundary: each
/// tenant owns a stream, and its agents are addressed by topic inside it, gated
/// per topic by the broker's access control. Addressing an agent on a *different*
/// stream is cross-tenant federation, which needs a reply-to address on the wire
/// so the target can reply back across streams, and is not yet supported, so a
/// route never crosses streams silently.
///
/// Set as a default at agent-build time ([`Agent::inbox_route`](crate::agent::Agent)),
/// where it sticks as the agent's routing convention.
#[derive(Debug, Clone, Default)]
pub enum InboxRoute {
    /// Resolve each target to the `inbox` topic it advertises in its live presence
    /// (`AGDX_SET_CLIENT_METADATA`, the registry fusion of presence and cards).
    /// The default. A target advertising no inbox is a per-target failure, surfaced,
    /// never silently rerouted to a shared topic.
    #[default]
    Advertised,
    /// Send every routed message to this fixed topic regardless of per-agent
    /// presence. For a workflow where the caller already owns one topic that every
    /// participant joined, or any custom convention the caller enforces. The
    /// explicit escape hatch, honored ahead of presence.
    Fixed(AgentTopic<'static>),
}

impl InboxRoute {
    /// Resolve `agent`'s inbox topic under this route, returning the owned Iggy
    /// `Identifier` to address. `advertised` is the agent's live-presence inbox
    /// (from [`AgentRegistry::inbox_for`](crate::agent::AgentRegistry::inbox_for)),
    /// consulted only by the [`Advertised`](Self::Advertised) path. The caller
    /// wraps the result as [`AgentTopic::Custom`](crate::provenance::AgentTopic::Custom)
    /// in its own scope (the topic enum borrows the identifier, so it cannot be
    /// returned). Errors with [`LaserError::NoInbox`] when an advertised route
    /// finds no inbox for the agent, rather than inventing a destination.
    pub fn resolve(
        &self,
        agent: &AgentId,
        advertised: Option<&str>,
    ) -> Result<iggy::prelude::Identifier, LaserError> {
        let topic = match self {
            Self::Fixed(topic) => topic.topic_string(),
            Self::Advertised => advertised
                .ok_or_else(|| LaserError::NoInbox {
                    agent: agent.as_str().to_owned(),
                })?
                .to_owned(),
        };
        iggy::prelude::Identifier::named(&topic).map_err(|error| {
            LaserError::Handler(format!("inbox topic `{topic}` is invalid: {error}"))
        })
    }
}

/// How a capability route picks one agent from the agents advertising the skill.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RoutePolicy {
    /// The lowest advertised cost class.
    Cheapest,
    /// The lowest advertised latency class.
    Fastest,
    /// The lowest advertised load.
    LeastLoaded,
    /// Prefer this agent when it is among the candidates, else fall back to
    /// [`Any`](Self::Any). Keeps a conversation pinned to one agent.
    Sticky(AgentId),
    /// Any candidate (the first resolved). The default.
    #[default]
    Any,
}

/// Pick one agent for `skill_id` from `candidates` (a registry
/// [`resolve`](crate::agent::AgentRegistry::resolve) result) by `policy`, or
/// `None` when there are no candidates. The class fields are advisory `u*` where
/// lower is better, and a candidate that does not advertise the ranked field
/// sorts last so an agent that publishes its class always beats one that omits it.
fn select(skill_id: &str, candidates: &[&RegisteredCard], policy: &RoutePolicy) -> Option<AgentId> {
    let descriptor = |card: &RegisteredCard| {
        card.card
            .capabilities
            .iter()
            .find(|capability| capability.skill_id == skill_id)
            .cloned()
    };
    let chosen = match policy {
        RoutePolicy::Any => candidates.first().copied(),
        RoutePolicy::Sticky(agent) => candidates
            .iter()
            .copied()
            .find(|card| &card.agent == agent)
            .or_else(|| candidates.first().copied()),
        RoutePolicy::Cheapest => candidates.iter().copied().min_by_key(|card| {
            descriptor(card)
                .and_then(|d| d.cost_class)
                .unwrap_or(u8::MAX)
        }),
        RoutePolicy::Fastest => candidates.iter().copied().min_by_key(|card| {
            descriptor(card)
                .and_then(|d| d.latency_class)
                .unwrap_or(u8::MAX)
        }),
        RoutePolicy::LeastLoaded => candidates
            .iter()
            .copied()
            .min_by_key(|card| descriptor(card).and_then(|d| d.load).unwrap_or(u16::MAX)),
    };
    chosen.map(|card| card.agent.clone())
}

impl Router {
    /// Route to one agent (stamps `agdx.to`).
    pub fn to(agent: AgentId) -> Self {
        Self::To(agent)
    }

    /// Clear any target so every consumer-group member may handle the message.
    pub fn broadcast() -> Self {
        Self::Broadcast
    }

    /// Route to the one agent advertising `skill` that `policy` picks.
    pub fn to_capable(skill: impl Into<String>, policy: RoutePolicy) -> Self {
        Self::ToCapable(CapabilitySelector::new(skill, policy))
    }

    /// Route to every agent advertising `skill` (fan-out).
    pub fn all_capable(skill: impl Into<String>, policy: RoutePolicy) -> Self {
        Self::AllCapable(CapabilitySelector::new(skill, policy))
    }

    /// Stamp (or clear) the target for the direct variants. The capability
    /// variants are resolved against the registry by
    /// [`resolve_targets`](Self::resolve_targets), not stamped directly, so here
    /// they leave the target unset (a safe broadcast) rather than mis-target.
    pub fn apply(&self, provenance: &mut Provenance) {
        match self {
            Self::To(agent) => provenance.target_agent_id = Some(agent.clone()),
            Self::Broadcast | Self::ToCapable(_) | Self::AllCapable(_) => {
                provenance.target_agent_id = None;
            }
        }
    }

    /// Resolve to the concrete target agent(s) against `registry` at
    /// `now_micros`: `To` is the one agent, `Broadcast` is the empty list (no
    /// target), `ToCapable` is the one the policy picks, and `AllCapable` is every
    /// capable agent. Errors with [`LaserError::NoCapableAgent`] when a capability
    /// variant matches no live agent.
    pub fn resolve_targets(
        &self,
        registry: &AgentRegistry,
        now_micros: u64,
    ) -> Result<Vec<AgentId>, LaserError> {
        match self {
            Self::To(agent) => Ok(vec![agent.clone()]),
            Self::Broadcast => Ok(Vec::new()),
            Self::ToCapable(selector) => {
                let candidates = registry.resolve(&selector.skill, now_micros);
                select(&selector.skill, &candidates, &selector.policy)
                    .map(|agent| vec![agent])
                    .ok_or_else(|| LaserError::NoCapableAgent {
                        skill: selector.skill.clone(),
                    })
            }
            Self::AllCapable(selector) => {
                let agents: Vec<AgentId> = registry
                    .resolve(&selector.skill, now_micros)
                    .iter()
                    .map(|card| card.agent.clone())
                    .collect();
                if agents.is_empty() {
                    Err(LaserError::NoCapableAgent {
                        skill: selector.skill.clone(),
                    })
                } else {
                    Ok(agents)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ConversationId;

    #[test]
    fn given_a_route_to_an_agent_when_applied_then_should_set_the_target() {
        let mut provenance = Provenance::builder()
            .conversation_id(ConversationId::new())
            .build();
        Router::to("executor".parse().expect("executor is a valid agent id"))
            .apply(&mut provenance);
        assert_eq!(
            provenance
                .target_agent_id
                .expect("the target should be set")
                .as_str(),
            "executor"
        );
    }

    #[test]
    fn given_a_broadcast_route_when_applied_then_should_clear_the_target() {
        let mut provenance = Provenance::builder()
            .conversation_id(ConversationId::new())
            .target_agent_id("executor".parse().expect("executor is a valid agent id"))
            .build();
        Router::broadcast().apply(&mut provenance);
        assert!(provenance.target_agent_id.is_none());
    }

    fn card(
        agent: &str,
        cost: Option<u8>,
        latency: Option<u8>,
        load: Option<u16>,
    ) -> RegisteredCard {
        use laser_wire::agent::{AgentCard, CapabilityDescriptor};
        RegisteredCard {
            agent: agent.parse().expect("valid agent id"),
            card: AgentCard {
                name: None,
                version: None,
                capabilities: vec![CapabilityDescriptor {
                    skill_id: "diagnose".to_owned(),
                    input: None,
                    output: None,
                    cost_class: cost,
                    latency_class: latency,
                    max_concurrency: None,
                    health: None,
                    load,
                }],
                ttl_micros: None,
            },
            observed_at_micros: 0,
        }
    }

    #[test]
    fn given_candidates_when_selected_by_policy_then_should_pick_the_best_by_that_class() {
        let cheap = card("cheap", Some(1), Some(9), Some(900));
        let fast = card("fast", Some(9), Some(1), Some(500));
        let idle = card("idle", Some(5), Some(5), Some(10));
        let candidates = [&cheap, &fast, &idle];

        let pick = |policy| select("diagnose", &candidates, &policy).map(|a| a.as_str().to_owned());
        assert_eq!(pick(RoutePolicy::Cheapest).as_deref(), Some("cheap"));
        assert_eq!(pick(RoutePolicy::Fastest).as_deref(), Some("fast"));
        assert_eq!(pick(RoutePolicy::LeastLoaded).as_deref(), Some("idle"));
        assert_eq!(
            pick(RoutePolicy::Sticky("fast".parse().unwrap())).as_deref(),
            Some("fast")
        );
        // Sticky to an absent agent falls back to a candidate.
        assert!(pick(RoutePolicy::Sticky("gone".parse().unwrap())).is_some());
        // No candidates resolves to None.
        assert!(select("diagnose", &[], &RoutePolicy::Any).is_none());
    }

    #[test]
    fn given_a_fixed_inbox_route_when_resolved_then_should_use_that_topic_ignoring_presence() {
        let agent: AgentId = "billing".parse().unwrap();
        let route = InboxRoute::Fixed(AgentTopic::Commands);
        // Fixed ignores the advertised inbox entirely.
        let id = route.resolve(&agent, Some("some.other.topic")).unwrap();
        assert_eq!(id.to_string(), "agent.commands");
    }

    #[test]
    fn given_an_advertised_route_when_an_inbox_is_present_then_should_resolve_to_it() {
        let agent: AgentId = "billing".parse().unwrap();
        let id = InboxRoute::Advertised
            .resolve(&agent, Some("billing.work.2026"))
            .unwrap();
        assert_eq!(id.to_string(), "billing.work.2026");
    }

    #[test]
    fn given_an_advertised_route_when_no_inbox_then_should_error_without_a_fallback() {
        let agent: AgentId = "billing".parse().unwrap();
        let error = InboxRoute::Advertised.resolve(&agent, None).unwrap_err();
        assert!(
            matches!(error, LaserError::NoInbox { agent } if agent == "billing"),
            "advertised route with no inbox must fail loud, never fall back to a shared topic",
        );
    }
}
