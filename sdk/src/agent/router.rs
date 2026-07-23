use crate::agent::{AgentRegistry, RegisteredCard};
use crate::error::LaserError;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, PrincipalId};
use laser_wire::agent::CapabilityDescriptor;

/// Where a message is addressed: one specific agent, every listener on the topic,
/// or resolved by capability against the card registry (one capable agent, or all
/// of them for fan-out).
#[derive(Debug, Clone)]
pub enum Router {
    To(AgentId),
    /// Route to one named agent only when its live connection authenticated as
    /// the required principal.
    ToPrincipal {
        agent: AgentId,
        principal: PrincipalId,
    },
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
    /// Require the selected live agent connection to authenticate as this
    /// principal. `None` preserves claim-only discovery.
    pub principal: Option<PrincipalId>,
}

impl CapabilitySelector {
    /// A selector for `skill`, picking by `policy`.
    pub fn new(skill: impl Into<String>, policy: RoutePolicy) -> Self {
        Self {
            skill: skill.into(),
            policy,
            principal: None,
        }
    }

    /// Bind this capability route to a server-authenticated principal.
    pub fn principal(mut self, principal: PrincipalId) -> Self {
        self.principal = Some(principal);
        self
    }
}

/// How a resolved target agent is turned into the topic its work is sent on. The
/// substrate has no single well-known inbox, on purpose: a real deployment scopes
/// each user to its own stream and each workflow to its own topic, and a topic
/// may be created for one process and dropped when done. So routing resolves a
/// target to where that agent actually consumes, never a shared name baked into
/// the SDK that breaks the moment two users share a server.
///
/// **Stream scope.** A route resolves a *topic*, addressed within the caller's
/// current stream (`laser.with_stream`). That stream is the isolation boundary: each
/// user owns a stream, and its agents are addressed by topic inside it, gated
/// per topic by the streaming server's access control. Addressing an agent on a *different*
/// stream is cross-stream federation, which needs a reply-to address on the wire
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
            LaserError::HandlerConfig(format!("inbox topic `{topic}` is invalid: {error}"))
        })
    }
}

/// How a capability route picks one agent from the agents advertising the skill.
#[derive(Clone, Default)]
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
    /// Your ranking over exactly the candidate view the shipped policies read,
    /// so a custom scorer can never see less than a built-in.
    Custom(std::sync::Arc<dyn RouteScorer>),
}

// Manual because `Arc<dyn RouteScorer>` has no useful `Debug` (and the derive
// would demand one of every scorer).
impl std::fmt::Debug for RoutePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cheapest => write!(f, "Cheapest"),
            Self::Fastest => write!(f, "Fastest"),
            Self::LeastLoaded => write!(f, "LeastLoaded"),
            Self::Sticky(agent) => f.debug_tuple("Sticky").field(agent).finish(),
            Self::Any => write!(f, "Any"),
            Self::Custom(_) => write!(f, "Custom(..)"),
        }
    }
}

/// A user ranking over capability-route candidates, plugged in with
/// [`RoutePolicy::Custom`]. `select` returns the index of the chosen candidate
/// (out of bounds or `None` means no pick, and the route errors with
/// [`LaserError::NoCapableAgent`](crate::error::LaserError::NoCapableAgent)
/// exactly as an empty candidate list does).
pub trait RouteScorer: Send + Sync {
    /// Pick one of `candidates` for `skill_id`, or `None` to refuse them all.
    fn select(&self, skill_id: &str, candidates: &[RouteCandidate<'_>]) -> Option<usize>;
}

/// One capability-route candidate as every policy sees it: the agent, its
/// registered card, and the descriptor it advertises for the routed skill.
pub struct RouteCandidate<'a> {
    /// The candidate agent.
    pub agent: &'a AgentId,
    /// The agent's registered card (freshness-filtered by the registry).
    pub card: &'a RegisteredCard,
    /// The candidate's descriptor for the routed skill, when its card names
    /// the skill explicitly. The advisory class fields (`cost_class`,
    /// `latency_class`, `load`) live here.
    pub capability: Option<CapabilityDescriptor>,
}

/// Pick one agent for `skill_id` from `candidates` (a registry
/// [`resolve`](crate::agent::AgentRegistry::resolve) result) by `policy`, or
/// `None` when there are no candidates. The class fields are advisory `u*` where
/// lower is better, and a candidate that does not advertise the ranked field
/// sorts last so an agent that publishes its class always beats one that omits it.
fn select(skill_id: &str, candidates: &[&RegisteredCard], policy: &RoutePolicy) -> Option<AgentId> {
    // Every policy, shipped or custom, ranks over the same candidate view, so
    // the `Custom` seam can never see less than a built-in.
    let view: Vec<RouteCandidate<'_>> = candidates
        .iter()
        .map(|card| RouteCandidate {
            agent: &card.agent,
            card,
            capability: card
                .card
                .capabilities
                .iter()
                .find(|capability| capability.skill_id == skill_id)
                .cloned(),
        })
        .collect();
    // The advisory class fields are `u*` where lower is better, and a candidate
    // that does not advertise the ranked field sorts last, so an agent that
    // publishes its class always beats one that omits it.
    let chosen = match policy {
        RoutePolicy::Any => view.first(),
        RoutePolicy::Sticky(agent) => view
            .iter()
            .find(|candidate| candidate.agent == agent)
            .or_else(|| view.first()),
        RoutePolicy::Cheapest => view.iter().min_by_key(|candidate| {
            candidate
                .capability
                .as_ref()
                .and_then(|d| d.cost_class)
                .unwrap_or(u8::MAX)
        }),
        RoutePolicy::Fastest => view.iter().min_by_key(|candidate| {
            candidate
                .capability
                .as_ref()
                .and_then(|d| d.latency_class)
                .unwrap_or(u8::MAX)
        }),
        RoutePolicy::LeastLoaded => view.iter().min_by_key(|candidate| {
            candidate
                .capability
                .as_ref()
                .and_then(|d| d.load)
                .unwrap_or(u16::MAX)
        }),
        RoutePolicy::Custom(scorer) => scorer.select(skill_id, &view).and_then(|i| view.get(i)),
    };
    chosen.map(|candidate| candidate.agent.clone())
}

impl Router {
    /// Route to one agent (stamps `agdx.to`).
    pub fn to(agent: AgentId) -> Self {
        Self::To(agent)
    }

    /// Route to `agent` only when its live presence is bound to `principal`.
    pub fn to_principal(agent: AgentId, principal: PrincipalId) -> Self {
        Self::ToPrincipal { agent, principal }
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
            Self::To(agent) | Self::ToPrincipal { agent, .. } => {
                provenance.target_agent_id = Some(agent.clone());
            }
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
            Self::ToPrincipal { agent, principal } => {
                require_principal(registry, agent, *principal)?;
                Ok(vec![agent.clone()])
            }
            Self::Broadcast => Ok(Vec::new()),
            Self::ToCapable(selector) => {
                let candidates = trusted_candidates(registry, selector, now_micros)?;
                select(&selector.skill, &candidates, &selector.policy)
                    .map(|agent| vec![agent])
                    .ok_or_else(|| LaserError::NoCapableAgent {
                        skill: selector.skill.clone(),
                    })
            }
            Self::AllCapable(selector) => {
                let agents: Vec<AgentId> = trusted_candidates(registry, selector, now_micros)?
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

    /// Whether resolving this route requires live, principal-bearing presence.
    #[cfg(feature = "query")]
    pub(crate) fn requires_presence(&self) -> bool {
        matches!(self, Self::ToPrincipal { .. })
            || matches!(self, Self::ToCapable(selector) | Self::AllCapable(selector) if selector.principal.is_some())
    }

    /// The authenticated principal a constrained route requires. Contract reply
    /// verification uses the same identity, joining discovery and authorship.
    #[cfg(any(feature = "sign", test))]
    pub(crate) fn required_principal(&self) -> Option<PrincipalId> {
        match self {
            Self::ToPrincipal { principal, .. } => Some(*principal),
            Self::ToCapable(selector) | Self::AllCapable(selector) => selector.principal,
            Self::To(_) | Self::Broadcast => None,
        }
    }
}

fn trusted_candidates<'a>(
    registry: &'a AgentRegistry<'_>,
    selector: &CapabilitySelector,
    now_micros: u64,
) -> Result<Vec<&'a RegisteredCard>, LaserError> {
    let candidates = registry.resolve(&selector.skill, now_micros);
    let Some(principal) = selector.principal else {
        return Ok(candidates);
    };
    let trusted = filter_candidates_by_principal(&candidates, principal, |agent| {
        registry.principal_for(agent)
    });
    if trusted.is_empty()
        && let Some(card) = candidates.first()
    {
        return Err(principal_mismatch(registry, &card.agent, principal));
    }
    Ok(trusted)
}

fn filter_candidates_by_principal<'a>(
    candidates: &[&'a RegisteredCard],
    required: PrincipalId,
    principal_for: impl Fn(&AgentId) -> Option<PrincipalId>,
) -> Vec<&'a RegisteredCard> {
    candidates
        .iter()
        .copied()
        .filter(|card| principal_for(&card.agent) == Some(required))
        .collect()
}

fn require_principal(
    registry: &AgentRegistry<'_>,
    agent: &AgentId,
    principal: PrincipalId,
) -> Result<(), LaserError> {
    if registry.principal_for(agent) == Some(principal) {
        Ok(())
    } else {
        Err(principal_mismatch(registry, agent, principal))
    }
}

fn principal_mismatch(
    registry: &AgentRegistry<'_>,
    agent: &AgentId,
    expected: PrincipalId,
) -> LaserError {
    LaserError::RoutePrincipalMismatch {
        agent: agent.to_string(),
        expected: expected.get(),
        actual: registry.principal_for(agent).map(PrincipalId::get),
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
    }

    #[test]
    fn given_a_principal_bound_selector_when_filtering_then_should_drop_foreign_claims() {
        let trusted = card("trusted", Some(1), None, None);
        let foreign = card("foreign", Some(2), None, None);
        let candidates = [&trusted, &foreign];

        let filtered = filter_candidates_by_principal(&candidates, PrincipalId::new(7), |agent| {
            match agent.as_str() {
                "trusted" => Some(PrincipalId::new(7)),
                "foreign" => Some(PrincipalId::new(9)),
                _ => None,
            }
        });

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].agent.as_str(), "trusted");
    }

    #[test]
    fn given_a_principal_bound_route_when_reading_required_identity_then_should_return_principal() {
        let route = Router::to_principal(
            "billing".parse().expect("billing is a valid agent id"),
            PrincipalId::new(42),
        );

        assert_eq!(route.required_principal(), Some(PrincipalId::new(42)));
    }

    struct HighestLoad;

    impl RouteScorer for HighestLoad {
        fn select(&self, _skill_id: &str, candidates: &[RouteCandidate<'_>]) -> Option<usize> {
            (0..candidates.len()).max_by_key(|&i| {
                candidates[i]
                    .capability
                    .as_ref()
                    .and_then(|d| d.load)
                    .unwrap_or(0)
            })
        }
    }

    struct RefuseAll;

    impl RouteScorer for RefuseAll {
        fn select(&self, _skill_id: &str, _candidates: &[RouteCandidate<'_>]) -> Option<usize> {
            None
        }
    }

    #[test]
    fn given_a_custom_scorer_when_selected_then_should_rank_over_the_same_candidate_view() {
        let cheap = card("cheap", Some(1), Some(9), Some(900));
        let idle = card("idle", Some(5), Some(5), Some(10));
        let candidates = [&cheap, &idle];

        let busiest = select(
            "diagnose",
            &candidates,
            &RoutePolicy::Custom(std::sync::Arc::new(HighestLoad)),
        );
        assert_eq!(
            busiest.map(|a| a.as_str().to_owned()).as_deref(),
            Some("cheap")
        );

        // A scorer that refuses every candidate picks nobody, exactly like an
        // empty candidate list.
        let refused = select(
            "diagnose",
            &candidates,
            &RoutePolicy::Custom(std::sync::Arc::new(RefuseAll)),
        );
        assert!(refused.is_none());
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
