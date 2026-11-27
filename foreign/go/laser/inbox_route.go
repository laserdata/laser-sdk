package laser

import (
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// InboxRouteKind is how a resolved target is turned into the topic its work
// is sent on.
type InboxRouteKind int

const (
	// InboxFixed sends every routed message to one topic the caller names,
	// regardless of per-agent presence.
	InboxFixed InboxRouteKind = iota
	// InboxAdvertised sends each target its work on the inbox topic that
	// target advertises in its own live presence.
	InboxAdvertised
)

// InboxRoute decides where a resolved target's work is addressed.
//
// The substrate has no single well-known inbox, on purpose: a real deployment
// scopes each user to its own stream and each workflow to its own topic, and
// a topic may be created for one process and dropped when it is done. So
// routing resolves a target to where that agent actually consumes, rather
// than to a shared name baked into the SDK that breaks the moment two users
// share a server.
type InboxRoute struct {
	Kind  InboxRouteKind
	Topic string
}

// FixedInbox sends every routed message to topic.
//
// For a workflow where the caller already owns one topic every participant
// joined, or any other convention the caller enforces. It is the explicit
// escape hatch and is honored ahead of presence.
func FixedInbox(topic string) InboxRoute {
	return InboxRoute{Kind: InboxFixed, Topic: topic}
}

// AdvertisedInbox resolves each target to the inbox it advertises in its live
// presence.
//
// A target advertising no inbox is a per-target failure, surfaced as a
// NoInboxError rather than silently rerouted to a shared topic: inventing a
// destination would deliver work somewhere nobody agreed to consume it.
//
// It needs a registry whose presence has been folded
// (AgentRegistry.RefreshPresence), which in turn needs a deployment that
// serves connection metadata. Apache Iggy does not, so against it every
// advertised route reports NoInboxError and a caller names a fixed topic
// instead.
func AdvertisedInbox() InboxRoute {
	return InboxRoute{Kind: InboxAdvertised}
}

// Resolve names the topic agent's work is addressed to under this route.
//
// advertised is the agent's live-presence inbox, which only the advertised
// route consults.
func (route InboxRoute) Resolve(agent wire.AgentID, advertised string) (string, error) {
	if route.Kind == InboxAdvertised {
		if advertised == "" {
			return "", &NoInboxError{Agent: agent}
		}
		return advertised, nil
	}
	if route.Topic == "" {
		return "", &InvalidError{Reason: "a fixed inbox route must name its topic"}
	}
	return route.Topic, nil
}

// resolvableFor reports whether this route can address agent at all, which is
// what lets a capability pick skip a candidate it could never reach.
func (route InboxRoute) resolvableFor(agent wire.AgentID, advertised string) bool {
	_, err := route.Resolve(agent, advertised)
	return err == nil
}
