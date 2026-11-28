package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// PresenceTTL is how long a paged presence read is reused before the next
// refresh pages the connection table again.
//
// Presence changes rarely, where paging the whole connection table is the
// dominant per-send cost, so a short reuse window turns a per-send full-table
// read into an occasional one without routing meaningfully stale inboxes.
const PresenceTTL = 2 * time.Second

// presenceEntry is one folded presence and the principal of the connection
// that advertised it.
//
// The principal is what the deployment authenticated: the trustworthy half.
// The agent id and inbox inside the presence are the connection's own claim,
// so an inbox is only ever safe to trust as that principal's.
type presenceEntry struct {
	presence  wire.AgentPresence
	principal *PrincipalID
}

// PrincipalFor is the principal the connection advertising this agent's
// presence authenticated as, or nil when no presence names it.
func (registry *AgentRegistry) PrincipalFor(agent wire.AgentID) *PrincipalID {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	entry, held := registry.presence[agent]
	if !held {
		return nil
	}
	return entry.principal
}

// InboxFor is the topic this agent advertised in its live presence, else the
// inbox its card names, or empty when neither does.
//
// The card inbox serves a server without live presence.
func (registry *AgentRegistry) InboxFor(agent wire.AgentID) string {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	if inbox := inboxOf(registry.presence[agent]); inbox != "" {
		return inbox
	}
	if inbox := registry.cards[agent].Card.Inbox; inbox != nil {
		return *inbox
	}
	return ""
}

// InboxForPrincipal is InboxFor, but only when the connection that advertised
// it authenticated as principal.
//
// This is the resolve a deployment with an enrolled agent-to-principal
// mapping uses, so a connection cannot route another principal's work to
// itself by advertising that principal's agent id.
func (registry *AgentRegistry) InboxForPrincipal(
	agent wire.AgentID, principal PrincipalID,
) string {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	entry, held := registry.presence[agent]
	if !held || entry.principal == nil || *entry.principal != principal {
		return ""
	}
	return inboxOf(entry)
}

// Presences is how many live presences the registry holds.
func (registry *AgentRegistry) Presences() int {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	return len(registry.presence)
}

// RefreshPresence pages the deployment's connection table and folds each
// connection's advertised metadata as presence, keyed by the agent id the
// connection claims, so a route can resolve where an agent is consuming right
// now. Reports how many presences are held.
//
// Within PresenceTTL of the last read this reuses what it holds rather than
// paging again. A later connection claiming an agent id already claimed wins,
// matching last-write presence.
//
// Apache Iggy keeps no connection metadata, so against it this folds nothing
// and reports zero, and a route falls through to the card's own inbox. A
// deployment not known to serve the read that refuses it is read the same
// way, rather than failing a send over a surface that was never promised.
func (registry *AgentRegistry) RefreshPresence(
	ctx context.Context, laser *Laser,
) (int, error) {
	if laser == nil {
		return 0, &InvalidError{Reason: "a presence refresh needs a client to read through"}
	}
	now := time.Now()
	registry.mutex.RLock()
	readAt, read := registry.presenceReadAt, !registry.presenceReadAt.IsZero()
	held := len(registry.presence)
	registry.mutex.RUnlock()
	if read && now.Sub(readAt) < PresenceTTL {
		return held, nil
	}

	hello := laser.Capabilities().Hello
	if hello == HelloRejected {
		return 0, nil
	}
	connections, err := laser.ClientMetadata().WithMetadataOnly(true).All(ctx)
	if err != nil {
		if hello != HelloAnswered {
			// Nothing promised this read, so a refusal is "no live presence
			// to read" rather than a failure. The read time is still marked,
			// so an unserved deployment is not re-probed on every send.
			registry.markPresenceRead(now)
			return 0, nil
		}
		return 0, err
	}

	fresh := make(map[wire.AgentID]presenceEntry, len(connections))
	for _, connection := range connections {
		if len(connection.Metadata) == 0 {
			continue
		}
		applyPresence(fresh, connection.Metadata, connection.UserID)
	}
	registry.mutex.Lock()
	registry.presence = fresh
	registry.presenceReadAt = now
	folded := len(fresh)
	registry.mutex.Unlock()
	return folded, nil
}

// markPresenceRead records that a read was attempted, so the ttl applies to a
// deployment that serves no presence just as it does to one that does.
func (registry *AgentRegistry) markPresenceRead(at time.Time) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.presenceReadAt = at
}

// applyPresence folds one connection's advertised metadata, keyed by the agent
// the presence claims, and reports whether it was applied.
//
// The metadata channel is opaque: a blob that is not a presence body, or one
// that fails its own contract, belongs to another application and is skipped
// rather than treated as a malformed agent. Pure, so the decode-and-key fold
// is testable without a connection.
func applyPresence(
	presence map[wire.AgentID]presenceEntry, metadata []byte, userID *uint32,
) bool {
	var body wire.AgentPresence
	if err := wire.Unmarshal(metadata, &body); err != nil {
		return false
	}
	if err := body.Validate(); err != nil {
		return false
	}
	var principal *PrincipalID
	if userID != nil {
		bound := PrincipalID(*userID)
		principal = &bound
	}
	presence[body.Agent] = presenceEntry{presence: body, principal: principal}
	return true
}

// inboxOf reads an entry's declared inbox, empty when it declared none.
func inboxOf(entry presenceEntry) string {
	if entry.presence.Inbox == nil {
		return ""
	}
	return *entry.presence.Inbox
}
