package laser

import "sort"

// AgentActivity is one agent's folded activity.
//
// It answers "what has this agent been doing" from evidence already read off
// the audit topic, so a coordinator does not re-derive the same counts by
// hand every time it needs them.
type AgentActivity struct {
	// Decisions is every decision folded in for this agent, whatever the
	// verdict.
	Decisions uint64
	// LastDecision is the most recent one, for a coordinator that wants the
	// detail behind the counts.
	LastDecision *PolicyEvidence
	byVerdict    map[string]uint64
}

// Count is how many decisions this agent triggered with one exact verdict,
// zero for a verdict this view never folded.
func (activity *AgentActivity) Count(verdict string) uint64 {
	return activity.byVerdict[verdict]
}

// Verdicts is every verdict this agent triggered, with its count.
func (activity *AgentActivity) Verdicts() map[string]uint64 {
	counts := make(map[string]uint64, len(activity.byVerdict))
	for verdict, count := range activity.byVerdict {
		counts[verdict] = count
	}
	return counts
}

// fold adds one record to this agent's activity.
func (activity *AgentActivity) fold(evidence PolicyEvidence) {
	activity.Decisions++
	if activity.byVerdict == nil {
		activity.byVerdict = map[string]uint64{}
	}
	activity.byVerdict[evidence.Decision]++
	// Evidence can be read back out of order, so the latest is decided by the
	// time it records rather than by the order it arrived. The decision
	// identity breaks a tie, so two decisions at one instant still have a
	// defined order.
	if activity.LastDecision == nil ||
		activity.LastDecision.AtMicros < evidence.AtMicros ||
		(activity.LastDecision.AtMicros == evidence.AtMicros &&
			activity.LastDecision.DecisionID < evidence.DecisionID) {
		kept := evidence
		activity.LastDecision = &kept
	}
}

// SwarmActivity is a swarm-wide read model built by folding evidence a caller
// has already read.
//
// It is a pure in-process fold rather than a reader of its own, so it
// composes with whatever offset-resume convention the caller already uses: a
// replay cursor, a projection, a live consumer. The role it answers is a
// coordinator's, "what has the swarm been doing", and it is read-only: it
// never writes anything anywhere.
type SwarmActivity struct {
	byAgent map[string]*AgentActivity
	seen    map[string]struct{}
}

// NewSwarmActivity opens an empty view.
func NewSwarmActivity() *SwarmActivity {
	return &SwarmActivity{
		byAgent: map[string]*AgentActivity{},
		seen:    map[string]struct{}{},
	}
}

// Observe folds one more evidence record in.
//
// A record naming no acting agent is not attributable to one, so it is
// dropped rather than counted against a name nobody chose. A decision already
// folded is dropped too: evidence is replayable, and a replay must not double
// what it reports.
func (swarm *SwarmActivity) Observe(evidence PolicyEvidence) {
	if evidence.Source == nil || *evidence.Source == "" {
		return
	}
	if _, folded := swarm.seen[evidence.DecisionID]; folded {
		return
	}
	swarm.seen[evidence.DecisionID] = struct{}{}
	activity, known := swarm.byAgent[*evidence.Source]
	if !known {
		activity = &AgentActivity{}
		swarm.byAgent[*evidence.Source] = activity
	}
	activity.fold(evidence)
}

// Agent is one agent's folded activity, nil when this view has never folded a
// decision it triggered.
func (swarm *SwarmActivity) Agent(agent string) *AgentActivity {
	return swarm.byAgent[agent]
}

// NamedActivity is one agent and what it has been doing.
type NamedActivity struct {
	Agent    string
	Activity *AgentActivity
}

// Agents is every agent this view holds, busiest first and ties broken by
// name, so a coordinator's summary reads in the order that matters most and
// reads the same way twice.
func (swarm *SwarmActivity) Agents() []NamedActivity {
	agents := make([]NamedActivity, 0, len(swarm.byAgent))
	for name, activity := range swarm.byAgent {
		agents = append(agents, NamedActivity{Agent: name, Activity: activity})
	}
	// Ordered by name first, so what the busiest-first sort starts from does
	// not depend on map iteration. A stable sort over an arbitrary order
	// would otherwise read differently between runs wherever the counts tie.
	sort.Slice(agents, func(first, second int) bool {
		return agents[first].Agent < agents[second].Agent
	})
	sort.SliceStable(agents, func(first, second int) bool {
		return agents[first].Activity.Decisions >
			agents[second].Activity.Decisions
	})
	return agents
}
