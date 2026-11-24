package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ScatterRequest is one scatter: a directed Contract sent to every agent
// advertising a skill.
type ScatterRequest struct {
	// Skill is what every target must advertise.
	Skill string
	// Routing ranks the capable agents. It does not narrow them: a scatter
	// asks all of them, the same contract as FanOutRequest.Routing.
	Routing RoutePolicy
	// Registry is where the capable agents are resolved from. Required.
	Registry *AgentRegistry
	// From names the agent every branch's contract sends as.
	From wire.AgentID
	// Topic is where each branch's command is published and its reply
	// awaited.
	Topic string
	// Payload is the body every branch carries.
	Payload []byte
	// Policy decides when the gather is done.
	Policy GatherPolicy
	// Deadline bounds every branch's Contract and the gather. Zero takes
	// DefaultContractDeadline.
	Deadline time.Duration
}

// ScatterOutcome is one agent's outcome in a ScatterReport.
type ScatterOutcome struct {
	// Agent is the agent this branch was contracted to.
	Agent wire.AgentID
	// Contract is the branch's terminal contract state. Completed, Failed,
	// NotConsumed, and TimedOut are all a branch that finished; Err is set
	// instead only when the branch itself could not run, an infrastructure
	// or send failure.
	Contract *Contract
	Err      error
}

// ScatterReport is the per-agent outcome of Laser.ScatterReport: every
// finished branch's terminal state, so an all-failed scatter is a report of
// failures rather than an empty success, plus the branches the gather
// policy cancelled and whether the policy was met.
type ScatterReport struct {
	// Outcomes is one entry per finished branch, in completion order.
	Outcomes []ScatterOutcome
	// Cancelled is the agents whose branch was still running when the
	// policy stopped the gather.
	Cancelled []wire.AgentID
	// Satisfied reports whether the gather met its GatherPolicy.
	Satisfied bool
}

// Completed is the outcomes whose branch completed, each with its agent and
// reply.
func (report *ScatterReport) Completed() []ScatterOutcome {
	var completed []ScatterOutcome
	for _, outcome := range report.Outcomes {
		if outcome.Err == nil && outcome.Contract != nil &&
			outcome.Contract.State == ContractCompleted {
			completed = append(completed, outcome)
		}
	}
	return completed
}

// Failures is the outcomes whose branch errored (an infrastructure or send
// failure). A non-completing terminal (Failed/NotConsumed/TimedOut) is a
// nil-Err outcome with its own Contract.State, read it from Outcomes.
func (report *ScatterReport) Failures() []ScatterOutcome {
	var failures []ScatterOutcome
	for _, outcome := range report.Outcomes {
		if outcome.Err != nil {
			failures = append(failures, outcome)
		}
	}
	return failures
}

// Scatter dispatches a directed contract to every agent advertising
// request.Skill, concurrently, gathers the branches under request.Policy
// within request.Deadline, and collects the reply body of each that
// completed (excluding unavailable and quarantined agents, which capability
// resolution already drops). Empty when no capable agent completes,
// NoCapableAgentError when none advertise the skill at all. A convenience
// over ScatterReport, which keeps per-agent outcomes, the cancelled
// branches, and whether the policy was met.
func (laser *Laser) Scatter(
	ctx context.Context, request ScatterRequest,
) ([][]byte, error) {
	report, err := laser.ScatterReport(ctx, request)
	if err != nil {
		return nil, err
	}
	var replies [][]byte
	for _, outcome := range report.Completed() {
		replies = append(replies, outcome.Contract.Reply.Envelope.Body)
	}
	return replies, nil
}

// scatterBranch is one finished (or errored) scatter branch.
type scatterBranch struct {
	agent    wire.AgentID
	contract *Contract
	err      error
}

// ScatterReport is Scatter but returns the outcome of every branch
// attributed to its agent, so a caller sees which agent completed, failed,
// or timed out rather than a bag of bodies. Errors on Quorum(0).
func (laser *Laser) ScatterReport(
	ctx context.Context, request ScatterRequest,
) (*ScatterReport, error) {
	if err := request.Policy.Validate(); err != nil {
		return nil, err
	}
	if request.Registry == nil {
		return nil, &InvalidError{
			Reason: "a scatter needs a registry to resolve its targets against"}
	}
	targets, err := AllCapable(request.Skill, request.Routing).
		ResolveTargets(request.Registry, time.Now())
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, &NoCapableAgentError{Skill: request.Skill}
	}
	deadline := request.Deadline
	if deadline <= 0 {
		deadline = DefaultContractDeadline
	}

	// Cancelling this context stops the branches the policy did not wait
	// for, rather than leaving them to run on past a gather nobody reads.
	branching, stop := context.WithCancel(ctx)
	defer stop()
	outcomes := make(chan scatterBranch, len(targets))
	running := map[wire.AgentID]bool{}
	for _, target := range targets {
		running[target] = true
		go func(target wire.AgentID) {
			contract, err := laser.Contract(To(target), request.Topic).
				From(request.From).
				Payload(request.Payload).
				Deadline(deadline).
				Send(branching)
			outcomes <- scatterBranch{agent: target, contract: contract, err: err}
		}(target)
	}

	report := &ScatterReport{}
	successes := 0
	wallClock := time.NewTimer(deadline)
	defer wallClock.Stop()
	for len(running) > 0 {
		var finished scatterBranch
		select {
		case finished = <-outcomes:
		case <-wallClock.C:
			if request.Policy.kind != gatherBestEffort {
				// Every branch is bounded by its own deadline, so the others
				// end on their own rather than being cut short here.
				wallClock.Stop()
				continue
			}
			stop()
			report.Cancelled = remaining(running)
			running = nil
			continue
		}
		delete(running, finished.agent)
		if finished.err == nil && finished.contract.State == ContractCompleted {
			successes++
		}
		report.Outcomes = append(report.Outcomes, ScatterOutcome{
			Agent: finished.agent, Contract: finished.contract, Err: finished.err,
		})
		if request.Policy.kind == gatherQuorum && successes >= request.Policy.quorum {
			stop()
			report.Cancelled = remaining(running)
			break
		}
	}
	report.Satisfied = request.Policy.Satisfied(successes, len(targets))
	return report, nil
}
