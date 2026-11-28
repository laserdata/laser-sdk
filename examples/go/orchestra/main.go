// One orchestrator coordinating a pool of long-running capability agents,
// entirely over the log. It is interactive and paced: it stops at each phase
// and waits for Enter, so every transition can be watched in the LaserData
// console's Orchestration view. LASER_NON_INTERACTIVE=1 runs it straight through.
//
// Routing resolves each agent's advertised inbox: the live presence on a
// managed deployment, and the inbox its capability card names on Apache Iggy.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	example   = "orchestra"
	classify  = "classify"
	diagnose  = "diagnose"
	remediate = "remediate"
	slowTask  = "slow-task"
	incident  = "auth API latency spike"
)

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) (err error) {
	if err := client.Bootstrap(ctx, 1, laser.ExpireAfter(examples.SessionRetention)); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	ids, err := examples.AgentIDs("orchestrator", "operator", "diag-alpha", "incident-response")
	if err != nil {
		return err
	}
	desk := &orchestra{client: client, orchestrator: ids[0], operator: ids[1], input: bufio.NewReader(os.Stdin)}
	alpha := ids[2]

	examples.Phase("Discovery: a pool of long-running capability agents connects")
	// Kept alive for the whole run so the console stays populated. Health is a
	// property of the card: diag-gamma advertises unavailable to prove routing
	// reads it, and laggard is deliberately slow to drive the expiry phase.
	pool := &workerPool{}
	defer func() { err = errors.Join(err, pool.close(context.WithoutCancel(ctx))) }()
	for _, worker := range []worker{
		{"triager", classify, wire.HealthHealthy, 200 * time.Millisecond},
		{"diag-alpha", diagnose, wire.HealthHealthy, 400 * time.Millisecond},
		{"diag-beta", diagnose, wire.HealthHealthy, 400 * time.Millisecond},
		{"diag-gamma", diagnose, wire.HealthUnavailable, 400 * time.Millisecond},
		{"executor", remediate, wire.HealthHealthy, 300 * time.Millisecond},
		{"laggard", slowTask, wire.HealthHealthy, 6 * time.Second},
	} {
		if err := pool.spawn(ctx, worker); err != nil {
			return err
		}
	}
	fmt.Println("six agents connected and advertised their capability cards")
	desk.pause("DISCOVERY: six agents are live in the registry (one unavailable)")

	examples.Phase("Contract: a directed task to one capable agent, with a deadline")
	// The orchestrator names a capability, not an agent. Routing resolves the one
	// classifier from the registry and waits for the reply or the deadline.
	classified, err := desk.contractSkill(ctx, classify, 10*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("classifier replied: %s\n", completedBody(classified, "<did not complete>"))
	desk.pause("CONTRACT: a directed task completed (see it in the Contracts panel)")

	examples.Phase("Fan-out: a panel scattered to every capable agent")
	// Three agents advertise diagnose, but one is unavailable, so the scatter
	// reaches the two healthy ones without the orchestrator knowing their ids.
	findings, err := desk.diagnosePanel(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("panel gathered %d findings (the unavailable agent was skipped)\n", len(findings))
	desk.pause("FAN-OUT: two healthy diagnosers answered, the unavailable one was skipped")

	examples.Phase("Workflow: triage, then a diagnose panel, then remediate (journalled)")
	// The run is a session and every step a child session of it, so the
	// sessions view shows the whole tree on any deployment.
	outcome, err := client.Workflow(ids[3], wire.AgentSessions).
		// Cap the dispatches and wall clock so a runaway fan-out cannot spin.
		Budget(laser.WorkflowBudget{}.WithInvocations(8).WithWallClock(time.Minute)).
		Step("triage", laser.ToCapable(classify, laser.RouteAny), func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte(incident), nil
		}).
		// Each step reads the prior steps' outputs from the journal, so the
		// dependency edge is data, not a shared variable.
		Step("diagnose", laser.AllCapable(diagnose, laser.RouteAny), func(_ context.Context, outputs map[string][]byte) ([]byte, error) {
			return []byte("diagnose: " + string(outputs["triage"])), nil
		}).
		After("triage").
		VerifyWith(func(_ context.Context, folded []byte) (bool, error) { return len(folded) > 0, nil }).
		Step("remediate", laser.ToCapable(remediate, laser.RouteAny), func(_ context.Context, outputs map[string][]byte) ([]byte, error) {
			return []byte("remediate: " + string(outputs["diagnose"])), nil
		}).
		After("diagnose").
		Run(ctx)
	if err != nil {
		return fmt.Errorf("workflow: %w", err)
	}
	fmt.Printf("workflow completed and journalled: %d steps\n", len(outcome.Outputs))
	desk.pause("WORKFLOW: the run journalled triage -> diagnose -> remediate (Workflow panel)")

	examples.Phase("Quarantine: an operator pulls a misbehaving agent")
	// Quarantine is a registry fact every fused registry folds, so the next panel
	// routes around diag-alpha with no change to the orchestrator.
	if err := client.Quarantine(ctx, desk.operator, alpha); err != nil {
		return fmt.Errorf("quarantine diag-alpha: %w", err)
	}
	quarantined, err := desk.diagnosePanel(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("panel after quarantine: %d findings (alpha routed around)\n", len(quarantined))
	desk.pause("QUARANTINE: diag-alpha is quarantined in the registry, the panel routes around it")

	examples.Phase("Recovery: the operator reinstates the agent")
	if err := client.Unquarantine(ctx, desk.operator, alpha); err != nil {
		return fmt.Errorf("unquarantine diag-alpha: %w", err)
	}
	reinstated, err := desk.diagnosePanel(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("panel after un-quarantine: %d findings (alpha is back)\n", len(reinstated))
	desk.pause("RECOVERY: diag-alpha is reinstated, the panel is whole again")

	examples.Phase("Expiry + recovery: a tight deadline times out, the orchestrator recovers")
	// The slow agent acks pickup but cannot finish inside the one-second deadline,
	// so the contract expires. The task carries that deadline and the slow agent
	// stops there instead of finishing late, so re-dispatching to a healthy agent
	// cannot run the task twice.
	slow, err := desk.contractSkill(ctx, slowTask, time.Second)
	if err != nil {
		return err
	}
	if slow.State == laser.ContractCompleted {
		fmt.Printf("unexpectedly fast: %s\n", completedBody(slow, ""))
	} else {
		fmt.Println("the slow agent missed the deadline, recovering on a healthy agent")
		recovered, err := desk.contractSkill(ctx, remediate, 10*time.Second)
		if err != nil {
			return err
		}
		fmt.Printf("recovered: %s\n", completedBody(recovered, "<did not complete>"))
	}
	desk.pause("EXPIRY: the slow agent timed out, the task recovered on a healthy agent")

	fmt.Println("\norchestra: discovery, routing, fan-out, a journalled workflow, health,\n" +
		"reversible quarantine, and deadline recovery, all coordinated over the log.")
	return nil
}

type orchestra struct {
	client                 *laser.Laser
	orchestrator, operator wire.AgentID
	input                  *bufio.Reader
}

func (desk *orchestra) contractSkill(ctx context.Context, skill string, deadline time.Duration) (*laser.Contract, error) {
	contract, err := desk.client.Contract(laser.ToCapable(skill, laser.RouteAny), wire.AgentSessions).
		From(desk.orchestrator).
		Payload([]byte(incident)).
		Deadline(deadline).
		Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("contract %s: %w", skill, err)
	}
	return contract, nil
}

// diagnosePanel scatters a diagnose panel to every capable agent. Capability
// resolution leaves unavailable agents out.
func (desk *orchestra) diagnosePanel(ctx context.Context) ([][]byte, error) {
	findings, err := desk.client.Scatter(ctx, laser.ScatterRequest{
		Skill:    diagnose,
		Routing:  laser.RouteAny,
		From:     desk.orchestrator,
		Topic:    wire.AgentSessions,
		Payload:  []byte(incident),
		Policy:   laser.GatherAll(),
		Deadline: 10 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("diagnose panel: %w", err)
	}
	return findings, nil
}

// pause prints what to watch, then waits for Enter unless LASER_NON_INTERACTIVE is set.
func (desk *orchestra) pause(prompt string) {
	fmt.Printf("\n  >>> %s\n      (watch the console's /orchestration view, then press Enter)\n", prompt)
	if examples.EnvBool("LASER_NON_INTERACTIVE", false) {
		return
	}
	// A closed input reads as Enter, so a piped run never hangs.
	_, _ = desk.input.ReadString('\n')
}

type worker struct {
	name, skill string
	health      wire.Health
	delay       time.Duration
}

// workerPool keeps every agent and its connection open until the run ends.
type workerPool struct {
	handles     []*laser.AgentHandle
	connections []*laser.Laser
}

// spawn starts one long-running capability agent on its own connection, so each
// is a distinct live presence in the console. It advertises its card on start.
func (pool *workerPool) spawn(ctx context.Context, worker worker) error {
	id, err := wire.NewAgentID(worker.name)
	if err != nil {
		return err
	}
	connection, err := examples.Connect(ctx, example)
	if err != nil {
		return fmt.Errorf("connect %s: %w", worker.name, err)
	}
	pool.connections = append(pool.connections, connection)
	health := worker.health
	handle, err := connection.SpawnAgent(ctx, id, laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{
			Topic:     wire.AgentSessions,
			RespondOn: wire.AgentSessions,
			// Ack on pickup so the orchestrator can tell a consumed task from an
			// expired one, which is what makes the expiry phase legible.
			AckOnPickup: true,
		},
		Capabilities: []wire.CapabilityDescriptor{{SkillID: worker.skill, Health: &health}},
	}, laser.HandlerFunc(worker.handle))
	if err != nil {
		return fmt.Errorf("spawn %s: %w", worker.name, err)
	}
	pool.handles = append(pool.handles, handle)
	if err := handle.Ready(ctx); err != nil {
		return fmt.Errorf("%s ready: %w", worker.name, err)
	}
	return nil
}

func (pool *workerPool) close(ctx context.Context) error {
	var err error
	for _, handle := range slices.Backward(pool.handles) {
		err = errors.Join(err, handle.Shutdown(ctx))
	}
	for _, connection := range slices.Backward(pool.connections) {
		err = errors.Join(err, connection.Close())
	}
	return err
}

func (worker worker) handle(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
	// A task carries its caller's deadline. Working past it would finish a
	// task the caller already gave up on and sent elsewhere, so stop there.
	remaining := worker.delay
	if provenance := message.Record.Provenance; provenance != nil && provenance.DeadlineAt != nil {
		remaining = time.Until(*provenance.DeadlineAt)
	}
	if remaining < worker.delay {
		return wait(ctx, max(remaining, 0))
	}
	if err := wait(ctx, worker.delay); err != nil {
		return err
	}
	return agent.Respond(ctx, []byte(worker.reply(string(message.Body()))))
}

func (worker worker) reply(task string) string {
	switch worker.skill {
	case classify:
		return fmt.Sprintf("severity=high (%s)", task)
	case diagnose:
		return fmt.Sprintf("%s: cache stampede on the hot key [%s]", worker.name, task)
	case remediate:
		return fmt.Sprintf("%s: drained the hot key, scaled the cache [%s]", worker.name, task)
	default:
		return fmt.Sprintf("%s: %s done [%s]", worker.name, worker.skill, task)
	}
}

func completedBody(contract *laser.Contract, otherwise string) string {
	if contract.State != laser.ContractCompleted || contract.Reply == nil {
		return otherwise
	}
	return string(contract.Reply.Envelope.Body)
}

func wait(ctx context.Context, delay time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}
