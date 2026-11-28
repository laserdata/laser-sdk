// One root incident with two child tasks whose results the parent collects
// explicitly, and a separate maintenance session sharing resources in the same stream.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// convergence bounds the wait for the managed session views, 600 polls of 100 ms.
const convergence = time.Minute

func main() { examples.RunExample("sessions", run) }

func run(ctx context.Context, client *laser.Laser) (err error) {
	desk := &desk{client: client, sessions: client.SessionsWith(laser.SessionConfig{Namespace: "ops"})}
	bootstrap, err := desk.sessions.Bootstrap(ctx, examples.Partitions, laser.ExpireAfter(examples.SessionRetention))
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	desk.managed = client.Capabilities().Sessions
	if desk.managed && !bootstrap.Registered {
		return errors.New("the session source registration is unavailable")
	}
	agents, err := examples.AgentIDs("triage", "specialist", "resolver")
	if err != nil {
		return err
	}
	desk.triage, desk.specialist, desk.resolver = agents[0], agents[1], agents[2]

	examples.Phase("one root incident, two child tasks, explicit result collection")
	root := desk.sessions.Create("incident-42").As(desk.triage)
	_, rootLease, err := root.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin incident-42: %w", err)
	}
	defer rootLease.Release()
	metrics, err := wire.DynValueOf(map[string]string{"service": "api"})
	if err != nil {
		return err
	}
	call, err := root.As(desk.specialist).Tool(ctx, "read_metrics", metrics)
	if err != nil {
		return fmt.Errorf("read_metrics: %w", err)
	}
	if _, err := call.Complete(ctx, []byte("latency increased")); err != nil {
		return fmt.Errorf("complete read_metrics: %w", err)
	}
	diagnosis, err := desk.task(ctx, root, "diagnosis", desk.specialist, "cache saturation")
	if err != nil {
		return err
	}
	remediation, err := desk.task(ctx, root, "remediation", desk.resolver, "reduce cache pressure")
	if err != nil {
		return err
	}
	assembled, err := root.Assemble(ctx, laser.LastN(20))
	if err != nil {
		return fmt.Errorf("assemble the findings: %w", err)
	}
	summary, err := root.Model(ctx, laser.ModelRequest{Model: "mock", Body: []byte("summarize the recorded findings")}, assembled)
	if err != nil {
		return fmt.Errorf("model call: %w", err)
	}
	if _, err := summary.Complete(ctx, laser.ModelResponse{Body: []byte("reduce cache pressure and observe latency")}); err != nil {
		return fmt.Errorf("complete the model call: %w", err)
	}

	examples.Phase("a separate maintenance session shares resources in the same stream")
	maintenance := desk.sessions.Create("maintenance-7").As(desk.resolver)
	_, maintenanceLease, err := maintenance.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin maintenance-7: %w", err)
	}
	defer maintenanceLease.Release()
	if err := setState(ctx, maintenance, "task", "verify cache capacity"); err != nil {
		return err
	}
	if err := desk.shareResources(ctx, root, maintenance); err != nil {
		return err
	}
	if _, err := root.Complete(ctx, ""); err != nil {
		return fmt.Errorf("end incident-42: %w", err)
	}
	if _, err := maintenance.Complete(ctx, ""); err != nil {
		return fmt.Errorf("end maintenance-7: %w", err)
	}
	if err := desk.converged(ctx, root); err != nil {
		return err
	}
	for _, session := range []*laser.Session{root, diagnosis, remediation, maintenance} {
		if err := desk.report(ctx, session); err != nil {
			return err
		}
	}
	fmt.Println("  2 independent roots, 2 child sessions, explicit parent results")
	return nil
}

type desk struct {
	client                       *laser.Laser
	sessions                     *laser.Sessions
	managed                      bool
	triage, specialist, resolver wire.AgentID
}

// task runs one child session and appends its result to the parent, caused by the child's record.
func (desk *desk) task(
	ctx context.Context, root *laser.Session, label string, agent wire.AgentID, result string,
) (*laser.Session, error) {
	sessions := desk.client.SessionsWith(laser.SessionConfig{Namespace: "ops/" + root.Conversation().String()})
	child := sessions.Create(label).As(agent).WithAncestry(root.Conversation(), root.Conversation())
	_, lease, err := child.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin %s: %w", label, err)
	}
	defer lease.Release()
	args, err := wire.DynValueOf(map[string]string{"service": "api", "task": label})
	if err != nil {
		return nil, err
	}
	call, err := child.Tool(ctx, "inspect_service", args)
	if err != nil {
		return nil, fmt.Errorf("%s inspect_service: %w", label, err)
	}
	receipt, err := call.Complete(ctx, []byte(result))
	if err != nil {
		return nil, fmt.Errorf("%s complete inspect_service: %w", label, err)
	}
	if err := setState(ctx, child, "result", result); err != nil {
		return nil, err
	}
	at, err := positionOf(ctx, child, receipt.Record)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	correlation := call.Correlation()
	reply := wire.AgentEnvelope{
		Kind:         wire.KindEvent,
		Record:       &wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())},
		Conversation: root.Conversation(),
		Source:       desk.triage,
		Body:         []byte(result),
		Correlation:  &correlation,
		Cause:        &receipt.Record,
		CauseAt:      &at,
	}
	if _, err := root.Append(ctx, reply); err != nil {
		return nil, fmt.Errorf("append the %s result to the parent: %w", label, err)
	}
	if err := setState(ctx, root, label, result); err != nil {
		return nil, err
	}
	if _, err := child.Complete(ctx, ""); err != nil {
		return nil, fmt.Errorf("end %s: %w", label, err)
	}
	return child, nil
}

// shareResources writes KV, graph and memory from both sessions, each linked to its own last record.
func (desk *desk) shareResources(ctx context.Context, root, maintenance *laser.Session) error {
	rootResources, err := desk.actingOnLast(ctx, root)
	if err != nil {
		return err
	}
	maintenanceResources, err := desk.actingOnLast(ctx, maintenance)
	if err != nil {
		return err
	}
	capabilities := desk.client.Capabilities()
	if capabilities.Kv.Available {
		if err := root.Kv("infra").Set([]byte("service:api")).
			JSON(map[string]string{"finding": "cache saturation"}).Send(ctx); err != nil {
			return fmt.Errorf("write service:api: %w", err)
		}
		if err := maintenance.Kv("infra").Set([]byte("maintenance:api")).
			JSON(map[string]string{"task": "verify cache capacity"}).Send(ctx); err != nil {
			return fmt.Errorf("write maintenance:api: %w", err)
		}
	}
	if capabilities.Graph {
		if err := rootResources.LinkedGraph("infra").Link(ctx, "service:api", "depends_on", "service:cache"); err != nil {
			return fmt.Errorf("link the cache dependency: %w", err)
		}
		if err := maintenanceResources.LinkedGraph("infra").Link(ctx, "service:api", "observed_by", "agent:resolver"); err != nil {
			return fmt.Errorf("link the observer: %w", err)
		}
	}
	_, err = rootResources.LinkedMemory().
		Remember(ctx, laser.MemoryFact, []byte("api latency increased when the cache saturated"))
	if err != nil {
		return fmt.Errorf("remember the finding: %w", err)
	}
	return nil
}

// actingOnLast returns the session acting on its own newest record, so what it writes links back to it.
func (desk *desk) actingOnLast(ctx context.Context, session *laser.Session) (*laser.Session, error) {
	turns, err := session.Context(ctx)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", session.Label(), err)
	}
	if len(turns) == 0 || turns[len(turns)-1].Message.Record == nil {
		return nil, errors.New("the recorded source is unavailable")
	}
	message := turns[len(turns)-1].Message
	conversation := session.Conversation().String()
	source := wire.MessageSource{
		Stream: message.StreamID, Topic: message.TopicID,
		Partition: message.Record.PartitionID, Offset: message.Record.Offset(),
		Conversation: &conversation,
	}
	if desk.managed {
		sources, err := desk.sessions.Sources(ctx, session.Conversation())
		if err != nil {
			return nil, fmt.Errorf("read the sources of %s: %w", session.Label(), err)
		}
		if sources.Lane != nil {
			source.Generation = &sources.Lane.Generation
		}
	}
	return session.ActingOn(wire.NewMessageSource(source)), nil
}

// converged waits until the managed views show the root completed and its memory link.
func (desk *desk) converged(ctx context.Context, root *laser.Session) error {
	if !desk.managed {
		return nil
	}
	deadline := time.Now().Add(convergence)
	for {
		info, err := desk.sessions.Get(ctx, root.Conversation())
		if err != nil && !laser.IsNotFound(err) && !laser.IsStale(err) {
			return fmt.Errorf("read the session index: %w", err)
		}
		if err == nil && info.Status == wire.SessionCompleted {
			links, err := desk.sessions.Links(ctx, root.Conversation(), nil)
			if err != nil {
				return fmt.Errorf("read the session links: %w", err)
			}
			for _, link := range links.Links {
				if link.Surface == wire.LinkMemory {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return errors.New("the session example views did not converge")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (desk *desk) report(ctx context.Context, session *laser.Session) error {
	turns, err := session.Context(ctx)
	if err != nil {
		return fmt.Errorf("read %s: %w", session.Label(), err)
	}
	links := 0
	if desk.managed {
		view, err := desk.sessions.Links(ctx, session.Conversation(), nil)
		if err != nil {
			return fmt.Errorf("read the links of %s: %w", session.Label(), err)
		}
		links = len(view.Links)
	}
	fmt.Printf("  %s: %d events, %d resource links\n", session.Label(), len(turns), links)
	return nil
}

func setState(ctx context.Context, session *laser.Session, key, value string) error {
	document, err := wire.DynValueOf(value)
	if err != nil {
		return err
	}
	if _, err := session.State().Set(ctx, key, document); err != nil {
		return fmt.Errorf("set %s on %s: %w", key, session.Label(), err)
	}
	return nil
}

// positionOf finds where the session's record with this id sits on the log.
func positionOf(ctx context.Context, session *laser.Session, record wire.RecordID) (wire.LogPosition, error) {
	turns, err := session.Context(ctx)
	if err != nil {
		return wire.LogPosition{}, fmt.Errorf("read the child session: %w", err)
	}
	for _, turn := range turns {
		message := turn.Message
		if message.Record == nil || message.Envelope == nil || message.Envelope.Record == nil ||
			*message.Envelope.Record != record {
			continue
		}
		return wire.LogPosition{
			StreamID: message.StreamID, TopicID: message.TopicID,
			PartitionID: message.Record.PartitionID, Offset: message.Record.Offset(),
		}, nil
	}
	return wire.LogPosition{}, errors.New("the recorded child result is unavailable")
}
