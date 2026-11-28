package bdd_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

type managedWorker struct {
	mu       sync.Mutex
	messages []wire.ConversationID
}

func (w *managedWorker) handle(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
	w.mu.Lock()
	w.messages = append(w.messages, message.Conversation())
	w.mu.Unlock()
	return agent.Respond(ctx, []byte("answered"))
}

func managedSessionRelations(ctx context.Context, state *world, flow *managedSessionFlow, text string, args []string) error {
	switch {
	case strings.HasPrefix(text, "agent ") && (strings.Contains(text, " answers every command") || strings.Contains(text, " counts the work")):
		id, err := bridgeAgent(args[0])
		if err != nil {
			return err
		}
		worker := &managedWorker{}
		opts := laser.SpawnOptions{}
		opts.Topic = wire.AgentSessions
		opts.RespondOn = wire.AgentSessions
		opts.Budget = laser.NewBudgetGate()
		handle, err := state.client.SpawnAgent(ctx, id, opts, laser.HandlerFunc(worker.handle))
		if err != nil {
			return err
		}
		flow.worker = worker
		state.t.Cleanup(func() {
			stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := handle.Shutdown(stop); err != nil {
				state.t.Errorf("shut down managed worker: %v", err)
			}
		})
		return handle.Ready(ctx)
	case strings.HasPrefix(text, "the workflow "):
		source, err := bridgeAgent(args[0])
		if err != nil {
			return err
		}
		name := "worker"
		if len(args) > 1 {
			name = args[1]
		}
		target, err := bridgeAgent(name)
		if err != nil {
			return err
		}
		job := state.client.Workflow(source, wire.AgentSessions).InboxRoute(laser.FixedInbox(wire.AgentSessions))
		if strings.Contains(text, "resumes") {
			job.RunID(flow.workflowRun)
		}
		job.Step("check", laser.To(target), func(context.Context, map[string][]byte) ([]byte, error) { return []byte("incident"), nil }).Deadline(20 * time.Second)
		result, err := job.Run(ctx)
		if err != nil {
			return err
		}
		if result.Status != wire.SessionCompleted {
			return fmt.Errorf("workflow status %s", result.Status)
		}
		flow.workflowRun = result.RunID
		return nil
	case strings.HasPrefix(text, "agent ") && strings.Contains(text, "handled 1 command"):
		flow.worker.mu.Lock()
		defer flow.worker.mu.Unlock()
		if len(flow.worker.messages) != 1 {
			return fmt.Errorf("worker handled %d commands", len(flow.worker.messages))
		}
		return nil
	case text == "the session submits an A2A task as a child":
		source, err := bridgeAgent("a2a-gateway")
		if err != nil {
			return err
		}
		parent := flow.session.Conversation()
		task, err := bridges.NewA2ABridge(state.client, source, wire.AgentSessions, wire.AgentSessions).SubmitIn(ctx, parent, parent, []byte(`{"message":{"role":"user","text":"look"}}`))
		if err != nil {
			return err
		}
		id, err := wire.ParseID(task.ID)
		if err != nil {
			return err
		}
		flow.children = append(flow.children, wire.ConversationID{ID: id})
		return nil
	case strings.HasPrefix(text, "the session calls the MCP tool "):
		source, err := bridgeAgent("mcp-gateway")
		if err != nil {
			return err
		}
		parent := flow.session.Conversation()
		result, err := bridges.NewMCPBridge(state.client, source, wire.AgentSessions, wire.AgentSessions, "bdd").WithTimeout(20*time.Second).CallToolIn(ctx, parent, parent, args[0], []byte(`{"q":"auth"}`))
		if err != nil {
			return err
		}
		if result.IsError {
			return fmt.Errorf("tool failed: %v", result.Content)
		}
		return nil
	case strings.HasPrefix(text, "the session fans out one branch to "):
		planner, err := bridgeAgent("planner")
		if err != nil {
			return err
		}
		target, err := bridgeAgent(args[0])
		if err != nil {
			return err
		}
		root := flow.session.Conversation()
		branch := state.client.SpawnSubconversation(&laser.Provenance{Conversation: root}, planner)
		result, err := state.client.Contract(laser.To(target), wire.AgentSessions).From(planner).InboxRoute(laser.FixedInbox(wire.AgentSessions)).
			Conversation(branch.Conversation).Parent(*branch.ParentConversation, *branch.RootConversation).Payload([]byte("branch")).Deadline(20 * time.Second).Send(ctx)
		if err != nil {
			return err
		}
		if result.Reply == nil {
			return fmt.Errorf("branch ended without a reply: %v", result.State)
		}
		flow.children = append(flow.children, branch.Conversation)
		return nil
	case strings.HasPrefix(text, "the session tree of ") || strings.HasPrefix(text, "every child names "):
		root := flow.workflowRun
		if len(args) > 0 {
			root = flow.byLabel[args[0]].Conversation()
		}
		count, hasCount := after(text, "holds ")
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			page, err := flow.sessions.List().Root(root).Results(100).Fetch(ctx)
			if err != nil {
				return false, err
			}
			var children []wire.ConversationID
			for _, child := range page.Items {
				if child.ID == root {
					continue
				}
				if child.Parent == nil || *child.Parent != root || child.Root == nil || *child.Root != root {
					return false, fmt.Errorf("child %s has incorrect parent or root", child.ID)
				}
				children = append(children, child.ID)
			}
			for _, want := range flow.children {
				if !slices.Contains(children, want) {
					return false, nil
				}
			}
			return len(children) > 0 && (!hasCount || uint64(len(children)) == count), nil
		})
	case strings.HasPrefix(text, "the session remembers "):
		id, err := flow.session.LinkedMemory().Remember(ctx, laser.MemoryFact, []byte(args[0]))
		flow.remembered = id
		return err
	case text == "the session records that it recalled the remembered item":
		var found laser.MemoryItem
		if err := waitForSessionIndex(ctx, "linked memory recall", func() (bool, error) {
			items, err := flow.session.LinkedMemory().Recall(ctx, laser.MemoryQuery{})
			if err != nil {
				return false, err
			}
			for _, item := range items {
				if item.ID == flow.remembered {
					found = item
					return true, nil
				}
			}
			return false, nil
		}); err != nil {
			return err
		}
		query := "deploy key"
		_, err := flow.session.RecordRetrieval(ctx, &query, []laser.MemoryItem{found})
		return err
	case text == "the session links the remembered item as written and recalled":
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			surface := wire.LinkMemory
			view, err := flow.sessions.Links(ctx, flow.session.Conversation(), &surface)
			if err != nil {
				return false, err
			}
			var wrote, recalled bool
			for _, link := range view.Links {
				if link.Item == flow.remembered.String() {
					wrote = wrote || link.Relation == wire.RelationWrote
					recalled = recalled || link.Relation == wire.RelationRecalled
				}
			}
			return wrote && recalled, nil
		})
	case strings.HasPrefix(text, "the session links ") && strings.Contains(text, " in graph "):
		return flow.session.LinkedGraph(args[2]).Link(ctx, args[0], "depends_on", args[1])
	case strings.HasPrefix(text, "the sessions ") && strings.Contains(text, " both link the graph node "):
		kind, value, ok := strings.Cut(args[2], ":")
		if !ok {
			return fmt.Errorf("malformed node %s", args[2])
		}
		for _, label := range args[:2] {
			if err := waitManagedLink(ctx, flow.sessions, flow.byLabel[label], wire.LinkGraphNode, fmt.Sprint(wire.EntityNode(kind, value).ID), ""); err != nil {
				return err
			}
		}
		return nil
	case strings.HasPrefix(text, "the session sets key "):
		return flow.session.Kv(args[2]).Set([]byte(args[0])).Bytes([]byte(args[1])).Send(ctx)
	case strings.HasPrefix(text, "the session links key "):
		return waitManagedLink(ctx, flow.sessions, flow.session, wire.LinkKv, args[0], args[1])
	case strings.HasPrefix(text, "the key-value lens of the session in namespace "):
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			page, err := state.client.Kv(args[0]).Scan().Conversation(flow.session.Conversation()).Fetch(ctx)
			if err != nil {
				return false, err
			}
			for _, item := range page.Items {
				if string(item.Key) == args[1] {
					return true, nil
				}
			}
			return false, nil
		})
	case text == "a second managed session stream":
		other := &world{t: state.t, client: state.client}
		if err := other.freshStream(ctx); err != nil {
			return err
		}
		flow.second = other.client.Sessions()
		_, err := flow.second.Bootstrap(ctx, flow.partitions, laser.ExpireAfter(24*time.Hour))
		return err
	case strings.HasSuffix(text, " on the second stream") && strings.Contains(text, " starts the session "):
		source, err := bridgeAgent(args[0])
		if err != nil {
			return err
		}
		flow.secondSession = flow.second.Create(args[1]).As(source)
		_, lease, err := flow.secondSession.Begin(ctx)
		if err == nil {
			lease.Release()
		}
		return err
	case strings.HasPrefix(text, "I set key ") && strings.Contains(text, " linked to the second stream"):
		state.failure = state.client.Kv(args[1]).InSession(flow.secondSession.Reference()).Set([]byte(args[0])).Bytes([]byte("value")).Send(ctx)
		return nil
	case text == "the linked write is refused":
		if state.failure == nil {
			return fmt.Errorf("cross-stream session write succeeded")
		}
		return nil
	case text == "the second stream's session links no key-value item":
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			row, err := flow.second.Get(ctx, flow.secondSession.Conversation())
			if err != nil || row == nil {
				return false, err
			}
			surface := wire.LinkKv
			links, err := flow.second.Links(ctx, row.ID, &surface)
			if err != nil {
				return false, err
			}
			if len(links.Links) != 0 {
				return false, fmt.Errorf("cross-stream write produced links: %v", links.Links)
			}
			return true, nil
		})
	}
	return managedSessionReadStep(ctx, state, flow, text, args)
}

func waitManagedLink(ctx context.Context, sessions *laser.Sessions, session *laser.Session, surface wire.LinkSurface, item, namespace string) error {
	return waitForSessionIndex(ctx, string(surface)+" link for "+item, func() (bool, error) {
		view, err := sessions.Links(ctx, session.Conversation(), &surface)
		if err != nil {
			return false, err
		}
		for _, link := range view.Links {
			if link.Item == item && strings.HasSuffix(link.Resource, namespace) {
				return true, nil
			}
		}
		return false, nil
	})
}
