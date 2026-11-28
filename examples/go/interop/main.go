// One set of agents reached through four edges: an A2A task, an MCP tool call,
// an AG-UI event stream, and a human decision, all riding the same log.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const bridgeTimeout = 15 * time.Second

func main() { examples.RunExample("interop", run) }

func run(ctx context.Context, client *laser.Laser) (err error) {
	examples.Phase("connecting")
	if err := client.Bootstrap(ctx, examples.Partitions, laser.ExpireAfter(examples.SessionRetention)); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	llm := examples.DefaultLLM()
	ids, err := examples.AgentIDs("assistant", "tool-runner", "approver", "a2a-gateway", "mcp-gateway", "orchestrator")
	if err != nil {
		return err
	}
	assistant, toolRunner, approver := ids[0], ids[1], ids[2]

	// Three responders share agent.sessions. Each bridge call and the input
	// request name the agent they are for, so only that agent takes them as work.
	var agents []*laser.AgentHandle
	defer func() {
		for index := len(agents) - 1; index >= 0; index-- {
			err = errors.Join(err, agents[index].Shutdown(context.WithoutCancel(ctx)))
		}
	}()
	for _, worker := range []wire.AgentID{assistant, toolRunner} {
		handle, err := spawnWorker(ctx, client, worker, llm)
		if err != nil {
			return err
		}
		agents = append(agents, handle)
	}
	approval, err := client.SpawnAgent(ctx, approver, laser.SpawnOptions{RuntimeOptions: laser.RuntimeOptions{
		Topic: wire.AgentSessions,
		// Input requests reach only an agent that serves them.
		Operations: []string{"input_request"},
	}}, laser.HandlerFunc(func(ctx context.Context, _ *laser.AgentMessage, agent *laser.AgentCtx) error {
		return agent.RespondInput(ctx, wire.AgentSessions, []byte("approved"))
	}))
	if err != nil {
		return fmt.Errorf("spawn the approver: %w", err)
	}
	agents = append(agents, approval)
	if err := approval.Ready(ctx); err != nil {
		return fmt.Errorf("approver ready: %w", err)
	}

	examples.Phase("A2A: SendMessage -> GetTask")
	a2a := bridges.NewA2ABridge(client, ids[3], wire.AgentSessions, wire.AgentSessions)
	submitted, err := a2a.SubmitTo(ctx, assistant,
		[]byte(`{"message":{"role":"user","parts":[{"kind":"text","text":"summarize the incident"}]}}`))
	if err != nil {
		return fmt.Errorf("submit the A2A task: %w", err)
	}
	a2aText, err := completedTask(ctx, a2a, submitted.ID)
	if err != nil {
		return err
	}
	fmt.Printf("A2A task %s -> completed: %s\n", submitted.ID, a2aText)

	examples.Phase("MCP: initialize / tools/list / tools/call")
	mcp, err := bridges.NewMCPBridge(client, ids[4], wire.AgentSessions, wire.AgentSessions, "laser-mcp").
		WithTool("ask", "ask the assistant a question", map[string]any{
			"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}},
		})
	if err != nil {
		return fmt.Errorf("declare the MCP tool: %w", err)
	}
	mcp = mcp.WithTimeout(bridgeTimeout)
	fmt.Printf("MCP tools/list: [%s]\n", strings.Join(toolNames(mcp.ListTools()), ", "))
	// The MCP tools/call params ride the command body unchanged.
	tool, err := mcp.CallToolTo(ctx, toolRunner, "ask",
		[]byte(`{"name":"ask","arguments":{"q":"what is the Agent Data Exchange Protocol?"}}`))
	if err != nil {
		return fmt.Errorf("call the MCP tool: %w", err)
	}
	mcpText := "(empty)"
	if len(tool.Content) > 0 {
		mcpText = tool.Content[0].Text
	}
	fmt.Printf("MCP tools/call -> isError=%t, content: %s\n", tool.IsError, mcpText)

	examples.Phase("AG-UI: render a chat stream as events")
	conversation := laser.NewConversationID()
	stream := client.Agdx(assistant, conversation).
		Stream(wire.CorrelationID{ID: conversation.ID}, wire.OperationChat)
	answer, err := llm.Complete(ctx, "give a one-line status update")
	if err != nil {
		return fmt.Errorf("model call: %w", err)
	}
	for _, token := range strings.SplitAfter(answer, " ") {
		if err := stream.Write(ctx, []byte(token)); err != nil {
			return fmt.Errorf("write a chat chunk: %w", err)
		}
	}
	if err := stream.Finish(ctx, "stop", nil); err != nil {
		return fmt.Errorf("finish the chat stream: %w", err)
	}
	events, err := bridges.AgUiEvents(ctx, client, conversation, wire.AgentSessions)
	if err != nil {
		return fmt.Errorf("render the AG-UI events: %w", err)
	}
	fmt.Printf("AG-UI rendered %d event(s) from the chat stream\n", len(events))

	// Human-in-the-loop: the orchestrator pauses for a human decision, the
	// approver resolves the interrupt it is handling. Built on AGDX
	// command and response, so it rides the same log as everything above.
	examples.Phase("Human-in-the-loop: request_input -> respond_input")
	decision, err := client.Agdx(ids[5], laser.NewConversationID()).
		RequestInputFrom(ctx, approver, wire.AgentSessions, []byte("approve draining node-7?"), bridgeTimeout)
	if err != nil {
		return fmt.Errorf("request input: %w", err)
	}
	fmt.Printf("HITL decision: %s\n", decision)
	return nil
}

// spawnWorker starts the worker behind every bridge: it reads the decoded AGDX
// command, asks the model, and answers with a response echoing the correlation.
func spawnWorker(ctx context.Context, client *laser.Laser, id wire.AgentID, llm examples.LLM) (*laser.AgentHandle, error) {
	handle, err := client.SpawnAgent(ctx, id, laser.SpawnOptions{RuntimeOptions: laser.RuntimeOptions{Topic: wire.AgentSessions}},
		laser.HandlerFunc(func(ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx) error {
			envelope := message.Envelope
			if envelope == nil || envelope.Correlation == nil {
				return errors.New("bridge command must carry an AGDX correlation")
			}
			answer, err := llm.Complete(ctx, string(envelope.Body))
			if err != nil {
				return err
			}
			_, err = agent.Laser().Agdx(id, message.Conversation()).
				Respond(*envelope.Correlation, []byte(answer)).Send(ctx)
			return err
		}))
	if err != nil {
		return nil, fmt.Errorf("spawn %s: %w", id, err)
	}
	if err := handle.Ready(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("%s ready: %w", id, err), handle.Shutdown(context.WithoutCancel(ctx)))
	}
	return handle, nil
}

// completedTask polls the A2A view until the task completes and returns its first artifact text.
func completedTask(ctx context.Context, bridge *bridges.A2ABridge, id string) (string, error) {
	deadline := time.Now().Add(bridgeTimeout)
	for time.Now().Before(deadline) {
		task, err := bridge.Task(ctx, id)
		if err != nil {
			return "", fmt.Errorf("read the A2A task: %w", err)
		}
		if task.Status.State == bridges.A2AState(wire.TaskCompleted) {
			if len(task.Artifacts) == 0 || len(task.Artifacts[0].Parts) == 0 {
				return "(no artifact)", nil
			}
			return task.Artifacts[0].Parts[0].Text, nil
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	return "", errors.New("A2A task did not complete before the deadline")
}

// toolNames reads the tool names of an MCP tools/list result.
func toolNames(listed map[string]any) []string {
	encoded, err := json.Marshal(listed)
	if err != nil {
		return nil
	}
	var result struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil
	}
	names := make([]string, 0, len(result.Tools))
	for _, tool := range result.Tools {
		names = append(names, tool.Name)
	}
	return names
}
