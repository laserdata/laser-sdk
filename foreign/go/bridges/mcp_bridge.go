package bridges

import (
	"context"
	"encoding/json"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// MCPSupportedProtocolVersions are the MCP protocol versions this bridge
// serves, newest first. Every one carries Streamable HTTP and the tool,
// resource and prompt shapes the bridge answers with.
var MCPSupportedProtocolVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26"}

// mcpDefaultProtocolVersion is answered on initialize when the client asks
// for a version this bridge does not serve: the latest it does.
const mcpDefaultProtocolVersion = "2025-11-25"

// mcpDefaultTimeout bounds a tool call whose caller named none.
const mcpDefaultTimeout = 30 * time.Second

// MCPBridge bridges the MCP JSON-RPC edge onto agent topics: tools/call
// becomes a typed AGDX command on toolTopic (the tool name carried on the
// envelope, the MCP params tunneled byte-identical in the body with
// agdx.ct = json), and the bridge awaits the correlated AGDX response/error on
// replyTopic within its timeout, rendering it back as an MCP tool result
// (isError on a failure).
//
// It owns no state: truth is the log, so the HTTP layer is a thin shell over
// CallTool and the JSON-RPC dispatch.
type MCPBridge struct {
	laser          *laser.Laser
	source         wire.AgentID
	toolTopic      string
	replyTopic     string
	serverName     string
	tools          []MCPTool
	resources      []mcpResourceEntry
	prompts        []mcpPromptEntry
	timeout        time.Duration
	hops           []string
	target         *wire.AgentID
	signingKey     *laser.SigningKey
	allowedOrigins []string
}

type mcpResourceEntry struct {
	resource MCPResource
	text     string
}

type mcpPromptEntry struct {
	prompt   MCPPrompt
	messages [][2]string
}

// NewMCPBridge opens a bridge serving MCP over client, publishing tool calls
// as source. Every topic lives on the stream client is scoped to.
// toolTopic/replyTopic may be the same topic, the shared-lane shape.
func NewMCPBridge(
	client *laser.Laser, source wire.AgentID, toolTopic, replyTopic, serverName string,
) *MCPBridge {
	return &MCPBridge{
		laser: client, source: source,
		toolTopic: toolTopic, replyTopic: replyTopic, serverName: serverName,
		timeout: mcpDefaultTimeout,
		hops:    []string{source.String()},
	}
}

// WithTarget addresses every call that names no target to agent, the agent
// this bridge fronts.
func (bridge *MCPBridge) WithTarget(agent wire.AgentID) *MCPBridge {
	derived := *bridge
	derived.target = &agent
	return &derived
}

// WithSigningKey signs every tool call with key, so an agent that verifies
// its input accepts the calls this bridge publishes.
func (bridge *MCPBridge) WithSigningKey(key *laser.SigningKey) *MCPBridge {
	derived := *bridge
	derived.signingKey = key
	return &derived
}

// WithBridgeHops continues the hop path of a call that already crossed other
// bridges: previous is its bridge_hops, and this bridge's id is appended to
// it. A path that already holds this bridge is refused before anything is
// published.
func (bridge *MCPBridge) WithBridgeHops(previous []string) (*MCPBridge, error) {
	hops, err := CallHops(bridge.hops, previous)
	if err != nil {
		return nil, err
	}
	derived := *bridge
	derived.hops = hops
	return &derived, nil
}

// WithAllowedOrigins names the browser origins the HTTP router accepts
// besides same-origin pages on an IP literal or localhost host.
func (bridge *MCPBridge) WithAllowedOrigins(origins []string) *MCPBridge {
	derived := *bridge
	derived.allowedOrigins = origins
	return &derived
}

// WithResource advertises a resource, served from text on resources/read.
func (bridge *MCPBridge) WithResource(
	uri, name, mimeType, text string,
) *MCPBridge {
	derived := *bridge
	derived.resources = append(append([]mcpResourceEntry(nil), bridge.resources...),
		mcpResourceEntry{
			resource: MCPResource{URI: uri, Name: name, MimeType: mimeType},
			text:     text,
		})
	return &derived
}

// WithPrompt advertises a prompt. messages are (role, text) pairs prompts/get
// renders into MCP prompt messages.
func (bridge *MCPBridge) WithPrompt(prompt MCPPrompt, messages [][2]string) *MCPBridge {
	derived := *bridge
	derived.prompts = append(append([]mcpPromptEntry(nil), bridge.prompts...),
		mcpPromptEntry{prompt: prompt, messages: messages})
	return &derived
}

// WithTool advertises a tool in tools/list. inputSchema is the raw JSON
// Schema, which MCP requires to be an object; anything else is refused.
func (bridge *MCPBridge) WithTool(
	name, description string, inputSchema any,
) (*MCPBridge, error) {
	if !isJSONObjectValue(inputSchema) {
		return nil, &BridgeError{Reason: "MCP tool input schema must be a JSON object"}
	}
	return bridge.pushTool(name, description, inputSchema), nil
}

func (bridge *MCPBridge) pushTool(name, description string, inputSchema any) *MCPBridge {
	derived := *bridge
	derived.tools = append(append([]MCPTool(nil), bridge.tools...),
		MCPTool{Name: name, Description: description, InputSchema: inputSchema})
	return &derived
}

// WithMemoryTools advertises the two canonical portable-memory tools,
// remember and recall, so an MCP host sees this bridge as a memory server.
// Both route through CallTool exactly like any other tool.
func (bridge *MCPBridge) WithMemoryTools() *MCPBridge {
	return bridge.pushTool(
		"remember", "Store a memory item for later recall.",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"text": map[string]any{"type": "string", "description": "The content to remember."},
			},
			"required": []string{"text"},
		},
	).pushTool(
		"recall", "Retrieve the memory items most relevant to a query.",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"query": map[string]any{"type": "string", "description": "What to recall."},
				"limit": map[string]any{"type": "integer", "minimum": 1, "description": "Max items to return."},
			},
			"required": []string{"query"},
		},
	)
}

// WithTimeout overrides the per-call reply timeout (default 30s).
func (bridge *MCPBridge) WithTimeout(timeout time.Duration) *MCPBridge {
	derived := *bridge
	derived.timeout = timeout
	return &derived
}

// Initialize answers protocolVersion when this bridge serves it
// (MCPSupportedProtocolVersions), else the latest it serves, and the
// capabilities the bridge actually serves.
func (bridge *MCPBridge) Initialize(protocolVersion string) map[string]any {
	version := mcpDefaultProtocolVersion
	for _, supported := range MCPSupportedProtocolVersions {
		if supported == protocolVersion {
			version = protocolVersion
			break
		}
	}
	capabilities := map[string]any{"tools": map[string]any{}}
	if len(bridge.resources) > 0 {
		capabilities["resources"] = map[string]any{}
	}
	if len(bridge.prompts) > 0 {
		capabilities["prompts"] = map[string]any{}
	}
	return map[string]any{
		"protocolVersion": version,
		"serverInfo":      map[string]any{"name": bridge.serverName, "version": laser.ClientVersion},
		"capabilities":    capabilities,
	}
}

// ListTools answers tools/list: the advertised tools.
func (bridge *MCPBridge) ListTools() map[string]any {
	tools := bridge.tools
	if tools == nil {
		tools = []MCPTool{}
	}
	return map[string]any{"tools": tools}
}

// ListResources answers resources/list: the advertised resources.
func (bridge *MCPBridge) ListResources() map[string]any {
	resources := make([]MCPResource, 0, len(bridge.resources))
	for _, entry := range bridge.resources {
		resources = append(resources, entry.resource)
	}
	return map[string]any{"resources": resources}
}

// ReadResource answers resources/read: the contents of the resource at uri.
func (bridge *MCPBridge) ReadResource(uri string) (map[string]any, error) {
	for _, entry := range bridge.resources {
		if entry.resource.URI == uri {
			return map[string]any{"contents": []mcpResourceContent{{
				URI: entry.resource.URI, MimeType: entry.resource.MimeType, Text: entry.text,
			}}}, nil
		}
	}
	return nil, &BridgeError{Reason: "unknown resource " + uri}
}

// ListPrompts answers prompts/list: the advertised prompts.
func (bridge *MCPBridge) ListPrompts() map[string]any {
	prompts := make([]MCPPrompt, 0, len(bridge.prompts))
	for _, entry := range bridge.prompts {
		prompts = append(prompts, entry.prompt)
	}
	return map[string]any{"prompts": prompts}
}

// GetPrompt answers prompts/get: the rendered messages of the prompt name.
func (bridge *MCPBridge) GetPrompt(name string) (map[string]any, error) {
	for _, entry := range bridge.prompts {
		if entry.prompt.Name == name {
			messages := make([]mcpPromptMessage, 0, len(entry.messages))
			for _, pair := range entry.messages {
				messages = append(messages, mcpPromptMessage{
					Role:    pair[0],
					Content: mcpPromptMessageText{Kind: "text", Text: pair[1]},
				})
			}
			rendered := map[string]any{"messages": messages}
			if entry.prompt.Description != "" {
				rendered["description"] = entry.prompt.Description
			}
			return rendered, nil
		}
	}
	return nil, &BridgeError{Reason: "unknown prompt " + name}
}

// isJSONObjectValue reports a value that renders as a JSON object: a Go map
// literal, as WithMemoryTools builds, or raw JSON bytes a caller already holds.
func isJSONObjectValue(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		return true
	case json.RawMessage:
		return isJSONObject(typed)
	case []byte:
		return isJSONObject(typed)
	default:
		return false
	}
}

// hasTool reports whether name is an advertised tool.
func (bridge *MCPBridge) hasTool(name string) bool {
	for _, tool := range bridge.tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}

// CallTool maps tools/call onto an AGDX command, awaits the correlated
// terminal, and renders the MCP tool result. The command is addressed to the
// WithTarget agent, else to every agent, so on a shared session topic every
// listening agent receives it. Use CallToolTo to address one agent.
func (bridge *MCPBridge) CallTool(
	ctx context.Context, name string, paramsJSON []byte,
) (*MCPToolResult, error) {
	return bridge.callToolAddressed(ctx, bridge.target, bridge.hops, name, paramsJSON)
}

// CallToolTo is CallTool addressed to target, so only that agent runs the
// tool on a shared session topic.
func (bridge *MCPBridge) CallToolTo(
	ctx context.Context, target wire.AgentID, name string, paramsJSON []byte,
) (*MCPToolResult, error) {
	return bridge.callToolAddressed(ctx, &target, bridge.hops, name, paramsJSON)
}

// CallToolWithHops is CallTool for work that already crossed other bridges:
// previous is its bridge_hops, and the call carries that path with this
// bridge appended. A path that already holds this bridge is refused before
// anything is published. target, when set, addresses one agent.
func (bridge *MCPBridge) CallToolWithHops(
	ctx context.Context, previous []string, target *wire.AgentID,
	name string, paramsJSON []byte,
) (*MCPToolResult, error) {
	hops, err := CallHops(bridge.hops, previous)
	if err != nil {
		return nil, err
	}
	if target == nil {
		target = bridge.target
	}
	return bridge.callToolAddressed(ctx, target, hops, name, paramsJSON)
}

func (bridge *MCPBridge) callToolAddressed(
	ctx context.Context, target *wire.AgentID, hops []string,
	name string, paramsJSON []byte,
) (*MCPToolResult, error) {
	envelope, err := bridge.sendToolCall(ctx, target, hops, name, paramsJSON)
	if err != nil {
		return nil, err
	}
	result := ToolResultFromEnvelope(envelope)
	return &result, nil
}

// CallToolIn maps tools/call onto an AGDX command as a child session of
// parent, in the tree rooted at root: the call's submitted start lands on
// agent.sessions with the ancestry before the command, and the child ends by
// the result, completed on a tool result and failed on a tool error. The
// command is addressed to every agent, like CallTool.
func (bridge *MCPBridge) CallToolIn(
	ctx context.Context, parent, root wire.ConversationID,
	name string, paramsJSON []byte,
) (*MCPToolResult, error) {
	return bridge.callToolInAddressed(ctx, bridge.target, parent, root, name, paramsJSON)
}

// CallToolInTo is CallToolIn addressed to target.
func (bridge *MCPBridge) CallToolInTo(
	ctx context.Context, target wire.AgentID, parent, root wire.ConversationID,
	name string, paramsJSON []byte,
) (*MCPToolResult, error) {
	return bridge.callToolInAddressed(ctx, &target, parent, root, name, paramsJSON)
}

func (bridge *MCPBridge) callToolInAddressed(
	ctx context.Context, target *wire.AgentID, parent, root wire.ConversationID,
	name string, paramsJSON []byte,
) (*MCPToolResult, error) {
	conversation := laser.NewConversationID()
	child, err := bridge.laser.Sessions().OpenChild(
		ctx, conversation, bridge.source, bridge.source, parent, root)
	if err != nil {
		return nil, err
	}
	envelope, callErr := bridge.sendToolCallOn(
		ctx, conversation, target, &parent, &root, bridge.hops, name, paramsJSON)
	var endErr error
	switch {
	case callErr != nil:
		_, endErr = child.Fail(ctx, toolFailure(callErr.Error()))
	case envelope.Kind == wire.KindError:
		_, endErr = child.Fail(ctx, toolFailure("the tool returned an error"))
	default:
		_, endErr = child.Complete(ctx, "")
	}
	if callErr != nil {
		return nil, callErr
	}
	if endErr != nil {
		return nil, endErr
	}
	result := ToolResultFromEnvelope(envelope)
	return &result, nil
}

// toolFailure is the structured error a child session ends with when a tool
// call cannot be completed.
func toolFailure(message string) wire.AgentErrorBody {
	return wire.AgentErrorBody{
		Code: wire.ErrorInternal, Message: &message, Retryable: false,
	}
}

// sendToolCall publishes one tool call command and awaits its correlated
// terminal on the reply topic.
func (bridge *MCPBridge) sendToolCall(
	ctx context.Context, target *wire.AgentID, hops []string,
	name string, paramsJSON []byte,
) (*wire.AgentEnvelope, error) {
	return bridge.sendToolCallOn(
		ctx, laser.NewConversationID(), target, nil, nil, hops, name, paramsJSON)
}

// sendToolCallOn is sendToolCall's common path, on an already-minted
// conversation and an optional ancestry stamped onto the command.
func (bridge *MCPBridge) sendToolCallOn(
	ctx context.Context, conversation wire.ConversationID, target *wire.AgentID,
	parent, root *wire.ConversationID, hops []string,
	name string, paramsJSON []byte,
) (*wire.AgentEnvelope, error) {
	correlation := wire.CorrelationID{ID: conversation.ID}
	deadline := time.Now().Add(bridge.timeout)

	producer := bridge.laser.
		AgdxOn(bridge.laser.DefaultStream(), bridge.toolTopic, bridge.source, conversation)
	if parent != nil && root != nil {
		producer = producer.WithAncestry(*parent, *root)
	}
	send := producer.
		Command(correlation, paramsJSON).
		WithTool(name).
		ContentType(wire.ContentJSON).
		WithMetadata(BridgeHopsMetaKey, HopsMetadata(hops))
	if target != nil {
		send = send.WithTarget(*target)
	}
	if bridge.signingKey != nil {
		send = send.Signed(bridge.signingKey)
	}
	if _, err := send.Send(ctx); err != nil {
		return nil, err
	}
	return bridge.awaitReply(ctx, conversation, correlation, deadline)
}
