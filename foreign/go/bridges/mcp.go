package bridges

import "github.com/laserdata/laser-sdk/foreign/go/wire"

// MCPMethod is a JSON-RPC method the MCP edge serves.
type MCPMethod string

const (
	MCPToolsList     MCPMethod = "tools/list"
	MCPToolsCall     MCPMethod = "tools/call"
	MCPResourcesList MCPMethod = "resources/list"
	MCPResourcesRead MCPMethod = "resources/read"
	MCPPromptsList   MCPMethod = "prompts/list"
	MCPPromptsGet    MCPMethod = "prompts/get"
)

// MCPContent is one piece of a tool result.
type MCPContent struct {
	Kind string `json:"type"`
	Text string `json:"text"`
}

// MCPToolResult is what a tool call answers with.
type MCPToolResult struct {
	Content []MCPContent `json:"content"`
	// IsError carries no omitempty, so a successful result says so explicitly
	// rather than leaving a reader to infer it from an absent field.
	IsError bool `json:"isError"`
}

// MCPTool is a tool the edge advertises.
type MCPTool struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	InputSchema any    `json:"inputSchema,omitempty"`
}

// ToolResultFromEnvelope renders an AGDX answer back as an MCP tool result.
//
// A failure is marked as one rather than returned as ordinary content, because
// an edge that cannot tell them apart will show an error to a user as though
// it were an answer.
func ToolResultFromEnvelope(envelope *wire.AgentEnvelope) MCPToolResult {
	result := MCPToolResult{IsError: envelope.Kind == wire.KindError}
	if len(envelope.Body) == 0 {
		// An empty body is no content rather than one empty block: a block
		// holding nothing renders as a blank message.
		return result
	}
	result.Content = []MCPContent{{Kind: "text", Text: string(envelope.Body)}}
	return result
}
