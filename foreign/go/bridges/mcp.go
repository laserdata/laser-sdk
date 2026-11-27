package bridges

import "github.com/laserdata/laser-sdk/foreign/go/wire"

// MCPMethod is a JSON-RPC method the MCP edge serves.
type MCPMethod string

const (
	MCPInitialize    MCPMethod = "initialize"
	MCPPing          MCPMethod = "ping"
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

// MCPTool is a tool the edge advertises, matching the MCP Tool schema.
type MCPTool struct {
	Name        string `json:"name"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	// InputSchema is the raw JSON Schema MCP requires, an object even for a
	// no-arg tool (`{"type":"object","additionalProperties":false}`).
	InputSchema any `json:"inputSchema"`
}

// MCPResource is a resource the bridge advertises in resources/list, served
// from its held text on resources/read.
type MCPResource struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
}

// MCPPromptArgument is one argument of an MCPPrompt.
type MCPPromptArgument struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

// MCPPrompt is a prompt the bridge advertises in prompts/list.
type MCPPrompt struct {
	Name        string              `json:"name"`
	Title       string              `json:"title,omitempty"`
	Description string              `json:"description,omitempty"`
	Arguments   []MCPPromptArgument `json:"arguments,omitempty"`
}

// mcpPromptMessage is one rendered message of a prompts/get answer.
type mcpPromptMessage struct {
	Role    string               `json:"role"`
	Content mcpPromptMessageText `json:"content"`
}

type mcpPromptMessageText struct {
	Kind string `json:"type"`
	Text string `json:"text"`
}

// mcpResourceContent is one entry of a resources/read answer.
type mcpResourceContent struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType,omitempty"`
	Text     string `json:"text"`
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
