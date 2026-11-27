package bridges

import (
	"encoding/json"
	"io"
	"net/http"
)

// mcpProtocolVersionHeader is the Streamable HTTP header naming the client's
// MCP protocol version. Go header lookups are case-insensitive, so the exact
// casing here is cosmetic.
const mcpProtocolVersionHeader = "Mcp-Protocol-Version"

// Handler serves the bridge over HTTP: the MCP JSON-RPC endpoint at POST /.
//
// It is a shell over HandleRPC: every protocol decision is made there, so
// serving the bridge another way answers identically.
func (bridge *MCPBridge) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", bridge.serveRPC)
	return mux
}

func (bridge *MCPBridge) serveRPC(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", "POST")
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !OriginAllowed(request, bridge.allowedOrigins) {
		writer.WriteHeader(http.StatusForbidden)
		return
	}
	if version := request.Header.Get(mcpProtocolVersionHeader); version != "" &&
		!supportedMCPVersion(version) {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if !jsonContentType(request.Header.Get("Content-Type")) {
		writer.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, maxRequestBytes))
	if err != nil {
		writer.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	if !json.Valid(body) {
		// A body that is not JSON never reaches the dispatch, which answers
		// in JSON-RPC and would otherwise have to invent a request to answer.
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	answer, answered := bridge.HandleRPC(request.Context(), body)
	if !answered {
		// A notification is accepted but carries no body: nobody is waiting
		// on an id it never sent.
		writer.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(writer, request, http.StatusOK, answer)
}

func supportedMCPVersion(version string) bool {
	for _, supported := range MCPSupportedProtocolVersions {
		if supported == version {
			return true
		}
	}
	return false
}
