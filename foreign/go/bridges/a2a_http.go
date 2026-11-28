package bridges

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// A2ACardPath is where the A2A discovery document is served.
const A2ACardPath = "/.well-known/agent-card.json"

// maxRequestBytes bounds a request body, as the Rust and TypeScript shells
// bound theirs. A bridge reads the whole body before it can dispatch, so the
// bound is what stops one caller from holding memory the others need.
const maxRequestBytes = 2 << 20

// WithAllowedOrigins names the browser origins the JSON-RPC endpoint serves.
//
// A request with no Origin header is not from a browser and passes. A
// same-origin page passes only on an IP-literal or localhost host, because a
// rebound page sends its own name as both origin and host, so a same-origin
// rule on names would wave it through. The card path is public discovery and
// is not guarded.
func (bridge *A2ABridge) WithAllowedOrigins(origins []string) *A2ABridge {
	derived := *bridge
	derived.allowedOrigins = origins
	return &derived
}

// WithCallerFrom names the authenticated caller of each request from the
// request itself, for a bridge that binds task ids to callers.
//
// The hook is the host's own authentication: returning an error refuses the
// request with 401 rather than serving it unauthenticated, because a bound
// bridge that fell back to no caller would mint ids nobody could open.
func (bridge *A2ABridge) WithCallerFrom(
	identify func(*http.Request) (string, error),
) *A2ABridge {
	derived := *bridge
	derived.callerFrom = identify
	return &derived
}

// Handler serves the bridge over HTTP: the JSON-RPC endpoint at POST / and
// the Agent Card at the A2A well-known path.
//
// It is a shell over HandleRPC and Card: every protocol decision is made
// there, so serving the bridge another way answers identically.
func (bridge *A2ABridge) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(A2ACardPath, bridge.serveCard)
	mux.HandleFunc("/", bridge.serveRPC)
	return mux
}

// serveCard answers the public discovery document.
func (bridge *A2ABridge) serveCard(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var card *AgentCard
	var err error
	if bridge.signingKey == nil {
		card, err = bridge.Card()
	} else {
		card, err = bridge.SignedCard(bridge.signingKey)
	}
	if err != nil {
		// A bridge with no public URL has no card to serve, which is an
		// absence rather than a failure: discovery found nothing here.
		http.NotFound(writer, request)
		return
	}
	writeJSON(writer, request, http.StatusOK, card)
}

// serveRPC answers one JSON-RPC call.
func (bridge *A2ABridge) serveRPC(writer http.ResponseWriter, request *http.Request) {
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
	caller := ""
	if bridge.callerFrom != nil {
		named, err := bridge.callerFrom(request)
		if err != nil {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		caller = named
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
	writeJSON(writer, request, http.StatusOK,
		bridge.handleRPCFor(request.Context(), body, caller))
}

// jsonContentType reports a content type the JSON-RPC endpoint accepts,
// ignoring the parameters a charset adds.
func jsonContentType(header string) bool {
	media, _, _ := strings.Cut(header, ";")
	return strings.EqualFold(strings.TrimSpace(media), "application/json")
}

// writeJSON answers with a JSON body, writing none for a HEAD.
func writeJSON(
	writer http.ResponseWriter, request *http.Request, status int, body any,
) {
	encoded, err := json.Marshal(body)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if request.Method == http.MethodHead {
		return
	}
	_, _ = writer.Write(encoded)
}
