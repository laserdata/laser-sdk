package bridges_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// call runs one request against a bridge's handler.
func call(
	t *testing.T, bridge *bridges.A2ABridge, request *http.Request,
) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	bridge.Handler().ServeHTTP(recorder, request)
	return recorder
}

func rpcRequest(body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestGivenAJSONRPCPostWhenServedThenShouldAnswerTheDispatch(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	answer := call(t, bridge, rpcRequest(
		`{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{}}}`))

	if answer.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", answer.Code)
	}
	var decoded bridges.JSONRPCResponse
	if err := json.Unmarshal(answer.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode the answer: %v", err)
	}
	if decoded.Error != nil {
		t.Fatalf("want a result, got %+v", decoded.Error)
	}
}

func TestGivenANonJSONContentTypeWhenPostedThenShouldRefuseTheMediaType(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	request.Header.Set("Content-Type", "text/plain")

	if answer := call(t, bridge, request); answer.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("want 415, got %d", answer.Code)
	}
}

func TestGivenACharsetOnTheContentTypeWhenPostedThenShouldStillServeIt(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})
	request := httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{}}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")

	// A charset is a parameter of the media type, not a different one.
	if answer := call(t, bridge, request); answer.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", answer.Code)
	}
}

func TestGivenABodyThatIsNotJSONWhenPostedThenShouldRefuseItBeforeDispatch(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	answer := call(t, bridge, rpcRequest("not json at all"))

	// The dispatch answers in JSON-RPC, and a body it cannot read names no
	// request to answer about.
	if answer.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", answer.Code)
	}
}

func TestGivenAGetOnTheRPCEndpointWhenServedThenShouldSayWhatIsAllowed(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	answer := call(t, bridge, httptest.NewRequest(http.MethodGet, "/", nil))

	if answer.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", answer.Code)
	}
	if allow := answer.Header().Get("Allow"); allow != "POST" {
		t.Errorf("want POST allowed, got %q", allow)
	}
}

func TestGivenAnUnknownPathWhenServedThenShouldNotFindIt(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	answer := call(t, bridge, httptest.NewRequest(http.MethodGet, "/tasks", nil))

	if answer.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", answer.Code)
	}
}

func TestGivenABrowserOriginOutsideTheListWhenPostingThenShouldRefuseIt(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{}).
		WithAllowedOrigins([]string{"https://console.example"})
	request := rpcRequest(`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{}}`)
	request.Header.Set("Origin", "https://evil.example")

	if answer := call(t, bridge, request); answer.Code != http.StatusForbidden {
		t.Fatalf("want 403, got %d", answer.Code)
	}
}

func TestGivenAnAllowedOriginWhenPostingThenShouldServeIt(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{}).
		WithAllowedOrigins([]string{"https://console.example"})
	request := rpcRequest(`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{}}`)
	request.Header.Set("Origin", "https://console.example")

	if answer := call(t, bridge, request); answer.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", answer.Code)
	}
}

func TestGivenNoPublicURLWhenTheCardIsFetchedThenShouldNotFindIt(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{})

	answer := call(t, bridge,
		httptest.NewRequest(http.MethodGet, bridges.A2ACardPath, nil))

	// Discovery found nothing here, which is an absence rather than a
	// failure of the bridge.
	if answer.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", answer.Code)
	}
}

func TestGivenAPublicURLWhenTheCardIsFetchedThenShouldServeItToAnyOrigin(t *testing.T) {
	bridge, err := bridgeOver(t, &bridgeLog{}).WithPublicURL("https://edge.example/a2a")
	if err != nil {
		t.Fatalf("set the public URL: %v", err)
	}
	bridge = bridge.WithAllowedOrigins([]string{"https://console.example"})
	request := httptest.NewRequest(http.MethodGet, bridges.A2ACardPath, nil)
	request.Header.Set("Origin", "https://anywhere.example")

	answer := call(t, bridge, request)

	// The card is public discovery, so the origin guard does not apply: a
	// client has to read it before it can be an allowed origin.
	if answer.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", answer.Code)
	}
	var card bridges.AgentCard
	if err := json.Unmarshal(answer.Body.Bytes(), &card); err != nil {
		t.Fatalf("decode the card: %v", err)
	}
	if card.Name != "edge" {
		t.Errorf("want the bridge's name, got %q", card.Name)
	}
}

func TestGivenACallerHookThatRefusesWhenPostingThenShouldAnswerUnauthorized(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{}).WithCallerFrom(
		func(*http.Request) (string, error) { return "", errors.New("no token") })

	answer := call(t, bridge, rpcRequest(
		`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{}}`))

	// A bound bridge that fell back to no caller would mint ids nobody
	// could open, so a refused caller refuses the request.
	if answer.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", answer.Code)
	}
}

func TestGivenABoundBridgeWhenTwoCallersUseOneTaskThenShouldHideItFromTheOther(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log).
		WithTaskBinding([32]byte{1: 7}, time.Hour)

	owner := bridge.Caller("caller-a")
	task, err := owner.Submit(context.Background(), []byte(sendParams))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Its owner reads it.
	if _, err := owner.Task(context.Background(), task.ID); err != nil {
		t.Fatalf("the owner must read its own task: %v", err)
	}
	// Anyone else sees an id that names nothing, rather than a task they
	// can read or cancel because they learned its conversation.
	if _, err := bridge.Caller("caller-b").Task(context.Background(), task.ID); err == nil {
		t.Fatal("a bound task must not open for another caller")
	}
	if _, err := bridge.Caller("caller-b").Cancel(context.Background(), task.ID); err == nil {
		t.Fatal("a bound task must not cancel for another caller")
	}
}

func TestGivenABoundBridgeWhenCalledWithoutACallerThenShouldRefuseRatherThanMintUnbound(t *testing.T) {
	bridge := bridgeOver(t, &bridgeLog{}).WithTaskBinding([32]byte{1: 7}, time.Hour)

	_, err := bridge.Submit(context.Background(), []byte(sendParams))

	if err == nil {
		t.Fatal("a bound bridge must not mint an unbound id")
	}
}

func TestGivenAnExpiredBindingWhenOpenedThenShouldReadAsNoTask(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log).WithTaskBinding([32]byte{1: 7}, -time.Second)
	caller := bridge.Caller("caller-a")
	task, err := caller.Submit(context.Background(), []byte(sendParams))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if _, err := caller.Task(context.Background(), task.ID); err == nil {
		t.Fatal("an expired id must not open")
	}
}

func TestGivenASigningKeyWhenFetchingTheCardThenShouldVerifyThePublishedJSON(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := bridgeOver(t, &bridgeLog{}).WithSigningKey(key).WithPublicURL("https://agents.example/")
	if err != nil {
		t.Fatal(err)
	}
	response := call(t, bridge, httptest.NewRequest(http.MethodGet, bridges.A2ACardPath, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected status %d", response.Code)
	}
	var card bridges.AgentCard
	if err := json.Unmarshal(response.Body.Bytes(), &card); err != nil {
		t.Fatal(err)
	}
	if len(card.Signatures) != 1 {
		t.Fatalf("expected one signature, got %d", len(card.Signatures))
	}
	if err := laser.VerifyCard(response.Body.Bytes(), card.Signatures[0], key.VerifyingKey()); err != nil {
		t.Fatal(err)
	}
}
