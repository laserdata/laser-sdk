package examples

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	anthropicEndpoint     = "https://api.anthropic.com/v1/messages"
	anthropicAPIVersion   = "2023-06-01"
	anthropicDefaultModel = "claude-sonnet-4-6"
	anthropicMaxTokens    = 1024
	openAIEndpoint        = "https://api.openai.com/v1/chat/completions"
	openAIDefaultModel    = "gpt-4o"
	llmRequestTimeout     = 30 * time.Second
)

// LLM is the model seam the examples own. The SDK itself never invokes a model.
type LLM interface {
	Complete(ctx context.Context, prompt string) (string, error)
}

// DefaultLLM selects an explicitly configured provider or the deterministic mock.
func DefaultLLM() LLM {
	if key := strings.TrimSpace(os.Getenv("ANTHROPIC_API_KEY")); key != "" {
		return anthropicLLM{key: func() string { return key }, model: envOr("ANTHROPIC_MODEL", anthropicDefaultModel)}
	}
	if key := strings.TrimSpace(os.Getenv("OPENAI_API_KEY")); key != "" {
		return openAILLM{key: func() string { return key }, model: envOr("OPENAI_MODEL", openAIDefaultModel)}
	}
	return MockLLM{}
}

// MockLLM answers with its prompt, so a run needs no model and prints the same text every time.
type MockLLM struct{}

// Complete echoes the prompt behind a marker.
func (MockLLM) Complete(ctx context.Context, prompt string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return "[mock-llm] " + prompt, nil
}

// anthropicLLM holds its key behind a function, so printing the client never shows it.
type anthropicLLM struct {
	key   func() string
	model string
}

func (llm anthropicLLM) Complete(ctx context.Context, prompt string) (string, error) {
	request := map[string]any{
		"model": llm.model, "max_tokens": anthropicMaxTokens,
		"messages": []map[string]string{{"role": "user", "content": prompt}},
	}
	var reply struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	headers := map[string]string{"x-api-key": llm.key(), "anthropic-version": anthropicAPIVersion}
	if failure := postJSON(ctx, anthropicEndpoint, headers, request, &reply); failure != "" {
		return "[anthropic-request-error] " + failure, nil
	}
	var text strings.Builder
	for _, block := range reply.Content {
		text.WriteString(block.Text)
	}
	return text.String(), nil
}

// openAILLM holds its key behind a function, so printing the client never shows it.
type openAILLM struct {
	key   func() string
	model string
}

func (llm openAILLM) Complete(ctx context.Context, prompt string) (string, error) {
	request := map[string]any{
		"model":    llm.model,
		"messages": []map[string]string{{"role": "user", "content": prompt}},
	}
	var reply struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	headers := map[string]string{"authorization": "Bearer " + llm.key()}
	if failure := postJSON(ctx, openAIEndpoint, headers, request, &reply); failure != "" {
		return "[openai-request-error] " + failure, nil
	}
	if len(reply.Choices) == 0 {
		return "", nil
	}
	return reply.Choices[0].Message.Content, nil
}

// postJSON returns why the call failed as text, because a provider outage is an answer the example prints.
func postJSON(ctx context.Context, endpoint string, headers map[string]string, body, into any) string {
	ctx, cancel := context.WithTimeout(ctx, llmRequestTimeout)
	defer cancel()
	encoded, err := json.Marshal(body)
	if err != nil {
		return err.Error()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(encoded))
	if err != nil {
		return err.Error()
	}
	request.Header.Set("content-type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err.Error()
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Sprintf("HTTP %d", response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(into); err != nil {
		return "unexpected response shape"
	}
	return ""
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
