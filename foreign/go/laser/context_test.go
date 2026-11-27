package laser_test

import (
	"context"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// contextRecord builds a record in one conversation at one position.
func contextRecord(
	t *testing.T, session wire.ConversationID, timestamp, offset uint64, body string,
) *laser.Record {
	t.Helper()
	message, err := iggcon.NewIggyMessage([]byte(body))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	message.Header.Timestamp, message.Header.Offset = timestamp, offset
	return &laser.Record{
		Message:    message,
		Provenance: &laser.Provenance{Conversation: session},
	}
}

// agentRecord builds a record carrying an agent envelope, so the assembler
// sees an author and an envelope body.
func agentRecord(
	t *testing.T, session wire.ConversationID, author string, timestamp uint64, body string,
) *laser.Record {
	t.Helper()
	server := &streamServer{}
	agdx := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Agdx(agentID(t, author), session)
	if _, err := agdx.Emit([]byte(body)).Send(context.Background()); err != nil {
		t.Fatalf("send: %v", err)
	}
	record, err := laser.OpenRecord(server.sentMessages[0])
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	record.Message.Header.Timestamp = timestamp
	return record
}

func bodies(messages []laser.ContextMessage) []string {
	var out []string
	for _, message := range messages {
		out = append(out, string(message.Body()))
	}
	return out
}

// A topic carries many conversations, so reading one and keeping what belongs
// is the normal path rather than an error.
func TestGivenRecordsFromAnotherConversationWhenAssembledThenShouldLeaveThemOut(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		contextRecord(t, conversation(1), 10, 0, "mine"),
		contextRecord(t, conversation(2), 20, 1, "theirs"),
		contextRecord(t, conversation(1), 30, 2, "mine too"),
	)

	got := bodies(assembler.Messages())
	if len(got) != 2 {
		t.Fatalf("want only this conversation, got %v", got)
	}
	if got[0] != "mine" || got[1] != "mine too" {
		t.Errorf("want this conversation's records, got %v", got)
	}
}

// Ordering is by the server's own clock, which is the one every topic shares.
func TestGivenRecordsAcrossTopicsWhenAssembledThenShouldOrderByAppendTime(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).
		Add("agent.memory", contextRecord(t, conversation(1), 30, 0, "third")).
		Add("agent.sessions",
			contextRecord(t, conversation(1), 10, 0, "first"),
			contextRecord(t, conversation(1), 20, 1, "second"))

	got := bodies(assembler.Messages())
	want := []string{"first", "second", "third"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("want %v, got %v", want, got)
	}
}

// Two records can share a timestamp across topics, and a prompt that reorders
// between runs is not reproducible.
func TestGivenRecordsSharingATimestampWhenAssembledThenShouldOrderDeterministically(t *testing.T) {
	build := func() []laser.ContextMessage {
		return laser.Assemble(conversation(1)).
			Add("b.topic", contextRecord(t, conversation(1), 10, 0, "from b")).
			Add("a.topic", contextRecord(t, conversation(1), 10, 0, "from a")).
			Messages()
	}

	first := bodies(build())
	for range 20 {
		if strings.Join(bodies(build()), ",") != strings.Join(first, ",") {
			t.Fatalf("assembly is not reproducible: %v then %v", first, bodies(build()))
		}
	}
	if first[0] != "from a" {
		t.Errorf("want the tie broken on something stable, got %v", first)
	}
}

// A prompt wants what just happened, so the recent tail survives.
func TestGivenMoreHistoryThanWantedWhenCutThenShouldKeepTheRecentTail(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		contextRecord(t, conversation(1), 10, 0, "oldest"),
		contextRecord(t, conversation(1), 20, 1, "middle"),
		contextRecord(t, conversation(1), 30, 2, "newest"),
	).WithPolicy(laser.LastN(2))

	got := bodies(assembler.Messages())
	if len(got) != 2 || got[0] != "middle" || got[1] != "newest" {
		t.Errorf("want the recent tail, got %v", got)
	}
}

func TestGivenLessHistoryThanWantedWhenCutThenShouldKeepItAll(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		contextRecord(t, conversation(1), 10, 0, "only"),
	).WithPolicy(laser.LastN(5))

	if got := bodies(assembler.Messages()); len(got) != 1 {
		t.Errorf("want the whole history, got %v", got)
	}
}

// Filtering by author must not keep a record that names none, or the filter
// does not actually filter.
func TestGivenARoleFilterWhenAppliedThenShouldKeepOnlyNamedAuthors(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		agentRecord(t, conversation(1), "planner", 10, "from the planner"),
		agentRecord(t, conversation(1), "executor", 20, "from the executor"),
		contextRecord(t, conversation(1), 30, 0, "from nobody"),
	).WithPolicy(laser.RoleFilter{Agents: []wire.AgentID{agentID(t, "planner")}})

	got := bodies(assembler.Messages())
	if len(got) != 1 || got[0] != "from the planner" {
		t.Errorf("want only the planner's records, got %v", got)
	}
}

// A chain lets a caller pipeline policies rather than choose exactly one, and
// each step narrows what the last kept.
func TestGivenAChainWhenAppliedThenShouldNarrowInOrder(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		agentRecord(t, conversation(1), "planner", 10, "planner one"),
		agentRecord(t, conversation(1), "executor", 20, "executor one"),
		agentRecord(t, conversation(1), "planner", 30, "planner two"),
		agentRecord(t, conversation(1), "planner", 40, "planner three"),
	).WithPolicy(laser.Chain{
		laser.RoleFilter{Agents: []wire.AgentID{agentID(t, "planner")}},
		laser.LastN(2),
	})

	got := bodies(assembler.Messages())
	if len(got) != 2 || got[0] != "planner two" || got[1] != "planner three" {
		t.Errorf("want the planner's recent two, got %v", got)
	}
	if !strings.Contains(assembler.Messages()[0].Topic, "agent.sessions") {
		t.Error("want the topic carried through")
	}
}

// A budget keeps the most recent that fit, because a prompt wants what just
// happened rather than how the conversation opened.
func TestGivenATokenBudgetWhenAppliedThenShouldKeepTheRecentTailThatFits(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		contextRecord(t, conversation(1), 10, 0, strings.Repeat("a", 400)),
		contextRecord(t, conversation(1), 20, 1, strings.Repeat("b", 40)),
		contextRecord(t, conversation(1), 30, 2, strings.Repeat("c", 40)),
	).WithPolicy(laser.TokenBudget{MaxTokens: 25})

	got := bodies(assembler.Messages())
	if len(got) != 2 {
		t.Fatalf("want the two that fit, got %d", len(got))
	}
	if !strings.HasPrefix(got[0], "b") || !strings.HasPrefix(got[1], "c") {
		t.Errorf("want the recent tail in order, got %v", summarise(got))
	}
}

// A single message larger than the whole budget must reach the model to be
// truncated there, rather than vanishing before it gets there.
func TestGivenOneOversizedMessageWhenBudgetedThenShouldStillKeepIt(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		contextRecord(t, conversation(1), 10, 0, strings.Repeat("a", 4000)),
	).WithPolicy(laser.TokenBudget{MaxTokens: 5})

	if got := assembler.Messages(); len(got) != 1 {
		t.Errorf("want the message kept, got %d", len(got))
	}
}

// A caller with a real tokenizer must be able to spend its own count rather
// than the estimate.
func TestGivenACallerEstimatorWhenBudgetedThenShouldUseIt(t *testing.T) {
	called := 0
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		contextRecord(t, conversation(1), 10, 0, "short"),
		contextRecord(t, conversation(1), 20, 1, "also short"),
	).WithPolicy(laser.TokenBudget{
		MaxTokens: 10,
		Estimate: func(laser.ContextMessage) int {
			called++
			return 9
		},
	})

	got := assembler.Messages()
	if called == 0 {
		t.Error("want the caller's estimator used")
	}
	// Nine each against a budget of ten leaves room for one.
	if len(got) != 1 {
		t.Errorf("want the caller's count respected, got %d", len(got))
	}
}

// A record that is not an agent message is still context: a plain data topic
// carries plenty worth putting in a prompt.
func TestGivenAPlainRecordWhenAssembledThenShouldStillBeContext(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("telemetry",
		contextRecord(t, conversation(1), 10, 0, "a reading"))

	messages := assembler.Messages()
	if len(messages) != 1 {
		t.Fatalf("want the record kept, got %d", len(messages))
	}
	if messages[0].Envelope != nil {
		t.Error("a plain record carries no envelope")
	}
	if string(messages[0].Body()) != "a reading" {
		t.Errorf("want the raw payload as the body, got %q", messages[0].Body())
	}
}

// An agent message's body is the envelope's, not the whole encoded envelope,
// since that is the text a prompt carries.
func TestGivenAnAgentRecordWhenAssembledThenShouldUseTheEnvelopeBody(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		agentRecord(t, conversation(1), "planner", 10, "the actual text"))

	messages := assembler.Messages()
	if len(messages) != 1 || messages[0].Envelope == nil {
		t.Fatalf("want the envelope decoded, got %+v", messages)
	}
	if string(messages[0].Body()) != "the actual text" {
		t.Errorf("want the envelope body, got %q", messages[0].Body())
	}
	if messages[0].Agent() == nil || *messages[0].Agent() != agentID(t, "planner") {
		t.Errorf("want the author, got %v", messages[0].Agent())
	}
}

// Without a policy everything read belongs.
func TestGivenNoPolicyWhenAssembledThenShouldKeepEverything(t *testing.T) {
	assembler := laser.Assemble(conversation(1)).Add("agent.sessions",
		contextRecord(t, conversation(1), 10, 0, "one"),
		contextRecord(t, conversation(1), 20, 1, "two"))

	if got := assembler.Messages(); len(got) != 2 {
		t.Errorf("want everything, got %d", len(got))
	}
}

// A policy's name is what a trace shows, so a chain names what it ran.
func TestGivenAChainWhenNamedThenShouldNameItsSteps(t *testing.T) {
	chain := laser.Chain{laser.LastN(3), laser.TokenBudget{MaxTokens: 100}}

	name := chain.Name()
	if !strings.Contains(name, "last_n(3)") || !strings.Contains(name, "token_budget(100)") {
		t.Errorf("want both steps named, got %q", name)
	}
}

func summarise(bodies []string) []string {
	var out []string
	for _, body := range bodies {
		if len(body) > 8 {
			body = body[:8] + "..."
		}
		out = append(out, body)
	}
	return out
}
