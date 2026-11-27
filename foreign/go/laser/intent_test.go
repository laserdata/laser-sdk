package laser_test

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The vectors below were printed by the Rust client, which owns the contract.
// A Go record that does not reproduce them byte for byte is a record the Rust
// and TypeScript clients would read differently, so they are pinned here
// rather than recomputed from this port's own encoder.
const (
	rustDigestOfNothing  = "64a1737bf6f1c4e48d491bf5e6a6cae65ac87ee51db8d7276eb07b48216fbbc5"
	rustDigestOfTransfer = "93daa878cad59dae1a8b72cba78631d5e752940cd3225723b8e5e1b00f51726a"

	rustIntentCBOR = "ab69696e74656e745f6964781a303141525a334e44454b545356345252464651363947354641566c636f" +
		"6e766572736174696f6e781a30314258355a5a4b424b41435441563957455647454d4d56525a6870726f" +
		"706f73657267706c616e6e657264626f64798d187418721861186e187318661865187218201824183118" +
		"301830666469676573747840393364616138373863616435396461653161386237326362613738363331" +
		"643565373532393430636433323235373233623865356531623030663531373236616f656c696769626c" +
		"655f766f746572738265616c69636563626f62706d616e6461746f72795f766f746572738165616c6963" +
		"6566706f6c696379a16861745f6c65617374026e706f6c6963795f76657273696f6e0772646561646c69" +
		"6e655f61745f6d6963726f731907d06961745f6d6963726f731903e8"

	rustIntentJSON = `{"intent_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
		`"conversation":"01BX5ZZKBKACTAV9WEVGEMMVRZ","proposer":"planner",` +
		`"body":[116,114,97,110,115,102,101,114,32,36,49,48,48],` +
		`"digest":"93daa878cad59dae1a8b72cba78631d5e752940cd3225723b8e5e1b00f51726a",` +
		`"eligible_voters":["alice","bob"],"mandatory_voters":["alice"],` +
		`"policy":{"at_least":2},"policy_version":7,"deadline_at_micros":2000,` +
		`"at_micros":1000}`

	rustVoteCBOR = "a669696e74656e745f6964781a303141525a334e44454b54535634525246465136394735464156" +
		"6d696e74656e745f64696765737478403933646161383738636164353964616531613862373263626137" +
		"38363331643565373532393430636433323235373233623865356531623030663531373236616e706f6c" +
		"6963795f76657273696f6e0765766f74657265616c6963656663686f69636565616c6c6f776961745f6d" +
		"6963726f731905dc"

	rustVoteJSON = `{"intent_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
		`"intent_digest":"93daa878cad59dae1a8b72cba78631d5e752940cd3225723b8e5e1b00f51726a",` +
		`"policy_version":7,"voter":"alice","choice":"allow","at_micros":1500}`

	rustDecisionCBOR = "a769696e74656e745f6964781a303141525a334e44454b545356345252464651363947354641566d696e" +
		"74656e745f64696765737478403933646161383738636164353964616531613862373263626137383633" +
		"31643565373532393430636433323235373233623865356531623030663531373236616e706f6c696379" +
		"5f76657273696f6e07676f7574636f6d6569636f6d6d697474656466726561736f6e6a71756f72756d20" +
		"6d657470766f7465735f636f6e73696465726564828265616c69636565616c6c6f778263626f6265616c" +
		"6c6f776961745f6d6963726f7319076c"

	rustDecisionJSON = `{"intent_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
		`"intent_digest":"93daa878cad59dae1a8b72cba78631d5e752940cd3225723b8e5e1b00f51726a",` +
		`"policy_version":7,"outcome":"committed","reason":"quorum met",` +
		`"votes_considered":[["alice","allow"],["bob","allow"]],"at_micros":1900}`
)

func agent(t *testing.T, name string) wire.AgentID {
	t.Helper()
	id, err := wire.NewAgentID(name)
	if err != nil {
		t.Fatalf("agent id %q: %v", name, err)
	}
	return id
}

func agents(t *testing.T, names ...string) []wire.AgentID {
	t.Helper()
	ids := make([]wire.AgentID, len(names))
	for index, name := range names {
		ids[index] = agent(t, name)
	}
	return ids
}

func intentConversation(t *testing.T, text string) wire.ConversationID {
	t.Helper()
	id, err := wire.ParseID(text)
	if err != nil {
		t.Fatalf("conversation %q: %v", text, err)
	}
	return wire.ConversationID{ID: id}
}

func intentID(t *testing.T, text string) laser.IntentID {
	t.Helper()
	id, err := laser.ParseIntentID(text)
	if err != nil {
		t.Fatalf("intent id %q: %v", text, err)
	}
	return id
}

// pinnedIntent is the exact intent the Rust vectors above were printed from.
func pinnedIntent(t *testing.T) laser.Intent {
	t.Helper()
	return laser.Intent{
		IntentID:         intentID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV"),
		Conversation:     intentConversation(t, "01BX5ZZKBKACTAV9WEVGEMMVRZ"),
		Proposer:         agent(t, "planner"),
		Body:             []byte("transfer $100"),
		Digest:           rustDigestOfTransfer,
		EligibleVoters:   agents(t, "alice", "bob"),
		MandatoryVoters:  agents(t, "alice"),
		Policy:           laser.IntentAtLeast(2),
		PolicyVersion:    7,
		DeadlineAtMicros: 2_000,
		AtMicros:         1_000,
	}
}

// builtIntent is an intent built the way an application builds one, with a
// window wide enough that the fold is never past its deadline.
func builtIntent(t *testing.T, policy laser.IntentPolicy, eligible, mandatory []string) laser.Intent {
	t.Helper()
	intent, err := laser.NewIntent(laser.IntentOptions{
		Conversation:     laser.NewConversationID(),
		Proposer:         agent(t, "planner"),
		Body:             []byte("transfer $100"),
		EligibleVoters:   agents(t, eligible...),
		MandatoryVoters:  agents(t, mandatory...),
		Policy:           policy,
		PolicyVersion:    3,
		DeadlineAtMicros: ^uint64(0),
	})
	if err != nil {
		t.Fatalf("build an intent: %v", err)
	}
	return intent
}

func observed(t *testing.T, intent laser.Intent, voter string, choice laser.VoteChoice, at uint64) laser.ObservedVote {
	t.Helper()
	vote, err := intent.CastVote(agent(t, voter), choice)
	if err != nil {
		t.Fatalf("cast a vote: %v", err)
	}
	return laser.ObservedVote{Vote: vote, ObservedAtMicros: at}
}

func TestGivenABodyWhenDigestedThenShouldMatchTheRustHash(t *testing.T) {
	intent := pinnedIntent(t)
	if err := intent.Validate(); err != nil {
		t.Fatalf("the pinned intent must validate: %v", err)
	}
	empty := intent
	empty.Body = nil
	empty.Digest = rustDigestOfNothing
	if err := empty.Validate(); err != nil {
		t.Fatalf("an empty body hashes to the Rust vector: %v", err)
	}
}

func TestGivenTheRustIntentWhenEncodedThenShouldProduceTheSameBytes(t *testing.T) {
	encoded, err := pinnedIntent(t).Encode()
	if err != nil {
		t.Fatalf("encode the intent: %v", err)
	}
	if got := hex.EncodeToString(encoded); got != rustIntentCBOR {
		t.Fatalf("intent encoding drifted from Rust\n got %s\nwant %s", got, rustIntentCBOR)
	}
}

func TestGivenTheRustIntentBytesWhenDecodedThenShouldRebuildIt(t *testing.T) {
	payload, err := hex.DecodeString(rustIntentCBOR)
	if err != nil {
		t.Fatalf("the vector must be hex: %v", err)
	}
	decoded, err := laser.DecodeIntent(payload)
	if err != nil {
		t.Fatalf("decode the Rust intent: %v", err)
	}
	want := pinnedIntent(t)
	if decoded.IntentID != want.IntentID || decoded.Conversation != want.Conversation ||
		decoded.Proposer != want.Proposer || decoded.Digest != want.Digest ||
		string(decoded.Body) != string(want.Body) || decoded.Policy != want.Policy ||
		decoded.PolicyVersion != want.PolicyVersion ||
		decoded.DeadlineAtMicros != want.DeadlineAtMicros || decoded.AtMicros != want.AtMicros {
		t.Fatalf("decoded %+v, want %+v", decoded, want)
	}
	if len(decoded.EligibleVoters) != 2 || decoded.EligibleVoters[0] != want.EligibleVoters[0] ||
		decoded.EligibleVoters[1] != want.EligibleVoters[1] {
		t.Fatalf("eligible voters decoded as %v", decoded.EligibleVoters)
	}
	if len(decoded.MandatoryVoters) != 1 || decoded.MandatoryVoters[0] != want.MandatoryVoters[0] {
		t.Fatalf("mandatory voters decoded as %v", decoded.MandatoryVoters)
	}
}

func TestGivenTheRustIntentWhenMarshaledAsJSONThenShouldMatchSerde(t *testing.T) {
	encoded, err := json.Marshal(pinnedIntent(t))
	if err != nil {
		t.Fatalf("marshal the intent: %v", err)
	}
	if string(encoded) != rustIntentJSON {
		t.Fatalf("intent JSON drifted from Rust\n got %s\nwant %s", encoded, rustIntentJSON)
	}
	var decoded laser.Intent
	if err := json.Unmarshal([]byte(rustIntentJSON), &decoded); err != nil {
		t.Fatalf("unmarshal the Rust intent: %v", err)
	}
	if decoded.Digest != rustDigestOfTransfer || decoded.Policy.Required() != 2 {
		t.Fatalf("round trip lost the record: %+v", decoded)
	}
}

func TestGivenTheRustVoteWhenEncodedThenShouldProduceTheSameBytes(t *testing.T) {
	vote := laser.Vote{
		IntentID:      intentID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV"),
		IntentDigest:  rustDigestOfTransfer,
		PolicyVersion: 7,
		Voter:         agent(t, "alice"),
		Choice:        laser.VoteAllow,
		AtMicros:      1_500,
	}
	encoded, err := vote.Encode()
	if err != nil {
		t.Fatalf("encode the vote: %v", err)
	}
	if got := hex.EncodeToString(encoded); got != rustVoteCBOR {
		t.Fatalf("vote encoding drifted from Rust\n got %s\nwant %s", got, rustVoteCBOR)
	}
	payload, err := hex.DecodeString(rustVoteCBOR)
	if err != nil {
		t.Fatalf("the vector must be hex: %v", err)
	}
	decoded, err := laser.DecodeVote(payload)
	if err != nil {
		t.Fatalf("decode the Rust vote: %v", err)
	}
	if decoded != vote {
		t.Fatalf("decoded %+v, want %+v", decoded, vote)
	}
	asJSON, err := json.Marshal(vote)
	if err != nil {
		t.Fatalf("marshal the vote: %v", err)
	}
	if string(asJSON) != rustVoteJSON {
		t.Fatalf("vote JSON drifted\n got %s\nwant %s", asJSON, rustVoteJSON)
	}
	var fromJSON laser.Vote
	if err := json.Unmarshal([]byte(rustVoteJSON), &fromJSON); err != nil {
		t.Fatalf("unmarshal the Rust vote: %v", err)
	}
	if fromJSON != vote {
		t.Fatalf("JSON round trip gave %+v", fromJSON)
	}
}

func TestGivenTheRustDecisionWhenEncodedThenShouldProduceTheSameBytes(t *testing.T) {
	decision := laser.IntentDecision{
		IntentID:      intentID(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV"),
		IntentDigest:  rustDigestOfTransfer,
		PolicyVersion: 7,
		Outcome:       laser.IntentCommitted,
		Reason:        "quorum met",
		VotesConsidered: []laser.Ballot{
			{Voter: agent(t, "alice"), Choice: laser.VoteAllow},
			{Voter: agent(t, "bob"), Choice: laser.VoteAllow},
		},
		AtMicros: 1_900,
	}
	encoded, err := decision.Encode()
	if err != nil {
		t.Fatalf("encode the decision: %v", err)
	}
	if got := hex.EncodeToString(encoded); got != rustDecisionCBOR {
		t.Fatalf("decision encoding drifted from Rust\n got %s\nwant %s", got, rustDecisionCBOR)
	}
	payload, err := hex.DecodeString(rustDecisionCBOR)
	if err != nil {
		t.Fatalf("the vector must be hex: %v", err)
	}
	decoded, err := laser.DecodeIntentDecision(payload)
	if err != nil {
		t.Fatalf("decode the Rust decision: %v", err)
	}
	if decoded.Outcome != laser.IntentCommitted || decoded.Reason != "quorum met" ||
		len(decoded.VotesConsidered) != 2 ||
		decoded.VotesConsidered[1] != decision.VotesConsidered[1] {
		t.Fatalf("decoded %+v", decoded)
	}
	asJSON, err := json.Marshal(decision)
	if err != nil {
		t.Fatalf("marshal the decision: %v", err)
	}
	if string(asJSON) != rustDecisionJSON {
		t.Fatalf("decision JSON drifted\n got %s\nwant %s", asJSON, rustDecisionJSON)
	}
	var fromJSON laser.IntentDecision
	if err := json.Unmarshal([]byte(rustDecisionJSON), &fromJSON); err != nil {
		t.Fatalf("unmarshal the Rust decision: %v", err)
	}
	if fromJSON.AtMicros != 1_900 || len(fromJSON.VotesConsidered) != 2 {
		t.Fatalf("JSON round trip gave %+v", fromJSON)
	}
}

func TestGivenEachPolicyWhenEncodedThenShouldRideAsSerdeWritesIt(t *testing.T) {
	intent := pinnedIntent(t)
	for _, probe := range []struct {
		policy laser.IntentPolicy
		json   string
	}{
		{laser.IntentAll(), `"all"`},
		{laser.IntentAny(), `"any"`},
		{laser.IntentAtLeast(2), `{"at_least":2}`},
	} {
		intent.Policy = probe.policy
		encoded, err := json.Marshal(intent)
		if err != nil {
			t.Fatalf("marshal %s: %v", probe.policy, err)
		}
		if !strings.Contains(string(encoded), `"policy":`+probe.json) {
			t.Fatalf("policy %s encoded as %s", probe.policy, encoded)
		}
		var decoded laser.Intent
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("unmarshal %s: %v", probe.policy, err)
		}
		if decoded.Policy != probe.policy {
			t.Fatalf("policy %s round tripped as %s", probe.policy, decoded.Policy)
		}
	}
}

func TestGivenAPolicyThisClientDoesNotNameWhenDecodedThenShouldRefuseIt(t *testing.T) {
	for _, probe := range []string{
		`"most"`, `{"at_least":2,"all":1}`, `{"at_most":2}`, `{"at_least":"two"}`, `7`,
	} {
		body := strings.Replace(rustIntentJSON, `{"at_least":2}`, probe, 1)
		var decoded laser.Intent
		if err := json.Unmarshal([]byte(body), &decoded); err == nil {
			t.Fatalf("policy %s decoded as %s", probe, decoded.Policy)
		}
	}
}

func TestGivenAnIntentWhenBuiltThenShouldStampItsOwnIdentity(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice", "bob"}, nil)
	if intent.IntentID.IsZero() {
		t.Fatal("an intent must carry a fresh id")
	}
	if intent.Digest != rustDigestOfTransfer {
		t.Fatalf("the body hashed to %s", intent.Digest)
	}
	if intent.AtMicros == 0 {
		t.Fatal("an intent must carry its proposal time")
	}
	other := builtIntent(t, laser.IntentAll(), []string{"alice", "bob"}, nil)
	if other.IntentID == intent.IntentID {
		t.Fatal("two intents must not share an id")
	}
}

func TestGivenAnIntentBodyWhenMutatedByTheCallerThenShouldNotChangeTheRecord(t *testing.T) {
	body := []byte("transfer $100")
	intent, err := laser.NewIntent(laser.IntentOptions{
		Conversation:     laser.NewConversationID(),
		Proposer:         agent(t, "planner"),
		Body:             body,
		EligibleVoters:   agents(t, "alice"),
		Policy:           laser.IntentAny(),
		DeadlineAtMicros: ^uint64(0),
	})
	if err != nil {
		t.Fatalf("build an intent: %v", err)
	}
	body[0] = 'X'
	if err := intent.Validate(); err != nil {
		t.Fatalf("a caller's later write must not break the record: %v", err)
	}
}

func TestGivenAnInvalidIntentWhenValidatedThenShouldSayWhichRuleFailed(t *testing.T) {
	base := pinnedIntent(t)
	for _, probe := range []struct {
		name  string
		shape func(laser.Intent) laser.Intent
		want  string
	}{
		{"no eligible voters", func(intent laser.Intent) laser.Intent {
			intent.EligibleVoters, intent.MandatoryVoters = nil, nil
			return intent
		}, "an intent requires at least one eligible voter"},
		{"no proposer", func(intent laser.Intent) laser.Intent {
			intent.Proposer = wire.AgentID{}
			return intent
		}, "an intent requires a proposer"},
		{"unnamed voter", func(intent laser.Intent) laser.Intent {
			intent.EligibleVoters = []wire.AgentID{{}}
			intent.MandatoryVoters = nil
			return intent
		}, "an eligible voter must be named"},
		{"duplicate eligible", func(intent laser.Intent) laser.Intent {
			intent.EligibleVoters = agents(t, "alice", "alice")
			return intent
		}, "eligible voter 'alice' appears more than once"},
		{"duplicate mandatory", func(intent laser.Intent) laser.Intent {
			intent.MandatoryVoters = agents(t, "alice", "alice")
			return intent
		}, "mandatory voter 'alice' appears more than once"},
		{"mandatory not eligible", func(intent laser.Intent) laser.Intent {
			intent.MandatoryVoters = agents(t, "carol")
			return intent
		}, "mandatory voter 'carol' is not eligible"},
		{"threshold of zero", func(intent laser.Intent) laser.Intent {
			intent.Policy = laser.IntentAtLeast(0)
			return intent
		}, "threshold 0 is invalid for 2 eligible voters"},
		{"threshold past the roll", func(intent laser.Intent) laser.Intent {
			intent.Policy = laser.IntentAtLeast(3)
			return intent
		}, "threshold 3 is invalid for 2 eligible voters"},
		{"negative threshold", func(intent laser.Intent) laser.Intent {
			intent.Policy = laser.IntentAtLeast(-1)
			return intent
		}, "threshold -1 is invalid for 2 eligible voters"},
		{"unset policy", func(intent laser.Intent) laser.Intent {
			intent.Policy = laser.IntentPolicy{}
			return intent
		}, `intent policy must be "all", "any", or {"at_least": n}`},
		{"deadline before the proposal", func(intent laser.Intent) laser.Intent {
			intent.DeadlineAtMicros = 500
			return intent
		}, "deadline 500 must be after proposal time 1000"},
		{"deadline at the proposal", func(intent laser.Intent) laser.Intent {
			intent.DeadlineAtMicros = intent.AtMicros
			return intent
		}, "deadline 1000 must be after proposal time 1000"},
		{"body that is not the digest", func(intent laser.Intent) laser.Intent {
			intent.Body = []byte("transfer $900")
			return intent
		}, "intent digest does not match its body"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			err := probe.shape(base).Validate()
			if err == nil || err.Error() != probe.want {
				t.Fatalf("got %v, want %q", err, probe.want)
			}
			var invalid *laser.InvalidError
			if !errors.As(err, &invalid) {
				t.Fatal("an intent failure must read as the invalid class")
			}
		})
	}
}

func TestGivenAnIneligibleVoterWhenCastingThenShouldRefuseTheBallot(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice", "bob"}, nil)
	_, err := intent.CastVote(agent(t, "carol"), laser.VoteAllow)
	if err == nil || err.Error() != "voter 'carol' is not eligible for this intent" {
		t.Fatalf("got %v", err)
	}
	var intentError *laser.IntentError
	if !errors.As(err, &intentError) || intentError.Voter != "carol" {
		t.Fatalf("the error must name the voter, got %v", err)
	}
}

func TestGivenABallotWhenCastThenShouldBindItToTheIntent(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice"}, nil)
	vote, err := intent.CastVote(agent(t, "alice"), laser.VoteBlock)
	if err != nil {
		t.Fatalf("cast a vote: %v", err)
	}
	if vote.IntentID != intent.IntentID || vote.IntentDigest != intent.Digest ||
		vote.PolicyVersion != intent.PolicyVersion || vote.Choice != laser.VoteBlock {
		t.Fatalf("the ballot is not bound to the intent: %+v", vote)
	}
	if vote.AtMicros == 0 {
		t.Fatal("a ballot must carry when it was cast")
	}
}

func TestGivenAnInvalidIntentWhenCastingThenShouldRefuseBeforeTheBallot(t *testing.T) {
	intent := pinnedIntent(t)
	intent.Digest = rustDigestOfNothing
	if _, err := intent.CastVote(agent(t, "alice"), laser.VoteAllow); err == nil {
		t.Fatal("a ballot on an unvalidated intent must be refused")
	}
}

func TestGivenAChoiceThisClientDoesNotNameWhenCastThenShouldRefuseIt(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice"}, nil)
	if _, err := intent.CastVote(agent(t, "alice"), laser.VoteChoice("maybe")); err == nil {
		t.Fatal("a ballot must be allow, block or abstain")
	}
}

func TestGivenEveryVoterAllowsWhenFoldedThenShouldCommit(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice", "bob"}, nil)
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1),
		observed(t, intent, "bob", laser.VoteAllow, intent.AtMicros+2),
	}, intent.AtMicros+3)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Outcome != laser.IntentCommitted ||
		decision.Reason != "quorum met" {
		t.Fatalf("got %+v", decision)
	}
	if decision.AtMicros != intent.AtMicros+2 {
		t.Fatalf("the decision is terminal at the last ballot, got %d", decision.AtMicros)
	}
	authorized, err := decision.Authorizes(intent)
	if err != nil || !authorized {
		t.Fatalf("a committed decision authorizes: %v %v", authorized, err)
	}
}

func TestGivenOneAllowUnderAnyWhenFoldedThenShouldCommit(t *testing.T) {
	intent := builtIntent(t, laser.IntentAny(), []string{"alice", "bob"}, nil)
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1),
	}, intent.AtMicros+2)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Outcome != laser.IntentCommitted {
		t.Fatalf("got %+v", decision)
	}
}

func TestGivenTheThresholdIsReachedWhenFoldedThenShouldCommit(t *testing.T) {
	intent := builtIntent(t, laser.IntentAtLeast(2), []string{"alice", "bob", "carol"}, nil)
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1),
		observed(t, intent, "bob", laser.VoteAllow, intent.AtMicros+2),
	}, intent.AtMicros+3)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Outcome != laser.IntentCommitted {
		t.Fatalf("got %+v", decision)
	}
}

func TestGivenOneBlockUnderAllWhenFoldedThenShouldAbortAsImpossible(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice", "bob"}, nil)
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteBlock, intent.AtMicros+1),
	}, intent.AtMicros+2)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Outcome != laser.IntentAborted ||
		decision.Reason != "quorum became impossible" {
		t.Fatalf("got %+v", decision)
	}
	authorized, err := decision.Authorizes(intent)
	if err != nil || authorized {
		t.Fatalf("an aborted decision does not authorize: %v %v", authorized, err)
	}
}

func TestGivenTooFewVotersLeftWhenFoldedThenShouldAbortAsImpossible(t *testing.T) {
	intent := builtIntent(t, laser.IntentAtLeast(2), []string{"alice", "bob", "carol"}, nil)
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteBlock, intent.AtMicros+1),
		observed(t, intent, "bob", laser.VoteAbstain, intent.AtMicros+2),
	}, intent.AtMicros+3)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Reason != "quorum became impossible" {
		t.Fatalf("got %+v", decision)
	}
}

func TestGivenEveryVoterAbstainsUnderAnyWhenFoldedThenShouldAbortAsImpossible(t *testing.T) {
	intent := builtIntent(t, laser.IntentAny(), []string{"alice", "bob"}, nil)
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteAbstain, intent.AtMicros+1),
		observed(t, intent, "bob", laser.VoteAbstain, intent.AtMicros+2),
	}, intent.AtMicros+3)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Reason != "quorum became impossible" {
		t.Fatalf("got %+v", decision)
	}
}

func TestGivenOneVoterContradictsItselfWhenFoldedThenShouldAbort(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice", "bob"}, nil)
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1),
		observed(t, intent, "alice", laser.VoteBlock, intent.AtMicros+2),
	}, intent.AtMicros+3)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Outcome != laser.IntentAborted ||
		decision.Reason != "voter 'alice' cast conflicting votes" {
		t.Fatalf("got %+v", decision)
	}
	if len(decision.VotesConsidered) != 2 {
		t.Fatalf("the conflicting ballot is evidence: %+v", decision.VotesConsidered)
	}
}

func TestGivenTheSameBallotTwiceWhenFoldedThenShouldBeIdempotent(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice", "bob"}, nil)
	allow := observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1)
	repeat := allow
	repeat.ObservedAtMicros = intent.AtMicros + 5
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		allow, repeat,
		observed(t, intent, "bob", laser.VoteAllow, intent.AtMicros+2),
	}, intent.AtMicros+6)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Outcome != laser.IntentCommitted {
		t.Fatalf("got %+v", decision)
	}
	if len(decision.VotesConsidered) != 2 {
		t.Fatalf("a repeat is not new evidence: %+v", decision.VotesConsidered)
	}
}

func TestGivenAMandatoryVoterBlocksWhenFoldedThenShouldAbort(t *testing.T) {
	intent := builtIntent(t, laser.IntentAny(), []string{"alice", "bob"}, []string{"alice"})
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteBlock, intent.AtMicros+1),
		observed(t, intent, "bob", laser.VoteAllow, intent.AtMicros+2),
	}, intent.AtMicros+3)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Reason != "mandatory voter 'alice' did not allow" {
		t.Fatalf("got %+v", decision)
	}
}

func TestGivenAMandatoryVoterStaysSilentWhenTheDeadlinePassesThenShouldAbort(t *testing.T) {
	intent := pinnedIntent(t)
	intent.Policy = laser.IntentAny()
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "bob", laser.VoteAllow, intent.AtMicros+1),
	}, intent.DeadlineAtMicros)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Reason != "mandatory approval not reached by deadline" {
		t.Fatalf("got %+v", decision)
	}
	if decision.AtMicros != intent.DeadlineAtMicros {
		t.Fatalf("a deadline abort is terminal at the deadline, got %d", decision.AtMicros)
	}
}

func TestGivenNobodyVotesWhenTheDeadlinePassesThenShouldAbort(t *testing.T) {
	intent := pinnedIntent(t)
	intent.MandatoryVoters = nil
	intent.Policy = laser.IntentAtLeast(2)
	decision, err := laser.Decide(intent, nil, intent.DeadlineAtMicros+1)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Reason != "quorum not reached by deadline" {
		t.Fatalf("got %+v", decision)
	}
	if len(decision.VotesConsidered) != 0 {
		t.Fatalf("no ballots is no evidence: %+v", decision.VotesConsidered)
	}
}

func TestGivenTheOutcomeIsStillReachableWhenFoldedThenShouldWait(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice", "bob"}, nil)
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1),
	}, intent.AtMicros+2)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision != nil {
		t.Fatalf("the fold must wait, got %+v", decision)
	}
}

func TestGivenBallotsInAnyOrderWhenFoldedThenShouldReachTheSameDecision(t *testing.T) {
	intent := builtIntent(t, laser.IntentAtLeast(2), []string{"alice", "bob", "carol"}, nil)
	ballots := []laser.ObservedVote{
		observed(t, intent, "carol", laser.VoteAllow, intent.AtMicros+3),
		observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1),
		observed(t, intent, "bob", laser.VoteBlock, intent.AtMicros+2),
	}
	forward, err := laser.Decide(intent, ballots, intent.AtMicros+4)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	reversed := []laser.ObservedVote{ballots[2], ballots[0], ballots[1]}
	backward, err := laser.Decide(intent, reversed, intent.AtMicros+4)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if forward == nil || backward == nil {
		t.Fatalf("both folds must decide: %+v %+v", forward, backward)
	}
	if forward.Outcome != backward.Outcome || forward.AtMicros != backward.AtMicros ||
		len(forward.VotesConsidered) != len(backward.VotesConsidered) {
		t.Fatalf("replay order changed the decision: %+v %+v", forward, backward)
	}
	for index := range forward.VotesConsidered {
		if forward.VotesConsidered[index] != backward.VotesConsidered[index] {
			t.Fatalf("replay order changed the evidence: %+v %+v",
				forward.VotesConsidered, backward.VotesConsidered)
		}
	}
	if forward.VotesConsidered[0].Voter != agent(t, "alice") {
		t.Fatalf("evidence is canonically ordered, got %+v", forward.VotesConsidered)
	}
}

func TestGivenABallotOutsideTheWindowWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	intent := pinnedIntent(t)
	intent.MandatoryVoters = nil
	intent.Policy = laser.IntentAny()
	for _, probe := range []struct {
		name string
		at   uint64
		now  uint64
	}{
		{"before the proposal", intent.AtMicros - 1, intent.DeadlineAtMicros - 1},
		{"after the deadline", intent.DeadlineAtMicros + 1, intent.DeadlineAtMicros - 1},
		{"after the reader's clock", intent.AtMicros + 10, intent.AtMicros + 5},
	} {
		t.Run(probe.name, func(t *testing.T) {
			decision, err := laser.Decide(intent, []laser.ObservedVote{
				observed(t, intent, "alice", laser.VoteAllow, probe.at),
			}, probe.now)
			if err != nil {
				t.Fatalf("fold: %v", err)
			}
			if decision != nil {
				t.Fatalf("the ballot must not count, got %+v", decision)
			}
		})
	}
}

func TestGivenABallotForAnotherIntentWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	intent := builtIntent(t, laser.IntentAny(), []string{"alice"}, nil)
	other := builtIntent(t, laser.IntentAny(), []string{"alice"}, nil)
	stray := observed(t, other, "alice", laser.VoteAllow, intent.AtMicros+1)
	wrongDigest := observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1)
	wrongDigest.Vote.IntentDigest = rustDigestOfNothing
	wrongVersion := observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1)
	wrongVersion.Vote.PolicyVersion++
	ineligible := wrongVersion
	ineligible.Vote.PolicyVersion--
	ineligible.Vote.Voter = agent(t, "mallory")
	for name, ballot := range map[string]laser.ObservedVote{
		"another intent": stray, "another body": wrongDigest,
		"another policy version": wrongVersion, "an ineligible voter": ineligible,
	} {
		t.Run(name, func(t *testing.T) {
			decision, err := laser.Decide(intent, []laser.ObservedVote{ballot}, intent.AtMicros+2)
			if err != nil {
				t.Fatalf("fold: %v", err)
			}
			if decision != nil {
				t.Fatalf("the ballot must not count, got %+v", decision)
			}
		})
	}
}

func TestGivenAnInvalidIntentWhenFoldedThenShouldRefuseToDecide(t *testing.T) {
	intent := pinnedIntent(t)
	intent.Body = []byte("transfer $900")
	if _, err := laser.Decide(intent, nil, intent.AtMicros+1); err == nil {
		t.Fatal("a fold against an unvalidated intent must be refused")
	}
}

func TestGivenADecisionForAnotherIntentWhenCheckedThenShouldRefuseIt(t *testing.T) {
	intent := builtIntent(t, laser.IntentAny(), []string{"alice"}, nil)
	other := builtIntent(t, laser.IntentAny(), []string{"alice"}, nil)
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteAllow, intent.AtMicros+1),
	}, intent.AtMicros+2)
	if err != nil || decision == nil {
		t.Fatalf("fold: %v %+v", err, decision)
	}
	want := "decision is not bound to this intent body and policy version"
	if _, err := decision.Authorizes(other); err == nil || err.Error() != want {
		t.Fatalf("got %v", err)
	}
	shifted := *decision
	shifted.PolicyVersion++
	if _, err := shifted.Authorizes(intent); err == nil || err.Error() != want {
		t.Fatalf("got %v", err)
	}
	rebodied := *decision
	rebodied.IntentDigest = rustDigestOfNothing
	if _, err := rebodied.Authorizes(intent); err == nil || err.Error() != want {
		t.Fatalf("got %v", err)
	}
}

func TestGivenACommittedDecisionWithoutItsVotesWhenCheckedThenShouldRefuseIt(t *testing.T) {
	intent := builtIntent(t, laser.IntentAtLeast(2), []string{"alice", "bob"}, []string{"alice"})
	base := laser.IntentDecision{
		IntentID:      intent.IntentID,
		IntentDigest:  intent.Digest,
		PolicyVersion: intent.PolicyVersion,
		Outcome:       laser.IntentCommitted,
		Reason:        "quorum met",
		AtMicros:      intent.AtMicros + 1,
	}
	want := "decision votes do not meet this intent's quorum"
	for _, probe := range []struct {
		name  string
		votes []laser.Ballot
	}{
		{"no votes at all", nil},
		{"short of the threshold", []laser.Ballot{
			{Voter: agent(t, "alice"), Choice: laser.VoteAllow}}},
		{"a voter who is not eligible", []laser.Ballot{
			{Voter: agent(t, "alice"), Choice: laser.VoteAllow},
			{Voter: agent(t, "mallory"), Choice: laser.VoteAllow}}},
		{"a voter counted twice", []laser.Ballot{
			{Voter: agent(t, "alice"), Choice: laser.VoteAllow},
			{Voter: agent(t, "alice"), Choice: laser.VoteBlock}}},
		{"the mandatory voter missing", []laser.Ballot{
			{Voter: agent(t, "bob"), Choice: laser.VoteAllow},
			{Voter: agent(t, "bob"), Choice: laser.VoteAllow}}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			decision := base
			decision.VotesConsidered = probe.votes
			authorized, err := decision.Authorizes(intent)
			if err == nil || err.Error() != want || authorized {
				t.Fatalf("got %v %v", authorized, err)
			}
		})
	}
	decision := base
	decision.VotesConsidered = []laser.Ballot{
		{Voter: agent(t, "alice"), Choice: laser.VoteAllow},
		{Voter: agent(t, "bob"), Choice: laser.VoteAllow},
	}
	authorized, err := decision.Authorizes(intent)
	if err != nil || !authorized {
		t.Fatalf("a supported commit authorizes: %v %v", authorized, err)
	}
	repeated := decision
	repeated.VotesConsidered = append(append([]laser.Ballot{}, decision.VotesConsidered...),
		laser.Ballot{Voter: agent(t, "alice"), Choice: laser.VoteAllow})
	if authorized, err := repeated.Authorizes(intent); err != nil || !authorized {
		t.Fatalf("an idempotent repeat still authorizes: %v %v", authorized, err)
	}
}

func TestGivenADecisionAgainstAnInvalidIntentWhenCheckedThenShouldRefuseIt(t *testing.T) {
	intent := pinnedIntent(t)
	intent.Body = []byte("transfer $900")
	decision := laser.IntentDecision{IntentID: intent.IntentID, Outcome: laser.IntentCommitted}
	if _, err := decision.Authorizes(intent); err == nil {
		t.Fatal("an unvalidated intent cannot be authorized against")
	}
}

func TestGivenARecordThisClientCannotReadWhenDecodedThenShouldRefuseIt(t *testing.T) {
	for _, probe := range []struct {
		name   string
		body   string
		decode func([]byte) error
	}{
		{"an intent id that is not a ULID",
			strings.Replace(rustIntentJSON, "01ARZ3NDEKTSV4RRFFQ69G5FAV", "not-a-ulid", 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Intent)) }},
		{"a conversation that is not a ULID",
			strings.Replace(rustIntentJSON, "01BX5ZZKBKACTAV9WEVGEMMVRZ", "nope", 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Intent)) }},
		{"a proposer with no name",
			strings.Replace(rustIntentJSON, `"proposer":"planner"`, `"proposer":""`, 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Intent)) }},
		{"a voter with no name",
			strings.Replace(rustIntentJSON, `["alice","bob"]`, `["",""]`, 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Intent)) }},
		{"a mandatory voter with no name",
			strings.Replace(rustIntentJSON, `"mandatory_voters":["alice"]`,
				`"mandatory_voters":[""]`, 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Intent)) }},
		{"a body that is not bytes",
			strings.Replace(rustIntentJSON, "[116,", "[1116,", 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Intent)) }},
		{"a digest that is not the body",
			strings.Replace(rustIntentJSON, rustDigestOfTransfer, rustDigestOfNothing, 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Intent)) }},
		{"a vote id that is not a ULID",
			strings.Replace(rustVoteJSON, "01ARZ3NDEKTSV4RRFFQ69G5FAV", "not-a-ulid", 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Vote)) }},
		{"a voter with no name",
			strings.Replace(rustVoteJSON, `"voter":"alice"`, `"voter":""`, 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Vote)) }},
		{"a choice this client does not name",
			strings.Replace(rustVoteJSON, `"choice":"allow"`, `"choice":"maybe"`, 1),
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Vote)) }},
		{"a decision id that is not a ULID",
			strings.Replace(rustDecisionJSON, "01ARZ3NDEKTSV4RRFFQ69G5FAV", "not-a-ulid", 1),
			func(payload []byte) error {
				return json.Unmarshal(payload, new(laser.IntentDecision))
			}},
		{"an outcome this client does not name",
			strings.Replace(rustDecisionJSON, `"outcome":"committed"`, `"outcome":"maybe"`, 1),
			func(payload []byte) error {
				return json.Unmarshal(payload, new(laser.IntentDecision))
			}},
		{"a considered vote that is not a pair",
			strings.Replace(rustDecisionJSON, `["alice","allow"]`, `["alice"]`, 1),
			func(payload []byte) error {
				return json.Unmarshal(payload, new(laser.IntentDecision))
			}},
		{"a considered vote that is not text",
			strings.Replace(rustDecisionJSON, `["alice","allow"]`, `[1,2]`, 1),
			func(payload []byte) error {
				return json.Unmarshal(payload, new(laser.IntentDecision))
			}},
		{"a considered voter with no name",
			strings.Replace(rustDecisionJSON, `["alice","allow"]`, `["","allow"]`, 1),
			func(payload []byte) error {
				return json.Unmarshal(payload, new(laser.IntentDecision))
			}},
		{"a considered choice this client does not name",
			strings.Replace(rustDecisionJSON, `["alice","allow"]`, `["alice","maybe"]`, 1),
			func(payload []byte) error {
				return json.Unmarshal(payload, new(laser.IntentDecision))
			}},
		{"a considered vote that is not an array",
			strings.Replace(rustDecisionJSON, `["alice","allow"]`, `"alice"`, 1),
			func(payload []byte) error {
				return json.Unmarshal(payload, new(laser.IntentDecision))
			}},
		{"a record that is not an object", "[]",
			func(payload []byte) error { return json.Unmarshal(payload, new(laser.Intent)) }},
	} {
		t.Run(probe.name, func(t *testing.T) {
			if err := probe.decode([]byte(probe.body)); err == nil {
				t.Fatalf("%s must be refused", probe.name)
			}
		})
	}
}

func TestGivenBytesThatAreNotARecordWhenDecodedThenShouldRefuseThem(t *testing.T) {
	if _, err := laser.DecodeIntent([]byte{0xff}); err == nil {
		t.Fatal("garbage is not an intent")
	}
	if _, err := laser.DecodeVote([]byte{0xff}); err == nil {
		t.Fatal("garbage is not a vote")
	}
	if _, err := laser.DecodeIntentDecision([]byte{0xff}); err == nil {
		t.Fatal("garbage is not a decision")
	}
}

func TestGivenACompactBodyWhenDecodedThenShouldReadItAsBytes(t *testing.T) {
	// A client that wrote the body as a CBOR byte string rather than serde's
	// array of integers is still read, because the two spell the same body.
	compact := strings.Replace(rustIntentCBOR,
		"8d187418721861186e187318661865187218201824183118301830",
		"4d"+hex.EncodeToString([]byte("transfer $100")), 1)
	payload, err := hex.DecodeString(compact)
	if err != nil {
		t.Fatalf("the probe must be hex: %v", err)
	}
	decoded, err := laser.DecodeIntent(payload)
	if err != nil {
		t.Fatalf("a compact body must still decode: %v", err)
	}
	if string(decoded.Body) != "transfer $100" {
		t.Fatalf("decoded body %q", decoded.Body)
	}
}

func TestGivenAnIntentIDWhenReadBackThenShouldKeepItsText(t *testing.T) {
	id := laser.NewIntentID()
	if id.IsZero() {
		t.Fatal("a fresh id must not be zero")
	}
	parsed, err := laser.ParseIntentID(id.String())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed != id {
		t.Fatalf("round trip gave %s, want %s", parsed, id)
	}
	if _, err := laser.ParseIntentID("short"); err == nil {
		t.Fatal("a short id is not a ULID")
	}
	if !(laser.IntentID{}).IsZero() {
		t.Fatal("the zero id must report itself")
	}
}

func TestGivenAPolicyWhenAskedThenShouldDescribeItself(t *testing.T) {
	for _, probe := range []struct {
		policy   laser.IntentPolicy
		kind     string
		required int
		text     string
	}{
		{laser.IntentAll(), "all", 0, "all"},
		{laser.IntentAny(), "any", 0, "any"},
		{laser.IntentAtLeast(3), "at_least", 3, "at_least 3"},
	} {
		if probe.policy.Kind() != probe.kind || probe.policy.Required() != probe.required ||
			probe.policy.String() != probe.text {
			t.Fatalf("%+v described itself as %s/%d/%s", probe.policy,
				probe.policy.Kind(), probe.policy.Required(), probe.policy.String())
		}
	}
	if (laser.IntentPolicy{}).Required() != 0 {
		t.Fatal("an unset policy carries no threshold")
	}
}

func TestGivenANegativeThresholdWhenEncodedThenShouldRefuseIt(t *testing.T) {
	intent := pinnedIntent(t)
	intent.Policy = laser.IntentAtLeast(-1)
	if _, err := intent.Encode(); err == nil {
		t.Fatal("a negative threshold is not encodable")
	}
	intent.Policy = laser.IntentPolicy{}
	if _, err := intent.Encode(); err == nil {
		t.Fatal("an unset policy is not encodable")
	}
	if _, err := json.Marshal(intent); err == nil {
		t.Fatal("an unset policy is not encodable as JSON")
	}
}

func TestGivenABallotChoiceThisClientDoesNotNameWhenEncodedThenShouldRefuseIt(t *testing.T) {
	decision := laser.IntentDecision{
		IntentID: laser.NewIntentID(),
		Outcome:  laser.IntentCommitted,
		VotesConsidered: []laser.Ballot{
			{Voter: agent(t, "alice"), Choice: laser.VoteChoice("maybe")},
		},
	}
	if _, err := decision.Encode(); err == nil {
		t.Fatal("an unnamed choice is not encodable")
	}
}

func TestGivenADigestFailureWhenRaisedThenShouldCarryItsCause(t *testing.T) {
	err := &laser.IntentError{Reason: "intent body does not encode for its digest: boom",
		Cause: errors.New("boom")}
	if !errors.Is(err, err.Cause) {
		t.Fatal("the cause must stay reachable")
	}
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatal("an intent failure must read as the invalid class")
	}
}

func TestGivenABallotObservedPastTheDeadlineWhenFoldedThenShouldNotCountIt(t *testing.T) {
	// The reader's clock is past the deadline too, so the only thing that can
	// keep this ballot out of the fold is the deadline ceiling itself.
	intent := pinnedIntent(t)
	intent.MandatoryVoters = nil
	intent.Policy = laser.IntentAny()
	decision, err := laser.Decide(intent, []laser.ObservedVote{
		observed(t, intent, "alice", laser.VoteAllow, intent.DeadlineAtMicros+1),
	}, intent.DeadlineAtMicros+2)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if decision == nil || decision.Outcome != laser.IntentAborted ||
		decision.Reason != "quorum not reached by deadline" {
		t.Fatalf("a ballot past the deadline must not commit it, got %+v", decision)
	}
}

func TestGivenOneVoterContradictsItselfWhenFoldedThenShouldOrderTheEvidence(t *testing.T) {
	intent := builtIntent(t, laser.IntentAll(), []string{"alice", "bob"}, nil)
	for _, probe := range []struct {
		name  string
		first laser.VoteChoice
		at    [2]uint64
		want  [2]laser.VoteChoice
	}{
		{"by when they were observed", laser.VoteAllow,
			[2]uint64{1, 2}, [2]laser.VoteChoice{laser.VoteAllow, laser.VoteBlock}},
		{"by choice when observed together", laser.VoteAllow,
			[2]uint64{1, 1}, [2]laser.VoteChoice{laser.VoteAllow, laser.VoteBlock}},
		{"block before abstain", laser.VoteBlock,
			[2]uint64{1, 1}, [2]laser.VoteChoice{laser.VoteBlock, laser.VoteAbstain}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			second := laser.VoteBlock
			if probe.first == laser.VoteBlock {
				second = laser.VoteAbstain
			}
			ballots := []laser.ObservedVote{
				observed(t, intent, "alice", probe.first, intent.AtMicros+probe.at[0]),
				observed(t, intent, "alice", second, intent.AtMicros+probe.at[1]),
			}
			// Both input orders must fold to the same evidence, which is the
			// whole claim: replay order changes nothing.
			for _, order := range [][]laser.ObservedVote{
				{ballots[0], ballots[1]}, {ballots[1], ballots[0]},
			} {
				decision, err := laser.Decide(intent, order, intent.AtMicros+3)
				if err != nil {
					t.Fatalf("fold: %v", err)
				}
				if decision == nil || decision.Reason != "voter 'alice' cast conflicting votes" {
					t.Fatalf("got %+v", decision)
				}
				if len(decision.VotesConsidered) != 2 ||
					decision.VotesConsidered[0].Choice != probe.want[0] ||
					decision.VotesConsidered[1].Choice != probe.want[1] {
					t.Fatalf("evidence ordered as %+v, want %v", decision.VotesConsidered, probe.want)
				}
			}
		})
	}
}

func TestGivenADecisionCountingOneVoterTwiceWhenCheckedThenShouldRefuseIt(t *testing.T) {
	// Any quorum and no mandatory voter, so the only rule that can refuse
	// this is the one ballot per voter rule itself.
	intent := builtIntent(t, laser.IntentAny(), []string{"alice", "bob"}, nil)
	decision := laser.IntentDecision{
		IntentID:      intent.IntentID,
		IntentDigest:  intent.Digest,
		PolicyVersion: intent.PolicyVersion,
		Outcome:       laser.IntentCommitted,
		VotesConsidered: []laser.Ballot{
			{Voter: agent(t, "alice"), Choice: laser.VoteBlock},
			{Voter: agent(t, "alice"), Choice: laser.VoteAllow},
		},
	}
	authorized, err := decision.Authorizes(intent)
	if err == nil || authorized {
		t.Fatalf("a voter cannot be counted twice, got %v %v", authorized, err)
	}
}

func TestGivenADecisionWithoutItsMandatoryVoterWhenCheckedThenShouldRefuseIt(t *testing.T) {
	// One allow meets the quorum, so the only rule that can refuse this is
	// the mandatory voter rule itself.
	intent := builtIntent(t, laser.IntentAny(), []string{"alice", "bob"}, []string{"alice"})
	decision := laser.IntentDecision{
		IntentID:      intent.IntentID,
		IntentDigest:  intent.Digest,
		PolicyVersion: intent.PolicyVersion,
		Outcome:       laser.IntentCommitted,
		VotesConsidered: []laser.Ballot{
			{Voter: agent(t, "bob"), Choice: laser.VoteAllow},
		},
	}
	authorized, err := decision.Authorizes(intent)
	if err == nil || authorized {
		t.Fatalf("a mandatory voter cannot be skipped, got %v %v", authorized, err)
	}
}

func TestGivenAPolicyValueWhenDecodedOnItsOwnThenShouldAcceptOnlyTheThreeKinds(t *testing.T) {
	for _, probe := range []struct {
		body string
		want laser.IntentPolicy
	}{
		{`"all"`, laser.IntentAll()},
		{`"any"`, laser.IntentAny()},
		{`{"at_least":2}`, laser.IntentAtLeast(2)},
	} {
		var policy laser.IntentPolicy
		if err := wire.UnmarshalJSON([]byte(probe.body), &policy); err != nil {
			t.Fatalf("decode %s: %v", probe.body, err)
		}
		if policy != probe.want {
			t.Fatalf("%s decoded as %s", probe.body, policy)
		}
	}
	for _, probe := range []string{
		`"most"`, `"at_least"`, `""`, `{"at_most":2}`, `{"at_least":4294967296}`,
		`{"at_least":-1}`, `{"at_least":2,"at_most":1}`, `7`, `null`, `[]`,
	} {
		var policy laser.IntentPolicy
		if err := wire.UnmarshalJSON([]byte(probe), &policy); err == nil {
			t.Fatalf("%s decoded as %s", probe, policy)
		}
	}
}

func TestGivenABodyValueThatIsNotAByteWhenDecodedThenShouldSayWhyItIsRefused(t *testing.T) {
	want := "an intent body must be an array of bytes"
	for _, probe := range []string{"[300,", "[70000,", `["t",`, "[-1,"} {
		body := strings.Replace(rustIntentJSON, "[116,", probe, 1)
		err := json.Unmarshal([]byte(body), new(laser.Intent))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s gave %v, want %q", probe, err, want)
		}
	}
}

func TestGivenAConsideredVoteOfTheWrongShapeWhenDecodedThenShouldSayWhyItIsRefused(t *testing.T) {
	// The voter and choice are each validated below this check, so without
	// the shape check a malformed pair would be reported as an empty agent
	// name rather than as the wrong shape. The message is pinned here so the
	// clearer one is what a caller actually sees.
	want := "a considered vote is a voter and a choice"
	for _, probe := range []string{`[1,2]`, `["alice",2]`, `[1,"allow"]`, `["alice"]`,
		`["alice","allow","extra"]`, `[]`} {
		body := strings.Replace(rustDecisionJSON, `["alice","allow"]`, probe, 1)
		err := json.Unmarshal([]byte(body), new(laser.IntentDecision))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s gave %v, want %q", probe, err, want)
		}
	}
}
