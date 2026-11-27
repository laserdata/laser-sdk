package laser

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// MaxPreviewRunes bounds how much of a payload a summary shows.
const MaxPreviewRunes = 200

// CrashContext is a recovery bundle for one conversation.
//
// It answers "what was happening right before this crashed" from one call
// rather than stitching three reads together by hand: the recent journal, the
// dead-letter capsule for the record that crashed, and the last governance
// decision.
//
// It is a pure combination of pieces the caller has already read. It does no
// reading of its own and never calls a model: assembling context is not
// deciding what to do with it.
type CrashContext struct {
	// Journal is the recent conversation history, oldest first, already
	// bounded by whatever policy the caller assembled it under.
	Journal []ContextMessage
	// DeadLetter is the capsule for the record that crashed, when the crash
	// went through the dead-letter path.
	DeadLetter *wire.AgentDeadLetter
	// LastDecision is the most recent governance decision for the
	// conversation, when there is one.
	LastDecision *PolicyEvidence
}

// AssembleCrashContext combines pieces the caller has already read.
func AssembleCrashContext(
	journal []ContextMessage, deadLetter *wire.AgentDeadLetter,
	lastDecision *PolicyEvidence,
) CrashContext {
	return CrashContext{
		Journal: journal, DeadLetter: deadLetter, LastDecision: lastDecision,
	}
}

// Summarize renders the bundle as plain text, in one fixed order every time.
//
// It is deterministic on purpose: this goes into a recovery agent's own
// prompt or into a log line, and a summary that reordered itself between runs
// would make two identical crashes look different. Nothing here is produced
// by a model, and nothing here is fed to one inside this client.
func (context CrashContext) Summarize() string {
	var out strings.Builder
	out.WriteString("journal (" + strconv.Itoa(len(context.Journal)) +
		" message(s)):\n")
	for _, message := range context.Journal {
		out.WriteString("  - [" + journalAuthor(message) + "] " +
			safePreview(string(journalBody(message))) + "\n")
	}
	switch capsule := context.DeadLetter; {
	case capsule == nil:
		out.WriteString("dead letter: none\n")
	default:
		out.WriteString("dead letter: " +
			strconv.FormatUint(uint64(capsule.Attempts), 10) +
			" attempt(s), reason " + string(capsule.Reason))
		if capsule.Detail != nil {
			out.WriteString(": " + safePreview(*capsule.Detail))
		}
		out.WriteString("\n")
	}
	switch decision := context.LastDecision; {
	case decision == nil:
		out.WriteString("last decision: none\n")
	default:
		out.WriteString("last decision: " + safePreview(decision.Decision) +
			" (" + safePreview(decision.Outcome) + ")")
		if decision.Reason != nil {
			out.WriteString(" - " + safePreview(*decision.Reason))
		}
		out.WriteString("\n")
	}
	return out.String()
}

// journalAuthor is who wrote one journal entry, from the envelope when there
// is one and from the record's own provenance otherwise.
func journalAuthor(message ContextMessage) string {
	if message.Envelope != nil && !message.Envelope.Source.IsZero() {
		return message.Envelope.Source.String()
	}
	if message.Record != nil && message.Record.Provenance != nil &&
		message.Record.Provenance.Agent != nil {
		return message.Record.Provenance.Agent.String()
	}
	return "unknown"
}

// journalBody is what one entry said: the envelope's own body for an agent
// record, the record's payload otherwise.
func journalBody(message ContextMessage) []byte {
	if message.Envelope != nil {
		return message.Envelope.Body
	}
	if message.Record != nil {
		return message.Record.Payload()
	}
	return nil
}

// safePreview truncates text to a bounded number of characters and escapes
// what would otherwise break the line.
//
// Truncation counts characters rather than bytes: a payload is arbitrary
// bytes, and cutting one at a byte offset can split a character in half.
// Control characters are escaped because one line per field is what makes the
// summary readable, and a payload carrying a newline would otherwise forge a
// field of its own.
func safePreview(text string) string {
	var out strings.Builder
	out.Grow(min(len(text), MaxPreviewRunes))
	shown := 0
	for _, character := range text {
		if shown == MaxPreviewRunes {
			out.WriteString("...")
			return out.String()
		}
		switch {
		case character == '\n':
			out.WriteString(`\n`)
		case character == '\r':
			out.WriteString(`\r`)
		case character == '\t':
			out.WriteString(`\t`)
		case unicode.IsControl(character):
			out.WriteString(`\u`)
			out.WriteString(strconv.FormatInt(int64(character), 16))
		default:
			out.WriteRune(character)
		}
		shown++
	}
	return out.String()
}
