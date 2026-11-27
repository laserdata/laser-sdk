package laser

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// The journal's wire shape is the Rust client's: an externally tagged enum,
// which is a one-entry CBOR map from the variant's name to a map of its
// fields. A run this client starts resumes on any client, and one another
// client started resumes here.
//
// It is spelled out here rather than derived from the Go struct because the
// two shapes genuinely differ: Go holds one flat struct with a `kind`
// discriminator, which is the natural Go form, and nothing about that struct
// would produce these bytes on its own.
const (
	journalVariantRunStarted  = "RunStarted"
	journalVariantDispatched  = "StepDispatched"
	journalVariantCompleted   = "StepCompleted"
	journalVariantCompensated = "StepCompensated"
	journalFieldWorkflow      = "workflow"
	journalFieldLabel         = "label"
	journalFieldAttempt       = "attempt"
	journalFieldReplyFrom     = "reply_from"
	journalFieldOutput        = "output"
	journalFieldAgents        = "agents"
	journalFieldInput         = "input"
)

// MarshalCBOR writes the entry in the cross-client journal shape.
func (entry workflowJournalEntry) MarshalCBOR() (cborform.Value, error) {
	variant, fields, err := entry.variant()
	if err != nil {
		return nil, err
	}
	return cborform.Map{{
		Key:   cborform.Text(variant),
		Value: fields,
	}}, nil
}

// variant is the entry's variant name and its field map.
func (entry workflowJournalEntry) variant() (string, cborform.Map, error) {
	switch entry.Kind {
	case journalRunStarted:
		return journalVariantRunStarted, cborform.Map{
			{Key: cborform.Text(journalFieldWorkflow), Value: cborform.Text(entry.Workflow)},
		}, nil

	case journalStepDispatched:
		return journalVariantDispatched, cborform.Map{
			{Key: cborform.Text(journalFieldLabel), Value: cborform.Text(entry.Label)},
			{Key: cborform.Text(journalFieldAttempt), Value: cborform.Uint(entry.Attempt)},
			{Key: cborform.Text(journalFieldReplyFrom), Value: offsetsValue(entry.ReplyFrom)},
		}, nil

	case journalStepCompleted:
		fields := cborform.Map{
			{Key: cborform.Text(journalFieldLabel), Value: cborform.Text(entry.Label)},
			// A byte array, not a byte string: Rust's `Vec<u8>` serialises
			// as a sequence of integers, and a CBOR byte string would be a
			// different shape that no other client reads back.
			{Key: cborform.Text(journalFieldOutput), Value: byteArrayValue(entry.Output)},
			{Key: cborform.Text(journalFieldAgents), Value: textArrayValue(entry.Agents)},
		}
		if entry.Input != "" {
			// Absent rather than empty when there is no digest, matching
			// the Option the other clients skip when it is None.
			fields = append(fields, cborform.Entry{
				Key: cborform.Text(journalFieldInput), Value: cborform.Text(entry.Input),
			})
		}
		return journalVariantCompleted, fields, nil

	case journalStepCompensated:
		return journalVariantCompensated, cborform.Map{
			{Key: cborform.Text(journalFieldLabel), Value: cborform.Text(entry.Label)},
		}, nil

	default:
		return "", nil, &InvalidError{
			Reason: fmt.Sprintf("unknown workflow journal entry kind %q", entry.Kind)}
	}
}

// UnmarshalCBOR reads an entry written by any client.
func (entry *workflowJournalEntry) UnmarshalCBOR(value cborform.Value) error {
	tagged, named := value.(cborform.Map)
	if !named || len(tagged) != 1 {
		return &CodecError{
			Reason: "a workflow journal entry is one variant name and its fields"}
	}
	variant, named := tagged[0].Key.(cborform.Text)
	if !named {
		return &CodecError{Reason: "a workflow journal entry's variant must be named"}
	}
	fields, mapped := tagged[0].Value.(cborform.Map)
	if !mapped {
		return &CodecError{Reason: "a workflow journal entry's fields must be a map"}
	}
	*entry = workflowJournalEntry{}
	switch string(variant) {
	case journalVariantRunStarted:
		entry.Kind = journalRunStarted
		entry.Workflow = textField(fields, journalFieldWorkflow)
	case journalVariantDispatched:
		entry.Kind = journalStepDispatched
		entry.Label = textField(fields, journalFieldLabel)
		entry.Attempt = uint32(uintField(fields, journalFieldAttempt))
		entry.ReplyFrom = offsetsField(fields, journalFieldReplyFrom)
	case journalVariantCompleted:
		entry.Kind = journalStepCompleted
		entry.Label = textField(fields, journalFieldLabel)
		entry.Output = byteArrayField(fields, journalFieldOutput)
		entry.Agents = textArrayField(fields, journalFieldAgents)
		entry.Input = textField(fields, journalFieldInput)
	case journalVariantCompensated:
		entry.Kind = journalStepCompensated
		entry.Label = textField(fields, journalFieldLabel)
	default:
		// A variant this client does not know is another client's newer
		// entry. Refused rather than skipped: a fold that silently ignored
		// a record it cannot read would resume a run from a history it only
		// partly understood.
		return &CodecError{
			Reason: "unknown workflow journal variant " + string(variant)}
	}
	return nil
}

// offsetsValue encodes reply positions as an array of unsigned integers.
func offsetsValue(offsets []uint64) cborform.Array {
	array := make(cborform.Array, 0, len(offsets))
	for _, offset := range offsets {
		array = append(array, cborform.Uint(offset))
	}
	return array
}

// byteArrayValue encodes a body as an array of integers, the shape a Vec<u8>
// takes across the clients.
func byteArrayValue(body []byte) cborform.Array {
	array := make(cborform.Array, 0, len(body))
	for _, octet := range body {
		array = append(array, cborform.Uint(octet))
	}
	return array
}

// textArrayValue encodes a list of names.
func textArrayValue(names []string) cborform.Array {
	array := make(cborform.Array, 0, len(names))
	for _, name := range names {
		array = append(array, cborform.Text(name))
	}
	return array
}

// lookup is one field of an entry's field map, or nil when absent.
func lookup(fields cborform.Map, name string) cborform.Value {
	for _, field := range fields {
		if key, named := field.Key.(cborform.Text); named && string(key) == name {
			return field.Value
		}
	}
	return nil
}

func textField(fields cborform.Map, name string) string {
	if text, named := lookup(fields, name).(cborform.Text); named {
		return string(text)
	}
	return ""
}

func uintField(fields cborform.Map, name string) uint64 {
	if number, counted := lookup(fields, name).(cborform.Uint); counted {
		return uint64(number)
	}
	return 0
}

func offsetsField(fields cborform.Map, name string) []uint64 {
	array, listed := lookup(fields, name).(cborform.Array)
	if !listed {
		return nil
	}
	offsets := make([]uint64, 0, len(array))
	for _, item := range array {
		if number, counted := item.(cborform.Uint); counted {
			offsets = append(offsets, uint64(number))
		}
	}
	return offsets
}

func byteArrayField(fields cborform.Map, name string) []byte {
	array, listed := lookup(fields, name).(cborform.Array)
	if !listed {
		return nil
	}
	body := make([]byte, 0, len(array))
	for _, item := range array {
		if number, counted := item.(cborform.Uint); counted {
			body = append(body, byte(number))
		}
	}
	return body
}

func textArrayField(fields cborform.Map, name string) []string {
	array, listed := lookup(fields, name).(cborform.Array)
	if !listed {
		return nil
	}
	names := make([]string, 0, len(array))
	for _, item := range array {
		if text, named := item.(cborform.Text); named {
			names = append(names, string(text))
		}
	}
	return names
}
