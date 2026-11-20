package laser

import (
	"strconv"
	"strings"
)

// MessageID is where one message sits on the log: its partition and its
// offset within that partition.
//
// It is the causal parent a plain record names, and it rides the headers as
// "partition:offset". A record id would not do: a consumer that wants to read
// the record a reply answers needs to know where it is, and only the log
// position says that.
type MessageID struct {
	PartitionID uint32
	Offset      uint64
}

// NewMessageID is the message at one partition and offset.
func NewMessageID(partitionID uint32, offset uint64) MessageID {
	return MessageID{PartitionID: partitionID, Offset: offset}
}

// String is the "partition:offset" text the headers carry.
func (id MessageID) String() string {
	return strconv.FormatUint(uint64(id.PartitionID), 10) + ":" +
		strconv.FormatUint(id.Offset, 10)
}

// ParseMessageID reads the "partition:offset" form.
//
// A padded number is refused: it parses the same as its canonical spelling,
// so two clients would disagree about whether "01:42" and "1:42" name the
// same record. It is the only malformed spelling the integer parse below
// accepts on its own, which already refuses a sign, whitespace, a non-digit
// and an empty half. The missing-separator check below is for a clearer
// failure rather than a different one: without it the empty half would be
// refused by the parse anyway.
func ParseMessageID(text string) (MessageID, error) {
	partition, offset, found := strings.Cut(text, ":")
	if !found || padded(partition) || padded(offset) {
		return MessageID{}, invalidMessageID(text)
	}
	parsedPartition, err := strconv.ParseUint(partition, 10, 32)
	if err != nil {
		return MessageID{}, invalidMessageID(text)
	}
	parsedOffset, err := strconv.ParseUint(offset, 10, 64)
	if err != nil {
		return MessageID{}, invalidMessageID(text)
	}
	return MessageID{
		PartitionID: uint32(parsedPartition), Offset: parsedOffset}, nil
}

// padded reports a number written with a leading zero, which is the one
// spelling that does not round-trip through String.
func padded(text string) bool {
	return len(text) > 1 && text[0] == '0'
}

func invalidMessageID(text string) error {
	return &InvalidError{Reason: "invalid message id: " + text}
}
