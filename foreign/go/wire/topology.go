package wire

import "github.com/laserdata/laser-sdk/foreign/go/internal/cborform"

// Default names for the log-first managed mutation topics.
const (
	DefaultKvMutationsTopic    = "kv.mutations"
	DefaultForkMutationsTopic  = "fork.mutations"
	DefaultGraphMutationsTopic = "graph.mutations"
)

// WireTopology is the stream and topic names a deployment's managed backend
// and streaming server use, provisioned or discovered rather than hardcoded.
//
// A deployment that never configures this sees exactly the pre-topology names,
// which live in one place here.
type WireTopology struct {
	OpsStream    string `cbor:"ops_stream"`
	ControlTopic string `cbor:"control_topic"`
	DLQTopic     string `cbor:"dlq_topic"`
	ChangesTopic string `cbor:"changes_topic"`
	// The log-first mutation topics.
	KvMutationsTopic    string `cbor:"kv_mutations_topic"`
	ForkMutationsTopic  string `cbor:"fork_mutations_topic"`
	GraphMutationsTopic string `cbor:"graph_mutations_topic"`
}

// DefaultWireTopology is today's topic constants.
func DefaultWireTopology() WireTopology {
	return WireTopology{
		OpsStream:           OpsStream,
		ControlTopic:        ControlTopic,
		DLQTopic:            DLQTopic,
		ChangesTopic:        ChangesTopic,
		KvMutationsTopic:    DefaultKvMutationsTopic,
		ForkMutationsTopic:  DefaultForkMutationsTopic,
		GraphMutationsTopic: DefaultGraphMutationsTopic,
	}
}

// UnmarshalCBOR decodes a topology over the defaults rather than over zero
// values.
//
// Every field carries its own default name, so a partial announcement from an
// older peer that omits a newer field still decodes to that field's real
// topic name instead of an empty string, which would address nothing.
func (topology *WireTopology) UnmarshalCBOR(item cborform.Value) error {
	// An alias avoids recursing back into this method.
	type plain WireTopology
	decoded := plain(DefaultWireTopology())
	if err := fromValueInto(item, &decoded); err != nil {
		return err
	}
	*topology = WireTopology(decoded)
	return nil
}
