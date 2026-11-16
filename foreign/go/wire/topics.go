package wire

import (
	"fmt"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// The ops stream and its topics. The control surface rides a dedicated _agdx
// ops stream, separate from the user data stream, so topic names drop the
// agdx. prefix. A pinned wire contract: drift breaks the managed control
// surface silently.
const (
	// OpsStream is the ops stream name.
	OpsStream = "_agdx"
	// ControlTopic carries projection control commands.
	ControlTopic = "control.commands"
	// DLQTopic is the universal dead-letter topic, and capsules carry a kind.
	DLQTopic = "dlq"
	// ChangesTopic carries one change record per committed projector batch
	// for a binding that opted into notify.
	ChangesTopic = "changes"
)

// The agent topics. Sessions ride agent.sessions with the others as satellites.
const (
	AgentSessions   = "agent.sessions"
	AgentStreams    = "agent.streams"
	AgentHeartbeats = "agent.heartbeats"
	AgentControl    = "agent.control"
	AgentMemory     = "agent.memory"
	AgentDLQ        = "agent.dlq"
	AgentAudit      = "agent.audit"
	AgentJournal    = "agent.workflow_journal"
	AgentRegistry   = "agent.registry"
)

// StreamOpsTopic names the ops topic holding one stream's records of an ops
// surface under stream tenancy: stream:<stream>/_agdx/<surface>.
func StreamOpsTopic(stream, surface string) string {
	return ScopedResource(stream, fmt.Sprintf("_agdx/%s", surface))
}

// StreamResourcePrefix marks a stream-scoped managed resource name.
const StreamResourcePrefix = "stream:"

// StreamResource names the stream resource itself.
func StreamResource(stream string) string {
	return StreamResourcePrefix + stream
}

// ScopedResource scopes a managed resource name to a stream, the form a
// deployment with stream tenancy enforces. An already-scoped name is returned
// unchanged, so scoping twice is harmless.
func ScopedResource(stream, local string) string {
	if strings.HasPrefix(local, StreamResourcePrefix) {
		return local
	}
	return fmt.Sprintf("%s%s/%s", StreamResourcePrefix, stream, local)
}

// SplitScopedResource splits a scoped name into its stream and local parts.
// The second result is false for a bare name, for the stream resource itself,
// and for a name with an empty stream or local part. Stream names never
// contain a slash, so the first slash ends the stream.
func SplitScopedResource(name string) (stream, local string, ok bool) {
	rest, found := strings.CutPrefix(name, StreamResourcePrefix)
	if !found {
		return "", "", false
	}
	stream, local, found = strings.Cut(rest, "/")
	if !found || stream == "" || local == "" {
		return "", "", false
	}
	return stream, local, true
}

// Tenancy is how a deployment scopes its managed resources, as its replicated
// authorization state records it.
type Tenancy string

const (
	// TenancyDeployment is one deployment-wide namespace, where every grant
	// pattern is accepted. The default.
	TenancyDeployment Tenancy = "deployment"
	// TenancyStream makes each stream an account: a role may allow access
	// inside one stream only, and the root role is bound to root alone.
	TenancyStream Tenancy = "stream"
)

// IsDeployment reports the deployment-wide default.
func (tenancy Tenancy) IsDeployment() bool {
	return tenancy == TenancyDeployment || tenancy == ""
}

// IsDefaultValue reports the default, which is skipped on encode.
//
// The default is a named value rather than an empty one, so the structural
// emptiness test cannot see it and a frame that spelled it out would differ
// from one that left it off.
func (tenancy Tenancy) IsDefaultValue() bool { return tenancy.IsDeployment() }

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *Tenancy) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "tenancy", TenancyDeployment, TenancyStream)
}
