package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// BackendDescriptorVersion is the version a backend descriptor carries, so a
// reader can tell a shape it understands from one it does not.
const BackendDescriptorVersion uint32 = 1

// BackendMode is what a materialization backend is for.
type BackendMode string

const (
	// BackendOperational serves the managed operational surfaces.
	BackendOperational BackendMode = "operational"
)

// BackendImplementation is the engine behind a backend, for display and for
// an operator's own compatibility rules. It is opaque to the protocol.
type BackendImplementation struct {
	Kind    string `cbor:"kind"`
	Version string `cbor:"version"`
}

// BackendDesiredState is the state an operator asked a backend to be in.
type BackendDesiredState string

const (
	BackendDisabledState BackendDesiredState = "disabled"
	BackendEnabledState  BackendDesiredState = "enabled"
)

// BackendObservedState is the state a backend is actually in.
type BackendObservedState string

const (
	BackendObservedDisabled    BackendObservedState = "disabled"
	BackendObservedStarting    BackendObservedState = "starting"
	BackendObservedReady       BackendObservedState = "ready"
	BackendObservedDegraded    BackendObservedState = "degraded"
	BackendObservedUnavailable BackendObservedState = "unavailable"
)

// BackendReadinessCode is why a backend is not ready.
type BackendReadinessCode string

const (
	ReadinessDisabled                BackendReadinessCode = "disabled"
	ReadinessConfigurationPending    BackendReadinessCode = "configuration_pending"
	ReadinessConfigurationRejected   BackendReadinessCode = "configuration_rejected"
	ReadinessCredentialUnavailable   BackendReadinessCode = "credential_unavailable"
	ReadinessObjectStoreUnavailable  BackendReadinessCode = "object_store_unavailable"
	ReadinessCatalogUnavailable      BackendReadinessCode = "catalog_unavailable"
	ReadinessQueryRuntimeUnavailable BackendReadinessCode = "query_runtime_unavailable"
	ReadinessGenerationMismatch      BackendReadinessCode = "generation_mismatch"
	ReadinessProbeFailed             BackendReadinessCode = "probe_failed"
	// ReadinessQueryIdentityRejected means the provisioned or configured
	// raw SQL query identity failed its privilege check, so raw SQL is
	// refused while materialization continues.
	ReadinessQueryIdentityRejected BackendReadinessCode = "query_identity_rejected"
)

// BackendReadinessReason is one reason a backend is not ready, with optional
// operator-facing detail.
type BackendReadinessReason struct {
	Code   BackendReadinessCode `cbor:"code"`
	Detail *string              `cbor:"detail,omitempty"`
}

// BackendReadiness is whether a backend can serve, and why not when it cannot.
type BackendReadiness struct {
	Ready            bool                     `cbor:"ready"`
	Reasons          []BackendReadinessReason `cbor:"reasons"`
	ObservedAtMicros uint64                   `cbor:"observed_at_micros"`
}

// ReadyBackend is a backend that can serve, observed at an instant.
func ReadyBackend(observedAtMicros uint64) BackendReadiness {
	return BackendReadiness{
		Ready: true, Reasons: []BackendReadinessReason{},
		ObservedAtMicros: observedAtMicros,
	}
}

// UnreadyBackend is a backend that cannot serve, for one reason.
func UnreadyBackend(code BackendReadinessCode) BackendReadiness {
	return BackendReadiness{
		Reasons: []BackendReadinessReason{{Code: code}},
	}
}

// Validate checks that readiness and its reasons agree.
//
// A ready backend with failure reasons, or an unready one with none, would
// leave a client unable to tell whether it may route work there.
func (readiness BackendReadiness) Validate() error {
	if readiness.Ready {
		if len(readiness.Reasons) != 0 || readiness.ObservedAtMicros == 0 {
			return &InvalidError{Reason: "ready backend requires an observation " +
				"time and no failure reasons"}
		}
		return nil
	}
	if len(readiness.Reasons) == 0 {
		return &InvalidError{Reason: "an unready backend must name a reason"}
	}
	return nil
}

// QueryCapabilities is what a backend's query surface serves.
//
// A client reads this rather than assuming: a query that names a dialect or a
// consistency the backend does not serve fails cleanly instead of silently
// answering differently.
type QueryCapabilities struct {
	Dialects     []SqlDialect      `cbor:"dialects"`
	Consistency  []Consistency     `cbor:"consistency"`
	LogicalTypes []LogicalTypeKind `cbor:"logical_types"`
	RawSQL       bool              `cbor:"raw_sql"`
	// Aggregates are the aggregate functions the backend computes.
	Aggregates []AggFunc `cbor:"aggregates,omitempty"`
	// VectorSearch reports whether the backend serves a vector query.
	VectorSearch bool `cbor:"vector_search"`
	// TextSearch reports whether the backend serves a text query.
	TextSearch bool `cbor:"text_search"`
}

// Validate checks that the announced coverage is usable at all.
func (capabilities QueryCapabilities) Validate() error {
	if len(capabilities.Dialects) == 0 || len(capabilities.Consistency) == 0 ||
		len(capabilities.LogicalTypes) == 0 {
		return &InvalidError{Reason: "query capabilities require dialect, " +
			"consistency, and logical-type coverage"}
	}
	for label, values := range map[string][]string{
		"query dialect":      stringsOf(capabilities.Dialects),
		"query consistency":  stringsOf(capabilities.Consistency),
		"query logical type": stringsOf(capabilities.LogicalTypes),
		"query aggregate":    stringsOf(capabilities.Aggregates),
	} {
		if err := ensureUnique(label, values); err != nil {
			return err
		}
	}
	return nil
}

// stringsOf renders a list of string-backed names for a uniqueness check.
func stringsOf[T ~string](values []T) []string {
	names := make([]string, len(values))
	for index, value := range values {
		names[index] = string(value)
	}
	return names
}

func ensureUnique(label string, values []string) error {
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		if seen[value] {
			return &InvalidError{Reason: fmt.Sprintf(
				"%s %q is announced twice", label, value)}
		}
		seen[value] = true
	}
	return nil
}

// SchemaCapabilities is what a backend's schema surface serves.
type SchemaCapabilities struct {
	LogicalSchema   bool `cbor:"logical_schema"`
	ArrowIPCStream  bool `cbor:"arrow_ipc_stream"`
	SchemaEvolution bool `cbor:"schema_evolution"`
}

// BackendLimits are the bounds a backend enforces, so a client can shape a
// request that will be accepted rather than discovering a cap by failing.
type BackendLimits struct {
	MaxQueryRows         uint64 `cbor:"max_query_rows"`
	MaxQueryBytes        uint64 `cbor:"max_query_bytes"`
	MaxScanBytes         uint64 `cbor:"max_scan_bytes"`
	MaxQueryMicros       uint64 `cbor:"max_query_micros"`
	MaxConcurrentQueries uint32 `cbor:"max_concurrent_queries"`
	MaxSchemaFields      uint32 `cbor:"max_schema_fields"`
	// MaxPayloadBytes is the largest record body a whole-record row stores.
	MaxPayloadBytes uint64 `cbor:"max_payload_bytes"`
}

// BackendDescriptor is one materialization backend a deployment exposes:
// its identity, its state, and exactly what it serves.
type BackendDescriptor struct {
	DescriptorVersion            uint32                `cbor:"descriptor_version"`
	ResourceID                   BackendResourceID     `cbor:"resource_id"`
	Mode                         BackendMode           `cbor:"mode"`
	Label                        string                `cbor:"label"`
	Implementation               BackendImplementation `cbor:"implementation"`
	ObservedBackendGeneration    uint64                `cbor:"observed_backend_generation"`
	RuntimeConfigurationRevision uint64                `cbor:"runtime_configuration_revision"`
	DesiredState                 BackendDesiredState   `cbor:"desired_state"`
	ObservedState                BackendObservedState  `cbor:"observed_state"`
	Readiness                    BackendReadiness      `cbor:"readiness"`
	Query                        *QueryCapabilities    `cbor:"query,omitempty"`
	Schema                       SchemaCapabilities    `cbor:"schema"`
	Limits                       BackendLimits         `cbor:"limits"`
}

// NewBackendDescriptor builds a descriptor at the current version.
func NewBackendDescriptor(
	resourceID BackendResourceID, mode BackendMode, label string,
	implementation BackendImplementation,
	observedBackendGeneration, runtimeConfigurationRevision uint64,
) BackendDescriptor {
	return BackendDescriptor{
		DescriptorVersion:            BackendDescriptorVersion,
		ResourceID:                   resourceID,
		Mode:                         mode,
		Label:                        label,
		Implementation:               implementation,
		ObservedBackendGeneration:    observedBackendGeneration,
		RuntimeConfigurationRevision: runtimeConfigurationRevision,
		DesiredState:                 BackendDisabledState,
		ObservedState:                BackendObservedDisabled,
		Readiness:                    UnreadyBackend(ReadinessDisabled),
	}
}

// Validate checks the descriptor's version, identity and readiness.
func (descriptor BackendDescriptor) Validate() error {
	if descriptor.DescriptorVersion != BackendDescriptorVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"backend descriptor version must be %d, got %d",
			BackendDescriptorVersion, descriptor.DescriptorVersion)}
	}
	if descriptor.ResourceID.IsZero() {
		return &InvalidError{Reason: "a backend descriptor must name its resource"}
	}
	if err := descriptor.Readiness.Validate(); err != nil {
		return err
	}
	if descriptor.Query != nil {
		return descriptor.Query.Validate()
	}
	return nil
}

// FilterAnnounce is the consumer-filter evaluation a server serves.
//
// A client refuses to run a filter the server would evaluate differently,
// which is what keeps a locally previewed filter and a server-side one
// agreeing on which records they select.
type FilterAnnounce struct {
	EvaluatorVersion uint32        `cbor:"evaluator_version"`
	Codecs           []FilterCodec `cbor:"codecs,omitempty"`
	// MaxWaitMs is the longest filtered wait the server holds. Zero means it
	// serves no filtered wait, so a reader paces idle rounds itself.
	MaxWaitMs uint32 `cbor:"max_wait_ms,omitempty"`
}

// ServedFilterAnnounce is what this build of the evaluator serves.
func ServedFilterAnnounce() FilterAnnounce {
	return FilterAnnounce{
		EvaluatorVersion: FilterEvaluatorVersion,
		Codecs: []FilterCodec{
			CodecJSON, CodecHeadersOnly, CodecAvro, CodecProtobuf, CodecCBOR,
		},
	}
}

// Evaluates reports whether a filter at this evaluator version and codec runs
// here exactly as it would in the client that built it.
func (announce FilterAnnounce) Evaluates(evaluatorVersion uint32, codec FilterCodec) bool {
	if announce.EvaluatorVersion != evaluatorVersion {
		return false
	}
	for _, served := range announce.Codecs {
		if served == codec {
			return true
		}
	}
	return false
}

// BackendAnnounce is the managed backend's capability announcement to the
// streaming server, sent over their private socket on connect.
//
// The streaming server caches the versions and the advertised backends and
// relays them verbatim when it answers a client probe, so it never hardcodes
// feature bits or backend identities the backend may or may not serve. That
// makes the backend the single source of its own capability truth.
type BackendAnnounce struct {
	Versions OpVersions `cbor:"versions"`
	// Ready defaults to true and is skipped when true, so an announce from a
	// ready backend stays byte-identical to one written before the field
	// existed, and an older announce reads as ready rather than unavailable.
	Ready    backendReady        `cbor:"ready,omitempty"`
	Backends []BackendDescriptor `cbor:"backends,omitempty"`
	Topology *WireTopology       `cbor:"topology,omitempty"`
	Filters  *FilterAnnounce     `cbor:"filters,omitempty"`
}

// backendReady is the announce's ready flag, whose default is true.
type backendReady bool

// IsDefaultValue reports the default, which is skipped on encode.
func (ready backendReady) IsDefaultValue() bool { return bool(ready) }

// NewBackendAnnounce builds a ready announcement.
func NewBackendAnnounce(versions OpVersions) BackendAnnounce {
	return BackendAnnounce{Versions: versions, Ready: true}
}

// IsReady reports whether the backend announced itself as able to serve.
func (announce BackendAnnounce) IsReady() bool { return bool(announce.Ready) }

// UnmarshalCBOR decodes an announce with ready defaulting to true.
//
// A missing field must not read as unavailable: an announce written before the
// flag existed came from a backend that was serving.
func (announce *BackendAnnounce) UnmarshalCBOR(item cborform.Value) error {
	type plain BackendAnnounce
	decoded := plain{Ready: true}
	if err := fromValueInto(item, &decoded); err != nil {
		return err
	}
	*announce = BackendAnnounce(decoded)
	return nil
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *BackendDesiredState) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "backend desired state", BackendDisabledState, BackendEnabledState)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *BackendMode) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "backend mode", BackendOperational)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *BackendObservedState) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "backend observed state", BackendObservedDisabled, BackendObservedStarting, BackendObservedReady, BackendObservedDegraded, BackendObservedUnavailable)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *BackendReadinessCode) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "backend readiness code", ReadinessDisabled, ReadinessConfigurationPending, ReadinessConfigurationRejected, ReadinessCredentialUnavailable, ReadinessObjectStoreUnavailable, ReadinessCatalogUnavailable, ReadinessQueryRuntimeUnavailable, ReadinessGenerationMismatch, ReadinessProbeFailed, ReadinessQueryIdentityRejected)
}
