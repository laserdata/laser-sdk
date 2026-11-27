package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// Effect is whether a grant allows or denies. Deny takes precedence.
type Effect string

const (
	// EffectAllow is the default.
	EffectAllow Effect = "allow"
	EffectDeny  Effect = "deny"
)

// Feature is the managed surface a grant applies to. Features map to the
// command bands, so a grant on one is orthogonal to a grant on another.
type Feature string

const (
	FeatureKv          Feature = "kv"
	FeatureMemory      Feature = "memory"
	FeatureProjection  Feature = "projection"
	FeatureFork        Feature = "fork"
	FeatureGraph       Feature = "graph"
	FeatureQuery       Feature = "query"
	FeatureAgent       Feature = "agent"
	FeatureWorkflow    Feature = "workflow"
	FeatureDestination Feature = "destination"
	FeatureCheckpoint  Feature = "checkpoint"
	// FeatureAuthz is administration of the authorization layer itself,
	// never derived from a command code.
	FeatureAuthz Feature = "authz"
	// FeatureKvLease is lease lifecycle in a coordination namespace. It is
	// split from the key-value feature so a state writer's write grant never
	// implies lease control.
	FeatureKvLease Feature = "kv_lease"
	// FeatureKvFence is the right to validate against a coordination
	// namespace's lease and fence rows without any right to mutate them.
	FeatureKvFence Feature = "kv_fence"
	// FeatureFilter is the saved consumer-filter catalog. Filtered reads
	// themselves need only native source permission.
	FeatureFilter Feature = "filter"
	// FeatureSession is managed reads of one stream's agent sessions.
	FeatureSession Feature = "session"
	// FeatureUnrecognized is a feature a newer peer used that this build does
	// not know.
	//
	// An unknown name decodes here rather than failing the whole grant set,
	// and it matches no request, so an unrecognized capability is inert:
	// default-deny, never a silent allow.
	FeatureUnrecognized Feature = "unrecognized"
)

// IsNamed reports whether this build names the feature.
func (feature Feature) IsNamed() bool {
	switch feature {
	case FeatureKv, FeatureMemory, FeatureProjection, FeatureFork, FeatureGraph,
		FeatureQuery, FeatureAgent, FeatureWorkflow, FeatureDestination,
		FeatureCheckpoint, FeatureAuthz, FeatureKvLease, FeatureKvFence,
		FeatureFilter, FeatureSession:
		return true
	default:
		return false
	}
}

// MarshalCBOR writes the snake_case name.
func (feature Feature) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(string(feature)), nil
}

// UnmarshalCBOR reads the name, sinking an unknown one into the inert
// passthrough so one foreign grant cannot fail a whole grant set.
func (feature *Feature) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a feature", item)
	}
	candidate := Feature(text)
	if !candidate.IsNamed() {
		candidate = FeatureUnrecognized
	}
	*feature = candidate
	return nil
}

// Action is what a grant permits on its feature. Every action widens the
// coarse mask by one bit per feature, so features split and actions do not
// grow.
type Action string

const (
	ActionRead   Action = "read"
	ActionWrite  Action = "write"
	ActionDelete Action = "delete"
	ActionAdmin  Action = "admin"
	// ActionUnrecognized is an action a newer peer used that this build does
	// not know. It matches no request, so it is inert: default-deny.
	ActionUnrecognized Action = "unrecognized"
)

// IsNamed reports whether this build names the action.
func (action Action) IsNamed() bool {
	switch action {
	case ActionRead, ActionWrite, ActionDelete, ActionAdmin:
		return true
	default:
		return false
	}
}

// MarshalCBOR writes the snake_case name.
func (action Action) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(string(action)), nil
}

// UnmarshalCBOR reads the name, sinking an unknown one into the inert
// passthrough.
func (action *Action) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as an action", item)
	}
	candidate := Action(text)
	if !candidate.IsNamed() {
		candidate = ActionUnrecognized
	}
	*action = candidate
	return nil
}

// ResourceKind is how a resource pattern matches a request's selector.
type ResourceKind string

const (
	// ResourceAll is the whole feature, ignoring the value. The default.
	ResourceAll ResourceKind = "all"
	// ResourceLiteral is one exact resource name.
	ResourceLiteral ResourceKind = "literal"
	// ResourcePrefixKind is every resource name under a prefix.
	ResourcePrefixKind ResourceKind = "prefix"
)

// ResourcePattern is a resource selector on a grant: literal, prefixed, or the
// whole feature.
type ResourcePattern struct {
	Kind  ResourceKind `cbor:"kind"`
	Value string       `cbor:"value,omitempty"`
}

// AllResources is the whole-feature pattern.
func AllResources() ResourcePattern { return ResourcePattern{Kind: ResourceAll} }

// LiteralResource is an exact-name pattern.
func LiteralResource(value string) ResourcePattern {
	return ResourcePattern{Kind: ResourceLiteral, Value: value}
}

// PrefixResource matches every name under a prefix.
func PrefixResource(value string) ResourcePattern {
	return ResourcePattern{Kind: ResourcePrefixKind, Value: value}
}

// Grant is one capability grant: an effect on a feature and action, optionally
// scoped to a resource pattern.
type Grant struct {
	Effect   Effect          `cbor:"effect"`
	Feature  Feature         `cbor:"feature"`
	Action   Action          `cbor:"action"`
	Resource ResourcePattern `cbor:"resource"`
}

// Role is a named set of grants, bound to users. A user's effective capability
// is the union of the grants of every bound role, minus any matching deny.
type Role struct {
	Name   string  `cbor:"name"`
	Grants []Grant `cbor:"grants"`
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *Effect) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "effect", EffectAllow, EffectDeny)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *ResourceKind) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "resource kind", ResourceAll, ResourceLiteral, ResourcePrefixKind)
}
