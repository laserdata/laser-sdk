package wire

import (
	"fmt"
	"strconv"
	"strings"
)

// The managed backend's HTTP routes. A browser client feature-detects at the
// capabilities path and then uses the surface paths below.
const (
	// CapabilitiesPath is the feature-detection probe.
	CapabilitiesPath = "/agdx/capabilities"
	// QueryPath runs a query, and with the query as a parameter reads one.
	QueryPath = "/agdx/query"
	// ProjectionsPath lists and registers projections.
	ProjectionsPath = "/agdx/projections"
	// BindingsPath applies and removes bindings.
	BindingsPath = "/agdx/bindings"
	// SchemasPath lists and registers logical schemas.
	SchemasPath = "/agdx/schemas"
	// KvPath is the base of the key-value routes.
	KvPath = "/agdx/kv"
	// ForksPath lists and creates forks.
	ForksPath = "/agdx/forks"
	// GraphsPath lists and registers graph projections.
	GraphsPath = "/agdx/graphs"
	// GraphPath prefixes the traversal and neighbor routes.
	GraphPath = "/agdx/graph"
	// ClientsPath lists live connections with their advertised metadata.
	ClientsPath = "/agdx/clients"
	// AuthzWhoamiPath reads the caller's effective roles and grants.
	AuthzWhoamiPath = "/agdx/authz/whoami"
	// AuthzRolesPath lists governance roles.
	AuthzRolesPath = "/agdx/authz/roles"
	// FiltersPath lists saved consumer filters.
	FiltersPath = "/agdx/filters"
	// FilterMetricsPath reads filter evaluation metrics.
	FilterMetricsPath = "/agdx/filters/metrics"
	// FilterMutationsPath applies one catalog mutation and reports its outcome.
	FilterMutationsPath = "/agdx/filters/mutations"
	// FilterValidatePath validates and compiles a filter.
	FilterValidatePath = "/agdx/filters/validate"
	// FilterTestPath evaluates a filter against a supplied sample.
	FilterTestPath = "/agdx/filters/test"
	// FilterPreviewPath previews a filter over stored records.
	FilterPreviewPath = "/agdx/filters/preview"
	// FilterBindingsPath lists consumer-group filter bindings.
	FilterBindingsPath = "/agdx/filter-bindings"
	// GroupPoliciesPath reads a confirmed group policy.
	GroupPoliciesPath = "/agdx/group-policies"
	// SessionsPath is the base of the session reads for one named stream.
	SessionsPath = "/agdx/sessions"
)

// EncodePathSegment percent-encodes text as one URL path segment: every byte outside
// the unreserved set becomes a percent escape.
//
// Every name this surface routes on goes through it, because a stream, topic
// or group name may hold a slash, a question mark, or a hash.
func EncodePathSegment(text string) string {
	var encoded strings.Builder
	encoded.Grow(len(text))
	for index := 0; index < len(text); index++ {
		octet := text[index]
		switch {
		case octet >= 'a' && octet <= 'z', octet >= 'A' && octet <= 'Z',
			octet >= '0' && octet <= '9',
			octet == '-', octet == '.', octet == '_', octet == '~':
			encoded.WriteByte(octet)
		default:
			fmt.Fprintf(&encoded, "%%%02X", octet)
		}
	}
	return encoded.String()
}

// The session reads for one stream.
func SessionsPathFor(stream string) string {
	return SessionsPath + "/" + EncodePathSegment(stream)
}

func SessionChangesPath(stream string) string {
	return SessionsPathFor(stream) + "/changes"
}

func SessionPath(stream string, id ConversationID) string {
	return SessionsPathFor(stream) + "/" + EncodePathSegment(id.String())
}

func SessionEventsPath(stream string, id ConversationID) string {
	return SessionPath(stream, id) + "/events"
}

func SessionStatePath(stream string, id ConversationID) string {
	return SessionPath(stream, id) + "/state"
}

func SessionLinksPath(stream string, id ConversationID) string {
	return SessionPath(stream, id) + "/links"
}

func SessionSourcesPath(stream string, id ConversationID) string {
	return SessionPath(stream, id) + "/sources"
}

// The saved consumer-filter routes.
func FilterPath(id uint32) string {
	return FiltersPath + "/" + strconv.FormatUint(uint64(id), 10)
}

func FilterRevisionsPath(id uint32) string {
	return FilterPath(id) + "/revisions"
}

func FilterOperationPath(operationID U128) string {
	return FiltersPath + "/operations/" + operationID.String()
}

// FilterBindingPath reads one consumer group's binding. Each name is one
// percent-encoded segment.
func FilterBindingPath(stream, topic, group string) string {
	return FilterBindingsPath + "/" + EncodePathSegment(stream) + "/" +
		EncodePathSegment(topic) + "/" + EncodePathSegment(group)
}

// GroupPolicyPath reads one consumer group's confirmed policy.
func GroupPolicyPath(stream, topic, group string) string {
	return GroupPoliciesPath + "/" + EncodePathSegment(stream) + "/" +
		EncodePathSegment(topic) + "/" + EncodePathSegment(group)
}

// The authorization routes. A role name is a safelisted name, so it is not
// re-encoded.
func AuthzRolePath(name string) string {
	return AuthzRolesPath + "/" + name
}

func AuthzUserRolesPath(userID uint32) string {
	return "/agdx/authz/users/" + strconv.FormatUint(uint64(userID), 10) + "/roles"
}

// The graph routes. A stream-scoped name holds a slash, so every one is
// percent-encoded.
func GraphProjectionPath(id string) string {
	return GraphsPath + "/" + EncodePathSegment(id)
}

func GraphQueryPath(name string) string {
	return GraphPath + "/" + EncodePathSegment(name) + "/query"
}

func GraphNeighborsPath(name, node string) string {
	return GraphPath + "/" + EncodePathSegment(name) + "/neighbors/" + EncodePathSegment(node)
}

// The projection and schema routes.
func ProjectionPath(id string) string {
	return ProjectionsPath + "/" + EncodePathSegment(id)
}

func SchemaPath(id uint32) string {
	return SchemasPath + "/" + strconv.FormatUint(uint64(id), 10)
}

func SchemaDecodePath(id uint32) string {
	return SchemaPath(id) + "/decode"
}

// The key-value routes.
func KvNamespacePath(namespace string) string {
	return KvPath + "/" + EncodePathSegment(namespace)
}

// KvEntryPath addresses one entry. The key is the URL-safe unpadded base64
// form of the key bytes, the encoding this surface uses for every binary body.
func KvEntryPath(namespace, keyBase64 string) string {
	return KvNamespacePath(namespace) + "/" + EncodePathSegment(keyBase64)
}

// KvCasPath is a conditional write. The precondition rides the query string
// and the value rides the raw body, like a plain write.
func KvCasPath(namespace, keyBase64 string) string {
	return KvEntryPath(namespace, keyBase64) + "/cas"
}

// The fork routes.
func ForkPath(id string) string {
	return ForksPath + "/" + EncodePathSegment(id)
}

func ForkPromotePath(id string) string {
	return ForkPath(id) + "/promote"
}

func ForkRowsPath(id string) string {
	return ForkPath(id) + "/rows"
}
