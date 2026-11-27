package wire

import "strings"

// Matches reports whether the pattern covers a request's resource selector.
//
// A whole-feature pattern covers an unkeyed op too. Every other kind needs a
// selector to compare against.
func (pattern ResourcePattern) Matches(resource string, keyed bool) bool {
	switch {
	case pattern.Kind == ResourceAll || pattern.Kind == "":
		return true
	case !keyed:
		return false
	case pattern.Kind == ResourceLiteral:
		return resource == pattern.Value
	case pattern.Kind == ResourcePrefixKind:
		return strings.HasPrefix(resource, pattern.Value)
	default:
		// A pattern kind this build does not know matches nothing, so an
		// unrecognized grant is inert rather than a silent allow.
		return false
	}
}

// GrantsAllow reports whether the grants permit an action on a resource.
//
// Deny wins, and an empty set permits nothing: there is no allow to match. Pass
// keyed false for an op that names no resource.
func GrantsAllow(
	grants []Grant, feature Feature, action Action, resource string, keyed bool,
) bool {
	allowed := false
	for _, grant := range grants {
		if grant.Feature != feature || grant.Action != action ||
			!grant.Resource.Matches(resource, keyed) {
			continue
		}
		if grant.Effect == EffectDeny {
			return false
		}
		allowed = true
	}
	return allowed
}

// DelegatedAllow is the on-behalf-of check.
//
// An agent acting for a user is permitted an op only when both its own grants
// and the invoking user's grants permit it, so the agent can never exceed the
// user who invoked it.
func DelegatedAllow(
	agent, user []Grant, feature Feature, action Action, resource string, keyed bool,
) bool {
	return GrantsAllow(agent, feature, action, resource, keyed) &&
		GrantsAllow(user, feature, action, resource, keyed)
}

// ActionCount is the number of actions, including the unrecognized sink: the
// stride of the shared coarse-capability bitmask layout.
//
// The stride must cover every action so one feature's last action bit never
// collides with the next feature's first.
const ActionCount = 5

// featureOrdinals and actionOrdinals are the wire-revision-stable positions
// the shared bitmask layout is built from. They follow the order the contract
// declares, which is what every enforcer indexes by.
var featureOrdinals = map[Feature]int{
	FeatureKv: 0, FeatureMemory: 1, FeatureProjection: 2, FeatureFork: 3,
	FeatureGraph: 4, FeatureQuery: 5, FeatureAgent: 6, FeatureWorkflow: 7,
	FeatureDestination: 8, FeatureCheckpoint: 9, FeatureAuthz: 10,
	FeatureKvLease: 11, FeatureKvFence: 12, FeatureFilter: 13,
	FeatureSession: 14, FeatureUnrecognized: 15,
}

var actionOrdinals = map[Action]int{
	ActionRead: 0, ActionWrite: 1, ActionDelete: 2, ActionAdmin: 3,
	ActionUnrecognized: 4,
}

// ActionIndex is the bit index of a feature and action in the coarse
// capability bitmask.
//
// It is a pure function shared by every enforcer, so the streaming server and
// the managed backend cannot drift. The second result is false for a name this
// build does not place, which has no bit.
func ActionIndex(feature Feature, action Action) (int, bool) {
	featureOrdinal, named := featureOrdinals[feature]
	if !named {
		featureOrdinal = featureOrdinals[FeatureUnrecognized]
	}
	actionOrdinal, named := actionOrdinals[action]
	if !named {
		actionOrdinal = actionOrdinals[ActionUnrecognized]
	}
	return featureOrdinal*ActionCount + actionOrdinal, true
}

// GrantSpansStreams reports whether an allow grant reaches resources in more
// than one stream: a whole-feature grant, or a prefix that is not inside one
// stream scope.
//
// A deny grant only narrows access, so it never spans.
func GrantSpansStreams(grant Grant) bool {
	if grant.Effect == EffectDeny {
		return false
	}
	switch grant.Resource.Kind {
	case ResourceLiteral:
		return false
	case ResourcePrefixKind:
		_, inOneStream := prefixStream(grant.Resource.Value)
		return !inOneStream
	default:
		// A whole-feature grant, and any pattern kind this build does not
		// know, reaches past one stream.
		return true
	}
}

// GrantsStream is the one stream every allow grant of a role stays inside,
// which its audit events are attributed to.
//
// The second result is false when a grant reaches more than one stream or
// none, and when the role allows nothing.
func GrantsStream(grants []Grant) (string, bool) {
	first, found := "", false
	for _, grant := range grants {
		if grant.Effect != EffectAllow {
			continue
		}
		stream, inOneStream := grantStream(grant)
		if !inOneStream {
			return "", false
		}
		if found && stream != first {
			return "", false
		}
		first, found = stream, true
	}
	return first, found
}

// grantStream is the single stream one allow grant stays inside.
func grantStream(grant Grant) (string, bool) {
	switch grant.Resource.Kind {
	case ResourceLiteral:
		if stream, _, scoped := SplitScopedResource(grant.Resource.Value); scoped {
			return stream, true
		}
		stream, prefixed := strings.CutPrefix(grant.Resource.Value, StreamResourcePrefix)
		if !prefixed || stream == "" || strings.Contains(stream, "/") {
			return "", false
		}
		return stream, true
	case ResourcePrefixKind:
		return prefixStream(grant.Resource.Value)
	default:
		return "", false
	}
}

// prefixStream is the one stream a prefix stays inside, if any.
func prefixStream(prefix string) (string, bool) {
	if stream, _, scoped := SplitScopedResource(prefix); scoped {
		return stream, true
	}
	scoped, prefixed := strings.CutPrefix(prefix, StreamResourcePrefix)
	if !prefixed {
		return "", false
	}
	stream, trailing := strings.CutSuffix(scoped, "/")
	if !trailing || stream == "" || strings.Contains(stream, "/") {
		return "", false
	}
	return stream, true
}

// FeatureAction is the capability a managed command code authorizes against.
//
// The third result is false for a code with no capability semantics, which is
// gated another way: hello, backend hello, client metadata, batch, and the
// authorization band itself.
func FeatureAction(code uint32) (Feature, Action, bool) {
	switch code {
	case AgdxQueryCode:
		return FeatureQuery, ActionRead, true
	case AgdxGetProjectionCode, AgdxListProjectionsCode, AgdxGetSchemaCode,
		AgdxListSchemasCode, AgdxDecodeRecordCode:
		return FeatureProjection, ActionRead, true
	case AgdxRegisterSchemaCode:
		return FeatureProjection, ActionAdmin, true
	case AgdxKvGetCode, AgdxKvScanCode, AgdxKvNamespacesCode, AgdxKvExistsCode:
		return FeatureKv, ActionRead, true
	// A fenced compare-and-swap additionally needs read on its coordination
	// namespace. This map carries the primary target-namespace grant and the
	// enforcer checks the fence grant beside it.
	case AgdxKvSetCode, AgdxKvCasCode, AgdxKvCasFencedCode, AgdxKvPatchCode,
		AgdxKvExpireCode, AgdxKvCopyCode, AgdxKvMoveCode:
		return FeatureKv, ActionWrite, true
	// Lease lifecycle is its own feature: a write grant on a namespace must
	// never imply acquiring, renewing, or releasing the lease that fences it.
	case AgdxKvLeaseCode, AgdxKvLeaseRenewCode, AgdxKvReleaseCode:
		return FeatureKvLease, ActionAdmin, true
	case AgdxKvDeleteCode, AgdxKvDeleteManyCode:
		return FeatureKv, ActionDelete, true
	case AgdxForkListCode:
		return FeatureFork, ActionRead, true
	case AgdxForkCreateCode, AgdxForkPutCode:
		return FeatureFork, ActionWrite, true
	case AgdxForkPromoteCode:
		return FeatureFork, ActionAdmin, true
	case AgdxForkDeleteCode:
		return FeatureFork, ActionDelete, true
	case AgdxGraphQueryCode, AgdxGraphNeighborsCode:
		return FeatureGraph, ActionRead, true
	case AgdxGraphUpsertCode:
		return FeatureGraph, ActionWrite, true
	case AgdxSessionGetCode, AgdxSessionListCode, AgdxSessionEventsCode,
		AgdxSessionStateCode, AgdxSessionLinksCode, AgdxSessionSourcesCode,
		AgdxSessionChangesCode:
		return FeatureSession, ActionRead, true
	case AgdxGetFilterCode, AgdxListFiltersCode, AgdxListFilterRevisionsCode,
		AgdxGetFilterBindingCode, AgdxListFilterBindingsCode, AgdxFilterOperationCode:
		return FeatureFilter, ActionRead, true
	default:
		return "", "", false
	}
}
