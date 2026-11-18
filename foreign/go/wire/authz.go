package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// RoleBinding is the roles bound to one user, by the server-stamped user id.
type RoleBinding struct {
	UserID uint32   `cbor:"user_id"`
	Roles  []string `cbor:"roles"`
}

// ValidateRoleName is the canonical role-name rule, shared by the SDK, the
// server and the console, so a name one tier accepts the next never rejects.
//
// It is enforced on define and bind, never on replay: a journaled role loads
// regardless, so tightening the rule cannot strand existing state.
func ValidateRoleName(name string) error {
	return ValidateSafelistedName("role name", name, MaxRoleNameBytes)
}

// The authorization read and write requests.
type (
	// WhoamiReq requests the caller's own effective capabilities.
	WhoamiReq struct {
		V uint32 `cbor:"v"`
	}

	// ListRolesReq lists roles, optionally filtered. Absent filters list
	// every role.
	ListRolesReq struct {
		V          uint32  `cbor:"v"`
		NamePrefix *string `cbor:"name_prefix,omitempty"`
		Search     *string `cbor:"search,omitempty"`
	}

	// GetRoleReq requests one role by name.
	GetRoleReq struct {
		V    uint32 `cbor:"v"`
		Name string `cbor:"name"`
	}

	// GetBindingsReq requests one user's bound role names.
	GetBindingsReq struct {
		V      uint32 `cbor:"v"`
		UserID uint32 `cbor:"user_id"`
	}

	// DefineRoleReq defines or replaces a role, carrying the full grant set.
	DefineRoleReq struct {
		V          uint32  `cbor:"v"`
		Role       Role    `cbor:"role"`
		MutationID *string `cbor:"mutation_id,omitempty"`
	}

	// DeleteRoleReq deletes a role by name.
	DeleteRoleReq struct {
		V          uint32  `cbor:"v"`
		Name       string  `cbor:"name"`
		MutationID *string `cbor:"mutation_id,omitempty"`
	}

	// BindRolesReq binds roles to a user, replacing the user's whole role set.
	BindRolesReq struct {
		V      uint32   `cbor:"v"`
		UserID uint32   `cbor:"user_id"`
		Roles  []string `cbor:"roles"`
		// ExpectRevision is a compare-and-swap precondition: apply only if
		// the binding's current revision equals this. Absent is an
		// unconditional replace, so a caller opts into optimistic
		// concurrency. A mismatch fails as a conflict.
		ExpectRevision *uint64 `cbor:"expect_revision,omitempty"`
		MutationID     *string `cbor:"mutation_id,omitempty"`
	}
)

// The authorization replies.
type (
	// WhoamiReply is the caller's bound roles and their flattened grants.
	WhoamiReply struct {
		V      uint32   `cbor:"v"`
		Roles  []string `cbor:"roles"`
		Grants []Grant  `cbor:"grants"`
	}

	// ListRolesReply is every matching role with its full grant set.
	ListRolesReply struct {
		V     uint32 `cbor:"v"`
		Roles []Role `cbor:"roles"`
	}

	// BindingsReply is one user's bound role names.
	BindingsReply struct {
		V     uint32   `cbor:"v"`
		Roles []string `cbor:"roles"`
		// Revision is the binding the roles were read from, the value a
		// bind names to replace exactly this binding. It is zero for a user
		// that was never bound.
		Revision uint64 `cbor:"revision"`
	}
)

// AuthzSubjectKind discriminates which subject a history read covers.
type AuthzSubjectKind uint8

const (
	// SubjectRole is one role by name.
	SubjectRole AuthzSubjectKind = iota
	// SubjectBinding is one user's bindings.
	SubjectBinding
	// SubjectAll is every authorization change.
	SubjectAll
	// SubjectStream is every change attributed to one stream: roles whose
	// grants stay inside it and bindings of such roles. What a stream's owner
	// may audit.
	SubjectStream
)

// AuthzSubject is which authorization subject a history request reads.
type AuthzSubject struct {
	Kind   AuthzSubjectKind
	Name   string
	UserID uint32
}

// The subject constructors.
func RoleSubject(name string) AuthzSubject {
	return AuthzSubject{Kind: SubjectRole, Name: name}
}
func BindingSubject(userID uint32) AuthzSubject {
	return AuthzSubject{Kind: SubjectBinding, UserID: userID}
}
func AllSubjects() AuthzSubject { return AuthzSubject{Kind: SubjectAll} }
func StreamSubject(stream string) AuthzSubject {
	return AuthzSubject{Kind: SubjectStream, Name: stream}
}

// The externally tagged, snake_case subject names.
const (
	subjectRoleVariant    = "role"
	subjectBindingVariant = "binding"
	subjectAllVariant     = "all"
	subjectStreamVariant  = "stream"
)

type bindingSubjectFields struct {
	UserID uint32 `cbor:"user_id"`
}

// MarshalCBOR writes the externally tagged subject.
func (subject AuthzSubject) MarshalCBOR() (cborform.Value, error) {
	switch subject.Kind {
	case SubjectRole:
		return taggedVariant(subjectRoleVariant, cborform.Text(subject.Name)), nil
	case SubjectBinding:
		return marshalVariantStruct(subjectBindingVariant,
			bindingSubjectFields{UserID: subject.UserID})
	case SubjectAll:
		return unitVariant(subjectAllVariant), nil
	case SubjectStream:
		return taggedVariant(subjectStreamVariant, cborform.Text(subject.Name)), nil
	default:
		return nil, fmt.Errorf("wire: unknown authorization subject %d", subject.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged subject.
func (subject *AuthzSubject) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	switch name {
	case subjectRoleVariant, subjectStreamVariant:
		if err := requirePayload("AuthzSubject", name, payload); err != nil {
			return err
		}
		text, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: %q carries a name, got %T", name, payload)
		}
		kind := SubjectRole
		if name == subjectStreamVariant {
			kind = SubjectStream
		}
		*subject = AuthzSubject{Kind: kind, Name: string(text)}
		return nil
	case subjectBindingVariant:
		if err := requirePayload("AuthzSubject", name, payload); err != nil {
			return err
		}
		var fields bindingSubjectFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*subject = BindingSubject(fields.UserID)
		return nil
	case subjectAllVariant:
		*subject = AllSubjects()
		return nil
	default:
		return unknownVariant("AuthzSubject", name)
	}
}

// AuthzHistoryReq reads the authorization change history for a subject, paged
// by revision: the audit surface for who granted what, and when.
type AuthzHistoryReq struct {
	V             uint32       `cbor:"v"`
	Subject       AuthzSubject `cbor:"subject"`
	AfterRevision *uint64      `cbor:"after_revision,omitempty"`
	Limit         uint32       `cbor:"limit"`
}

// Tenancy is how a deployment scopes its managed resources, as its replicated
// authorization state records it. It is declared alongside the topic helpers.

// AuthzEventKindTag discriminates what an authorization event recorded.
type AuthzEventKindTag uint8

const (
	// EventRoleDefined means a role was defined or replaced.
	EventRoleDefined AuthzEventKindTag = iota
	// EventRoleDeleted means a role was deleted.
	EventRoleDeleted
	// EventRolesBound means a user's role set was rebound.
	EventRolesBound
	// EventTenancySet means the deployment's tenancy was set.
	EventTenancySet
)

type rolesBoundFields struct {
	UserID uint32   `cbor:"user_id"`
	Roles  []string `cbor:"roles"`
}

// AuthzEventKind is what an authorization event recorded.
type AuthzEventKind struct {
	Kind    AuthzEventKindTag
	Name    string
	UserID  uint32
	Roles   []string
	Tenancy Tenancy
}

// The externally tagged, snake_case event names.
const (
	eventRoleDefinedVariant = "role_defined"
	eventRoleDeletedVariant = "role_deleted"
	eventRolesBoundVariant  = "roles_bound"
	eventTenancySetVariant  = "tenancy_set"
)

// MarshalCBOR writes the externally tagged event kind.
func (kind AuthzEventKind) MarshalCBOR() (cborform.Value, error) {
	switch kind.Kind {
	case EventRoleDefined:
		return taggedVariant(eventRoleDefinedVariant, cborform.Text(kind.Name)), nil
	case EventRoleDeleted:
		return taggedVariant(eventRoleDeletedVariant, cborform.Text(kind.Name)), nil
	case EventRolesBound:
		return marshalVariantStruct(eventRolesBoundVariant, rolesBoundFields{
			UserID: kind.UserID, Roles: kind.Roles,
		})
	case EventTenancySet:
		return taggedVariant(eventTenancySetVariant, cborform.Text(string(kind.Tenancy))), nil
	default:
		return nil, fmt.Errorf("wire: unknown authorization event %d", kind.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged event kind.
func (kind *AuthzEventKind) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("AuthzEventKind", name, payload); err != nil {
		return err
	}
	switch name {
	case eventRoleDefinedVariant, eventRoleDeletedVariant:
		text, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: %q carries a role name, got %T", name, payload)
		}
		tag := EventRoleDefined
		if name == eventRoleDeletedVariant {
			tag = EventRoleDeleted
		}
		*kind = AuthzEventKind{Kind: tag, Name: string(text)}
		return nil
	case eventRolesBoundVariant:
		var fields rolesBoundFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*kind = AuthzEventKind{
			Kind: EventRolesBound, UserID: fields.UserID, Roles: fields.Roles,
		}
		return nil
	case eventTenancySetVariant:
		text, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: %q carries a tenancy, got %T", name, payload)
		}
		*kind = AuthzEventKind{Kind: EventTenancySet, Tenancy: Tenancy(text)}
		return nil
	default:
		return unknownVariant("AuthzEventKind", name)
	}
}

// AuthzEvent is one recorded authorization change: its revision, who made it,
// when, and what.
type AuthzEvent struct {
	Revision uint64         `cbor:"revision"`
	Actor    string         `cbor:"actor"`
	AtMicros uint64         `cbor:"at_micros"`
	Op       AuthzEventKind `cbor:"op"`
	// Stream is the stream the change is attributed to, absent for a change
	// that concerns no single stream.
	Stream *string `cbor:"stream,omitempty"`
}

// AuthzHistoryReply is a page of authorization change history.
type AuthzHistoryReply struct {
	V                 uint32       `cbor:"v"`
	Events            []AuthzEvent `cbor:"events"`
	NextAfterRevision *uint64      `cbor:"next_after_revision,omitempty"`
}

// AuthzErrorKind discriminates an authorization command failure.
type AuthzErrorKind uint8

const (
	AuthzUnsupported AuthzErrorKind = iota
	AuthzUnauthorized
	AuthzUnknownRole
	// AuthzInvalidName means a define or bind named a role failing the
	// role-name rule.
	AuthzInvalidName
	// AuthzConflict means a compare-and-swap bind lost its precondition: a
	// concurrent admin got there first, so the caller re-reads and retries.
	AuthzConflict
	AuthzVersionMismatch
	// AuthzTenancyViolation means a grant reaches past one stream while the
	// server runs with stream tenancy.
	AuthzTenancyViolation
	// AuthzInvalidGrant means a grant failed the grant grammar, so it would
	// never match a request and could only widen or narrow a role by
	// accident.
	AuthzInvalidGrant
	// AuthzMutationConflict means a mutation id was already used for another
	// mutation, so applying this one would overwrite a recorded outcome.
	AuthzMutationConflict
	// AuthzResourceLimit means the receipt capacity is exhausted.
	AuthzResourceLimit
)

type authzConflictFields struct {
	CurrentRevision uint64 `cbor:"current_revision"`
}

// AuthzError is an authorization command failure.
type AuthzError struct {
	Kind            AuthzErrorKind
	Message         string
	CurrentRevision uint64
	Expected        uint32
	Got             uint32
}

// The externally tagged authorization error names.
const (
	authzUnsupportedVariant      = "Unsupported"
	authzUnauthorizedVariant     = "Unauthorized"
	authzUnknownRoleVariant      = "UnknownRole"
	authzInvalidNameVariant      = "InvalidName"
	authzConflictVariant         = "Conflict"
	authzVersionVariant          = "Version"
	authzTenancyViolationVariant = "TenancyViolation"
	authzInvalidGrantVariant     = "InvalidGrant"
	authzMutationConflictVariant = "MutationConflict"
	authzResourceLimitVariant    = "ResourceLimit"
)

// MarshalCBOR writes the externally tagged error.
func (authzError AuthzError) MarshalCBOR() (cborform.Value, error) {
	text := func(name string) (cborform.Value, error) {
		return taggedVariant(name, cborform.Text(authzError.Message)), nil
	}
	switch authzError.Kind {
	case AuthzUnsupported:
		return text(authzUnsupportedVariant)
	case AuthzUnauthorized:
		return unitVariant(authzUnauthorizedVariant), nil
	case AuthzUnknownRole:
		return text(authzUnknownRoleVariant)
	case AuthzInvalidName:
		return text(authzInvalidNameVariant)
	case AuthzTenancyViolation:
		return text(authzTenancyViolationVariant)
	case AuthzInvalidGrant:
		return text(authzInvalidGrantVariant)
	case AuthzMutationConflict:
		return text(authzMutationConflictVariant)
	case AuthzResourceLimit:
		return text(authzResourceLimitVariant)
	case AuthzConflict:
		return marshalVariantStruct(authzConflictVariant, authzConflictFields{
			CurrentRevision: authzError.CurrentRevision,
		})
	case AuthzVersionMismatch:
		return marshalVariantStruct(authzVersionVariant, kvVersionPayloadPair{
			Expected: authzError.Expected, Got: authzError.Got,
		})
	default:
		return nil, fmt.Errorf("wire: unknown authorization error %d", authzError.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged error.
func (authzError *AuthzError) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}

	text := func(kind AuthzErrorKind) error {
		if err := requirePayload("AuthzError", name, payload); err != nil {
			return err
		}
		message, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: %q carries a message, got %T", name, payload)
		}
		*authzError = AuthzError{Kind: kind, Message: string(message)}
		return nil
	}

	switch name {
	case authzUnsupportedVariant:
		return text(AuthzUnsupported)
	case authzUnknownRoleVariant:
		return text(AuthzUnknownRole)
	case authzInvalidNameVariant:
		return text(AuthzInvalidName)
	case authzTenancyViolationVariant:
		return text(AuthzTenancyViolation)
	case authzInvalidGrantVariant:
		return text(AuthzInvalidGrant)
	case authzMutationConflictVariant:
		return text(AuthzMutationConflict)
	case authzResourceLimitVariant:
		return text(AuthzResourceLimit)
	case authzUnauthorizedVariant:
		*authzError = AuthzError{Kind: AuthzUnauthorized}
		return nil
	case authzConflictVariant:
		if err := requirePayload("AuthzError", name, payload); err != nil {
			return err
		}
		var fields authzConflictFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*authzError = AuthzError{Kind: AuthzConflict, CurrentRevision: fields.CurrentRevision}
		return nil
	case authzVersionVariant:
		if err := requirePayload("AuthzError", name, payload); err != nil {
			return err
		}
		var fields kvVersionPayloadPair
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*authzError = AuthzError{
			Kind: AuthzVersionMismatch, Expected: fields.Expected, Got: fields.Got,
		}
		return nil
	default:
		return unknownVariant("AuthzError", name)
	}
}

// Error renders the failure, matching the Rust messages.
func (authzError AuthzError) Error() string {
	switch authzError.Kind {
	case AuthzUnsupported:
		return "authz not supported: " + authzError.Message
	case AuthzUnauthorized:
		return "unauthorized"
	case AuthzUnknownRole:
		return "unknown role: " + authzError.Message
	case AuthzInvalidName:
		return "invalid role name: " + authzError.Message
	case AuthzConflict:
		return fmt.Sprintf("revision conflict: current is %d", authzError.CurrentRevision)
	case AuthzVersionMismatch:
		return fmt.Sprintf("unsupported authz op version (expected %d, got %d)",
			authzError.Expected, authzError.Got)
	case AuthzTenancyViolation:
		return "refused by stream tenancy: " + authzError.Message
	case AuthzInvalidGrant:
		return "invalid grant: " + authzError.Message
	case AuthzMutationConflict:
		return "mutation id conflict: " + authzError.Message
	case AuthzResourceLimit:
		return "authorization receipt capacity exhausted: " + authzError.Message
	default:
		return "unknown authz error"
	}
}

// ResultCode projects the failure onto the shared outcome dictionary.
func (authzError AuthzError) ResultCode() ResultCode {
	switch authzError.Kind {
	case AuthzUnsupported:
		return ResultUnsupported
	case AuthzUnauthorized:
		return ResultForbidden
	case AuthzUnknownRole:
		return ResultNotFound
	case AuthzInvalidName, AuthzInvalidGrant, AuthzTenancyViolation:
		return ResultInvalidArgument
	case AuthzConflict, AuthzMutationConflict:
		return ResultConflict
	case AuthzResourceLimit:
		return ResultResourceLimit
	case AuthzVersionMismatch:
		return ResultVersionSkew
	default:
		return ResultBackend
	}
}

// AuthzReplyKind discriminates a reply, shaped per request.
type AuthzReplyKind uint8

const (
	// AuthzOk means a mutating command applied.
	AuthzOk AuthzReplyKind = iota
	AuthzWhoami
	AuthzRoles
	// AuthzRole is one role, or none when the name is unknown.
	AuthzRole
	AuthzBindings
	AuthzHistory
	AuthzErr
)

// AuthzReply is the reply to any authorization command.
type AuthzReply struct {
	Kind     AuthzReplyKind
	Whoami   *WhoamiReply
	Roles    *ListRolesReply
	Role     *Role
	Bindings *BindingsReply
	History  *AuthzHistoryReply
	Err      *AuthzError
}

// The externally tagged authorization reply names.
const (
	authzOkVariant       = "Ok"
	authzWhoamiVariant   = "Whoami"
	authzRolesVariant    = "Roles"
	authzRoleVariant     = "Role"
	authzBindingsVariant = "Bindings"
	authzHistoryVariant  = "History"
	authzErrVariant      = "Err"
)

// MarshalCBOR writes the externally tagged reply.
func (reply AuthzReply) MarshalCBOR() (cborform.Value, error) {
	switch reply.Kind {
	case AuthzOk:
		return unitVariant(authzOkVariant), nil
	case AuthzWhoami:
		if reply.Whoami == nil {
			return nil, fmt.Errorf("wire: a whoami reply carries no capabilities")
		}
		return marshalVariantStruct(authzWhoamiVariant, *reply.Whoami)
	case AuthzRoles:
		if reply.Roles == nil {
			return nil, fmt.Errorf("wire: a roles reply carries no roles")
		}
		return marshalVariantStruct(authzRolesVariant, *reply.Roles)
	case AuthzRole:
		// A role lookup answers an optional role, so an unknown name rides as
		// an explicit null rather than an error.
		if reply.Role == nil {
			return taggedVariant(authzRoleVariant, cborform.Null{}), nil
		}
		return marshalVariantStruct(authzRoleVariant, *reply.Role)
	case AuthzBindings:
		if reply.Bindings == nil {
			return nil, fmt.Errorf("wire: a bindings reply carries no roles")
		}
		return marshalVariantStruct(authzBindingsVariant, *reply.Bindings)
	case AuthzHistory:
		if reply.History == nil {
			return nil, fmt.Errorf("wire: a history reply carries no page")
		}
		return marshalVariantStruct(authzHistoryVariant, *reply.History)
	case AuthzErr:
		if reply.Err == nil {
			return nil, fmt.Errorf("wire: an error reply carries no error")
		}
		payload, err := reply.Err.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(authzErrVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: unknown authorization reply %d", reply.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged reply.
func (reply *AuthzReply) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	switch name {
	case authzOkVariant:
		*reply = AuthzReply{Kind: AuthzOk}
		return nil
	case authzWhoamiVariant:
		if err := requirePayload("AuthzReply", name, payload); err != nil {
			return err
		}
		var whoami WhoamiReply
		if err := fromValueInto(payload, &whoami); err != nil {
			return err
		}
		*reply = AuthzReply{Kind: AuthzWhoami, Whoami: &whoami}
		return nil
	case authzRolesVariant:
		if err := requirePayload("AuthzReply", name, payload); err != nil {
			return err
		}
		var roles ListRolesReply
		if err := fromValueInto(payload, &roles); err != nil {
			return err
		}
		*reply = AuthzReply{Kind: AuthzRoles, Roles: &roles}
		return nil
	case authzRoleVariant:
		if err := requirePayload("AuthzReply", name, payload); err != nil {
			return err
		}
		if _, isNull := payload.(cborform.Null); isNull {
			*reply = AuthzReply{Kind: AuthzRole}
			return nil
		}
		var role Role
		if err := fromValueInto(payload, &role); err != nil {
			return err
		}
		*reply = AuthzReply{Kind: AuthzRole, Role: &role}
		return nil
	case authzBindingsVariant:
		if err := requirePayload("AuthzReply", name, payload); err != nil {
			return err
		}
		var bindings BindingsReply
		if err := fromValueInto(payload, &bindings); err != nil {
			return err
		}
		*reply = AuthzReply{Kind: AuthzBindings, Bindings: &bindings}
		return nil
	case authzHistoryVariant:
		if err := requirePayload("AuthzReply", name, payload); err != nil {
			return err
		}
		var history AuthzHistoryReply
		if err := fromValueInto(payload, &history); err != nil {
			return err
		}
		*reply = AuthzReply{Kind: AuthzHistory, History: &history}
		return nil
	case authzErrVariant:
		if err := requirePayload("AuthzReply", name, payload); err != nil {
			return err
		}
		var authzError AuthzError
		if err := authzError.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = AuthzReply{Kind: AuthzErr, Err: &authzError}
		return nil
	default:
		return unknownVariant("AuthzReply", name)
	}
}

// ValidateMutationID bounds an authorization mutation id.
//
// The id keys a recorded outcome, so it is deliberately narrow: an id that
// varied by case or carried a separator would let two spellings of one
// mutation record two outcomes.
func ValidateMutationID(mutationID string) error {
	if mutationID == "" || len(mutationID) > MaxAuthzMutationIDBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"mutation id must be 1 to %d bytes", MaxAuthzMutationIDBytes)}
	}
	for i := 0; i < len(mutationID); i++ {
		b := mutationID[i]
		letter := (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
		if !letter && !(b >= '0' && b <= '9') && b != '-' {
			return &InvalidError{
				Reason: "mutation id may hold only ASCII letters, digits and '-'",
			}
		}
	}
	return nil
}

// ValidateRole checks a role before it is defined: a valid name, and no grant
// on a feature no request is authorized against.
//
// A grant on an inert feature is refused rather than stored, because it reads
// as a permission that was given while matching nothing, which is how an
// operator believes access was granted when it was not.
func ValidateRole(role Role) error {
	if err := ValidateRoleName(role.Name); err != nil {
		return err
	}
	for _, grant := range role.Grants {
		switch grant.Feature {
		case FeatureMemory, FeatureAgent, FeatureWorkflow, FeatureUnrecognized:
			return &InvalidError{Reason: fmt.Sprintf(
				"no request is authorized against the `%s` feature, "+
					"so a grant on it has no effect", grant.Feature)}
		}
	}
	return nil
}
