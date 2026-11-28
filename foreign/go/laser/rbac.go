package laser

import (
	"context"
	"encoding/base32"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The authorization surface: roles carry grants, bindings attach roles to a
// server-stamped user, and deny wins over allow with a default of deny.
//
// It is served by the streaming server itself rather than by a managed
// backend, so a deployment announcing the authorization feature serves these
// verbs whether or not it has one.

// Whoami reads the caller's own effective capabilities: the roles bound to it
// and their flattened grants. Always answered to the authenticated caller, so
// it needs no permission of its own.
func (laser *Laser) Whoami(ctx context.Context) (*wire.WhoamiReply, error) {
	reply, err := laser.executeAuthz(ctx, wire.AgdxAuthzWhoamiCode,
		wire.WhoamiReq{V: wire.AuthzOpVersion})
	if err != nil {
		return nil, err
	}
	if reply.Kind != wire.AuthzWhoami {
		return nil, unexpectedAuthzReply("whoami", reply.Kind)
	}
	return reply.Whoami, nil
}

// ListRoles lists defined roles, narrowed by a name prefix and a substring
// search. Both are optional: pass empty strings to list every role.
func (laser *Laser) ListRoles(
	ctx context.Context, namePrefix, search string,
) ([]wire.Role, error) {
	request := wire.ListRolesReq{V: wire.AuthzOpVersion}
	if namePrefix != "" {
		request.NamePrefix = &namePrefix
	}
	if search != "" {
		request.Search = &search
	}
	reply, err := laser.executeAuthz(ctx, wire.AgdxAuthzListRolesCode, request)
	if err != nil {
		return nil, err
	}
	if reply.Kind != wire.AuthzRoles {
		return nil, unexpectedAuthzReply("list roles", reply.Kind)
	}
	if reply.Roles == nil {
		return nil, nil
	}
	return reply.Roles.Roles, nil
}

// GetRole reads one role by name, nil when no role has it.
func (laser *Laser) GetRole(ctx context.Context, name string) (*wire.Role, error) {
	if err := wire.ValidateRoleName(name); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	reply, err := laser.executeAuthz(ctx, wire.AgdxAuthzGetRoleCode,
		wire.GetRoleReq{V: wire.AuthzOpVersion, Name: name})
	if err != nil {
		return nil, err
	}
	if reply.Kind != wire.AuthzRole {
		return nil, unexpectedAuthzReply("get role", reply.Kind)
	}
	return reply.Role, nil
}

// GetBindings reads one user's bound role names and the revision they were
// read at, which is the value BindRolesExpectRevision names to replace exactly
// this binding.
func (laser *Laser) GetBindings(
	ctx context.Context, userID uint32,
) (*wire.BindingsReply, error) {
	reply, err := laser.executeAuthz(ctx, wire.AgdxAuthzGetBindingsCode,
		wire.GetBindingsReq{V: wire.AuthzOpVersion, UserID: userID})
	if err != nil {
		return nil, err
	}
	if reply.Kind != wire.AuthzBindings {
		return nil, unexpectedAuthzReply("get bindings", reply.Kind)
	}
	return reply.Bindings, nil
}

// DefineRole defines or replaces a role, carrying its whole grant set.
func (laser *Laser) DefineRole(ctx context.Context, role wire.Role) error {
	return laser.DefineRoleAs(ctx, role, NewMutationID())
}

// DefineRoleAs is DefineRole under a caller-chosen mutation id, the receipt
// identity of the request.
//
// The server keeps the outcome of an applied mutation under the account that
// sent it, so an exact repeat after a lost reply returns the original outcome
// without a second effect, and the same id with other content is a conflict.
func (laser *Laser) DefineRoleAs(
	ctx context.Context, role wire.Role, mutationID string,
) error {
	if err := wire.ValidateRole(role); err != nil {
		return &InvalidError{Reason: err.Error()}
	}
	if err := wire.ValidateMutationID(mutationID); err != nil {
		return &InvalidError{Reason: err.Error()}
	}
	return laser.executeAuthzOk(ctx, wire.AgdxAuthzDefineRoleCode, wire.DefineRoleReq{
		V: wire.AuthzOpVersion, Role: role, MutationID: &mutationID,
	})
}

// DeleteRole removes a role by name.
func (laser *Laser) DeleteRole(ctx context.Context, name string) error {
	return laser.DeleteRoleAs(ctx, name, NewMutationID())
}

// DeleteRoleAs is DeleteRole under a caller-chosen mutation id, with the
// receipt semantics of DefineRoleAs.
func (laser *Laser) DeleteRoleAs(ctx context.Context, name, mutationID string) error {
	if err := wire.ValidateRoleName(name); err != nil {
		return &InvalidError{Reason: err.Error()}
	}
	if err := wire.ValidateMutationID(mutationID); err != nil {
		return &InvalidError{Reason: err.Error()}
	}
	return laser.executeAuthzOk(ctx, wire.AgdxAuthzDeleteRoleCode, wire.DeleteRoleReq{
		V: wire.AuthzOpVersion, Name: name, MutationID: &mutationID,
	})
}

// BindRoles replaces a user's whole role set, returning the binding as
// committed with the revision a later conditional bind names.
func (laser *Laser) BindRoles(
	ctx context.Context, userID uint32, roles []string,
) (*wire.BindingsReply, error) {
	return laser.BindRolesAs(ctx, userID, roles, nil, NewMutationID())
}

// BindRolesExpectRevision replaces a user's role set only while the binding is
// still at expectRevision, so two administrators editing one user cannot both
// win.
func (laser *Laser) BindRolesExpectRevision(
	ctx context.Context, userID uint32, roles []string, expectRevision uint64,
) (*wire.BindingsReply, error) {
	return laser.BindRolesAs(ctx, userID, roles, &expectRevision, NewMutationID())
}

// BindRolesAs is BindRoles with an optional expected revision, under a
// caller-chosen mutation id.
//
// An administrator that revokes its own administration keeps that request's
// receipt, so its exact repeat after a lost reply returns the original success
// and the binding revision moves once.
func (laser *Laser) BindRolesAs(
	ctx context.Context, userID uint32, roles []string,
	expectRevision *uint64, mutationID string,
) (*wire.BindingsReply, error) {
	for _, role := range roles {
		if err := wire.ValidateRoleName(role); err != nil {
			return nil, &InvalidError{Reason: err.Error()}
		}
	}
	if err := wire.ValidateMutationID(mutationID); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	reply, err := laser.executeAuthz(ctx, wire.AgdxAuthzBindRolesCode, wire.BindRolesReq{
		V: wire.AuthzOpVersion, UserID: userID, Roles: roles,
		ExpectRevision: expectRevision, MutationID: &mutationID,
	})
	if err != nil {
		return nil, err
	}
	if reply.Kind != wire.AuthzBindings {
		return nil, unexpectedAuthzReply("bind roles", reply.Kind)
	}
	return reply.Bindings, nil
}

// AuthzHistory reads a page of the authorization change history for one
// subject, after a revision and bounded by limit.
func (laser *Laser) AuthzHistory(
	ctx context.Context, subject wire.AuthzSubject, afterRevision *uint64, limit uint32,
) (*wire.AuthzHistoryReply, error) {
	reply, err := laser.executeAuthz(ctx, wire.AgdxAuthzHistoryCode, wire.AuthzHistoryReq{
		V: wire.AuthzOpVersion, Subject: subject, AfterRevision: afterRevision, Limit: limit,
	})
	if err != nil {
		return nil, err
	}
	if reply.Kind != wire.AuthzHistory {
		return nil, unexpectedAuthzReply("authz history", reply.Kind)
	}
	return reply.History, nil
}

// executeAuthz sends one authorization command and opens its reply.
//
// The surface is gated on the authorization feature rather than on a managed
// backend, because the streaming server serves it directly.
func (laser *Laser) executeAuthz(
	ctx context.Context, code uint32, request any,
) (*wire.AuthzReply, error) {
	if !laser.Capabilities().Authz {
		return nil, &UnsupportedError{Surface: "the authorization surface"}
	}
	if err := laser.requireOpVersion("authorization", wire.AuthzOpVersion,
		func(versions wire.OpVersions) uint32 { return versions.Control }); err != nil {
		return nil, err
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode authorization request", Cause: err}
	}
	raw, err := laser.SendManaged(ctx, code, payload)
	if err != nil {
		return nil, err
	}
	var reply wire.AuthzReply
	if err := decodeManagedReply(raw, &reply, "decode authorization reply"); err != nil {
		return nil, err
	}
	if reply.Kind == wire.AuthzErr {
		if reply.Err == nil {
			return nil, &CodecError{Reason: "authorization reply reported a failure with no detail"}
		}
		return nil, &AuthzError{Failure: *reply.Err}
	}
	return &reply, nil
}

// executeAuthzOk runs a verb whose success carries nothing back.
func (laser *Laser) executeAuthzOk(ctx context.Context, code uint32, request any) error {
	reply, err := laser.executeAuthz(ctx, code, request)
	if err != nil {
		return err
	}
	if reply.Kind != wire.AuthzOk {
		return unexpectedAuthzReply("authorization write", reply.Kind)
	}
	return nil
}

// AuthzError reports an authorization command the server refused, carrying the
// contract's own classification so a caller can tell a denial from a conflict
// without matching on message text.
type AuthzError struct{ Failure wire.AuthzError }

func (e *AuthzError) Error() string { return "authorization: " + e.Failure.Error() }

// Kind is the contract's classification of the failure.
func (e *AuthzError) Kind() wire.AuthzErrorKind { return e.Failure.Kind }

// NewMutationID mints the receipt identity of an authorization write.
//
// It is the text form of the same ULID layout a managed operation uses, so ids
// from one client sort by the order they were minted in.
func NewMutationID() string {
	return crockford(NewOperationID())
}

// crockfordAlphabet is ULID's: Crockford base32, which drops the letters that
// look like digits so an id can be read aloud and typed back.
const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var crockfordEncoding = base32.NewEncoding(crockfordAlphabet).WithPadding(base32.NoPadding)

// crockford renders 16 bytes as the 26 character ULID text.
//
// Base32 covers 130 bits, so the value is left-padded by two bits and the
// leading character carries only the low three of its five, which is what
// keeps the text exactly 26 characters and byte order the same as text order.
func crockford(value wire.U128) string {
	padded := make([]byte, 0, 17)
	padded = append(padded, 0)
	padded = append(padded, value[:]...)
	encoded := crockfordEncoding.EncodeToString(padded)
	return strings.TrimPrefix(encoded[len(encoded)-26:], "")
}

func unexpectedAuthzReply(verb string, kind wire.AuthzReplyKind) error {
	return &CodecError{Reason: "authorization " + verb +
		" answered with an unexpected reply: " + authzReplyName(kind)}
}

func authzReplyName(kind wire.AuthzReplyKind) string {
	switch kind {
	case wire.AuthzOk:
		return "ok"
	case wire.AuthzWhoami:
		return "whoami"
	case wire.AuthzRoles:
		return "roles"
	case wire.AuthzRole:
		return "role"
	case wire.AuthzBindings:
		return "bindings"
	case wire.AuthzHistory:
		return "history"
	case wire.AuthzErr:
		return "error"
	default:
		return "kind " + itoa(int(kind))
	}
}
