package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The authorization surface. Deny takes precedence and a missing grant denies,
// so a reply that decoded wrongly could widen access rather than narrow it.
func TestGivenAuthzFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	t.Run("authz_role.bin", func(t *testing.T) {
		role := roundTripFixture[wire.Role](t, "authz_role.bin")
		if role.Name == "" {
			t.Error("a role names itself")
		}
		if err := wire.ValidateRoleName(role.Name); err != nil {
			t.Errorf("the corpus role name must validate: %v", err)
		}
		if len(role.Grants) == 0 {
			t.Error("the corpus role carries grants")
		}
	})
	t.Run("authz_whoami_reply.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.AuthzReply](t, "authz_whoami_reply.bin")
		if reply.Kind != wire.AuthzWhoami || reply.Whoami == nil {
			t.Fatalf("want a whoami reply, got kind %d", reply.Kind)
		}
	})
	t.Run("authz_get_role_reply.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.AuthzReply](t, "authz_get_role_reply.bin")
		if reply.Kind != wire.AuthzRole {
			t.Fatalf("want a role reply, got kind %d", reply.Kind)
		}
	})
	t.Run("authz_bind_roles.bin", func(t *testing.T) {
		request := roundTripFixture[wire.BindRolesReq](t, "authz_bind_roles.bin")
		if len(request.Roles) == 0 {
			t.Error("a bind names the roles it sets")
		}
	})
	t.Run("authz_history_request.bin", func(t *testing.T) {
		request := roundTripFixture[wire.AuthzHistoryReq](t, "authz_history_request.bin")
		if request.Limit == 0 {
			t.Error("a history read bounds its page")
		}
	})
	t.Run("authz_history_reply.bin", func(t *testing.T) {
		reply := roundTripFixture[wire.AuthzReply](t, "authz_history_reply.bin")
		if reply.Kind != wire.AuthzHistory || reply.History == nil {
			t.Fatalf("want a history reply, got kind %d", reply.Kind)
		}
		if len(reply.History.Events) == 0 {
			t.Error("the corpus history carries events")
		}
	})
}

// A role lookup answers an optional role, so an unknown name rides as an
// explicit null rather than an error. Decoding that as an error would turn a
// routine miss into a failure.
func TestGivenAnUnknownRoleLookupWhenEncodedThenShouldRideAsAnExplicitNull(t *testing.T) {
	absent := wire.AuthzReply{Kind: wire.AuthzRole}

	encoded, err := wire.Marshal(absent)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.AuthzReply
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Kind != wire.AuthzRole {
		t.Fatalf("want a role reply, got kind %d", back.Kind)
	}
	if back.Role != nil {
		t.Fatal("an unknown role must decode as absent")
	}

	present := wire.AuthzReply{Kind: wire.AuthzRole, Role: &wire.Role{Name: "reader"}}
	presentEncoded, err := wire.Marshal(present)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var presentBack wire.AuthzReply
	if err := wire.Unmarshal(presentEncoded, &presentBack); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if presentBack.Role == nil || presentBack.Role.Name != "reader" {
		t.Fatalf("a present role must survive: %+v", presentBack.Role)
	}
}

// Every authorization subject and event must keep its variant, since the audit
// trail is what answers who granted what.
func TestGivenAuthzSubjectsAndEventsWhenRoundTrippedThenShouldKeepTheirVariant(t *testing.T) {
	subjects := map[string]wire.AuthzSubject{
		"role":    wire.RoleSubject("reader"),
		"binding": wire.BindingSubject(7),
		"all":     wire.AllSubjects(),
		"stream":  wire.StreamSubject("orders"),
	}
	for name, subject := range subjects {
		encoded, err := wire.Marshal(subject)
		if err != nil {
			t.Errorf("%s: marshal: %v", name, err)
			continue
		}
		var back wire.AuthzSubject
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", name, err)
			continue
		}
		if back != subject {
			t.Errorf("%s: round trip changed the subject: %+v", name, back)
		}
	}

	events := map[string]wire.AuthzEventKind{
		"role defined": {Kind: wire.EventRoleDefined, Name: "reader"},
		"role deleted": {Kind: wire.EventRoleDeleted, Name: "reader"},
		"roles bound":  {Kind: wire.EventRolesBound, UserID: 7, Roles: []string{"reader"}},
		"tenancy set":  {Kind: wire.EventTenancySet, Tenancy: wire.TenancyStream},
	}
	for name, event := range events {
		encoded, err := wire.Marshal(event)
		if err != nil {
			t.Errorf("%s: marshal: %v", name, err)
			continue
		}
		var back wire.AuthzEventKind
		if err := wire.Unmarshal(encoded, &back); err != nil {
			t.Errorf("%s: unmarshal: %v", name, err)
			continue
		}
		if back.Kind != event.Kind {
			t.Errorf("%s: variant changed from %d to %d", name, event.Kind, back.Kind)
			continue
		}
		again, err := wire.Marshal(back)
		if err != nil {
			t.Errorf("%s: re-marshal: %v", name, err)
			continue
		}
		if string(again) != string(encoded) {
			t.Errorf("%s: re-encode differs", name)
		}
	}
}

// A lost compare-and-swap bind must report the current revision, or the caller
// cannot re-read and retry.
func TestGivenAuthzErrorsWhenClassifiedThenShouldCarryTheirDetail(t *testing.T) {
	conflict := wire.AuthzError{Kind: wire.AuthzConflict, CurrentRevision: 9}

	encoded, err := wire.Marshal(conflict)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back wire.AuthzError
	if err := wire.Unmarshal(encoded, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.CurrentRevision != 9 {
		t.Fatalf("want the current revision, got %d", back.CurrentRevision)
	}
	if back.ResultCode() != wire.ResultConflict {
		t.Errorf("a lost precondition is a conflict, got %s", back.ResultCode())
	}

	// Unauthorized carries no message, so it rides as a bare name.
	unauthorized, err := wire.Marshal(wire.AuthzError{Kind: wire.AuthzUnauthorized})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if unauthorized[0]>>5 != 3 {
		t.Fatalf("want a bare variant name, got head 0x%02x", unauthorized[0])
	}

	for kind, want := range map[wire.AuthzErrorKind]wire.ResultCode{
		wire.AuthzUnsupported:      wire.ResultUnsupported,
		wire.AuthzUnauthorized:     wire.ResultForbidden,
		wire.AuthzUnknownRole:      wire.ResultNotFound,
		wire.AuthzInvalidName:      wire.ResultInvalidArgument,
		wire.AuthzTenancyViolation: wire.ResultInvalidArgument,
		wire.AuthzConflict:         wire.ResultConflict,
		wire.AuthzVersionMismatch:  wire.ResultVersionSkew,
	} {
		if got := (wire.AuthzError{Kind: kind}).ResultCode(); got != want {
			t.Errorf("kind %d: want %s, got %s", kind, want, got)
		}
	}
}

// The role-name rule is shared by the SDK, the server and the console, so a
// name one tier accepts the next never rejects.
func TestGivenARoleNameWhenValidatedThenShouldApplyTheSharedSafelist(t *testing.T) {
	for _, good := range []string{"reader", "ops-admin", "role_7.v2"} {
		if err := wire.ValidateRoleName(good); err != nil {
			t.Errorf("%q must be accepted: %v", good, err)
		}
	}
	for _, bad := range []string{"", "bad name", "role/admin", "role;drop"} {
		if err := wire.ValidateRoleName(bad); err == nil {
			t.Errorf("%q must be refused", bad)
		}
	}
	if err := wire.ValidateRoleName(string(make([]byte, wire.MaxRoleNameBytes+1))); err == nil {
		t.Error("a name past the cap must be refused")
	}
}
