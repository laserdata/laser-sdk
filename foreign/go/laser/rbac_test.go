package laser_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// authzServer answers authorization commands from a script.
type authzServer struct {
	clusterOfOne
	reply    wire.AuthzReply
	codes    []uint32
	requests [][]byte
}

func (server *authzServer) Close() error { return nil }

func (server *authzServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.codes = append(server.codes, code)
	server.requests = append(server.requests, payload)
	return wire.Marshal(server.reply)
}

// authzCapabilities is a deployment serving the authorization verbs. They come
// from the streaming server, so no managed backend is announced here: a client
// that needed one would fail this.
func authzCapabilities() *laser.Capabilities {
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion)
	capabilities := laser.OpenCapabilities()
	capabilities.Authz = true
	capabilities.Hello = laser.HelloAnswered
	capabilities.Versions = &versions
	return &capabilities
}

func authzHandle(server iggcon.Client) *laser.Laser {
	return laser.FromClient(server, laser.Options{Capabilities: authzCapabilities()})
}

func decodeAuthzRequest[T any](t *testing.T, server *authzServer, index int, target *T) {
	t.Helper()
	if err := wire.Unmarshal(server.requests[index], target); err != nil {
		t.Fatalf("decode request %d: %v", index, err)
	}
}

// The authorization verbs are served by the streaming server, so a deployment
// with no managed backend still answers them.
func TestGivenNoManagedBackendWhenReadingWhoamiThenShouldStillAnswer(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{
		Kind: wire.AuthzWhoami,
		Whoami: &wire.WhoamiReply{
			V:     wire.AuthzOpVersion,
			Roles: []string{"reader"},
			Grants: []wire.Grant{{Effect: wire.EffectAllow, Feature: wire.FeatureKv,
				Action: wire.Action("read"), Resource: wire.AllResources()}},
		},
	}}

	whoami, err := authzHandle(server).Whoami(context.Background())
	if err != nil {
		t.Fatalf("whoami: %v", err)
	}
	if len(whoami.Roles) != 1 || whoami.Roles[0] != "reader" {
		t.Errorf("want the bound role, got %+v", whoami.Roles)
	}
	if server.codes[0] != wire.AgdxAuthzWhoamiCode {
		t.Errorf("want the whoami code, got %d", server.codes[0])
	}
}

func TestGivenNoAuthorizationSurfaceWhenCalledThenShouldRefuseIt(t *testing.T) {
	server := &authzServer{}
	handle := laser.FromClient(server, laser.Options{})

	_, err := handle.Whoami(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported call must not reach the socket")
	}
}

// Both filters are optional, and an empty one must be absent on the wire
// rather than sent as a filter matching only the empty string.
func TestGivenRoleFiltersWhenListingThenShouldSendOnlyTheOnesGiven(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{
		Kind:  wire.AuthzRoles,
		Roles: &wire.ListRolesReply{V: wire.AuthzOpVersion, Roles: []wire.Role{{Name: "reader"}}},
	}}

	roles, err := authzHandle(server).ListRoles(context.Background(), "read", "")
	if err != nil {
		t.Fatalf("list roles: %v", err)
	}
	if len(roles) != 1 {
		t.Fatalf("want one role, got %d", len(roles))
	}
	var request wire.ListRolesReq
	decodeAuthzRequest(t, server, 0, &request)
	if request.NamePrefix == nil || *request.NamePrefix != "read" {
		t.Errorf("want the prefix sent, got %v", request.NamePrefix)
	}
	if request.Search != nil {
		t.Errorf("an omitted filter must not be sent, got %v", *request.Search)
	}
}

func TestGivenAnUnknownRoleWhenReadThenShouldReportItAbsent(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{Kind: wire.AuthzRole}}

	role, err := authzHandle(server).GetRole(context.Background(), "missing")
	if err != nil {
		t.Fatalf("get role: %v", err)
	}
	if role != nil {
		t.Errorf("want no role, got %+v", role)
	}
}

// A grant on a feature no request is authorized against reads as a permission
// that was given while matching nothing, which is how an operator believes
// access was granted when it was not.
func TestGivenAGrantOnAnInertFeatureWhenDefinedThenShouldRefuseTheRole(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{Kind: wire.AuthzOk}}

	err := authzHandle(server).DefineRole(context.Background(), wire.Role{
		Name: "writer",
		Grants: []wire.Grant{
			{Effect: wire.EffectAllow, Feature: wire.FeatureKv,
				Action: wire.Action("write"), Resource: wire.AllResources()},
			{Effect: wire.EffectAllow, Feature: wire.FeatureMemory,
				Action: wire.Action("write"), Resource: wire.AllResources()},
		},
	})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "memory") {
		t.Errorf("want the inert feature named, got %v", err)
	}
	if len(server.codes) != 0 {
		t.Error("a refused role must not reach the socket")
	}
}

// The receipt identity is what makes an exact repeat after a lost reply return
// the original outcome instead of applying twice.
func TestGivenARoleDefinitionWhenSentThenShouldCarryAReceiptIdentity(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{Kind: wire.AuthzOk}}

	if err := authzHandle(server).DefineRole(context.Background(), wire.Role{
		Name: "reader",
		Grants: []wire.Grant{{Effect: wire.EffectAllow, Feature: wire.FeatureKv,
			Action: wire.Action("read"), Resource: wire.AllResources()}},
	}); err != nil {
		t.Fatalf("define role: %v", err)
	}

	var request wire.DefineRoleReq
	decodeAuthzRequest(t, server, 0, &request)
	if request.MutationID == nil || *request.MutationID == "" {
		t.Fatal("a definition must carry a receipt identity")
	}
	if err := wire.ValidateMutationID(*request.MutationID); err != nil {
		t.Errorf("the minted id must satisfy the contract: %v", err)
	}
}

// A caller that keeps the id across a restart gets the receipt semantics the
// server offers, so the id must reach the wire unchanged.
func TestGivenACallerChosenReceiptWhenSentThenShouldCarryItUnchanged(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{Kind: wire.AuthzOk}}

	if err := authzHandle(server).DeleteRoleAs(
		context.Background(), "reader", "my-own-receipt"); err != nil {
		t.Fatalf("delete role: %v", err)
	}

	var request wire.DeleteRoleReq
	decodeAuthzRequest(t, server, 0, &request)
	if request.MutationID == nil || *request.MutationID != "my-own-receipt" {
		t.Errorf("want the caller's receipt, got %v", request.MutationID)
	}
}

func TestGivenAMalformedReceiptWhenSentThenShouldRefuseBeforeSending(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{Kind: wire.AuthzOk}}

	err := authzHandle(server).DeleteRoleAs(context.Background(), "reader", "has spaces")

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("a refused receipt must not reach the socket")
	}
}

// Two administrators editing one user cannot both win, which is what the
// expected revision is for.
func TestGivenAConditionalBindWhenSentThenShouldCarryTheExpectedRevision(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{
		Kind:     wire.AuthzBindings,
		Bindings: &wire.BindingsReply{V: wire.AuthzOpVersion, Roles: []string{"reader"}, Revision: 5},
	}}

	bindings, err := authzHandle(server).BindRolesExpectRevision(
		context.Background(), 42, []string{"reader"}, 4)
	if err != nil {
		t.Fatalf("bind roles: %v", err)
	}
	if bindings.Revision != 5 {
		t.Errorf("want the committed revision, got %d", bindings.Revision)
	}
	var request wire.BindRolesReq
	decodeAuthzRequest(t, server, 0, &request)
	if request.ExpectRevision == nil || *request.ExpectRevision != 4 {
		t.Errorf("want the expected revision sent, got %v", request.ExpectRevision)
	}
	if request.UserID != 42 {
		t.Errorf("want the user named, got %d", request.UserID)
	}
}

// An unconditional bind must send no precondition, or it would silently become
// a conditional one against revision zero.
func TestGivenAnUnconditionalBindWhenSentThenShouldCarryNoPrecondition(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{
		Kind:     wire.AuthzBindings,
		Bindings: &wire.BindingsReply{V: wire.AuthzOpVersion},
	}}

	if _, err := authzHandle(server).BindRoles(
		context.Background(), 42, []string{"reader"}); err != nil {
		t.Fatalf("bind roles: %v", err)
	}

	var request wire.BindRolesReq
	decodeAuthzRequest(t, server, 0, &request)
	if request.ExpectRevision != nil {
		t.Errorf("want no precondition, got %d", *request.ExpectRevision)
	}
}

func TestGivenAnInvalidRoleNameWhenBoundThenShouldRefuseBeforeSending(t *testing.T) {
	server := &authzServer{}

	_, err := authzHandle(server).BindRoles(context.Background(), 1, []string{"ok", "not ok!"})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an invalid role name must not reach the socket")
	}
}

func TestGivenAnAuthorizationFailureWhenReportedThenShouldCarryItsKind(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{
		Kind: wire.AuthzErr,
		Err:  &wire.AuthzError{Kind: wire.AuthzUnauthorized, Message: "authz:admin required"},
	}}

	err := authzHandle(server).DeleteRole(context.Background(), "reader")

	var failure *laser.AuthzError
	if !errors.As(err, &failure) {
		t.Fatalf("want an AuthzError, got %T: %v", err, err)
	}
	if failure.Kind() != wire.AuthzUnauthorized {
		t.Errorf("want an unauthorized failure, got %d", failure.Kind())
	}
}

// A reply shaped for another verb means the client and server disagree about
// the contract, which is reported rather than read as a success.
func TestGivenAReplyForAnotherVerbWhenReadThenShouldRefuseIt(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{
		Kind:     wire.AuthzBindings,
		Bindings: &wire.BindingsReply{V: wire.AuthzOpVersion},
	}}

	_, err := authzHandle(server).Whoami(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}

func TestGivenAHistoryReadWhenSentThenShouldCarryTheSubjectAndBound(t *testing.T) {
	server := &authzServer{reply: wire.AuthzReply{
		Kind:    wire.AuthzHistory,
		History: &wire.AuthzHistoryReply{V: wire.AuthzOpVersion},
	}}
	after := uint64(7)

	if _, err := authzHandle(server).AuthzHistory(
		context.Background(), wire.RoleSubject("reader"), &after, 50); err != nil {
		t.Fatalf("history: %v", err)
	}

	var request wire.AuthzHistoryReq
	decodeAuthzRequest(t, server, 0, &request)
	if request.Limit != 50 {
		t.Errorf("want the bound sent, got %d", request.Limit)
	}
	if request.AfterRevision == nil || *request.AfterRevision != 7 {
		t.Errorf("want the cursor sent, got %v", request.AfterRevision)
	}
	if request.Subject.Kind != wire.SubjectRole || request.Subject.Name != "reader" {
		t.Errorf("want the role subject, got %+v", request.Subject)
	}
}

// A receipt keys a recorded outcome, so two of them must never collide and
// each must satisfy the contract's own rule.
func TestGivenMintedReceiptsWhenComparedThenShouldBeDistinctAndValid(t *testing.T) {
	seen := map[string]bool{}
	for range 1000 {
		id := laser.NewMutationID()
		if err := wire.ValidateMutationID(id); err != nil {
			t.Fatalf("minted receipt %q is invalid: %v", id, err)
		}
		if len(id) != 26 {
			t.Fatalf("want a 26 character receipt, got %d in %q", len(id), id)
		}
		if seen[id] {
			t.Fatal("two minted receipts collided")
		}
		seen[id] = true
	}
}
