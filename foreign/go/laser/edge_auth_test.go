package laser_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A token minted for this server and granting what is asked for passes.
func TestGivenAMatchingTokenWhenAuthorizedThenShouldPass(t *testing.T) {
	claims := laser.EdgeClaims{
		Audience: []string{"other", "this-server"},
		Scopes:   []string{"read", "tools:call"},
	}

	if err := laser.AuthorizeEdge(claims, "this-server", "tools:call"); err != nil {
		t.Fatalf("want it authorized, got %v", err)
	}
}

// A token minted for another server is not a credential here at all, so it is
// refused outright rather than being offered a way to widen it.
func TestGivenAForeignAudienceWhenAuthorizedThenShouldRefuseItOutright(t *testing.T) {
	claims := laser.EdgeClaims{
		Audience: []string{"another-server"},
		// It grants the scope, which must not matter: a credential issued to
		// somebody else is not read for what it permits.
		Scopes: []string{"tools:call"},
	}

	err := laser.AuthorizeEdge(claims, "this-server", "tools:call")

	var denial *laser.EdgeDenial
	if !errors.As(err, &denial) {
		t.Fatalf("want an EdgeDenial, got %T: %v", err, err)
	}
	if denial.Kind != laser.DenialWrongAudience {
		t.Errorf("want a wrong audience, got %q", denial.Kind)
	}
	if denial.Expected != "this-server" {
		t.Errorf("want the audience named, got %q", denial.Expected)
	}
	// It is a missing credential rather than an insufficient one.
	if denial.ResultCode() != wire.ResultUnauthenticated {
		t.Errorf("want unauthenticated, got %q", denial.ResultCode())
	}
	if denial.Challenge() != `Bearer error="invalid_token"` {
		t.Errorf("want the invalid-token challenge, got %q", denial.Challenge())
	}
}

// A token valid here but granting too little is something the caller can fix,
// so the refusal names what to acquire.
func TestGivenAMissingScopeWhenAuthorizedThenShouldStepUpWithTheScope(t *testing.T) {
	claims := laser.EdgeClaims{
		Audience: []string{"this-server"}, Scopes: []string{"read"},
	}

	err := laser.AuthorizeEdge(claims, "this-server", "tools:call")

	var denial *laser.EdgeDenial
	if !errors.As(err, &denial) {
		t.Fatalf("want an EdgeDenial, got %T: %v", err, err)
	}
	if denial.Kind != laser.DenialStepUp {
		t.Errorf("want a step up, got %q", denial.Kind)
	}
	if denial.RequiredScope != "tools:call" {
		t.Errorf("want the scope named, got %q", denial.RequiredScope)
	}
	if denial.ResultCode() != wire.ResultStepUpRequired {
		t.Errorf("want a step up code, got %q", denial.ResultCode())
	}
	// The challenge names what to acquire, which is what an edge client reads
	// to start re-authorizing.
	challenge := denial.Challenge()
	if !strings.Contains(challenge, "insufficient_scope") ||
		!strings.Contains(challenge, `scope="tools:call"`) {
		t.Errorf("want the scope in the challenge, got %q", challenge)
	}
}

// A scope carrying a quote must not end the parameter early and smuggle
// another one in.
func TestGivenAScopeWithQuotesWhenChallengedThenShouldEscapeThem(t *testing.T) {
	claims := laser.EdgeClaims{Audience: []string{"this-server"}}

	err := laser.AuthorizeEdge(claims, "this-server",
		`tools:call", scope="admin`)

	var denial *laser.EdgeDenial
	if !errors.As(err, &denial) {
		t.Fatalf("want an EdgeDenial, got %T: %v", err, err)
	}
	challenge := denial.Challenge()
	if strings.Count(challenge, `scope="`) != 1 {
		t.Errorf("a scope must not open a second parameter, got %q", challenge)
	}
	if !strings.Contains(challenge, `\"`) {
		t.Errorf("want the quotes escaped, got %q", challenge)
	}

	// A backslash is escaped too, so it cannot escape the closing quote.
	slashed := laser.AuthorizeEdge(claims, "this-server", `tools\`)
	var backslash *laser.EdgeDenial
	if !errors.As(slashed, &backslash) {
		t.Fatalf("want an EdgeDenial, got %T: %v", slashed, slashed)
	}
	if !strings.HasSuffix(backslash.Challenge(), `scope="tools\\"`) {
		t.Errorf("want the backslash escaped, got %q", backslash.Challenge())
	}
}

// A token with nothing in it is refused on the audience, since it names none.
func TestGivenAnEmptyTokenWhenAuthorizedThenShouldRefuseTheAudience(t *testing.T) {
	err := laser.AuthorizeEdge(laser.EdgeClaims{}, "this-server", "tools:call")

	var denial *laser.EdgeDenial
	if !errors.As(err, &denial) {
		t.Fatalf("want an EdgeDenial, got %T: %v", err, err)
	}
	if denial.Kind != laser.DenialWrongAudience {
		t.Errorf("want the audience refused first, got %q", denial.Kind)
	}
}

// A denial says what went wrong in words, for a log or a handler that does
// not branch on the kind.
func TestGivenADenialWhenReportedThenShouldSayWhatIsWrong(t *testing.T) {
	audience := &laser.EdgeDenial{
		Kind: laser.DenialWrongAudience, Expected: "this-server",
	}
	if !strings.Contains(audience.Error(), "this-server") {
		t.Errorf("want the audience named, got %q", audience.Error())
	}
	stepUp := &laser.EdgeDenial{
		Kind: laser.DenialStepUp, RequiredScope: "tools:call",
	}
	if !strings.Contains(stepUp.Error(), "tools:call") {
		t.Errorf("want the scope named, got %q", stepUp.Error())
	}
}
