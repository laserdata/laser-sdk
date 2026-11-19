package laser

import (
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// EdgeClaims are the bearer-token claims an incoming edge request asserts.
//
// This client parses no tokens. The transport decodes one and this decides on
// what it decoded, which keeps the token format the deployment's business and
// the authorization rule this client's.
type EdgeClaims struct {
	// Audience are the audiences the token was minted for.
	Audience []string
	// Scopes are what it grants.
	Scopes []string
}

// EdgeDenialKind is why an edge request was refused.
type EdgeDenialKind string

const (
	// DenialWrongAudience means the token was minted for another server. A
	// server accepts only tokens minted for itself, so this is a hard
	// refusal rather than something a caller can fix by asking for more.
	DenialWrongAudience EdgeDenialKind = "wrong_audience"
	// DenialStepUp means the token is valid here but grants too little. The
	// caller re-authorizes for the named scope, so it is a step-up rather
	// than a permanent denial.
	DenialStepUp EdgeDenialKind = "step_up"
)

// EdgeDenial is a refused edge request.
type EdgeDenial struct {
	Kind EdgeDenialKind
	// Expected is the audience a wrong-audience token should have named.
	Expected string
	// RequiredScope is what a step-up has to acquire.
	RequiredScope string
}

func (e *EdgeDenial) Error() string {
	if e.Kind == DenialWrongAudience {
		return "the token was not minted for " + e.Expected
	}
	return "the token does not grant " + e.RequiredScope
}

// ResultCode is what the refusal maps to.
//
// A token minted elsewhere is not a credential for this server at all, where
// a missing scope is a credential that needs widening. The two call for
// different things from the caller, so they are not one code.
func (e *EdgeDenial) ResultCode() wire.ResultCode {
	if e.Kind == DenialWrongAudience {
		return wire.ResultUnauthenticated
	}
	return wire.ResultStepUpRequired
}

// Challenge is the authentication challenge the refusal is sent with, the
// header an edge client reads to start re-authorizing.
func (e *EdgeDenial) Challenge() string {
	if e.Kind == DenialWrongAudience {
		return `Bearer error="invalid_token"`
	}
	return `Bearer error="insufficient_scope", scope="` +
		quotedStringBody(e.RequiredScope) + `"`
}

// AuthorizeEdge decides an external edge request against a decoded token:
// the audience first, then the scope.
//
// The order matters. A token minted for another audience is never inspected
// further, because deciding anything from its scopes would be trusting a
// credential that was not issued to this server. And a bridge never forwards
// the caller's token upstream: it acts under its own enrolled identity and
// mints a fresh credential, so a compromised bridge cannot replay the
// caller's.
func AuthorizeEdge(
	claims EdgeClaims, expectedAudience, requiredScope string,
) error {
	if !contains(claims.Audience, expectedAudience) {
		return &EdgeDenial{
			Kind: DenialWrongAudience, Expected: expectedAudience,
		}
	}
	if !contains(claims.Scopes, requiredScope) {
		return &EdgeDenial{
			Kind: DenialStepUp, RequiredScope: requiredScope,
		}
	}
	return nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

// quotedStringBody escapes a value for a quoted header parameter, so a scope
// carrying a quote cannot end the parameter early and smuggle another one in.
func quotedStringBody(value string) string {
	var escaped strings.Builder
	escaped.Grow(len(value))
	for _, character := range value {
		if character == '"' || character == '\\' {
			escaped.WriteByte('\\')
		}
		escaped.WriteRune(character)
	}
	return escaped.String()
}
