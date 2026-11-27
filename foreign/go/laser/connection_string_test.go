package laser_test

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// The Rust client embeds this file directly. Every other client carries a copy,
// so each one checks the copy against the original rather than trusting it.
const rustCertificatePath = "../../../sdk/certs/laserdata.crt"

func noEnv() laser.Environment { return laser.EnvMap(nil) }

func mustParse(t *testing.T, value string, env laser.Environment) *laser.ConnectionString {
	t.Helper()
	parsed, err := laser.ParseConnectionStringWithEnv(value, env)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

func refuses(t *testing.T, value string, env laser.Environment) error {
	t.Helper()
	parsed, err := laser.ParseConnectionStringWithEnv(value, env)
	if err == nil {
		t.Fatalf("want a refusal for %q, got %+v", value, parsed)
	}
	var config *laser.ConfigError
	if !errors.As(err, &config) {
		t.Fatalf("want a ConfigError for %q, got %T", value, err)
	}
	return err
}

func TestGivenTheSdkCertificateWhenComparedWithGoThenShouldBeByteIdentical(t *testing.T) {
	rust, err := os.ReadFile(rustCertificatePath)
	if err != nil {
		t.Fatalf("read the Rust certificate: %v", err)
	}
	if laser.LaserDataRootCA != string(rust) {
		t.Error("the bundled CA has drifted from sdk/certs/laserdata.crt")
	}
}

// A LaserData-operated host is encrypted with no extra setup, which is what
// makes a Cloud connection string work on its own.
func TestGivenALaserdataHostWhenParsedThenShouldEnableTLSWithTheEmbeddedCA(t *testing.T) {
	for _, host := range []string{
		"laserdata.cloud", "api.laserdata.cloud", "laserdata.com", "api.laserdata.com",
	} {
		parsed := mustParse(t, "iggy+tcp://token@"+host, noEnv())
		if !parsed.TLS {
			t.Errorf("%s must be encrypted", host)
		}
		if parsed.CA != laser.LaserDataRootCA {
			t.Errorf("%s must verify against the bundled CA", host)
		}
		if parsed.Credentials.Token != "token" || !parsed.Credentials.IsToken() {
			t.Errorf("%s: want the token credential, got %+v", host, parsed.Credentials)
		}
	}
}

// The suffix match is dot-anchored, so a host that merely begins with the
// LaserData name does not inherit the bundled trust anchor.
func TestGivenALaserdataLookalikeWhenParsedThenShouldNotEnableTLS(t *testing.T) {
	parsed := mustParse(t, "iggy://user:password@laserdata.cloud.attacker.example:8090", noEnv())
	if parsed.TLS || parsed.CA != "" {
		t.Error("a look-alike host must not reach the bundled CA")
	}
	if parsed.Credentials.Username != "user" || parsed.Credentials.Password != "password" {
		t.Errorf("want the user and password, got %+v", parsed.Credentials)
	}
}

func TestGivenTLSIsDisabledWhenALaserdataHostIsParsedThenShouldNotAttachTheCA(t *testing.T) {
	parsed := mustParse(t, "iggy://token@api.laserdata.cloud",
		laser.EnvMap(map[string]string{"LASER_NO_TLS": "1"}))
	if parsed.TLS || parsed.CA != "" {
		t.Error("LASER_NO_TLS must stop automatic TLS")
	}
}

func TestGivenAnExplicitCAWhenParsedThenShouldOverrideTheEmbeddedCA(t *testing.T) {
	env := laser.EnvMap(map[string]string{"LASER_TLS_CERT": rustCertificatePath})
	parsed := mustParse(t, "iggy://token@api.laserdata.cloud?tls=true", env)
	if !parsed.TLS || parsed.CA != laser.LaserDataRootCA {
		t.Error("the named CA file must be read and used")
	}
	if parsed.CAPath != rustCertificatePath {
		t.Errorf("want the CA path recorded, got %q", parsed.CAPath)
	}
}

// A named CA states TLS intent, so it encrypts a host that would otherwise stay
// plaintext.
func TestGivenACustomCAWhenALocalHostIsParsedThenShouldEnableTLS(t *testing.T) {
	env := laser.EnvMap(map[string]string{"LASER_TLS_CERT": rustCertificatePath})
	parsed := mustParse(t, "iggy://token@demo.localhost", env)
	if !parsed.TLS || parsed.CA != laser.LaserDataRootCA {
		t.Error("a custom CA must encrypt any host")
	}
}

func TestGivenAConnectionStringWithoutASchemeWhenParsedThenShouldStillParse(t *testing.T) {
	parsed := mustParse(t, "iggy:iggy@127.0.0.1:8090", noEnv())
	if parsed.Host != "127.0.0.1" || parsed.Port != 8090 || parsed.TLS {
		t.Errorf("want plaintext 127.0.0.1:8090, got %+v", parsed)
	}
	if parsed.Credentials.Username != "iggy" || parsed.Credentials.Password != "iggy" {
		t.Errorf("want the iggy credentials, got %+v", parsed.Credentials)
	}
	if parsed.Reconnection.Interval != time.Second || parsed.Reconnection.MaxRetries != nil {
		t.Errorf("want the default reconnection, got %+v", parsed.Reconnection)
	}
}

// An IPv6 address is dialed without its brackets, which is the form the socket
// layer takes.
func TestGivenABracketedIPv6AddressWhenParsedThenShouldDialItWithoutBrackets(t *testing.T) {
	parsed := mustParse(t, "iggy://user:password@[2001:db8::7]:9080", noEnv())
	if parsed.Host != "2001:db8::7" || parsed.Port != 9080 {
		t.Errorf("want 2001:db8::7 port 9080, got %s port %d", parsed.Host, parsed.Port)
	}
	portless := mustParse(t, "token@[::ffff:192.0.2.1]?nodelay=true", noEnv())
	if portless.Host != "::ffff:192.0.2.1" || portless.Port != 8090 {
		t.Errorf("want the mapped address on the default port, got %s port %d",
			portless.Host, portless.Port)
	}
}

func TestGivenAMalformedAddressWhenParsedThenShouldRefuseIt(t *testing.T) {
	for _, value := range []string{
		"iggy://user:password@host/path",
		"iggy://user:password@host#frag",
		"iggy://user:password@[::1",
		"iggy://user:password@[::1]x:8090",
		"iggy://user:password@[::1]:",
		"iggy://user:password@[::1]:0",
		"iggy://user:password@[::1]:65536",
		"iggy://user:password@[]:8090",
		"iggy://user:password@[localhost]:8090",
		"iggy://user:password@[gggg::1]:8090",
		"iggy://user:password@::1:8090",
		"iggy://user:password@2001:db8::1:8090",
		"iggy://user:password@[::1]:8090/path",
	} {
		refuses(t, value, noEnv())
	}
}

func TestGivenNoPortWhenParsedThenShouldDefaultTo8090(t *testing.T) {
	if port := mustParse(t, "user:password@host", noEnv()).Port; port != 8090 {
		t.Errorf("want 8090, got %d", port)
	}
	if port := mustParse(t, "iggy://user:password@host?nodelay=true", noEnv()).Port; port != 8090 {
		t.Errorf("want 8090, got %d", port)
	}
}

func TestGivenAnOutOfRangePortWhenParsedThenShouldRefuseIt(t *testing.T) {
	for _, port := range []string{"0", "65536", "+80", "8o90", ""} {
		refuses(t, "iggy://user:password@host:"+port, noEnv())
	}
	if port := mustParse(t, "iggy://user:password@host:65535", noEnv()).Port; port != 65535 {
		t.Errorf("want 65535, got %d", port)
	}
}

// The Go client reaches the server over TCP only, because the Apache Iggy Go
// SDK implements that transport alone. The refusal names where the others live.
func TestGivenASchemeOtherThanTCPWhenParsedThenShouldNameTheTCPTransport(t *testing.T) {
	for _, value := range []string{"iggy+quic://u:p@host", "iggy+ws://u:p@host"} {
		if err := refuses(t, value, noEnv()); !strings.Contains(err.Error(), "TCP, plain or TLS") {
			t.Errorf("%s: want the transport hint, got %v", value, err)
		}
	}
	err := refuses(t, "iggy+http://u:p@host", noEnv())
	if !strings.Contains(err.Error(), "HTTP has no binary request path") {
		t.Errorf("want the HTTP hint, got %v", err)
	}
	refuses(t, "http://u:p@host", noEnv())
	if host := mustParse(t, "iggy+tcp://u:p@host", noEnv()).Host; host != "host" {
		t.Errorf("want host, got %q", host)
	}
}

func TestGivenMissingOrMalformedCredentialsWhenParsedThenShouldRefuseThem(t *testing.T) {
	for _, value := range []string{
		"host:8090",
		"iggy://host:8090",
		"iggy://@host",
		"iggy://:password@host",
		"iggy://user:@host",
		"iggy://a:b:c@host",
	} {
		refuses(t, value, noEnv())
	}
}

// A secret carries its reserved characters percent-encoded, and the decoded
// value is what reaches the server.
func TestGivenPercentEncodedCredentialsWhenParsedThenShouldCarryTheExactSecret(t *testing.T) {
	pair := mustParse(t, "user:p%3Ass%40w%3Frd@host", noEnv()).Credentials
	if pair.Username != "user" || pair.Password != "p:ss@w?rd" {
		t.Errorf("want the decoded pair, got %+v", pair)
	}
	slashed := mustParse(t, "iggy://us%40er:p%40ss/w0rd%25@host", noEnv()).Credentials
	if slashed.Username != "us@er" || slashed.Password != "p@ss/w0rd%" {
		t.Errorf("want the decoded pair, got %+v", slashed)
	}
	token := mustParse(t, "tok%2Fen@host", noEnv()).Credentials
	if token.Token != "tok/en" {
		t.Errorf("want the decoded token, got %+v", token)
	}
}

func TestGivenARawAtSignOrABrokenEscapeWhenParsedThenShouldRefuseThem(t *testing.T) {
	for _, value := range []string{
		"user:p@ss@host",
		"user:p%zzss@host",
		"user:pass%@host",
		"user:p%FFss@host",
		"%:pass@host",
	} {
		refuses(t, value, noEnv())
	}
}

func TestGivenUnknownRepeatedOrMalformedOptionsWhenParsedThenShouldRefuseThem(t *testing.T) {
	for _, query := range []string{
		"x=1", "TLS=true", "tls=true&tls=true", "tls", "tls=true=false", "", "tls=yes", "nodelay=1",
	} {
		refuses(t, "iggy://user:password@host?"+query, noEnv())
	}
}

func TestGivenTransportOptionsWhenParsedThenShouldApplyThem(t *testing.T) {
	parsed := mustParse(t, "iggy://user:password@host?heartbeat_interval=2s&nodelay=true"+
		"&reestablish_after=100ms&tls=true&tls_domain=node.example", noEnv())
	if parsed.HeartbeatInterval == nil || *parsed.HeartbeatInterval != 2*time.Second {
		t.Errorf("want a 2s heartbeat, got %v", parsed.HeartbeatInterval)
	}
	if parsed.NoDelay == nil || !*parsed.NoDelay {
		t.Errorf("want nodelay on, got %v", parsed.NoDelay)
	}
	if parsed.ReestablishAfter == nil || *parsed.ReestablishAfter != 100*time.Millisecond {
		t.Errorf("want a 100ms reestablish, got %v", parsed.ReestablishAfter)
	}
	if !parsed.TLS || parsed.ServerName != "node.example" {
		t.Errorf("want TLS verifying node.example, got %+v", parsed)
	}
}

// A CA file states TLS intent, so it outranks the opt-out flag.
func TestGivenACAFileWithoutTLSWhenParsedThenShouldTurnTLSOnEvenUnderNoTLS(t *testing.T) {
	for _, env := range []laser.Environment{
		noEnv(), laser.EnvMap(map[string]string{"LASER_NO_TLS": "1"}),
	} {
		parsed := mustParse(t, "iggy://user:password@host?tls_ca_file="+rustCertificatePath, env)
		if !parsed.TLS || parsed.CA != laser.LaserDataRootCA {
			t.Error("a named CA must encrypt the connection")
		}
	}
}

func TestGivenTLSFalseWithACAFileWhenParsedThenShouldRefuseIt(t *testing.T) {
	refuses(t, "iggy://user:password@host?tls=false&tls_ca_file="+rustCertificatePath, noEnv())
}

func TestGivenAnExplicitTLSFalseWhenParsedThenShouldDisableAutomaticTLS(t *testing.T) {
	managed := mustParse(t, "iggy://token@api.laserdata.cloud?tls=false", noEnv())
	if managed.TLS || managed.CA != "" {
		t.Error("tls=false must beat the managed-host default")
	}
	env := laser.EnvMap(map[string]string{"LASER_TLS_CERT": rustCertificatePath})
	custom := mustParse(t, "iggy://token@demo.localhost?tls=false", env)
	if custom.TLS || custom.CA != "" {
		t.Error("tls=false must beat LASER_TLS_CERT")
	}
}

// An explicit tls=true is the caller's own instruction, so the opt-out flag,
// which exists to stop automatic TLS, must leave it alone.
func TestGivenAnExplicitTLSTrueWhenNoTLSIsSetThenShouldStayEncrypted(t *testing.T) {
	parsed := mustParse(t, "iggy://user:password@host?tls=true",
		laser.EnvMap(map[string]string{"LASER_NO_TLS": "1"}))
	if !parsed.TLS {
		t.Error("LASER_NO_TLS must not undo an explicit tls=true")
	}
}

func TestGivenReconnectionOptionsWhenParsedThenShouldMatchTheRustGrammar(t *testing.T) {
	parsed := mustParse(t, "iggy://user:password@127.0.0.1:8090"+
		"?reconnection_retries=unlimited&reconnection_interval=250ms", noEnv())
	if parsed.Reconnection.Interval != 250*time.Millisecond {
		t.Errorf("want a 250ms interval, got %v", parsed.Reconnection.Interval)
	}
	if parsed.Reconnection.MaxRetries != nil {
		t.Errorf("unlimited must carry no budget, got %v", *parsed.Reconnection.MaxRetries)
	}
	counted := mustParse(t, "iggy://user:password@host?reconnection_retries=3", noEnv())
	if counted.Reconnection.MaxRetries == nil || *counted.Reconnection.MaxRetries != 3 {
		t.Errorf("want three retries, got %v", counted.Reconnection.MaxRetries)
	}
}

func TestGivenInvalidReconnectionOptionsWhenParsedThenShouldFailBeforeIO(t *testing.T) {
	err := refuses(t, "iggy://user:password@127.0.0.1:8090?reconnection_retries=-1", noEnv())
	if !strings.Contains(err.Error(), "reconnection_retries") {
		t.Errorf("want the option named, got %v", err)
	}
	err = refuses(t, "iggy://user:password@127.0.0.1:8090?reconnection_interval=soon", noEnv())
	if !strings.Contains(err.Error(), "reconnection_interval") {
		t.Errorf("want the option named, got %v", err)
	}
}

// A connection string carries a password, so a refusal must never put it where
// a log or a crash report would pick it up.
func TestGivenAMalformedConnectionStringWhenRefusedThenShouldNotEchoTheCredential(t *testing.T) {
	const secret = "sup3rs3cr3t"
	for _, value := range []string{
		"iggy://user:" + secret + "@[",
		"iggy://user:" + secret + "@host:99999",
		"iggy+quic://user:" + secret + "@host",
		"iggy+ws://" + secret + "@host",
		"iggy+http://user:" + secret + "@host",
		"iggy://user:" + secret + "@host?unknown=1",
		"iggy://user:" + secret + "@host?tls_ca_file=/missing/ca.pem",
		"iggy://user:" + secret + "%zz@host",
	} {
		if err := refuses(t, value, noEnv()); strings.Contains(err.Error(), secret) {
			t.Errorf("credential leaked: %v", err)
		}
	}
}

func TestGivenANegatedNoTLSFlagWhenParsedThenShouldStillEnableTLS(t *testing.T) {
	for _, value := range []string{"0", "false", "", "no"} {
		parsed := mustParse(t, "iggy://u:p@api.laserdata.cloud:8090",
			laser.EnvMap(map[string]string{"LASER_NO_TLS": value}))
		if !parsed.TLS {
			t.Errorf("LASER_NO_TLS=%q must not disable TLS", value)
		}
	}
}

func TestGivenAnAffirmativeNoTLSFlagWhenParsedThenShouldDisableTLS(t *testing.T) {
	for _, value := range []string{"1", "true", "TRUE", " yes "} {
		parsed := mustParse(t, "iggy://u:p@api.laserdata.cloud:8090",
			laser.EnvMap(map[string]string{"LASER_NO_TLS": value}))
		if parsed.TLS {
			t.Errorf("LASER_NO_TLS=%q must disable TLS", value)
		}
	}
}
