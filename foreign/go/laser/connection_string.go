package laser

import (
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// DefaultTCPPort is the port a connection string without one dials, the same
// default the Rust and TypeScript clients apply.
const DefaultTCPPort uint16 = 8090

// defaultReconnectInterval is the wait between reconnection attempts when the
// connection string names none.
const defaultReconnectInterval = time.Second

// Credentials are either a username and password pair or a personal access
// token. Exactly one form is populated.
type Credentials struct {
	Username string
	Password string
	Token    string
}

// IsToken reports whether these credentials are a personal access token rather
// than a username and password.
func (c Credentials) IsToken() bool { return c.Token != "" }

// Reconnection is how the transport retries a dropped connection. A nil
// MaxRetries means unlimited, which is what `reconnection_retries=unlimited`
// asks for and what the Rust client defaults to.
type Reconnection struct {
	Interval   time.Duration
	MaxRetries *uint32
}

// ConnectionString is a parsed and TLS-resolved connection string. Parsing
// answers every question the transport asks before it dials, so a
// misconfigured client fails without touching the network.
type ConnectionString struct {
	Host        string
	Port        uint16
	Credentials Credentials
	// ServerName is the `tls_domain` option. The client dials every node at the
	// address the cluster roster advertises, and node certificates carry the
	// deployment host name, so a TLS connection keeps verifying the host it was
	// given instead of each node's address.
	ServerName string
	TLS        bool
	// CA is the PEM of the certificate authority to verify the server with,
	// empty when the system trust store applies.
	CA string
	// CAPath names where CA was read from, empty when CA is the bundled
	// LaserData root.
	CAPath            string
	Reconnection      Reconnection
	HeartbeatInterval *time.Duration
	NoDelay           *bool
	ReestablishAfter  *time.Duration
}

// Environment reads an environment variable, reporting whether it is set.
// Passing one explicitly keeps a test hermetic and parallel safe.
type Environment func(name string) (string, bool)

// OSEnvironment reads the process environment.
func OSEnvironment(name string) (string, bool) { return os.LookupEnv(name) }

// EnvMap reads a fixed set of variables, for tests and for a caller that
// configures a client from something other than the process environment.
func EnvMap(values map[string]string) Environment {
	return func(name string) (string, bool) {
		value, present := values[name]
		return value, present
	}
}

// ParseConnectionString parses a connection string using the process
// environment for LASER_NO_TLS and LASER_TLS_CERT.
func ParseConnectionString(value string) (*ConnectionString, error) {
	return ParseConnectionStringWithEnv(value, OSEnvironment)
}

// ParseConnectionStringWithEnv parses a connection string with the Rust
// grammar: an optional `iggy://` or `iggy+tcp://` scheme, required
// percent-decoded credentials, `host[:port]`, and only Apache Iggy's TCP
// options.
func ParseConnectionStringWithEnv(value string, env Environment) (*ConnectionString, error) {
	rest, err := stripScheme(strings.TrimSpace(value))
	if err != nil {
		return nil, err
	}
	beforeQuery, query, hasQuery := strings.Cut(rest, "?")
	// The userinfo terminator is found before the query is split off, because a
	// generated password routinely contains `/` and an `@` inside a query value
	// must not be mistaken for the terminator.
	at := strings.LastIndex(beforeQuery, "@")
	if at < 0 {
		return nil, configError(
			"connection string missing credentials: use user:password@host or token@host")
	}
	credentials, err := parseCredentials(beforeQuery[:at])
	if err != nil {
		return nil, err
	}
	host, port, err := parseAddress(beforeQuery[at+1:])
	if err != nil {
		return nil, err
	}
	options := map[string]string{}
	if hasQuery {
		if options, err = parseOptions(query); err != nil {
			return nil, err
		}
	}
	parsed := &ConnectionString{
		Host:        host,
		Port:        port,
		Credentials: credentials,
		ServerName:  options["tls_domain"],
	}
	if err := resolveTLS(parsed, options, env); err != nil {
		return nil, err
	}
	if err := applyTransportOptions(parsed, options); err != nil {
		return nil, err
	}
	return parsed, nil
}

// resolveTLS decides whether the connection is encrypted and which authority
// verifies it, matching Rust's resolve_tls.
func resolveTLS(parsed *ConnectionString, options map[string]string, env Environment) error {
	// Read by value, not by presence: LASER_NO_TLS=0 and LASER_NO_TLS=false
	// must not silently downgrade a managed host to plaintext.
	noTLSValue, _ := env("LASER_NO_TLS")
	noTLS := flagValueEnabled(noTLSValue)
	envCert, _ := env("LASER_TLS_CERT")
	explicitTLS, hasExplicitTLS := options["tls"]
	stringCA, hasStringCA := options["tls_ca_file"]

	// An explicit tls=true is the caller's own instruction, so it holds whatever
	// the environment says. Only automatic TLS is what LASER_NO_TLS stops.
	parsed.TLS = hasExplicitTLS && explicitTLS == "true"

	// A CA file in the string states TLS intent, so it turns TLS on even under
	// LASER_NO_TLS. An explicit tls=false or LASER_NO_TLS stops automatic TLS.
	switch {
	case hasStringCA:
		parsed.TLS = true
		parsed.CAPath = stringCA
	case (!hasExplicitTLS || explicitTLS != "false") && !noTLS:
		switch {
		case envCert != "":
			parsed.TLS = true
			parsed.CAPath = envCert
		case isLaserDataHost(parsed.Host):
			parsed.TLS = true
			parsed.CA = LaserDataRootCA
		}
	}
	if parsed.CAPath == "" {
		return nil
	}
	pem, err := os.ReadFile(parsed.CAPath)
	if err != nil {
		// The path is not a credential, so naming it is what makes the failure
		// actionable.
		return configError("failed to read TLS CA file: " + parsed.CAPath)
	}
	parsed.CA = string(pem)
	return nil
}

func applyTransportOptions(parsed *ConnectionString, options map[string]string) error {
	interval := defaultReconnectInterval
	if value, present := options["reconnection_interval"]; present {
		custom, err := parseDuration("reconnection_interval", value)
		if err != nil {
			return err
		}
		interval = custom
	}
	retries, err := parseReconnectRetries(options)
	if err != nil {
		return err
	}
	parsed.Reconnection = Reconnection{Interval: interval, MaxRetries: retries}
	if value, present := options["heartbeat_interval"]; present {
		heartbeat, err := parseDuration("heartbeat_interval", value)
		if err != nil {
			return err
		}
		parsed.HeartbeatInterval = &heartbeat
	}
	if value, present := options["reestablish_after"]; present {
		reestablish, err := parseDuration("reestablish_after", value)
		if err != nil {
			return err
		}
		parsed.ReestablishAfter = &reestablish
	}
	if value, present := options["nodelay"]; present {
		noDelay := value == "true"
		parsed.NoDelay = &noDelay
	}
	return nil
}

// isLaserDataHost reports a LaserData-operated host. The dot-anchored suffix
// match rejects look-alikes such as laserdata.cloud.attacker.example.
func isLaserDataHost(host string) bool {
	normalized := strings.ToLower(host)
	return normalized == "laserdata.cloud" ||
		strings.HasSuffix(normalized, ".laserdata.cloud") ||
		normalized == "laserdata.com" ||
		strings.HasSuffix(normalized, ".laserdata.com")
}

// flagValueEnabled reads an opt-out flag by value. Bare presence is not
// enough: LASER_NO_TLS=0 and LASER_NO_TLS=false must not disable TLS.
func flagValueEnabled(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// stripScheme removes the optional scheme. A `://` that appears after the
// userinfo has begun belongs to a password, not to a scheme.
func stripScheme(value string) (string, error) {
	for _, scheme := range []string{"iggy://", "iggy+tcp://"} {
		if strings.HasPrefix(value, scheme) {
			return value[len(scheme):], nil
		}
	}
	separator := strings.Index(value, "://")
	if separator < 0 || strings.ContainsAny(value[:separator], ":@") {
		return value, nil
	}
	switch value[:separator] {
	case "iggy+quic", "iggy+ws":
		return "", configError(
			"the Go client connects over TCP, plain or TLS: use iggy:// or iggy+tcp://, " +
				"QUIC and WebSocket are served by the Rust and Python clients")
	case "iggy+http":
		return "", configError(
			"iggy+http:// is not supported: HTTP has no binary request path for Laser " +
				"features, use iggy:// or iggy+tcp://")
	default:
		return "", configError("unsupported connection string scheme: use iggy:// or iggy+tcp://")
	}
}

// parseCredentials reads `user:password` or a personal access token, both
// non-empty and percent-decoded, so a secret carries `:`, `@`, `?` and `%` as
// %3A, %40, %3F and %25. A raw `@` or a second raw `:` would split them again.
func parseCredentials(userinfo string) (Credentials, error) {
	if strings.Contains(userinfo, "@") {
		return Credentials{}, configError(
			"connection string credentials cannot contain a raw `@`, write it as %40")
	}
	parts := strings.Split(userinfo, ":")
	if len(parts) > 2 {
		return Credentials{}, configError(
			"connection string credentials cannot contain more than one raw `:`, write it as %3A")
	}
	if len(parts) == 1 {
		if parts[0] == "" {
			return Credentials{}, configError("connection string has an empty access token")
		}
		token, err := decodeCredential(parts[0])
		if err != nil {
			return Credentials{}, err
		}
		return Credentials{Token: token}, nil
	}
	if parts[0] == "" || parts[1] == "" {
		return Credentials{}, configError("connection string has an empty username or password")
	}
	username, err := decodeCredential(parts[0])
	if err != nil {
		return Credentials{}, err
	}
	password, err := decodeCredential(parts[1])
	if err != nil {
		return Credentials{}, err
	}
	return Credentials{Username: username, Password: password}, nil
}

// decodeCredential percent-decodes one credential. The error never echoes it.
// An escape that decodes to bytes which are not UTF-8 is refused rather than
// carried, so a secret means the same thing in every client.
func decodeCredential(encoded string) (string, error) {
	// PathUnescape rather than QueryUnescape, because `+` is a literal in a
	// credential and must not become a space.
	decoded, err := url.PathUnescape(encoded)
	if err != nil || !utf8.ValidString(decoded) {
		return "", configError("connection string credentials have a malformed percent escape")
	}
	return decoded, nil
}

// parseAddress reads `host`, `host:port`, `[ipv6]` or `[ipv6]:port`. A missing
// port becomes DefaultTCPPort, and an IPv6 host is dialed without its brackets.
func parseAddress(address string) (string, uint16, error) {
	malformed := configError(
		"connection string address must be host, host:port, [ipv6] or [ipv6]:port, without a path")
	if strings.ContainsAny(address, "/#") {
		return "", 0, malformed
	}
	var host, portText string
	var hasPort bool
	if strings.HasPrefix(address, "[") {
		closing := strings.Index(address, "]")
		if closing < 0 {
			return "", 0, malformed
		}
		host = address[1:closing]
		if !isIPv6(host) {
			return "", 0, configError("connection string brackets must hold an IPv6 address")
		}
		rest := address[closing+1:]
		if rest != "" && !strings.HasPrefix(rest, ":") {
			return "", 0, malformed
		}
		if rest != "" {
			portText, hasPort = rest[1:], true
		}
	} else {
		if strings.ContainsAny(address, "[]") {
			return "", 0, malformed
		}
		host, portText, hasPort = strings.Cut(address, ":")
	}
	if host == "" {
		return "", 0, configError("connection string missing host")
	}
	if !hasPort {
		return host, DefaultTCPPort, nil
	}
	if !decimal.MatchString(portText) {
		return "", 0, configError("connection string port must be a number from 1 to 65535")
	}
	port, err := strconv.ParseUint(portText, 10, 32)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, configError("connection string port must be a number from 1 to 65535")
	}
	return host, uint16(port), nil
}

// isIPv6 reports an address that net can parse and that is written in the IPv6
// form. An IPv4-mapped address such as ::ffff:192.0.2.1 is IPv6, so the colon
// rather than the parsed width decides.
func isIPv6(host string) bool {
	return net.ParseIP(host) != nil && strings.Contains(host, ":")
}

// connectionOptions is Apache Iggy's TCP connection string option set, the
// only one Rust, TypeScript and Python accept.
var connectionOptions = map[string]struct{}{
	"tls":                   {},
	"tls_domain":            {},
	"tls_ca_file":           {},
	"reconnection_retries":  {},
	"reconnection_interval": {},
	"reestablish_after":     {},
	"heartbeat_interval":    {},
	"nodelay":               {},
}

var decimal = regexp.MustCompile(`^\d+$`)

var durationPattern = regexp.MustCompile(`^(\d+)(ms|s|m)$`)

// parseOptions reads the query. Every option is `key=value`, names one of
// Apache Iggy's TCP options, and appears once. Boolean options take `true` or
// `false` only.
func parseOptions(query string) (map[string]string, error) {
	options := map[string]string{}
	for _, pair := range strings.Split(query, "&") {
		key, value, found := strings.Cut(pair, "=")
		if !found || strings.Contains(value, "=") {
			return nil, configError("connection string option must be key=value")
		}
		if _, known := connectionOptions[key]; !known {
			return nil, configError(
				"connection string has an unknown option: supported are tls, tls_domain, " +
					"tls_ca_file, reconnection_retries, reconnection_interval, " +
					"reestablish_after, heartbeat_interval, nodelay")
		}
		if _, repeated := options[key]; repeated {
			return nil, configError("connection string repeats an option")
		}
		if (key == "tls" || key == "nodelay") && value != "true" && value != "false" {
			return nil, configError("connection string tls and nodelay take true or false")
		}
		options[key] = value
	}
	if options["tls"] == "false" {
		if _, present := options["tls_ca_file"]; present {
			return nil, configError("connection string sets tls=false together with tls_ca_file")
		}
	}
	return options, nil
}

func parseDuration(name, value string) (time.Duration, error) {
	match := durationPattern.FindStringSubmatch(value)
	if match == nil {
		return 0, configError(name + " must use ms, s, or m")
	}
	amount, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return 0, configError(name + " is outside the supported range")
	}
	unit := map[string]time.Duration{
		"ms": time.Millisecond,
		"s":  time.Second,
		"m":  time.Minute,
	}[match[2]]
	// The multiply is checked rather than wrapped, so a huge literal is refused
	// instead of becoming a small or negative interval.
	if amount != 0 && amount > int64(time.Duration(1<<63-1)/unit) {
		return 0, configError(name + " is outside the supported range")
	}
	return time.Duration(amount) * unit, nil
}

// parseReconnectRetries reads the retry budget. A missing option and the word
// `unlimited` both mean no limit.
func parseReconnectRetries(options map[string]string) (*uint32, error) {
	value, present := options["reconnection_retries"]
	if !present || value == "unlimited" {
		return nil, nil
	}
	if !decimal.MatchString(value) {
		return nil, configError("reconnection_retries must be a non-negative integer or unlimited")
	}
	retries, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return nil, configError("reconnection_retries must be a non-negative integer or unlimited")
	}
	converted := uint32(retries)
	return &converted, nil
}
