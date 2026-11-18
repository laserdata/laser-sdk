package laser

import (
	"net"
	"strconv"
	"time"

	"github.com/apache/iggy/foreign/go/client/tcp"
)

// TCPSettings is exactly what this client hands the Apache Iggy TCP transport.
// The lowering is a value rather than a list of opaque option closures so it
// can be read back, which is what lets a test state what a connection string
// does to the socket rather than only that it parsed.
type TCPSettings struct {
	// Address is host:port, with an IPv6 host bracketed.
	Address     string
	Credentials Credentials
	TLS         bool
	// TLSDomain is the name the server certificate must carry.
	TLSDomain string
	// TLSCAFile is the certificate authority to verify against, empty when the
	// operating system trust store applies.
	TLSCAFile              string
	TLSValidateCertificate bool
	NoDelay                *bool
	HeartbeatInterval      *time.Duration
}

// TCPSettings lowers a parsed connection string onto the transport settings it
// describes.
//
// The Laser grammar is parsed by this package rather than by the Iggy SDK's own
// ParseConnectionString, because the TLS decision is Laser's: a LaserData host
// is encrypted automatically against the bundled root, LASER_TLS_CERT names a
// CA for any host, and LASER_NO_TLS opts out. Parsing it here is also what
// keeps the accepted grammar identical to the Rust and TypeScript clients,
// which the cross-SDK parity matrix compares.
//
// Resolving the certificate can write to the per-user cache directory, so this
// is called once at connect time rather than on every request.
func (c *ConnectionString) TCPSettings() (*TCPSettings, error) {
	settings := &TCPSettings{
		Address:           net.JoinHostPort(c.Host, strconv.Itoa(int(c.Port))),
		Credentials:       c.Credentials,
		TLS:               c.TLS,
		NoDelay:           c.NoDelay,
		HeartbeatInterval: c.HeartbeatInterval,
	}
	if !c.TLS {
		return settings, nil
	}
	// The client dials every node at the address the cluster roster advertises,
	// and node certificates carry the deployment host name, so verification
	// stays pinned to the host the caller named instead of each node's address.
	settings.TLSDomain = c.ServerName
	if settings.TLSDomain == "" {
		settings.TLSDomain = c.Host
	}
	settings.TLSValidateCertificate = true
	path, err := c.certificatePath()
	if err != nil {
		return nil, err
	}
	settings.TLSCAFile = path
	return settings, nil
}

// Options renders the settings as the Apache Iggy Go SDK's TCP options.
func (s *TCPSettings) Options() []tcp.Option {
	options := []tcp.Option{
		tcp.WithServerAddress(s.Address),
		tcp.WithAutoLogin(s.credentials()),
	}
	if s.TLS {
		tlsOptions := []tcp.TLSOption{
			tcp.WithTLSDomain(s.TLSDomain),
			tcp.WithTLSValidateCertificate(s.TLSValidateCertificate),
		}
		if s.TLSCAFile != "" {
			tlsOptions = append(tlsOptions, tcp.WithTLSCAFile(s.TLSCAFile))
		}
		options = append(options, tcp.WithTLS(tlsOptions...))
	}
	if s.NoDelay != nil {
		options = append(options, tcp.WithNoDelay(*s.NoDelay))
	}
	if s.HeartbeatInterval != nil {
		options = append(options, tcp.WithHeartbeatInterval(*s.HeartbeatInterval))
	}
	// The reconnection options are parsed and validated but not forwarded: the
	// Apache Iggy Go SDK keeps its own reconnect pacing and exposes no setting
	// for it. Rejecting a value the other clients reject is still worth doing,
	// so one connection string means the same thing everywhere it is read.
	return options
}

func (s *TCPSettings) credentials() tcp.Credentials {
	if s.Credentials.IsToken() {
		return tcp.NewPersonalAccessTokenCredentials(s.Credentials.Token)
	}
	return tcp.NewUsernamePasswordCredentials(s.Credentials.Username, s.Credentials.Password)
}

// TCPOptions lowers a parsed connection string straight onto the Apache Iggy
// TCP options, which is how this client dials.
func (c *ConnectionString) TCPOptions() ([]tcp.Option, error) {
	settings, err := c.TCPSettings()
	if err != nil {
		return nil, err
	}
	return settings.Options(), nil
}

// certificatePath names the CA file to verify the server with, empty when the
// operating system trust store applies.
//
// The Iggy SDK reads a CA from a path, so the bundled root has to exist as a
// file. It is cached in a private per-user directory rather than a temporary
// one, so repeated connections do not rewrite it.
func (c *ConnectionString) certificatePath() (string, error) {
	if c.CAPath != "" {
		return c.CAPath, nil
	}
	if c.CA == "" {
		return "", nil
	}
	dir, err := certificateCacheDir()
	if err != nil {
		return "", err
	}
	return installCertificate(dir)
}
