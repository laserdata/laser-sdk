package laser_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// cacheIn points the per-user cache directory at a temporary one, so a test
// never writes to the developer's real cache and never reads a certificate
// another test installed.
func cacheIn(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "cache"))
	t.Setenv("LOCALAPPDATA", filepath.Join(home, "cache"))
	return home
}

// cachedCertificate finds the installed CA under a temporary home, whichever
// per-user cache location this platform uses.
func cachedCertificate(t *testing.T, home string) string {
	t.Helper()
	var found []string
	err := filepath.Walk(home, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && info.Name() == "laserdata.crt" {
			found = append(found, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the cache: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("want one cached certificate under %s, got %v", home, found)
	}
	return found[0]
}

func settingsFor(t *testing.T, value string, env laser.Environment) *laser.TCPSettings {
	t.Helper()
	settings, err := mustParse(t, value, env).TCPSettings()
	if err != nil {
		t.Fatalf("lower %q: %v", value, err)
	}
	return settings
}

// A LaserData host is encrypted against the bundled root with no setup, and the
// Iggy transport reads a CA from a path, so the bundled bytes have to land in a
// file before the first dial.
func TestGivenALaserdataHostWhenLoweredThenShouldInstallTheBundledCA(t *testing.T) {
	home := cacheIn(t)
	settings := settingsFor(t, "iggy://token@api.laserdata.cloud", noEnv())

	if !settings.TLS || !settings.TLSValidateCertificate {
		t.Fatalf("want a verified TLS connection, got %+v", settings)
	}
	if settings.TLSDomain != "api.laserdata.cloud" {
		t.Errorf("want the deployment host verified, got %q", settings.TLSDomain)
	}
	installed := cachedCertificate(t, home)
	if settings.TLSCAFile != installed {
		t.Errorf("want the CA file at %q, got %q", installed, settings.TLSCAFile)
	}
	pem, err := os.ReadFile(installed)
	if err != nil {
		t.Fatalf("read the installed CA: %v", err)
	}
	if string(pem) != laser.LaserDataRootCA {
		t.Error("the installed CA must be the bundled root byte for byte")
	}
}

// The cache holds a trust anchor, so it is owner-only. A directory another
// local user can write is one where they can become the anchor.
func TestGivenTheCertificateCacheWhenInstalledThenShouldBeOwnerOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not carry these mode bits")
	}
	home := cacheIn(t)
	settingsFor(t, "iggy://token@api.laserdata.cloud", noEnv())

	installed := cachedCertificate(t, home)
	file, err := os.Stat(installed)
	if err != nil {
		t.Fatalf("stat the CA: %v", err)
	}
	if mode := file.Mode().Perm(); mode != 0o600 {
		t.Errorf("want the certificate at 0600, got %#o", mode)
	}
	dir, err := os.Stat(filepath.Dir(installed))
	if err != nil {
		t.Fatalf("stat the cache directory: %v", err)
	}
	if mode := dir.Mode().Perm(); mode != 0o700 {
		t.Errorf("want the cache directory at 0700, got %#o", mode)
	}
}

// A file already holding the bundled bytes is reused, so a second connection
// does not rewrite the trust anchor.
func TestGivenAnInstalledCertificateWhenLoweredAgainThenShouldReuseIt(t *testing.T) {
	home := cacheIn(t)
	first := settingsFor(t, "iggy://token@api.laserdata.cloud", noEnv())
	installed := cachedCertificate(t, home)
	before, err := os.Stat(installed)
	if err != nil {
		t.Fatalf("stat the CA: %v", err)
	}
	// A rewrite would move the modification time, so the timestamp is what
	// separates reuse from a silent reinstall.
	time.Sleep(10 * time.Millisecond)

	second := settingsFor(t, "iggy://token@api.laserdata.cloud", noEnv())
	if second.TLSCAFile != first.TLSCAFile {
		t.Errorf("want the same path, got %q then %q", first.TLSCAFile, second.TLSCAFile)
	}
	after, err := os.Stat(installed)
	if err != nil {
		t.Fatalf("stat the CA: %v", err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Error("a cached certificate that already matches must not be rewritten")
	}
}

// A certificate that does not match the bundled bytes is replaced rather than
// trusted, which is what stops a planted anchor and what lets a rotated root
// take effect.
func TestGivenAPlantedCertificateWhenLoweredThenShouldReplaceIt(t *testing.T) {
	home := cacheIn(t)
	settingsFor(t, "iggy://token@api.laserdata.cloud", noEnv())
	installed := cachedCertificate(t, home)
	if err := os.WriteFile(installed, []byte("-----BEGIN CERTIFICATE-----\nplanted\n"), 0o600); err != nil {
		t.Fatalf("plant a certificate: %v", err)
	}

	settingsFor(t, "iggy://token@api.laserdata.cloud", noEnv())

	pem, err := os.ReadFile(installed)
	if err != nil {
		t.Fatalf("read the CA: %v", err)
	}
	if string(pem) != laser.LaserDataRootCA {
		t.Error("a planted trust anchor must be replaced with the bundled root")
	}
}

// A CA the caller named is used as given, and nothing is written to the cache:
// there is no bundled certificate to install.
func TestGivenANamedCAWhenLoweredThenShouldUseItWithoutTouchingTheCache(t *testing.T) {
	home := cacheIn(t)
	settings := settingsFor(t,
		"iggy://user:password@host?tls_ca_file="+rustCertificatePath, noEnv())

	if settings.TLSCAFile != rustCertificatePath {
		t.Errorf("want the named CA, got %q", settings.TLSCAFile)
	}
	if entries, err := os.ReadDir(filepath.Join(home, "cache", "laser-sdk")); err == nil &&
		len(entries) > 0 {
		t.Errorf("a named CA must not install the bundled one, found %v", entries)
	}
}

func TestGivenAPlaintextConnectionWhenLoweredThenShouldCarryNoTLS(t *testing.T) {
	cacheIn(t)
	settings := settingsFor(t, "iggy://user:password@127.0.0.1:8090", noEnv())

	if settings.TLS || settings.TLSCAFile != "" || settings.TLSDomain != "" {
		t.Errorf("want a plaintext connection, got %+v", settings)
	}
	if count := len(settings.Options()); count != 2 {
		t.Errorf("want the address and the login alone, got %d options", count)
	}
}

// tls_domain exists because the roster advertises node addresses while the
// certificates carry the deployment name.
func TestGivenATLSDomainWhenLoweredThenShouldVerifyThatNameInsteadOfTheHost(t *testing.T) {
	cacheIn(t)
	settings := settingsFor(t, "iggy://user:password@10.0.0.4?tls=true&tls_domain=node.example",
		noEnv())

	if settings.TLSDomain != "node.example" {
		t.Errorf("want node.example verified, got %q", settings.TLSDomain)
	}
	if settings.TLSCAFile != "" {
		t.Errorf("want the system trust store, got %q", settings.TLSCAFile)
	}
}

func TestGivenAnAddressWhenLoweredThenShouldBracketAnIPv6Host(t *testing.T) {
	cacheIn(t)
	if address := settingsFor(t, "iggy://user:password@[2001:db8::7]:9080", noEnv()).Address; address != "[2001:db8::7]:9080" {
		t.Errorf("want a bracketed address, got %q", address)
	}
	if address := settingsFor(t, "iggy://user:password@host", noEnv()).Address; address != "host:8090" {
		t.Errorf("want host:8090, got %q", address)
	}
}

func TestGivenEitherCredentialFormWhenLoweredThenShouldCarryIt(t *testing.T) {
	cacheIn(t)
	token := settingsFor(t, "iggy://tok%2Fen@host", noEnv())
	if !token.Credentials.IsToken() || token.Credentials.Token != "tok/en" {
		t.Errorf("want the decoded token, got %+v", token.Credentials)
	}
	pair := settingsFor(t, "iggy://user:p%40ss@host", noEnv())
	if pair.Credentials.IsToken() || pair.Credentials.Password != "p@ss" {
		t.Errorf("want the decoded pair, got %+v", pair.Credentials)
	}
}

func TestGivenTransportOptionsWhenLoweredThenShouldForwardThem(t *testing.T) {
	cacheIn(t)
	settings := settingsFor(t,
		"iggy://user:password@host?nodelay=true&heartbeat_interval=2s", noEnv())

	if settings.NoDelay == nil || !*settings.NoDelay {
		t.Errorf("want nodelay forwarded, got %v", settings.NoDelay)
	}
	if settings.HeartbeatInterval == nil || *settings.HeartbeatInterval != 2*time.Second {
		t.Errorf("want the heartbeat forwarded, got %v", settings.HeartbeatInterval)
	}
	if count := len(settings.Options()); count != 4 {
		t.Errorf("want address, login, nodelay and heartbeat, got %d options", count)
	}
}

// A CA that cannot be read is a configuration failure, reported before any
// dial rather than as a handshake error later.
func TestGivenAnUnreadableCAWhenParsedThenShouldFailBeforeConnecting(t *testing.T) {
	cacheIn(t)
	_, err := laser.ParseConnectionStringWithEnv(
		"iggy://user:password@host?tls_ca_file=/missing/ca.pem", noEnv())
	if err == nil {
		t.Fatal("a missing CA file must be refused")
	}
	if !strings.Contains(err.Error(), "/missing/ca.pem") {
		t.Errorf("want the path named, got %v", err)
	}
}

// A cache directory that already exists wide open is narrowed before the
// certificate is installed into it. Creating it is not enough: a directory left
// behind by another tool, or by an earlier release, is the case where another
// local user could still plant the trust anchor.
func TestGivenAWorldWritableCacheWhenInstallingThenShouldNarrowItFirst(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not carry these mode bits")
	}
	home := cacheIn(t)
	for _, dir := range []string{
		filepath.Join(home, "cache", "laser-sdk"),
		filepath.Join(home, "Library", "Caches", "laser-sdk"),
	} {
		if err := os.MkdirAll(dir, 0o777); err != nil {
			t.Fatalf("pre-create the cache: %v", err)
		}
		// MkdirAll applies the umask, so the mode is set again explicitly to
		// reach the state this test is about.
		if err := os.Chmod(dir, 0o777); err != nil {
			t.Fatalf("widen the cache: %v", err)
		}
	}

	settingsFor(t, "iggy://token@api.laserdata.cloud", noEnv())

	installed := cachedCertificate(t, home)
	dir, err := os.Stat(filepath.Dir(installed))
	if err != nil {
		t.Fatalf("stat the cache directory: %v", err)
	}
	if mode := dir.Mode().Perm(); mode != 0o700 {
		t.Errorf("an existing world-writable cache must be narrowed, got %#o", mode)
	}
}

func TestGivenACachedSymlinkWhenLoweringTLSOptionsThenShouldInstallARegularFile(t *testing.T) {
	home := cacheIn(t)
	parsed, err := laser.ParseConnectionStringWithEnv("iggy://u:p@eu.laserdata.cloud", noEnv())
	if err != nil {
		t.Fatal(err)
	}
	settings, err := parsed.TCPSettings()
	if err != nil {
		t.Fatal(err)
	}
	path := settings.TLSCAFile
	target := filepath.Join(home, "other.pem")
	if err := os.WriteFile(target, []byte(laser.LaserDataRootCA), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := parsed.TCPSettings(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("expected a regular CA file: %v, %v", info, err)
	}
}
