package laser

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
)

// certificateFileName is what the cached CA is called inside the cache
// directory, the same name the Rust client installs.
const certificateFileName = "laserdata.crt"

// certificateCacheDir is a private, user-owned directory to cache the bundled
// CA in. A world-writable shared directory is never used with a fixed name:
// another local user could pre-create the file and become the trust anchor for
// every connection to a LaserData host.
func certificateCacheDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", configError("no per-user cache directory to store the LaserData CA in: " +
			"set LASER_TLS_CERT to a CA file path")
	}
	return filepath.Join(base, "laser-sdk"), nil
}

// restrictToOwner narrows a directory to its owner. A cache directory that
// cannot be made private is refused rather than used, so the CA is never read
// from a path another local user can write.
func restrictToOwner(dir string) error {
	if err := os.Chmod(dir, 0o700); err != nil {
		return configError("restrict CA cache directory: " + err.Error())
	}
	return nil
}

// attempt makes each scratch name unique within this process, so concurrent
// installs never collide on it.
var attempt atomic.Uint64

// writeCertificate writes the bundled CA to a fresh owner-only file and then
// renames it over the target. A reader never observes a partial certificate,
// and a pre-planted file or symlink at the target is replaced rather than
// followed.
func writeCertificate(dir, path string) error {
	temp := filepath.Join(dir, certificateFileName+"."+
		strconv.Itoa(os.Getpid())+"."+strconv.FormatUint(attempt.Add(1), 10)+".tmp")
	_ = os.Remove(temp)
	// O_EXCL so an existing scratch file is never written through, and 0600 so
	// the bytes are owner-only from the moment they exist.
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return configError("create CA cert: " + err.Error())
	}
	if _, err := file.WriteString(LaserDataRootCA); err != nil {
		file.Close()
		_ = os.Remove(temp)
		return configError("write CA cert: " + err.Error())
	}
	if err := file.Sync(); err != nil {
		file.Close()
		_ = os.Remove(temp)
		return configError("write CA cert: " + err.Error())
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(temp)
		return configError("write CA cert: " + err.Error())
	}
	if err := os.Rename(temp, path); err != nil {
		_ = os.Remove(temp)
		return configError("install CA cert: " + err.Error())
	}
	return nil
}

// installCertificate caches the bundled CA in dir and returns its path. The
// cached file is reused only when its bytes are exactly the bundled CA, so a
// rotated certificate is rewritten instead of going stale and a planted trust
// anchor is replaced instead of being trusted.
func installCertificate(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", configError("create CA cache directory: " + err.Error())
	}
	if err := restrictToOwner(dir); err != nil {
		return "", err
	}
	path := filepath.Join(dir, certificateFileName)
	if cached, err := os.ReadFile(path); err == nil &&
		bytes.Equal(cached, []byte(LaserDataRootCA)) {
		return path, nil
	}
	if err := writeCertificate(dir, path); err != nil {
		return "", err
	}
	return path, nil
}
