// Package laser is the LaserData client for Go. It connects to a LaserData
// deployment over Apache Iggy and speaks the AGDX contract that
// [github.com/laserdata/laser-sdk/foreign/go/wire] defines.
//
// The client connects over TCP, plain or TLS. QUIC and WebSocket are served by
// the Rust and Python clients, because the Apache Iggy Go SDK this client
// builds on implements the TCP transport only.
package laser

import (
	"errors"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

// ConfigError reports a connection string, environment variable, or builder
// setting the client cannot act on, the Go form of laser_sdk's
// LaserError::Config. It is raised before any network call, so a misconfigured
// client fails without dialing anything.
//
// A connection string carries a password, so no ConfigError ever quotes the
// value it rejected.
type ConfigError struct{ Reason string }

func (e *ConfigError) Error() string { return e.Reason }

// configError keeps the reason a constant at every call site, which is what
// stops a credential reaching a log through an error message.
func configError(reason string) error { return &ConfigError{Reason: reason} }

// TransportError reports a connection that could not be established, used, or
// closed, the Go form of laser_sdk's LaserError::Transport.
//
// The cause is kept so a caller can match on the Apache Iggy error underneath,
// and the reason names the stage rather than echoing the connection string,
// which carries a password.
type TransportError struct {
	Reason string
	Cause  error
}

func (e *TransportError) Error() string {
	if e.Cause == nil {
		return e.Reason
	}
	return e.Reason + ": " + e.Cause.Error()
}

func (e *TransportError) Unwrap() error { return e.Cause }

// IggyErrorCode is the Apache Iggy error code underneath, and whether one was
// there to read.
//
// A failure that came from the transport carries a code every Iggy client
// classifies the same way, which is what lets a caller branch on the server's
// own vocabulary rather than on message text.
func (e *TransportError) IggyErrorCode() (ierror.Code, bool) {
	var coded ierror.IggyError
	if errors.As(e.Cause, &coded) {
		return coded.Code(), true
	}
	return 0, false
}

// UncertainOperation is the request this failure left with an unknown
// outcome, to settle with Laser.ResolveOperation or Laser.AbandonOperation,
// and whether this failure named one at all.
//
// The transport retains such a request so its outcome can still be proved.
// Until it is resolved or abandoned the lane it blocks stays blocked, so a
// caller that ignores the id stalls the operations behind it.
func (e *TransportError) UncertainOperation() (uint64, bool) {
	var uncertain ierror.OperationUncertain
	if errors.As(e.Cause, &uncertain) {
		return uncertain.OperationID, true
	}
	var blocked ierror.LaneBlocked
	if errors.As(e.Cause, &blocked) {
		return blocked.OperationID, true
	}
	var partition ierror.PartitionLaneBlocked
	if errors.As(e.Cause, &partition) {
		return partition.OperationID, true
	}
	return 0, false
}

// InvalidError reports a value the client refuses to send, the Go form of
// laser_sdk's LaserError::Invalid.
type InvalidError struct{ Reason string }

func (e *InvalidError) Error() string { return e.Reason }

// UnsupportedError reports a surface the connected deployment does not serve,
// the Go form of laser_sdk's LaserError::Unsupported.
//
// It is raised before the call leaves, because plain Apache Iggy answers an
// unknown managed command with a transport error that says nothing about why.
type UnsupportedError struct{ Surface string }

func (e *UnsupportedError) Error() string {
	return e.Surface + " need a managed backend, which this deployment does not have"
}

// CodecError reports a value that could not be encoded or decoded, the Go form
// of laser_sdk's LaserError::Codec.
type CodecError struct {
	Reason string
	Cause  error
}

func (e *CodecError) Error() string {
	if e.Cause == nil {
		return e.Reason
	}
	return e.Reason + ": " + e.Cause.Error()
}

func (e *CodecError) Unwrap() error { return e.Cause }

// VersionError reports a surface whose advertised op version is not the one
// this client pins, the Go form of a surface's typed Version error.
//
// It is raised before the round trip, because a payload sent across a version
// gap would be read under a contract neither side agreed to.
type VersionError struct {
	Surface string
	Pinned  uint32
	Served  uint32
}

func (e *VersionError) Error() string {
	return e.Surface + " is served at op version " + itoa(int(e.Served)) +
		", but this client speaks " + itoa(int(e.Pinned))
}

// NoProvenanceError reports a record that carries no agentic spine at all,
// which is the normal case on a plain data topic.
//
// It is kept apart from a spine that is present but broken, because the two
// want opposite handling: a record with no spine is delivered without one,
// while a record whose fence or deadline failed to decode must not be
// delivered as though it had neither.
type NoProvenanceError struct{}

func (e *NoProvenanceError) Error() string {
	return "the record names no conversation, so it carries no provenance"
}
