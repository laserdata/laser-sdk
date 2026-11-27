package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// PublishControl appends one control command.
//
// A control command is how a deployment's registries are changed: projections,
// bindings and writer schemas are all declared this way. The command is
// durably appended and applied asynchronously, so a caller observes the apply
// by reading the registry back rather than by the append succeeding.
//
// It needs a managed backend. Apache Iggy on its own serves no control topic,
// so the call is refused rather than appending a command nothing will fold.
func (laser *Laser) PublishControl(
	ctx context.Context, command wire.ControlCommand,
) error {
	return laser.PublishControlIn(ctx, command, laser.resourceStream())
}

// PublishControlIn appends a control command naming the stream its resource
// names belong to, for a caller administering a stream other than its own.
func (laser *Laser) PublishControlIn(
	ctx context.Context, command wire.ControlCommand, stream string,
) error {
	return laser.PublishControlAs(ctx, command, stream, NewOperationID())
}

// PublishControlAs appends a control command under a caller-chosen operation
// identity, so a retry that outlives the process is still applied once.
func (laser *Laser) PublishControlAs(
	ctx context.Context, command wire.ControlCommand, stream string,
	operationID wire.U128,
) error {
	if !laser.capabilities.Managed {
		return &UnsupportedError{Surface: "control commands"}
	}
	// A server of another control version would fold this command under a
	// contract neither side agreed to, which is worse than refusing it.
	if err := laser.requireOpVersion("control commands", wire.ControlOpVersion,
		func(versions wire.OpVersions) uint32 { return versions.Control }); err != nil {
		return err
	}
	if err := command.Validate(); err != nil {
		return &InvalidError{Reason: err.Error()}
	}
	envelope := wire.ControlEnvelope{V: wire.ControlOpVersion, Command: command}
	if stream != "" {
		envelope.Stream = &stream
	}
	payload, err := wire.Marshal(envelope)
	if err != nil {
		return &CodecError{Reason: "encode the control command", Cause: err}
	}
	reply, err := laser.SendManagedWithOperation(
		ctx, wire.AgdxControlPublishCode, payload, operationID)
	if err != nil {
		return err
	}
	var outcome wire.CommandError
	if err := wire.Unmarshal(reply, &outcome); err != nil {
		return &CodecError{Reason: "decode the control reply", Cause: err}
	}
	if outcome.Code != wire.ResultOk {
		return &CommandFailure{Failure: outcome}
	}
	return nil
}

// CommandFailure reports a control command the deployment refused.
type CommandFailure struct{ Failure wire.CommandError }

func (e *CommandFailure) Error() string {
	return "control command: " + e.Failure.Code.String() + ": " + e.Failure.Message
}

// ResultCode is the shared code this failure projects onto.
func (e *CommandFailure) ResultCode() wire.ResultCode { return e.Failure.Code }

// Retryable reports a condition where the identical command may succeed later.
func (e *CommandFailure) Retryable() bool { return e.Failure.Code.IsRetryable() }

// executeBrowse runs one read-only registry browse.
//
// Browse is served off the registry snapshot by a managed backend and has no
// topic transport, so a deployment without one is refused rather than left to
// time out waiting for a reply nothing will send.
func (laser *Laser) executeBrowse(
	ctx context.Context, code uint32, request any,
) (*wire.BrowseOutcome, error) {
	if !laser.capabilities.Managed {
		return nil, &UnsupportedError{Surface: "the registry browse"}
	}
	// Browse rides the query envelope version, so a skew is refused here
	// rather than read as an outcome of another shape.
	if err := laser.requireOpVersion("the registry browse", wire.QueryOpVersion,
		func(versions wire.OpVersions) uint32 { return versions.Query }); err != nil {
		return nil, err
	}
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode the browse request", Cause: err}
	}
	raw, err := laser.SendManaged(ctx, code, payload)
	if err != nil {
		return nil, err
	}
	var reply wire.BrowseReply
	if err := wire.Unmarshal(raw, &reply); err != nil {
		return nil, &CodecError{Reason: "decode the browse reply", Cause: err}
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{
				Reason: "the browse reply carried neither an outcome nor an error"}
		}
		return nil, &QueryError{Failure: *reply.Err}
	}
	if reply.Outcome == nil {
		return nil, &CodecError{Reason: "the browse reply carried no outcome"}
	}
	return reply.Outcome, nil
}

func unexpectedBrowseOutcome(verb string, kind wire.BrowseOutcomeKind) error {
	return &CodecError{Reason: "the registry " + verb +
		" answered with an unexpected outcome: " + itoa(int(kind))}
}
