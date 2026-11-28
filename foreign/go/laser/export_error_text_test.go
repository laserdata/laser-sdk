package laser

// MissingLaneForTest is the refusal a session write gets on a stream without its lane.
func MissingLaneForTest(missing string) error { return missingLane(missing) }

// ConnectAcceptStageForTest is the text of a connect that timed out before the transport came up.
func ConnectAcceptStageForTest(host string) string {
	return (&StageTimeoutError{Stage: acceptStage(host)}).Error()
}
