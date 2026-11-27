package wire

// InvalidError reports a value that does not satisfy the contract, the Go form
// of laser_wire's InvalidError.
type InvalidError struct{ Reason string }

func (e *InvalidError) Error() string { return e.Reason }
