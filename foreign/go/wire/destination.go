package wire

// BackendBinding names a backend resource at a generation. A generation rises
// whenever the operator replaces the resource behind the same identity, so a
// reader can tell a reconfigured backend from the one it last saw.
type BackendBinding struct {
	ResourceID BackendResourceID `cbor:"resource_id"`
	Generation uint64            `cbor:"generation"`
}

// Validate checks the binding names a real resource and generation.
func (binding BackendBinding) Validate() error {
	if binding.ResourceID.IsZero() {
		return &InvalidError{Reason: "backend resource id must be nonzero"}
	}
	if binding.Generation == 0 {
		return &InvalidError{Reason: "backend generation must be nonzero"}
	}
	return nil
}
