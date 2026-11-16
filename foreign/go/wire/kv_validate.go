package wire

import "fmt"

// The key-value surface's validation rules.
//
// Every bound here is the contract's, not a backend's. A request outside one
// is refused locally rather than after a round trip, and the ones a store
// could still narrow, such as a granted lease lifetime, are left to the store.

// validateKvKey bounds a key, which addresses one entry and so can be neither
// absent nor larger than a key the store indexes.
func validateKvKey(label string, key []byte) error {
	if len(key) == 0 {
		return &InvalidError{Reason: label + " is empty"}
	}
	if len(key) > MaxKeyBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s is %dB, exceeds cap %dB", label, len(key), MaxKeyBytes)}
	}
	return nil
}

func validateKvValue(value []byte) error {
	if len(value) > MaxValueBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"value is %dB, exceeds cap %dB", len(value), MaxValueBytes)}
	}
	return nil
}

// Validate checks a write's namespace, key, value and expiry.
func (request KvSet) Validate() error {
	if err := ValidateNamespace(request.Namespace); err != nil {
		return err
	}
	// Resolved against a zero clock, which checks the two forms are not both
	// named without needing to know the time.
	if _, err := ResolveExpiry(
		request.ExpiresAtMicros, request.TTLMicros, 0); err != nil {
		return err
	}
	if len(request.Key) == 0 {
		return &InvalidError{Reason: "key-value key is empty"}
	}
	if len(request.Key) > MaxKeyBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"key is %dB, exceeds cap %dB", len(request.Key), MaxKeyBytes)}
	}
	return validateKvValue(request.Value)
}

// Validate checks a scan's namespace and page size.
func (request KvScan) Validate() error {
	if err := ValidateNamespace(request.Namespace); err != nil {
		return err
	}
	if int(request.Results) > MaxScanLimit {
		return &InvalidError{Reason: fmt.Sprintf(
			"scan page size %d exceeds cap %d", request.Results, MaxScanLimit)}
	}
	return nil
}

// Validate checks a fenced conditional write, which spans two namespaces: the
// one it writes and the one holding the lease that gates the write.
func (request KvCasFenced) Validate() error {
	if request.V != KvLeaseOpVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"unsupported fenced-lease op version (expected %d, got %d)",
			KvLeaseOpVersion, request.V)}
	}
	if err := ValidateNamespace(request.Namespace); err != nil {
		return err
	}
	if err := ValidateNamespace(request.FenceNamespace); err != nil {
		return err
	}
	if _, err := ResolveExpiry(
		request.ExpiresAtMicros, request.TTLMicros, 0); err != nil {
		return err
	}
	if err := validateKvKey("key-value key", request.Key); err != nil {
		return err
	}
	if err := validateKvKey("fence key", request.FenceKey); err != nil {
		return err
	}
	if err := validateKvValue(request.Value); err != nil {
		return err
	}
	// Zero is the reserved unversioned sentinel, never a granted token, so a
	// zero fence would gate the write on nothing.
	if request.FenceToken == 0 {
		return &InvalidError{Reason: "fence token must not be zero"}
	}
	return nil
}

// validateLeaseShape is the shape every lease request shares: the op version,
// the coordination namespace, the lease key, and the stable holder identity
// that renewal and release must present again.
func validateLeaseShape(v uint32, namespace string, key []byte, holderID string) error {
	if v != KvLeaseOpVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"unsupported fenced-lease op version (expected %d, got %d)",
			KvLeaseOpVersion, v)}
	}
	if err := ValidateNamespace(namespace); err != nil {
		return err
	}
	if err := validateKvKey("lease key", key); err != nil {
		return err
	}
	if holderID == "" {
		return &InvalidError{Reason: "lease holder id must not be empty"}
	}
	if len(holderID) > MaxHolderIDBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"lease holder id is %dB, exceeds cap %dB", len(holderID), MaxHolderIDBytes)}
	}
	if hasControl(holderID) {
		return &InvalidError{Reason: "lease holder id contains a control character"}
	}
	return nil
}

// validateLeaseTTL bounds a requested lease lifetime on both sides.
//
// The bound is the contract's, not each backend's: a floor so a grant
// survives one round trip plus the holder's renewal cadence, a ceiling so a
// crashed holder's ownership always expires. The store may still grant less,
// so this refuses only what no deployment would serve.
func validateLeaseTTL(ttlMicros uint64) error {
	if ttlMicros < MinLeaseTTLMicros || ttlMicros > MaxLeaseTTLMicros {
		return &InvalidError{Reason: fmt.Sprintf(
			"lease ttl is %dµs, outside the supported range %d..=%dµs",
			ttlMicros, MinLeaseTTLMicros, MaxLeaseTTLMicros)}
	}
	return nil
}

// Validate checks a lease acquisition.
func (request KvLease) Validate() error {
	if err := validateLeaseShape(
		request.V, request.Namespace, request.Key, request.HolderID); err != nil {
		return err
	}
	return validateLeaseTTL(request.LeaseTTLMicros)
}

// Validate checks a lease renewal, which presents the token it extends.
func (request KvLeaseRenew) Validate() error {
	if err := validateLeaseShape(
		request.V, request.Namespace, request.Key, request.HolderID); err != nil {
		return err
	}
	if err := validateLeaseTTL(request.LeaseTTLMicros); err != nil {
		return err
	}
	if request.LeaseToken == 0 {
		return &InvalidError{Reason: "lease token must not be zero"}
	}
	return nil
}

// Validate checks an early release, which presents the token it drops.
func (request KvRelease) Validate() error {
	if err := validateLeaseShape(
		request.V, request.Namespace, request.Key, request.HolderID); err != nil {
		return err
	}
	if request.LeaseToken == 0 {
		return &InvalidError{Reason: "lease token must not be zero"}
	}
	return nil
}
