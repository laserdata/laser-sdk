package wire

import "fmt"

// The remaining request and record rules.

// validateCatalogResults bounds a catalog listing's page length.
func validateCatalogResults(results uint32) error {
	if results == 0 || results > MaxFilterCatalogPage {
		return &InvalidError{Reason: fmt.Sprintf(
			"results must be 1..=%d", MaxFilterCatalogPage)}
	}
	return nil
}

// Validate checks a saved-filter listing.
func (request ListFilters) Validate() error {
	if err := validateFilterVersion(request.V); err != nil {
		return err
	}
	if err := validateCatalogResults(request.Results); err != nil {
		return err
	}
	if request.NameContains != nil && len(*request.NameContains) > MaxFilterNameBytes {
		return &InvalidError{Reason: "name filter exceeds the name cap"}
	}
	if request.Owner != nil {
		return validateGroupID(*request.Owner)
	}
	return nil
}

// Validate checks a revision listing.
func (request ListFilterRevisions) Validate() error {
	if err := validateFilterVersion(request.V); err != nil {
		return err
	}
	return validateCatalogResults(request.Results)
}

// Validate checks a binding listing and the source it narrows to.
func (request ListFilterBindings) Validate() error {
	if err := validateFilterVersion(request.V); err != nil {
		return err
	}
	if err := validateCatalogResults(request.Results); err != nil {
		return err
	}
	if request.Stream != nil {
		if err := validateFilterName("stream", *request.Stream); err != nil {
			return err
		}
	}
	if request.Topic != nil {
		return validateFilterName("topic", *request.Topic)
	}
	return nil
}

// Validate checks a binding read and the group it names.
func (request GetFilterBinding) Validate() error {
	if err := validateFilterVersion(request.V); err != nil {
		return err
	}
	return request.Group.Validate()
}

// Validate checks a synchronous schema registration, which names no id
// because the backend allocates one.
func (request RegisterSchema) Validate() error {
	if request.Name != nil {
		if err := ValidateBoundedText(
			"schema name", *request.Name, MaxSchemaNameBytes); err != nil {
			return err
		}
	}
	return request.Source.Validate()
}

// Validate checks a backend's announcement.
//
// Each resource appears once, since the streaming server relays the set
// verbatim and a repeated id would leave two descriptors claiming one
// backend. A ready announcement that lists backends must hold a ready one, or
// a client would route work to a deployment that serves none.
func (announce BackendAnnounce) Validate() error {
	seen := make(map[BackendResourceID]bool, len(announce.Backends))
	anyReady := false
	for _, backend := range announce.Backends {
		if err := backend.Validate(); err != nil {
			return err
		}
		if seen[backend.ResourceID] {
			return &InvalidError{Reason: "backend announcement repeats a resource id"}
		}
		seen[backend.ResourceID] = true
		if backend.Readiness.Ready {
			anyReady = true
		}
	}
	if announce.IsReady() && len(announce.Backends) != 0 && !anyReady {
		return &InvalidError{
			Reason: "ready backend announcement must contain a ready backend",
		}
	}
	return nil
}

// Validate checks a memory record's identity, body and conversation scope.
func (record MemoryRecord) Validate() error {
	switch record.Kind {
	case MemoryItem:
		if err := ValidateBoundedText(
			"memory item id", record.ID, MaxMemoryIDBytes); err != nil {
			return err
		}
		if err := ValidateBoundedText(
			"memory item kind", record.ItemKind, MaxMemoryIDBytes); err != nil {
			return err
		}
		if len(record.Body) > MaxMemoryBodyBytes {
			return &InvalidError{Reason: fmt.Sprintf(
				"memory body is %dB, exceeds cap %dB",
				len(record.Body), MaxMemoryBodyBytes)}
		}
	case MemoryForget, MemoryFeedback:
		if err := ValidateBoundedText(
			"memory target", record.Target, MaxMemoryIDBytes); err != nil {
			return err
		}
	default:
		return &InvalidError{Reason: fmt.Sprintf(
			"unknown memory record kind %d", record.Kind)}
	}

	// A tombstone or a feedback signal is scoped to the conversation the item
	// was remembered in, so an unparseable id would scope it to nothing.
	if record.Kind != MemoryItem && record.Conversation != nil {
		if _, err := ParseID(*record.Conversation); err != nil {
			return &InvalidError{
				Reason: "memory record conversation is not a valid id",
			}
		}
	}
	return nil
}
