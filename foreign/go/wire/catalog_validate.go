package wire

import "fmt"

// The consumer-filter catalog's validation rules.
//
// A catalog mutation is applied from an ordered log, so every replica folds
// the same outcome. A malformed one is refused before it is appended rather
// than refused once per replica after it is.

// ValidateFilterName bounds a saved filter's name, which is a safelisted name
// because it is quoted into views and group policies.
func ValidateFilterName(name string) error {
	return ValidateSafelistedName("filter name", name, MaxFilterNameBytes)
}

func validateFilterDescription(description string) error {
	if len(description) > MaxFilterDescriptionBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"description exceeds %dB", MaxFilterDescriptionBytes)}
	}
	return nil
}

// validateGroupID keeps a consumer group id inside the range a native offset
// key can carry, since the group's progress is stored under it.
func validateGroupID(identity FilterGroupIdentity) error {
	if identity.GroupID > uint64(^uint32(0)) {
		return &InvalidError{
			Reason: "consumer group id exceeds the native offset-key range",
		}
	}
	return nil
}

// Validate checks that a group reference names a real stream, topic and
// group.
func (group FilterGroupRef) Validate() error {
	if err := validateFilterName("stream", group.Stream); err != nil {
		return err
	}
	if err := validateFilterName("topic", group.Topic); err != nil {
		return err
	}
	return validateFilterName("consumer group", group.Group)
}

// Validate checks whichever mutation the command carries.
func (mutation FilterMutation) Validate() error {
	switch mutation.Kind {
	case MutateRegister:
		if err := ValidateFilterName(mutation.Name); err != nil {
			return err
		}
		if err := validateFilterDescription(mutation.Description); err != nil {
			return err
		}
		return mutation.validateFilterBody()

	case MutateRevise:
		return mutation.validateFilterBody()

	case MutateDescribe:
		return validateFilterDescription(mutation.Description)

	case MutateArchive, MutateDrop:
		return nil

	case MutateSetRevisionEnabled:
		// Revisions count from one, so a zero names none.
		if mutation.Revision == 0 {
			return &InvalidError{Reason: "revision must be positive"}
		}
		return nil

	case MutateBind:
		if mutation.Group == nil {
			return &InvalidError{Reason: "a bind names no consumer group"}
		}
		return mutation.Group.Validate()

	case MutateUnbind:
		if mutation.Group == nil {
			return &InvalidError{Reason: "an unbind names no consumer group"}
		}
		if err := mutation.Group.Validate(); err != nil {
			return err
		}
		if mutation.ExpectedDigest == nil {
			return &InvalidError{Reason: "an unbind names no expected digest"}
		}
		return mutation.ExpectedDigest.Validate()

	case MutateConfigureGroup:
		if mutation.Group == nil {
			return &InvalidError{Reason: "a configure names no consumer group"}
		}
		if err := mutation.Group.Validate(); err != nil {
			return err
		}
		if mutation.ExpectedIdentity != nil {
			if err := validateGroupID(*mutation.ExpectedIdentity); err != nil {
				return err
			}
		}
		if mutation.Policy == nil {
			return &InvalidError{Reason: "a configure names no policy"}
		}
		if err := ValidateFilterName(mutation.Policy.Name); err != nil {
			return err
		}
		return mutation.Policy.Filter.Validate()

	default:
		return &InvalidError{Reason: fmt.Sprintf(
			"unknown filter mutation %d", mutation.Kind)}
	}
}

func (mutation FilterMutation) validateFilterBody() error {
	if mutation.Filter == nil {
		return &InvalidError{Reason: "a filter mutation carries no filter"}
	}
	return mutation.Filter.Validate()
}
