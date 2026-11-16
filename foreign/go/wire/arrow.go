package wire

import (
	"fmt"
	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// The Arrow IPC admission contract: one message carries one complete,
// self-contained stream with its schema, dictionaries and record batches.
const (
	ArrowIpcContractVersion uint32 = 1
	ArrowIpcMediaType              = "application/vnd.apache.arrow.stream"
	MaxArrowIpcMessageBytes uint64 = 8 * 1024 * 1024
	MaxArrowIpcFields       uint32 = 4_096
	MaxArrowIpcBatches      uint32 = 64
	MaxArrowIpcRows         uint64 = 1_000_000
	MaxArrowIpcDictionaries uint32 = 4_096
	MaxArrowDecimalBits     uint16 = 128
)

// ArrowTimestampUnit is the timestamp resolution the gate accepts.
type ArrowTimestampUnit string

// ArrowMicrosecond is the only accepted unit.
const ArrowMicrosecond ArrowTimestampUnit = "microsecond"

// ArrowIpcRejectionCode is the stable reason an Arrow stream could not enter
// the logical schema gate.
type ArrowIpcRejectionCode string

const (
	ArrowRejectFileFormat            ArrowIpcRejectionCode = "file_format"
	ArrowRejectMissingSchema         ArrowIpcRejectionCode = "missing_schema"
	ArrowRejectMissingDictionary     ArrowIpcRejectionCode = "missing_dictionary"
	ArrowRejectDictionaryDelta       ArrowIpcRejectionCode = "dictionary_delta"
	ArrowRejectDictionaryReplacement ArrowIpcRejectionCode = "dictionary_replacement"
	ArrowRejectUnion                 ArrowIpcRejectionCode = "union"
	ArrowRejectExtensionType         ArrowIpcRejectionCode = "extension_type"
	ArrowRejectTimestampUnit         ArrowIpcRejectionCode = "timestamp_unit"
	ArrowRejectDecimalWidth          ArrowIpcRejectionCode = "decimal_width"
	ArrowRejectSchemaFingerprint     ArrowIpcRejectionCode = "schema_fingerprint"
	ArrowRejectFieldLimit            ArrowIpcRejectionCode = "field_limit"
	ArrowRejectBatchLimit            ArrowIpcRejectionCode = "batch_limit"
	ArrowRejectRowLimit              ArrowIpcRejectionCode = "row_limit"
	ArrowRejectByteLimit             ArrowIpcRejectionCode = "byte_limit"
	ArrowRejectMalformedStream       ArrowIpcRejectionCode = "malformed_stream"
)

// ArrowIpcMessageMetadata is the validated facts about one self-contained
// Arrow IPC stream carried by one message. The raw IPC stream stays the body.
type ArrowIpcMessageMetadata struct {
	ContractVersion   uint32            `cbor:"contract_version"`
	SchemaFingerprint SchemaFingerprint `cbor:"schema_fingerprint"`
	EncodedBytes      uint64            `cbor:"encoded_bytes"`
	FieldCount        uint32            `cbor:"field_count"`
	RecordBatchCount  uint32            `cbor:"record_batch_count"`
	RowCount          uint64            `cbor:"row_count"`
	DictionaryCount   uint32            `cbor:"dictionary_count"`
}

// Validate enforces every admission bound, so an oversized or empty stream is
// refused before it reaches the gate.
func (metadata ArrowIpcMessageMetadata) Validate() error {
	if metadata.ContractVersion != ArrowIpcContractVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"Arrow IPC contract version must be %d, got %d",
			ArrowIpcContractVersion, metadata.ContractVersion)}
	}
	if err := metadata.SchemaFingerprint.Validate(); err != nil {
		return err
	}
	if metadata.EncodedBytes == 0 || metadata.EncodedBytes > MaxArrowIpcMessageBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"Arrow IPC message bytes must be in 1..=%d", MaxArrowIpcMessageBytes)}
	}
	if metadata.FieldCount == 0 || metadata.FieldCount > MaxArrowIpcFields {
		return &InvalidError{Reason: fmt.Sprintf(
			"Arrow IPC field count must be in 1..=%d", MaxArrowIpcFields)}
	}
	if metadata.RecordBatchCount == 0 || metadata.RecordBatchCount > MaxArrowIpcBatches {
		return &InvalidError{Reason: fmt.Sprintf(
			"Arrow IPC batch count must be in 1..=%d", MaxArrowIpcBatches)}
	}
	if metadata.RowCount > MaxArrowIpcRows {
		return &InvalidError{Reason: fmt.Sprintf(
			"Arrow IPC row count exceeds cap %d", MaxArrowIpcRows)}
	}
	if metadata.DictionaryCount > MaxArrowIpcDictionaries {
		return &InvalidError{Reason: fmt.Sprintf(
			"Arrow IPC dictionary count exceeds cap %d", MaxArrowIpcDictionaries)}
	}
	return nil
}

// ArrowIpcPolicy is the frozen producer-side Arrow input policy.
type ArrowIpcPolicy struct {
	ContractVersion         uint32             `cbor:"contract_version"`
	StreamFormatOnly        bool               `cbor:"stream_format_only"`
	SelfContained           bool               `cbor:"self_contained"`
	DictionaryDeltas        bool               `cbor:"dictionary_deltas"`
	ReplacementDictionaries bool               `cbor:"replacement_dictionaries"`
	TimestampUnit           ArrowTimestampUnit `cbor:"timestamp_unit"`
	MaxDecimalBits          uint16             `cbor:"max_decimal_bits"`
	Unions                  bool               `cbor:"unions"`
	ExtensionTypes          bool               `cbor:"extension_types"`
}

// DefaultArrowIpcPolicy is the frozen policy: the supported Arrow subset.
func DefaultArrowIpcPolicy() ArrowIpcPolicy {
	return ArrowIpcPolicy{
		ContractVersion:  ArrowIpcContractVersion,
		StreamFormatOnly: true,
		SelfContained:    true,
		TimestampUnit:    ArrowMicrosecond,
		MaxDecimalBits:   MaxArrowDecimalBits,
	}
}

// Validate accepts only the supported Arrow subset. The policy is frozen, so
// anything a build cannot decode identically is refused rather than
// best-effort decoded.
func (policy ArrowIpcPolicy) Validate() error {
	if policy.ContractVersion != ArrowIpcContractVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"Arrow IPC policy version must be %d, got %d",
			ArrowIpcContractVersion, policy.ContractVersion)}
	}
	if !policy.StreamFormatOnly || !policy.SelfContained {
		return &InvalidError{
			Reason: "Arrow IPC input must be one self-contained stream per message",
		}
	}
	if policy.DictionaryDeltas || policy.ReplacementDictionaries {
		return &InvalidError{
			Reason: "Arrow IPC dictionary deltas and replacements are not supported",
		}
	}
	if policy.MaxDecimalBits != MaxArrowDecimalBits {
		return &InvalidError{Reason: fmt.Sprintf(
			"Arrow IPC decimal width must be %d bits", MaxArrowDecimalBits)}
	}
	if policy.Unions || policy.ExtensionTypes {
		return &InvalidError{
			Reason: "Arrow IPC unions and extension types are not supported",
		}
	}
	return nil
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *ArrowIpcRejectionCode) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "arrow ipc rejection code", ArrowRejectFileFormat, ArrowRejectMissingSchema, ArrowRejectMissingDictionary, ArrowRejectDictionaryDelta, ArrowRejectDictionaryReplacement, ArrowRejectUnion, ArrowRejectExtensionType, ArrowRejectTimestampUnit, ArrowRejectDecimalWidth, ArrowRejectSchemaFingerprint, ArrowRejectFieldLimit, ArrowRejectBatchLimit, ArrowRejectRowLimit, ArrowRejectByteLimit, ArrowRejectMalformedStream)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *ArrowTimestampUnit) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "arrow timestamp unit", ArrowMicrosecond)
}
