package wire

// The shared caps. Every client enforces the same bounds, so a record one
// client accepts is a record the others accept. Ported from wire/src/limits.rs.
// Changing a value here without changing it there splits the contract.
const (
	MaxPageSize                         = 1000
	DefaultStreamPageSize               = 100
	MaxIndexEntriesPerRecord            = 32
	MaxKeyBytes                         = 512
	MaxValueBytes                       = 8 * 1024 * 1024
	MaxProjectorPayloadBytes            = MaxValueBytes
	MaxScanLimit                        = 1000
	DefaultScanLimit                    = 100
	MaxNamespaceBytes                   = 128
	MaxSessionLabelBytes                = 256
	MaxSessionTextQueryBytes            = 1024
	MaxHeartbeatSessions                = 2048
	MaxStatePatchOps                    = 256
	MaxStateDocumentBytes               = 8 * 1024 * 1024
	MaxManifestFragments                = 1024
	MaxHolderIDBytes                    = 128
	MinLeaseTTLMicros            uint64 = 1_000_000
	MaxLeaseTTLMicros            uint64 = 5 * 60 * 1_000_000
	MaxForkIDBytes                      = 128
	MaxRoleNameBytes                    = 64
	MaxAuthzMutationIDBytes             = 64
	MaxFrameBytes                       = 64 * 1024 * 1024
	MaxQueryReplyBytes                  = MaxFrameBytes
	MaxQueryNameBytes                   = 255
	MaxProjectionIDBytes                = 255
	MaxProjectionNameBytes              = 255
	MaxIndexFieldBytes                  = 255
	MaxSchemaNameBytes                  = 255
	MaxRawSQLBytes                      = 64 * 1024
	MaxQueryParameters                  = 1024
	MaxQueryOffset               uint64 = 1_000_000
	MaxQueryPredicates                  = 4096
	MaxQueryFields                      = 4096
	MaxVectorDimensions                 = 65_536
	MaxBatchOps                         = 64
	MaxAgentStringBytes                 = 256
	MaxIdempotencyKeyBytes              = 64
	MaxMetadataEntries                  = 32
	MaxMetadataKeyBytes                 = 256
	MaxMetadataValueBytes               = 1024
	MaxMetadataTotalBytes               = 8192
	MaxBodyReferenceBytes               = 1024
	MaxCardCapabilities                 = 64
	RegistryFactTTLMicros        uint64 = 5 * 60 * 1_000_000
	MaxRegistryFactTTLMicros     uint64 = 24 * 60 * 60 * 1_000_000
	MaxClientMetadata                   = 64 * 1024
	MaxMemoryBodyBytes                  = MaxValueBytes
	MaxRecallLimit                      = MaxPageSize
	MaxTextQueryBytes                   = 1024
	DefaultRecallLimit                  = DefaultStreamPageSize
	MaxMemoryTags                       = 16
	MaxMemoryTagBytes                   = 64
	MaxMemoryIDBytes                    = 255
	MaxGraphNameBytes                   = 128
	MaxGraphTraverseDepth               = 8
	MaxGraphResultElements              = 10_000
	MaxGraphNodeLabels                  = 16
	MaxGraphLabelBytes                  = 128
	MaxSourceRefBytes                   = 2 * MaxKeyBytes
	MaxFilterSchemaBytes                = 1024 * 1024
	MaxFilterSchemaDepth                = 64
	MaxFilterBytes                      = 8192
	MaxFilterNodes                      = 128
	MaxFilterDepth                      = 8
	MaxFilterParseDepth                 = 127
	MaxFilterPathSegments               = 16
	MaxFilterPathBytes                  = 256
	MaxFilterListItems                  = 64
	MaxFilterStringBytes                = 1024
	MaxFilterNameBytes                  = 128
	MaxFilterDescriptionBytes           = 1024
	MaxFilterSourceNameBytes            = 255
	MaxFilteredPageRecords              = 1000
	MaxFilteredPageBytes                = 8 * 1024 * 1024
	MaxFilteredPageExamined             = 100_000
	MaxFilteredWaitMs                   = 20_000
	MaxFilteredWaitPartitions           = 1024
	MaxFilteredWaitCancelIDs            = 64
	MaxFilterPreviewExamined            = 10_000
	MaxFilterPreviewRecords             = 100
	MaxFilterPreviewPayloadBytes        = 4096
	MaxFilterSampleBytes                = 1024 * 1024
	MaxFilterSampleHeaders              = 64
	MaxFilterCatalogPage                = 200
)

// DefaultNamespace is the namespace a call uses when it names none.
const DefaultNamespace = "default"
