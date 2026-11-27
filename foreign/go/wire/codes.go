package wire

// The AGDX command codes, sent over the Iggy connection as raw binary requests.
//
// They sit above Iggy's own command space so the two never collide. Codes are a
// pinned cross-repo contract shared with LaserData Cloud and the streaming
// server: renumbering one silently breaks the managed surface.
const (
	// AgdxCommandBase is the first code in the AGDX space.
	AgdxCommandBase uint32 = 1_000_000

	AgdxHelloCode              = AgdxCommandBase
	AgdxBackendHelloCode       = AgdxCommandBase + 1
	AgdxSetClientMetadataCode  = AgdxCommandBase + 2
	AgdxGetClientsMetadataCode = AgdxCommandBase + 3
	AgdxBatchCode              = AgdxCommandBase + 20
	// Codes 1_000_021 to 1_000_025 belonged to the retired lakehouse
	// destinations and are never reused.
)

// The authorization command group.
const (
	AgdxAuthzBase = AgdxCommandBase + 100

	AgdxAuthzWhoamiCode      = AgdxAuthzBase
	AgdxAuthzListRolesCode   = AgdxAuthzBase + 1
	AgdxAuthzGetRoleCode     = AgdxAuthzBase + 2
	AgdxAuthzGetBindingsCode = AgdxAuthzBase + 3
	AgdxAuthzDefineRoleCode  = AgdxAuthzBase + 4
	AgdxAuthzDeleteRoleCode  = AgdxAuthzBase + 5
	AgdxAuthzBindRolesCode   = AgdxAuthzBase + 6
	AgdxAuthzHistoryCode     = AgdxAuthzBase + 7
	AgdxControlPublishCode   = AgdxAuthzBase + 8
)

// The query command group.
const (
	AgdxQueryBase = AgdxCommandBase + 200

	AgdxQueryCode = AgdxQueryBase
	// Codes 1_000_201 to 1_000_203 belonged to the retired query execution
	// paging, cancellation and status operations and are never reused.
	AgdxGetProjectionCode   = AgdxQueryBase + 10
	AgdxListProjectionsCode = AgdxQueryBase + 11
	AgdxGetSchemaCode       = AgdxQueryBase + 20
	AgdxListSchemasCode     = AgdxQueryBase + 21
	AgdxRegisterSchemaCode  = AgdxQueryBase + 22
	AgdxDecodeRecordCode    = AgdxQueryBase + 23
)

// The key-value command group.
const (
	AgdxKvBase = AgdxCommandBase + 300

	AgdxKvGetCode        = AgdxKvBase
	AgdxKvSetCode        = AgdxKvBase + 1
	AgdxKvScanCode       = AgdxKvBase + 2
	AgdxKvDeleteCode     = AgdxKvBase + 3
	AgdxKvDeleteManyCode = AgdxKvBase + 4
	AgdxKvNamespacesCode = AgdxKvBase + 5
	AgdxKvCasCode        = AgdxKvBase + 6
	AgdxKvExistsCode     = AgdxKvBase + 7
	AgdxKvExpireCode     = AgdxKvBase + 8
	AgdxKvPatchCode      = AgdxKvBase + 9
	AgdxKvLeaseCode      = AgdxKvBase + 10
	AgdxKvReleaseCode    = AgdxKvBase + 11
	AgdxKvCasFencedCode  = AgdxKvBase + 12
	AgdxKvCopyCode       = AgdxKvBase + 13
	AgdxKvMoveCode       = AgdxKvBase + 14
	AgdxKvLeaseRenewCode = AgdxKvBase + 15
)

// The fork command group.
const (
	AgdxForkBase = AgdxCommandBase + 400

	AgdxForkCreateCode  = AgdxForkBase
	AgdxForkDeleteCode  = AgdxForkBase + 1
	AgdxForkPromoteCode = AgdxForkBase + 2
	AgdxForkListCode    = AgdxForkBase + 3
	AgdxForkPutCode     = AgdxForkBase + 4
)

// The graph command group.
const (
	AgdxGraphBase = AgdxCommandBase + 600

	AgdxGraphQueryCode     = AgdxGraphBase
	AgdxGraphUpsertCode    = AgdxGraphBase + 1
	AgdxGraphNeighborsCode = AgdxGraphBase + 2
)

// The agent and session command group.
const (
	AgdxAgentBase = AgdxCommandBase + 700

	AgdxSessionGetCode     = AgdxAgentBase + 10
	AgdxSessionListCode    = AgdxAgentBase + 11
	AgdxSessionEventsCode  = AgdxAgentBase + 12
	AgdxSessionStateCode   = AgdxAgentBase + 13
	AgdxSessionLinksCode   = AgdxAgentBase + 14
	AgdxSessionSourcesCode = AgdxAgentBase + 15
	AgdxSessionChangesCode = AgdxAgentBase + 16
)

// The filtered-delivery command group, served natively by the streaming server.
const (
	AgdxDeliveryBase = AgdxCommandBase + 800

	AgdxFilteredPollCode        = AgdxDeliveryBase
	AgdxFilteredAckCode         = AgdxDeliveryBase + 1
	AgdxFilterPreviewCode       = AgdxDeliveryBase + 2
	AgdxFilterTestCode          = AgdxDeliveryBase + 3
	AgdxFilterValidateCode      = AgdxDeliveryBase + 4
	AgdxFilteredWaitCode        = AgdxDeliveryBase + 5
	AgdxFilteredWaitCancelCode  = AgdxDeliveryBase + 6
	AgdxFilterMutateCode        = AgdxDeliveryBase + 10
	AgdxGetFilterCode           = AgdxDeliveryBase + 11
	AgdxListFiltersCode         = AgdxDeliveryBase + 12
	AgdxListFilterRevisionsCode = AgdxDeliveryBase + 13
	AgdxGetFilterBindingCode    = AgdxDeliveryBase + 14
	AgdxListFilterBindingsCode  = AgdxDeliveryBase + 15
	AgdxFilterOperationCode     = AgdxDeliveryBase + 16
	AgdxResolveFilterPolicyCode = AgdxDeliveryBase + 17
	AgdxWatchFilterCatalogCode  = AgdxDeliveryBase + 18
)

// The per-surface operation versions. They stay at 1 through the pre-1.0 stage.
const (
	AuthzOpVersion          uint32 = 1
	QueryOpVersion          uint32 = 1
	ControlOpVersion        uint32 = 1
	KvOpVersion             uint32 = 1
	KvLeaseOpVersion        uint32 = 1
	ForkOpVersion           uint32 = 1
	GraphOpVersion          uint32 = 1
	FilterOpVersion         uint32 = 1
	BatchOpVersion          uint32 = 1
	ChangeOpVersion         uint32 = 1
	ClientMetadataOpVersion uint32 = 1
	PresenceOpVersion       uint32 = 1
	AgentOpVersion          uint32 = 1
)

// IsBarrierRead reports whether a managed read may carry a ManagedReadEnvelope
// barrier: the key-value, fork and graph reads, whose state the mutation logs
// fold, and the session reads, whose state the session fold builds.
func IsBarrierRead(code uint32) bool {
	switch code {
	case AgdxKvGetCode, AgdxKvScanCode, AgdxKvNamespacesCode, AgdxKvExistsCode,
		AgdxForkListCode, AgdxGraphQueryCode, AgdxGraphNeighborsCode,
		AgdxSessionGetCode, AgdxSessionListCode, AgdxSessionEventsCode,
		AgdxSessionStateCode, AgdxSessionLinksCode, AgdxSessionSourcesCode,
		AgdxSessionChangesCode:
		return true
	default:
		return false
	}
}

// IsIdempotentManagedRequest reports whether a backend-served managed command
// changes state and so must carry a stable operation identity across transport
// retries. Iggy's own authorization mutations use their established request
// mutation id instead of this managed envelope.
func IsIdempotentManagedRequest(code uint32) bool {
	switch code {
	case AgdxBatchCode, AgdxKvSetCode, AgdxKvDeleteCode, AgdxKvDeleteManyCode,
		AgdxKvCasCode, AgdxKvExpireCode, AgdxKvPatchCode, AgdxKvLeaseCode,
		AgdxKvLeaseRenewCode, AgdxKvReleaseCode, AgdxKvCasFencedCode,
		AgdxKvCopyCode, AgdxKvMoveCode, AgdxForkCreateCode, AgdxForkDeleteCode,
		AgdxForkPromoteCode, AgdxForkPutCode, AgdxGraphUpsertCode,
		AgdxControlPublishCode, AgdxRegisterSchemaCode:
		return true
	default:
		return false
	}
}
