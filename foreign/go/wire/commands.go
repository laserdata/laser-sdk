package wire

// The pairing of each managed command code with its request type.
//
// Rust states this as one trait per request, a compile-time assertion that the
// code and type pairings match. Go has no associated constants, so the same
// contract is a method on each request type: the SDK gets one generic send
// path, and a test asserts every pairing against the pinned codes.
//
// The reply type is named in each method's documentation rather than in the
// type system, since a Go method cannot carry an associated type.

// Command is a managed request that knows the code it rides.
type Command interface {
	// CommandCode is the managed command code this request is sent under.
	CommandCode() uint32
}

// The authorization band. Every request is answered with an AuthzReply.
func (WhoamiReq) CommandCode() uint32      { return AgdxAuthzWhoamiCode }
func (ListRolesReq) CommandCode() uint32   { return AgdxAuthzListRolesCode }
func (GetRoleReq) CommandCode() uint32     { return AgdxAuthzGetRoleCode }
func (GetBindingsReq) CommandCode() uint32 { return AgdxAuthzGetBindingsCode }
func (DefineRoleReq) CommandCode() uint32  { return AgdxAuthzDefineRoleCode }
func (DeleteRoleReq) CommandCode() uint32  { return AgdxAuthzDeleteRoleCode }
func (BindRolesReq) CommandCode() uint32   { return AgdxAuthzBindRolesCode }

// The query band. A query is answered with a QueryReply.
func (QueryEnvelope) CommandCode() uint32 { return AgdxQueryCode }

// The registry browse band. Every request is answered with a BrowseReply.
func (GetProjection) CommandCode() uint32   { return AgdxGetProjectionCode }
func (ListProjections) CommandCode() uint32 { return AgdxListProjectionsCode }
func (GetSchema) CommandCode() uint32       { return AgdxGetSchemaCode }
func (ListSchemas) CommandCode() uint32     { return AgdxListSchemasCode }
func (RegisterSchema) CommandCode() uint32  { return AgdxRegisterSchemaCode }
func (DecodeRecord) CommandCode() uint32    { return AgdxDecodeRecordCode }

// The key-value band. Every request is answered with a KvReply.
func (KvGet) CommandCode() uint32        { return AgdxKvGetCode }
func (KvSet) CommandCode() uint32        { return AgdxKvSetCode }
func (KvScan) CommandCode() uint32       { return AgdxKvScanCode }
func (KvDelete) CommandCode() uint32     { return AgdxKvDeleteCode }
func (KvDeleteMany) CommandCode() uint32 { return AgdxKvDeleteManyCode }
func (KvNamespaces) CommandCode() uint32 { return AgdxKvNamespacesCode }

// The fork band. Every request is answered with a ForkReply.
func (ForkCreate) CommandCode() uint32  { return AgdxForkCreateCode }
func (ForkDelete) CommandCode() uint32  { return AgdxForkDeleteCode }
func (ForkPromote) CommandCode() uint32 { return AgdxForkPromoteCode }
func (ForkList) CommandCode() uint32    { return AgdxForkListCode }
func (ForkPut) CommandCode() uint32     { return AgdxForkPutCode }

// The session read band. Every request is answered with a SessionReply.
func (SessionGetRequest) CommandCode() uint32     { return AgdxSessionGetCode }
func (SessionListRequest) CommandCode() uint32    { return AgdxSessionListCode }
func (SessionEventsRequest) CommandCode() uint32  { return AgdxSessionEventsCode }
func (SessionStateRequest) CommandCode() uint32   { return AgdxSessionStateCode }
func (SessionLinksRequest) CommandCode() uint32   { return AgdxSessionLinksCode }
func (SessionSourcesRequest) CommandCode() uint32 { return AgdxSessionSourcesCode }
func (SessionChangesRequest) CommandCode() uint32 { return AgdxSessionChangesCode }

// The batch band. A batch is answered with a BatchReply.
func (BatchRequest) CommandCode() uint32 { return AgdxBatchCode }
