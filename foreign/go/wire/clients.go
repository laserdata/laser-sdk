package wire

// The transport codes a connection record carries, the same dictionary the
// upstream Iggy binding uses.
const (
	TransportTCP       uint8 = 1
	TransportQUIC      uint8 = 2
	TransportHTTP      uint8 = 3
	TransportWebSocket uint8 = 4
)

// ClientMetadataQuery is the discovery read request.
//
// It is filtered and paginated, because a busy server may hold thousands of
// connections and a caller must not have to pull them all at once. The server
// orders by client id, applies the filters, skips past the cursor, and returns
// up to the limit plus a cursor when more remain.
type ClientMetadataQuery struct {
	V uint32 `cbor:"v"`
	// WithMetadataOnly returns only connections that advertised metadata, the
	// common case for discovery where unannounced connections are noise.
	WithMetadataOnly bool `cbor:"with_metadata_only,omitempty"`
	// UserID returns only connections authenticated as this principal.
	UserID *uint32 `cbor:"user_id,omitempty"`
	// AfterClientID returns only connections past this id. Absent starts from
	// the beginning.
	AfterClientID *uint32 `cbor:"after_client_id,omitempty"`
	// Limit is clamped server-side to the page cap.
	Limit uint32 `cbor:"limit"`
}

// ClientMetadata is one connection's discovery record: the identity the
// streaming server holds plus the opaque metadata the client advertised.
//
// It is deliberately distinct from the upstream Iggy client info, which stays
// byte-identical so an upstream SDK keeps working. The metadata is opaque: an
// agent advertises its card, another application sets any blob its consumer
// interprets.
type ClientMetadata struct {
	ClientID uint32  `cbor:"client_id"`
	UserID   *uint32 `cbor:"user_id,omitempty"`
	// Transport is the transport code.
	Transport           uint8  `cbor:"transport"`
	Address             string `cbor:"address"`
	ConsumerGroupsCount uint32 `cbor:"consumer_groups_count"`
	Metadata            []byte `cbor:"metadata,omitempty"`
}

// ClientMetadataList is one page of connections with their advertised
// metadata.
//
// NextCursor is the last client id in the page when more remain, which the
// caller passes back as the next AfterClientID. Absent means the last page.
type ClientMetadataList struct {
	Clients    []ClientMetadata `cbor:"clients"`
	NextCursor *uint32          `cbor:"next_cursor,omitempty"`
}

// ProducerLatency is a producer's observed publish latency.
type ProducerLatency struct {
	Samples    uint64  `cbor:"samples"`
	P50Micros  *uint64 `cbor:"p50_micros"`
	P99Micros  *uint64 `cbor:"p99_micros"`
	P999Micros *uint64 `cbor:"p999_micros"`
}

// ProducerStatistics is one producer instance's observed activity.
type ProducerStatistics struct {
	InstanceID            string          `cbor:"instance_id"`
	Stream                string          `cbor:"stream"`
	Topic                 string          `cbor:"topic"`
	FirstActivityMillis   uint64          `cbor:"first_activity_millis"`
	LastActivityMillis    uint64          `cbor:"last_activity_millis"`
	SubmittedRecords      uint64          `cbor:"submitted_records"`
	SubmittedPayloadBytes uint64          `cbor:"submitted_payload_bytes"`
	ConfirmedRecords      *uint64         `cbor:"confirmed_records"`
	ConfirmedPayloadBytes *uint64         `cbor:"confirmed_payload_bytes"`
	Retries               *uint64         `cbor:"retries"`
	FailedCalls           uint64          `cbor:"failed_calls"`
	LastSuccessMillis     *uint64         `cbor:"last_success_millis"`
	Latency               ProducerLatency `cbor:"latency"`
}

// ProducerPresence is the observed set of live producers and how long the
// observation stays valid.
type ProducerPresence struct {
	ProducerPresenceVersion uint32               `cbor:"producer_presence_version"`
	ObservedAtMillis        uint64               `cbor:"observed_at_millis"`
	ExpiresAfterMillis      uint64               `cbor:"expires_after_millis"`
	Producers               []ProducerStatistics `cbor:"producers"`
}
