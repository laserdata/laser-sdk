export const MAX_PAGE_SIZE = 1000
export const DEFAULT_STREAM_PAGE_SIZE = 100
export const MAX_INDEX_ENTRIES_PER_RECORD = 32

export const MAX_KEY_BYTES = 512
export const MAX_VALUE_BYTES = 8 * 1024 * 1024
export const MAX_PROJECTOR_PAYLOAD_BYTES = MAX_VALUE_BYTES
export const MAX_SCAN_LIMIT = 1000
export const DEFAULT_SCAN_LIMIT = 100
export const DEFAULT_NAMESPACE = "default"
export const MAX_NAMESPACE_BYTES = 128
export const MAX_SESSION_LABEL_BYTES = 256
/** Most session ids one heartbeat record lists. A process with more leases in
 * one stream splits them over several records. */
export const MAX_HEARTBEAT_SESSIONS = 2048
export const MAX_STATE_PATCH_OPS = 256
export const MAX_STATE_DOCUMENT_BYTES = 8 * 1024 * 1024
export const MAX_MANIFEST_FRAGMENTS = 1024
export const MAX_HOLDER_ID_BYTES = 128
export const MIN_LEASE_TTL_MICROS = 1_000_000
export const MAX_LEASE_TTL_MICROS = 300_000_000

export const MAX_FORK_ID_BYTES = 128

export const MAX_ROLE_NAME_BYTES = 64

export const MAX_FRAME_BYTES = 64 * 1024 * 1024
export const MAX_QUERY_REPLY_BYTES = MAX_FRAME_BYTES
export const MAX_QUERY_NAME_BYTES = 255
export const MAX_RAW_SQL_BYTES = 64 * 1024
export const MAX_QUERY_PARAMETERS = 1024
export const MAX_QUERY_CURSOR_BYTES = 512
export const MAX_QUERY_PREDICATES = 4096
export const MAX_QUERY_FIELDS = 4096
export const MAX_VECTOR_DIMENSIONS = 65_536

export const MAX_BATCH_OPS = 64

export const MAX_AGENT_STRING_BYTES = 256
export const MAX_IDEMPOTENCY_KEY_BYTES = 64
export const MAX_METADATA_ENTRIES = 32
export const MAX_METADATA_KEY_BYTES = 256
export const MAX_METADATA_VALUE_BYTES = 1024
export const MAX_METADATA_TOTAL_BYTES = 8192
export const MAX_BODY_REFERENCE_BYTES = 1024
export const MAX_CARD_CAPABILITIES = 64
export const MAX_CLIENT_METADATA = 64 * 1024

export const MAX_MEMORY_BODY_BYTES = MAX_VALUE_BYTES
export const MAX_RECALL_LIMIT = MAX_PAGE_SIZE

export const MAX_TEXT_QUERY_BYTES = 1024
export const DEFAULT_RECALL_LIMIT = DEFAULT_STREAM_PAGE_SIZE
export const MAX_MEMORY_TAGS = 16
export const MAX_MEMORY_TAG_BYTES = 64
export const MAX_GRAPH_NAME_BYTES = 128
export const MAX_GRAPH_TRAVERSE_DEPTH = 8
export const MAX_GRAPH_RESULT_ELEMENTS = 10_000
export const MAX_GRAPH_NODE_LABELS = 16
export const MAX_SOURCE_REF_BYTES = 2 * MAX_KEY_BYTES

export const MAX_FILTER_BYTES = 8192
export const MAX_FILTER_NODES = 128
export const MAX_FILTER_DEPTH = 8
export const MAX_FILTER_PATH_SEGMENTS = 16
export const MAX_FILTER_PATH_BYTES = 256
export const MAX_FILTER_LIST_ITEMS = 64
export const MAX_FILTER_STRING_BYTES = 1024
export const MAX_FILTER_NAME_BYTES = 128
export const MAX_FILTER_DESCRIPTION_BYTES = 1024
export const MAX_FILTER_SOURCE_NAME_BYTES = 255
export const MAX_FILTERED_PAGE_RECORDS = 1000
export const MAX_FILTERED_PAGE_BYTES = 8 * 1024 * 1024
/** Max source records one filtered page may ask to examine. */
export const MAX_FILTERED_PAGE_EXAMINED = 100_000
export const MAX_FILTER_PREVIEW_EXAMINED = 10_000
export const MAX_FILTER_PREVIEW_RECORDS = 100
export const MAX_FILTER_PREVIEW_PAYLOAD_BYTES = 4096
export const MAX_FILTER_SAMPLE_BYTES = 1024 * 1024
export const MAX_FILTER_SAMPLE_HEADERS = 64
export const MAX_FILTER_CATALOG_PAGE = 200
/**
 * Max JSON nesting depth a payload decoder accepts. Every server bound and SDK
 * guard uses this one value, below the server parser's recursion limit.
 */
export const MAX_FILTER_PARSE_DEPTH = 127

export const MAX_FILTER_SCHEMA_BYTES = 1024 * 1024
export const MAX_FILTER_SCHEMA_DEPTH = 64
