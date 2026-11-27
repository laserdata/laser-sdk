package wire

import (
	"context"
	"strconv"
)

// The typed calls of the managed HTTP surface.
//
// Every call takes a context, so a caller's deadline and cancellation reach
// the transport. A read that addresses one entity returns a false second
// result when the server answers that it does not exist, which is distinct
// from an error: the request succeeded and the answer is "no such thing".
//
// The calls whose name ends in As take an operation identity. A write whose
// outcome is unknown is safe to re-send with the same identity: the server
// answers the recorded outcome instead of applying it twice.

// Capabilities feature-detects the managed surface.
func (client *HTTPClient) Capabilities(ctx context.Context) (Capabilities, error) {
	return sendEmpty[Capabilities](ctx, client, MethodGet, CapabilitiesPath)
}

// Query runs one query and returns its rows.
func (client *HTTPClient) Query(
	ctx context.Context, query Query,
) (QueryResult, error) {
	return sendJSON[QueryResult](ctx, client, MethodPost, QueryPath, query)
}

// ListProjections reads the projection registry.
func (client *HTTPClient) ListProjections(
	ctx context.Context, query ProjectionListQuery,
) ([]ProjectionInfo, error) {
	return sendEmpty[[]ProjectionInfo](ctx, client, MethodGet,
		withQuery(ProjectionsPath, projectionListParameters(query)))
}

// ListSchemas reads the schema registry.
func (client *HTTPClient) ListSchemas(
	ctx context.Context, query SchemaListQuery,
) ([]SchemaDef, error) {
	return sendEmpty[[]SchemaDef](ctx, client, MethodGet,
		withQuery(SchemasPath, schemaListParameters(query)))
}

// RegisterSchema registers a schema, which the backend allocates an id for.
func (client *HTTPClient) RegisterSchema(
	ctx context.Context, body RegisterSchemaBody,
) (uint32, error) {
	return sendJSON[uint32](ctx, client, MethodPost, SchemasPath, body)
}

// GetSchema reads one schema, and reports false when there is none.
func (client *HTTPClient) GetSchema(
	ctx context.Context, id uint32, stream *string,
) (SchemaDef, bool, error) {
	return getOptional[SchemaDef](ctx, client,
		withQuery(SchemaPath(id), streamParameters(stream)))
}

// DropSchema removes one schema.
func (client *HTTPClient) DropSchema(
	ctx context.Context, id uint32, stream *string,
) error {
	return client.expectOk(ctx, MethodDelete,
		withQuery(SchemaPath(id), streamParameters(stream)), nil)
}

// GetProjection reads one projection and its bindings.
func (client *HTTPClient) GetProjection(
	ctx context.Context, id string,
) (ProjectionInfo, bool, error) {
	return getOptional[ProjectionInfo](ctx, client, ProjectionPath(id))
}

// RegisterProjection registers or replaces a projection.
func (client *HTTPClient) RegisterProjection(
	ctx context.Context, projection Projection,
) error {
	return client.sendJSONOk(ctx, MethodPost, ProjectionsPath, projection)
}

// RegisterProjectionAs registers a projection under an operation identity.
func (client *HTTPClient) RegisterProjectionAs(
	ctx context.Context, projection Projection, operationID U128,
) error {
	return client.sendJSONOk(ctx, MethodPost,
		withQuery(ProjectionsPath, operationParameters(operationID)), projection)
}

// DropProjection removes one projection.
func (client *HTTPClient) DropProjection(ctx context.Context, id string) error {
	return client.expectOk(ctx, MethodDelete, ProjectionPath(id), nil)
}

// DropProjectionAs removes a projection under an operation identity.
func (client *HTTPClient) DropProjectionAs(
	ctx context.Context, id string, operationID U128,
) error {
	return client.expectOk(ctx, MethodDelete,
		withQuery(ProjectionPath(id), operationParameters(operationID)), nil)
}

// ApplyBinding binds projections to a source.
func (client *HTTPClient) ApplyBinding(
	ctx context.Context, binding ProjectionBinding,
) error {
	return client.sendJSONOk(ctx, MethodPost, BindingsPath, binding)
}

// ApplyBindingAs binds projections under an operation identity.
func (client *HTTPClient) ApplyBindingAs(
	ctx context.Context, binding ProjectionBinding, operationID U128,
) error {
	return client.sendJSONOk(ctx, MethodPost,
		withQuery(BindingsPath, operationParameters(operationID)), binding)
}

// RemoveBinding removes a binding, or one projection from it.
func (client *HTTPClient) RemoveBinding(
	ctx context.Context, body RemoveBindingBody,
) error {
	return client.sendJSONOk(ctx, MethodDelete, BindingsPath, body)
}

// RemoveBindingAs removes a binding under an operation identity.
func (client *HTTPClient) RemoveBindingAs(
	ctx context.Context, body RemoveBindingBody, operationID U128,
) error {
	return client.sendJSONOk(ctx, MethodDelete,
		withQuery(BindingsPath, operationParameters(operationID)), body)
}

// KvGet reads one entry, and reports false when it is missing or expired.
//
// The value rides the raw response body and the expiry rides a header,
// because a JSON string cannot carry arbitrary bytes.
func (client *HTTPClient) KvGet(
	ctx context.Context, namespace string, key []byte,
) (KvReadValue, bool, error) {
	response, err := client.dispatch(ctx, MethodGet,
		KvEntryPath(namespace, Base64URLEncode(key)), nil)
	if err != nil {
		return KvReadValue{}, false, err
	}
	if response.Status == 404 {
		return KvReadValue{}, false, nil
	}
	if !response.IsSuccess() {
		return KvReadValue{}, false, apiError(response)
	}
	value := KvReadValue{Value: response.Body}
	if text, present := response.Header(KvExpiresAtMicrosHeader); present {
		if expiry, err := strconv.ParseUint(text, 10, 64); err == nil {
			value.ExpiresAtMicros = &expiry
		}
	}
	return value, true, nil
}

// KvSet writes one entry, with an optional absolute expiry.
func (client *HTTPClient) KvSet(
	ctx context.Context, namespace string, key, value []byte, expiresAtMicros *uint64,
) error {
	return client.kvSet(ctx, namespace, key, value, expiresAtMicros, nil)
}

// KvSetAs writes one entry under an operation identity.
func (client *HTTPClient) KvSetAs(
	ctx context.Context, namespace string, key, value []byte,
	expiresAtMicros *uint64, operationID U128,
) error {
	return client.kvSet(ctx, namespace, key, value, expiresAtMicros, &operationID)
}

func (client *HTTPClient) kvSet(
	ctx context.Context, namespace string, key, value []byte,
	expiresAtMicros *uint64, operationID *U128,
) error {
	parameters := expiryParameters(expiresAtMicros)
	if operationID != nil {
		parameters = append(parameters, operationParameters(*operationID)...)
	}
	// The key is base64 in the path, but the value rides the raw body, so a
	// large value carries no encoding inflation.
	return client.expectOk(ctx, MethodPut,
		withQuery(KvEntryPath(namespace, Base64URLEncode(key)), parameters), value)
}

// KvCas writes one entry only if its precondition holds, returning the version
// it committed at.
//
// A precondition miss surfaces as a conflict, whose detail carries the version
// the entry currently holds.
func (client *HTTPClient) KvCas(
	ctx context.Context, namespace string, key, value []byte,
	expect CasExpect, expiresAtMicros *uint64,
) (uint64, error) {
	return client.kvCas(ctx, namespace, key, value, expect, expiresAtMicros, nil)
}

// KvCasAs writes conditionally under an operation identity.
func (client *HTTPClient) KvCasAs(
	ctx context.Context, namespace string, key, value []byte,
	expect CasExpect, expiresAtMicros *uint64, operationID U128,
) (uint64, error) {
	return client.kvCas(ctx, namespace, key, value, expect, expiresAtMicros, &operationID)
}

func (client *HTTPClient) kvCas(
	ctx context.Context, namespace string, key, value []byte,
	expect CasExpect, expiresAtMicros *uint64, operationID *U128,
) (uint64, error) {
	parameters := casParameters(expect)
	parameters = append(parameters, expiryParameters(expiresAtMicros)...)
	if operationID != nil {
		parameters = append(parameters, operationParameters(*operationID)...)
	}
	path := withQuery(KvCasPath(namespace, Base64URLEncode(key)), parameters)
	response, err := client.dispatch(ctx, MethodPut, path, value)
	if err != nil {
		return 0, err
	}
	committed, err := decodeOk[CasCommittedView](response)
	if err != nil {
		return 0, err
	}
	return committed.Version, nil
}

// KvDelete removes one entry, reporting whether it was there.
func (client *HTTPClient) KvDelete(
	ctx context.Context, namespace string, key []byte,
) (bool, error) {
	return client.kvDelete(ctx, namespace, key, nil)
}

// KvDeleteAs removes one entry under an operation identity.
func (client *HTTPClient) KvDeleteAs(
	ctx context.Context, namespace string, key []byte, operationID U128,
) (bool, error) {
	return client.kvDelete(ctx, namespace, key, &operationID)
}

func (client *HTTPClient) kvDelete(
	ctx context.Context, namespace string, key []byte, operationID *U128,
) (bool, error) {
	path := KvEntryPath(namespace, Base64URLEncode(key))
	if operationID != nil {
		path = withQuery(path, operationParameters(*operationID))
	}
	response, err := client.dispatch(ctx, MethodDelete, path, nil)
	if err != nil {
		return false, err
	}
	// A delete of something absent is not a failure: the entry is gone
	// either way, which is what makes a retry safe.
	if response.Status == 404 {
		return false, nil
	}
	if !response.IsSuccess() {
		return false, apiError(response)
	}
	return true, nil
}

// KvScan reads one page of a namespace.
func (client *HTTPClient) KvScan(
	ctx context.Context, namespace string, query KvScanQuery,
) (KvPageView, error) {
	return sendEmpty[KvPageView](ctx, client, MethodGet,
		withQuery(KvNamespacePath(namespace), kvScanParameters(query)))
}

// KvNamespaces reads the namespaces the caller can see.
func (client *HTTPClient) KvNamespaces(
	ctx context.Context,
) ([]KvNamespaceInfo, error) {
	return sendEmpty[[]KvNamespaceInfo](ctx, client, MethodGet, KvPath)
}

// KvDeleteNamespace removes every entry of a namespace.
func (client *HTTPClient) KvDeleteNamespace(
	ctx context.Context, namespace string,
) (uint64, error) {
	deleted, err := sendEmpty[DeletedManyView](ctx, client, MethodDelete,
		KvNamespacePath(namespace))
	return deleted.Deleted, err
}

// CreateFork creates a copy-on-write fork.
func (client *HTTPClient) CreateFork(
	ctx context.Context, body ForkCreateBody,
) (ForkInfo, error) {
	return sendJSON[ForkInfo](ctx, client, MethodPost, ForksPath, body)
}

// CreateForkAs creates a fork under an operation identity.
func (client *HTTPClient) CreateForkAs(
	ctx context.Context, body ForkCreateBody, operationID U128,
) (ForkInfo, error) {
	return sendJSON[ForkInfo](ctx, client, MethodPost,
		withQuery(ForksPath, operationParameters(operationID)), body)
}

// ListForks reads the forks the caller can see.
func (client *HTTPClient) ListForks(ctx context.Context) ([]ForkInfo, error) {
	return sendEmpty[[]ForkInfo](ctx, client, MethodGet, ForksPath)
}

// DeleteFork removes one fork.
func (client *HTTPClient) DeleteFork(ctx context.Context, id string) error {
	return client.expectOk(ctx, MethodDelete, ForkPath(id), nil)
}

// PromoteFork applies a fork's rows, returning how many were applied.
func (client *HTTPClient) PromoteFork(ctx context.Context, id string) (uint64, error) {
	promoted, err := sendEmpty[PromotedView](ctx, client, MethodPost, ForkPromotePath(id))
	return promoted.Rows, err
}

// PutForkRow writes one row into a fork.
func (client *HTTPClient) PutForkRow(
	ctx context.Context, id string, body ForkPutBody,
) error {
	return client.sendJSONOk(ctx, MethodPut, ForkRowsPath(id), body)
}

// Clients reads one page of live connections and what they advertise.
func (client *HTTPClient) Clients(
	ctx context.Context, query ClientsQuery,
) (ClientMetadataListView, error) {
	return sendEmpty[ClientMetadataListView](ctx, client, MethodGet,
		withQuery(ClientsPath, clientsParameters(query)))
}

// ListSessions reads one page of a stream's sessions.
func (client *HTTPClient) ListSessions(
	ctx context.Context, stream string, query SessionsQuery,
) (SessionPage, error) {
	return sendEmpty[SessionPage](ctx, client, MethodGet,
		withQuery(SessionsPathFor(stream), sessionsParameters(query)))
}

// Session reads one session's summary.
func (client *HTTPClient) Session(
	ctx context.Context, stream string, id ConversationID,
) (SessionInfo, bool, error) {
	return getOptional[SessionInfo](ctx, client, SessionPath(stream, id))
}

// SessionEvents reads one page of a session's events.
func (client *HTTPClient) SessionEvents(
	ctx context.Context, stream string, id ConversationID, query SessionEventsQuery,
) (SessionEventsPage, error) {
	return sendEmpty[SessionEventsPage](ctx, client, MethodGet,
		withQuery(SessionEventsPath(stream, id), sessionEventsParameters(query)))
}

// SessionState reads a session's state document.
func (client *HTTPClient) SessionState(
	ctx context.Context, stream string, id ConversationID, query SessionStateQuery,
) (SessionStateView, error) {
	return sendEmpty[SessionStateView](ctx, client, MethodGet,
		withQuery(SessionStatePath(stream, id), sessionStateParameters(query)))
}

// SessionLinks reads the resources a session wrote, recalled, or touched.
func (client *HTTPClient) SessionLinks(
	ctx context.Context, stream string, id ConversationID, query SessionLinksQuery,
) (SessionLinksView, error) {
	return sendEmpty[SessionLinksView](ctx, client, MethodGet,
		withQuery(SessionLinksPath(stream, id), sessionLinksParameters(query)))
}

// SessionSources reads the source partitions holding a session's records.
func (client *HTTPClient) SessionSources(
	ctx context.Context, stream string, id ConversationID,
) (SessionSources, error) {
	return sendEmpty[SessionSources](ctx, client, MethodGet,
		SessionSourcesPath(stream, id))
}

// SessionChanges reads a stream's change rows after a position.
func (client *HTTPClient) SessionChanges(
	ctx context.Context, stream string, query SessionChangesQuery,
) (SessionChanges, error) {
	return sendEmpty[SessionChanges](ctx, client, MethodGet,
		withQuery(SessionChangesPath(stream), sessionChangesParameters(query)))
}

// GraphQuery runs one traversal.
func (client *HTTPClient) GraphQuery(
	ctx context.Context, name string, query GraphQuery,
) (GraphResultView, error) {
	return sendJSON[GraphResultView](ctx, client, MethodPost, GraphQueryPath(name), query)
}

// GraphNeighbors reads one node's neighbors.
func (client *HTTPClient) GraphNeighbors(
	ctx context.Context, name, node string, query GraphNeighborsQuery,
) (GraphResultView, error) {
	return sendEmpty[GraphResultView](ctx, client, MethodGet,
		withQuery(GraphNeighborsPath(name, node), graphNeighborsParameters(query)))
}

// getOptional reads one entity, reporting false when the server answers that
// it does not exist.
//
// That is distinct from an error: the request succeeded, and the answer is
// that there is no such thing.
func getOptional[Reply any](
	ctx context.Context, client *HTTPClient, path string,
) (Reply, bool, error) {
	var reply Reply
	response, err := client.dispatch(ctx, MethodGet, path, nil)
	if err != nil {
		return reply, false, err
	}
	if response.Status == 404 {
		return reply, false, nil
	}
	decoded, err := decodeOk[Reply](response)
	if err != nil {
		return reply, false, err
	}
	return decoded, true, nil
}
