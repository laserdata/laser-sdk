package wire

import "strconv"

// Each typed call renders its query struct through one of these, so the
// parameter names the client sends and the ones the routes read come from the
// same place and cannot drift.

// optionalText adds a named parameter when it has a value.
func optionalText(parameters []QueryParameter, name string, value *string) []QueryParameter {
	if value == nil || *value == "" {
		return parameters
	}
	return append(parameters, QueryParameter{Name: name, Value: *value})
}

func optionalUint(parameters []QueryParameter, name string, value *uint64) []QueryParameter {
	if value == nil {
		return parameters
	}
	return append(parameters, QueryParameter{
		Name: name, Value: strconv.FormatUint(*value, 10)})
}

func optionalUint32(parameters []QueryParameter, name string, value *uint32) []QueryParameter {
	if value == nil {
		return parameters
	}
	return append(parameters, QueryParameter{
		Name: name, Value: strconv.FormatUint(uint64(*value), 10)})
}

func optionalInt(parameters []QueryParameter, name string, value *int64) []QueryParameter {
	if value == nil {
		return parameters
	}
	return append(parameters, QueryParameter{
		Name: name, Value: strconv.FormatInt(*value, 10)})
}

// flag adds a boolean parameter only when it is set, since the absent form
// already means false.
func flag(parameters []QueryParameter, name string, value bool) []QueryParameter {
	if !value {
		return parameters
	}
	return append(parameters, QueryParameter{Name: name, Value: "true"})
}

func operationParameters(operationID U128) []QueryParameter {
	return []QueryParameter{{Name: "operation_id", Value: operationID.String()}}
}

func streamParameters(stream *string) []QueryParameter {
	return optionalText(nil, ParamStream, stream)
}

func expiryParameters(expiresAtMicros *uint64) []QueryParameter {
	return optionalUint(nil, ParamExpiresAtMicros, expiresAtMicros)
}

// casParameters renders a conditional write's precondition. Exactly one of the
// two forms is sent, because exactly one can hold.
func casParameters(expect CasExpect) []QueryParameter {
	if expect.Kind == CasAbsent {
		return []QueryParameter{{Name: ParamExpectAbsent, Value: "true"}}
	}
	return []QueryParameter{{
		Name: ParamExpectVersion, Value: strconv.FormatUint(expect.Version, 10),
	}}
}

func projectionListParameters(query ProjectionListQuery) []QueryParameter {
	parameters := optionalText(nil, ParamTopic, query.Topic)
	parameters = optionalText(parameters, ParamNameContains, query.NameContains)
	parameters = optionalText(parameters, ParamIDPrefix, query.IDPrefix)
	parameters = optionalText(parameters, ParamSearch, query.Search)
	parameters = optionalText(parameters, ParamStream, query.Stream)
	parameters = optionalUint32(parameters, ParamPage, query.Page)
	return optionalUint32(parameters, ParamResults, query.Results)
}

func schemaListParameters(query SchemaListQuery) []QueryParameter {
	parameters := optionalText(nil, ParamNameContains, query.NameContains)
	parameters = optionalText(parameters, ParamStream, query.Stream)
	parameters = optionalUint32(parameters, ParamPage, query.Page)
	return optionalUint32(parameters, ParamResults, query.Results)
}

func kvScanParameters(query KvScanQuery) []QueryParameter {
	parameters := optionalText(nil, ParamPrefix, query.Prefix)
	parameters = optionalText(parameters, ParamStart, query.Start)
	parameters = optionalText(parameters, ParamEnd, query.End)
	parameters = optionalText(parameters, ParamKeyContains, query.KeyContains)
	parameters = optionalText(parameters, "conversation", query.Conversation)
	parameters = optionalUint32(parameters, ParamPage, query.Page)
	return optionalUint32(parameters, ParamResults, query.Results)
}

func clientsParameters(query ClientsQuery) []QueryParameter {
	parameters := flag(nil, "with_metadata_only", query.WithMetadataOnly)
	parameters = optionalUint32(parameters, "user_id", query.UserID)
	parameters = optionalUint32(parameters, "after", query.After)
	return optionalUint32(parameters, ParamLimit, query.Limit)
}

func sessionsParameters(query SessionsQuery) []QueryParameter {
	parameters := optionalText(nil, ParamStatus, query.Status)
	parameters = optionalText(parameters, ParamRoot, query.Root)
	parameters = optionalText(parameters, ParamLabelPrefix, query.LabelPrefix)
	parameters = optionalText(parameters, ParamAgent, query.Agent)
	parameters = optionalText(parameters, ParamText, query.Text)
	parameters = optionalUint32(parameters, ParamPage, query.Page)
	parameters = optionalUint32(parameters, ParamResults, query.Results)
	parameters = optionalUint(parameters, ParamAsOf, query.AsOf)
	return optionalUint(parameters, ParamSeries, query.Series)
}

func sessionEventsParameters(query SessionEventsQuery) []QueryParameter {
	parameters := optionalUint32(nil, ParamPage, query.Page)
	parameters = optionalUint32(parameters, ParamResults, query.Results)
	parameters = optionalUint(parameters, ParamAsOf, query.AsOf)
	return optionalUint(parameters, ParamSeries, query.Series)
}

func sessionStateParameters(query SessionStateQuery) []QueryParameter {
	return optionalUint32(nil, ParamHistoryLimit, query.HistoryLimit)
}

func sessionLinksParameters(query SessionLinksQuery) []QueryParameter {
	return optionalText(nil, ParamSurface, query.Surface)
}

func sessionChangesParameters(query SessionChangesQuery) []QueryParameter {
	parameters := optionalUint32(nil, ParamPage, query.Page)
	return optionalUint32(parameters, ParamResults, query.Results)
}

func graphNeighborsParameters(query GraphNeighborsQuery) []QueryParameter {
	parameters := optionalText(nil, "dir", query.Dir)
	parameters = optionalText(parameters, "edge_type", query.EdgeType)
	parameters = optionalUint32(parameters, "depth", query.Depth)
	parameters = optionalUint(parameters, ParamLimit, query.Limit)
	parameters = optionalUint(parameters, "as_of", query.AsOf)
	return optionalText(parameters, "conversation", query.Conversation)
}
