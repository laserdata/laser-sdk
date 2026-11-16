package wire

import "fmt"

// The graph and provenance validation rules.
//
// A graph element is content-addressed, so its stored bytes are its identity.
// Refusing an element here keeps one identity from being derived from values
// the store would have rejected or truncated.

// validateAttrs bounds an element's attribute set and each attribute's name.
// The values are free-form, so only their count and their names are checked.
func validateAttrs(label string, attrs []Attr) error {
	if len(attrs) > MaxMetadataEntries {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s carries %d attributes, exceeds cap %d",
			label, len(attrs), MaxMetadataEntries)}
	}
	for _, attr := range attrs {
		if len(attr.Key) > MaxMetadataKeyBytes {
			return &InvalidError{Reason: fmt.Sprintf(
				"%s attribute name is %dB, exceeds cap %dB",
				label, len(attr.Key), MaxMetadataKeyBytes)}
		}
	}
	return nil
}

func validateGraphLimit(limit uint64) error {
	if limit > uint64(MaxGraphResultElements) {
		return &InvalidError{Reason: fmt.Sprintf(
			"graph result limit %d exceeds cap %d", limit, MaxGraphResultElements)}
	}
	return nil
}

// Validate checks a node's labels, attributes and provenance.
func (node GraphNode) Validate() error {
	if len(node.Labels) > MaxGraphNodeLabels {
		return &InvalidError{Reason: fmt.Sprintf(
			"graph node carries %d labels, exceeds cap %d",
			len(node.Labels), MaxGraphNodeLabels)}
	}
	for _, label := range node.Labels {
		if err := ValidateBoundedText(
			"graph node label", label, MaxGraphLabelBytes); err != nil {
			return err
		}
	}
	if err := validateAttrs("graph node", node.Attrs); err != nil {
		return err
	}
	if node.Source != nil {
		return node.Source.Validate()
	}
	return nil
}

// Validate checks an edge's type, attributes and provenance.
func (edge GraphEdge) Validate() error {
	if err := ValidateBoundedText(
		"graph edge type", edge.EdgeType, MaxGraphLabelBytes); err != nil {
		return err
	}
	if err := validateAttrs("graph edge", edge.Attrs); err != nil {
		return err
	}
	if edge.Source != nil {
		return edge.Source.Validate()
	}
	return nil
}

// Validate checks an upsert's graph name and every element it carries.
func (upsert GraphUpsert) Validate() error {
	if err := ValidateGraphName(upsert.Graph); err != nil {
		return err
	}
	elements := len(upsert.Nodes) + len(upsert.Edges)
	if elements > MaxGraphResultElements {
		return &InvalidError{Reason: fmt.Sprintf(
			"graph upsert carries %d elements, exceeds cap %d",
			elements, MaxGraphResultElements)}
	}
	for _, node := range upsert.Nodes {
		if err := node.Validate(); err != nil {
			return err
		}
	}
	for _, edge := range upsert.Edges {
		if err := edge.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks a traversal's graph, its hop depths and its result bound.
//
// The hops add up: each one bounds its own expansion, and their sum bounds
// the whole walk, so a chain of shallow hops cannot reach further than one
// deep hop may.
func (query GraphQuery) Validate() error {
	if err := ValidateGraphName(query.Graph); err != nil {
		return err
	}
	depth := uint32(0)
	for _, hop := range query.Traverse {
		if hop.Max == 0 || hop.Max > MaxGraphTraverseDepth {
			return &InvalidError{Reason: fmt.Sprintf(
				"graph hop max %d must be between 1 and %d",
				hop.Max, MaxGraphTraverseDepth)}
		}
		if depth > MaxGraphTraverseDepth-hop.Max {
			depth = MaxGraphTraverseDepth + 1
			break
		}
		depth += hop.Max
	}
	if depth > MaxGraphTraverseDepth {
		return &InvalidError{Reason: fmt.Sprintf(
			"graph traversal depth %d exceeds cap %d", depth, MaxGraphTraverseDepth)}
	}
	return validateGraphLimit(query.Limit)
}

// Validate checks a neighbor read's graph, depth and result bound.
func (neighbors GraphNeighbors) Validate() error {
	if err := ValidateGraphName(neighbors.Graph); err != nil {
		return err
	}
	if neighbors.Depth > MaxGraphTraverseDepth {
		return &InvalidError{Reason: fmt.Sprintf(
			"graph neighbor depth %d exceeds cap %d",
			neighbors.Depth, MaxGraphTraverseDepth)}
	}
	return validateGraphLimit(neighbors.Limit)
}

// Validate bounds a provenance reference.
//
// A log pointer is fixed-width numerics plus an optional conversation id, so
// only the id needs a bound. The other two forms carry text whose whole
// length counts.
func (source SourceRef) Validate() error {
	size := 0
	switch source.Kind {
	case SourceMessage:
		if source.Message == nil {
			return &InvalidError{Reason: "a message source ref carries no record"}
		}
		if source.Message.Conversation != nil {
			if err := ValidateStoredText(
				"source conversation", *source.Message.Conversation); err != nil {
				return err
			}
			size = len(*source.Message.Conversation)
		}
	case SourceKv:
		if source.Kv == nil {
			return &InvalidError{Reason: "a key-value source ref carries no key"}
		}
		size = len(source.Kv.Namespace) + len(source.Kv.Key)
	case SourceMemory:
		if source.Memory == nil {
			return &InvalidError{Reason: "a memory source ref carries no item"}
		}
		size = len(source.Memory.ID)
	default:
		return &InvalidError{Reason: fmt.Sprintf(
			"unknown source reference kind %d", source.Kind)}
	}
	if size > MaxSourceRefBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"source reference is %dB, exceeds cap %dB", size, MaxSourceRefBytes)}
	}
	return nil
}

// Validate bounds a connection's advertised metadata, which is opaque bytes
// any client may set.
func (metadata ClientMetadata) Validate() error {
	size := 0
	if metadata.Metadata != nil {
		size = len(metadata.Metadata)
	}
	if size > MaxClientMetadata {
		return &InvalidError{Reason: fmt.Sprintf(
			"client metadata is %dB, exceeds cap %dB", size, MaxClientMetadata)}
	}
	return nil
}

// Validate checks a fork row write. Every text it carries is stored, so each
// must be storable: no NUL, which no text column holds.
func (put ForkPut) Validate() error {
	if err := ValidateForkID(put.ForkID); err != nil {
		return err
	}
	if err := ValidateStoredText("fork table", put.Table); err != nil {
		return err
	}
	if err := ValidateStoredText("fork projection id", put.ProjectionID); err != nil {
		return err
	}
	for name, value := range put.Fields {
		if err := ValidateStoredText("fork field name", name); err != nil {
			return err
		}
		if err := ValidateStoredText("fork field value", value); err != nil {
			return err
		}
	}
	for name, value := range put.Metadata {
		if err := ValidateStoredText("fork metadata name", name); err != nil {
			return err
		}
		if err := ValidateStoredText("fork metadata value", value); err != nil {
			return err
		}
	}
	if put.Embedding != nil {
		return ValidateStoredText("fork embedding", *put.Embedding)
	}
	return nil
}
