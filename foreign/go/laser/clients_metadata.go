package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// PrincipalID is the identity the deployment authenticated a connection as,
// the trustworthy half of a connection's claims.
//
// The agent id a connection advertises is its own claim and can name anyone;
// the principal is what the server checked at login. Routing that binds the
// two (Router.ToPrincipal) is what stops a connection advertising another
// agent's id from being handed that agent's work.
type PrincipalID uint32

// ClientMetadataPage is one page of the discovery read plus the cursor that
// fetches the next.
//
// NextCursor is the AfterClientID for the following page, and nil on the last
// one.
type ClientMetadataPage struct {
	Clients    []wire.ClientMetadata
	NextCursor *uint32
}

// ClientMetadataRequest builds the client-metadata discovery read: which
// connections the deployment is holding and what each advertised.
//
// The metadata channel is opaque. An agent advertises a wire.AgentPresence
// there, but another application may advertise any blob its own consumer
// interprets, so a reader decodes defensively rather than assuming presence.
type ClientMetadataRequest struct {
	laser *Laser
	query wire.ClientMetadataQuery
}

// ClientMetadata opens the discovery read.
func (laser *Laser) ClientMetadata() *ClientMetadataRequest {
	return &ClientMetadataRequest{
		laser: laser,
		query: wire.ClientMetadataQuery{
			V:     wire.ClientMetadataOpVersion,
			Limit: wire.MaxPageSize,
		},
	}
}

// WithMetadataOnly returns only connections that advertised metadata, the
// common case for discovery where unannounced connections are noise.
func (request *ClientMetadataRequest) WithMetadataOnly(value bool) *ClientMetadataRequest {
	request.query.WithMetadataOnly = value
	return request
}

// Principal returns only connections the deployment authenticated as this
// principal.
func (request *ClientMetadataRequest) Principal(principal PrincipalID) *ClientMetadataRequest {
	value := uint32(principal)
	request.query.UserID = &value
	return request
}

// Limit caps the page size. The deployment clamps it to the page cap.
func (request *ClientMetadataRequest) Limit(limit uint32) *ClientMetadataRequest {
	request.query.Limit = limit
	return request
}

// After starts the page past this client id, which is the previous page's
// NextCursor.
func (request *ClientMetadataRequest) After(clientID uint32) *ClientMetadataRequest {
	request.query.AfterClientID = &clientID
	return request
}

// Page fetches one page from whichever node answers.
func (request *ClientMetadataRequest) Page(ctx context.Context) (*ClientMetadataPage, error) {
	payload, err := wire.Marshal(request.query)
	if err != nil {
		return nil, &CodecError{Reason: "encode client metadata query", Cause: err}
	}
	reply, err := request.laser.sendManagedWalking(
		ctx, wire.AgdxGetClientsMetadataCode, payload, walkingUntilServed())
	if err != nil {
		return nil, err
	}
	return decodeClientMetadataPage(reply)
}

// All walks every page on every node and collects the connections.
//
// Every node, because connection metadata is per node: a connection lives on
// the one node it dialled, so a read answered by a single node sees only that
// node's share and would read an agent connected elsewhere as absent. Use
// Page instead when the set may be large.
func (request *ClientMetadataRequest) All(ctx context.Context) ([]wire.ClientMetadata, error) {
	client, err := request.laser.connected()
	if err != nil {
		return nil, err
	}
	replicas, err := clusterRoster(ctx, client)
	if err != nil {
		return nil, err
	}
	all := make([]wire.ClientMetadata, 0, len(replicas))
	for _, replica := range replicas {
		query := request.query
		for {
			payload, err := wire.Marshal(query)
			if err != nil {
				return nil, &CodecError{Reason: "encode client metadata query", Cause: err}
			}
			// sendToNode, not an explicit replica target: on a single-node
			// deployment it addresses the node directly, which is what a
			// server with no replica addressing to offer can answer. Walking
			// replicas by id only earns its keep once there are several.
			reply, err := sendToNode(ctx, client, wire.AgdxGetClientsMetadataCode,
				payload, replica, len(replicas))
			request.laser.reportLink(err)
			if err != nil {
				return nil, &TransportError{Reason: "read client metadata", Cause: err}
			}
			page, err := decodeClientMetadataPage(reply)
			if err != nil {
				return nil, err
			}
			all = append(all, page.Clients...)
			if page.NextCursor == nil {
				break
			}
			// A cursor that does not advance would page the same connections
			// forever. Report it rather than spin: the deployment is wrong,
			// and a caller waiting on a read that never ends cannot tell that
			// from a slow one.
			if query.AfterClientID != nil && *page.NextCursor <= *query.AfterClientID {
				return nil, &ProtocolError{
					Reason: "the client metadata page did not advance"}
			}
			next := *page.NextCursor
			query.AfterClientID = &next
		}
	}
	return all, nil
}

// decodeClientMetadataPage opens one reply as a page.
func decodeClientMetadataPage(reply []byte) (*ClientMetadataPage, error) {
	var list wire.ClientMetadataList
	if err := wire.Unmarshal(reply, &list); err != nil {
		return nil, &CodecError{Reason: "decode client metadata", Cause: err}
	}
	return &ClientMetadataPage{Clients: list.Clients, NextCursor: list.NextCursor}, nil
}

// clusterRoster reads the deployment's replica ids, deduplicated and ordered, so a
// walk over every node is deterministic rather than roster-order dependent.
func clusterRoster(ctx context.Context, client ClusterRequester) ([]uint8, error) {
	metadata, err := client.GetClusterMetadata(ctx)
	if err != nil {
		return nil, &TransportError{Reason: "read cluster metadata", Cause: err}
	}
	seen := make(map[uint8]struct{}, len(metadata.Nodes))
	roster := make([]uint8, 0, len(metadata.Nodes))
	for _, node := range metadata.Nodes {
		if _, repeated := seen[node.ReplicaID]; repeated {
			continue
		}
		seen[node.ReplicaID] = struct{}{}
		roster = append(roster, node.ReplicaID)
	}
	sortReplicas(roster)
	return roster, nil
}
