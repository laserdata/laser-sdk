package laser

import (
	"context"
	"errors"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ClusterRequester is what routing a managed request over a cluster needs:
// the band itself, one node of it by replica id, and the roster.
//
// The Apache Iggy Go SDK's client satisfies it. Naming the three calls keeps
// the routing testable without a cluster, the way BinaryRequester does for
// the probe.
type ClusterRequester interface {
	BinaryRequester
	SendBinaryRequestToReplica(
		ctx context.Context, code uint32, payload []byte, replicaID uint8) ([]byte, error)
	GetClusterMetadata(ctx context.Context) (*iggcon.ClusterMetadata, error)
}

// getPartitionRouteCode asks one node which replica leads a partition.
//
// The Apache Iggy Go SDK keeps its command codes in an internal package, so
// the one code this client needs is spelled here. It is part of the server's
// wire protocol rather than that module's API, so it moves with the protocol
// and not with the SDK.
const getPartitionRouteCode uint32 = 105

// The node a managed request goes to.
const (
	// targetCaller is the node this client spreads its requests to. Every
	// node answers every managed request from shared state, so a client keeps
	// to one node picked by a key of its own, which spreads clients over the
	// cluster rather than piling them onto whichever node they dialled.
	targetCaller = iota
	// targetPrimary is the metadata primary, the only node that submits the
	// replicated authorization writes.
	targetPrimary
	// targetReplica is one named replica, by the id cluster metadata uses.
	targetReplica
)

// managedTarget is where one managed request goes.
type managedTarget struct {
	kind    int
	replica uint8
}

// callerTarget spreads the request over the cluster.
func callerTarget() managedTarget { return managedTarget{kind: targetCaller} }

// primaryTarget sends to the metadata primary.
func primaryTarget() managedTarget { return managedTarget{kind: targetPrimary} }

// replicaTarget sends to one named replica only.
func replicaTarget(replica uint8) managedTarget {
	return managedTarget{kind: targetReplica, replica: replica}
}

// targetOfCode is where code goes when nothing more specific names its node.
//
// The three replicated authorization writes go to the metadata primary
// because it is the only node that submits them. Everything else is answered
// from shared state by any node.
func targetOfCode(code uint32) managedTarget {
	switch code {
	case wire.AgdxAuthzDefineRoleCode, wire.AgdxAuthzDeleteRoleCode,
		wire.AgdxAuthzBindRolesCode:
		return primaryTarget()
	default:
		return callerTarget()
	}
}

// answered is one managed reply and the replica that sent it.
type answered struct {
	// reply is the body, opened from the positioned envelope a managed node
	// may wrap it in.
	reply []byte
	// frontier is what that envelope carried, nil when the reply was not
	// positioned.
	frontier *wire.AppliedFrontier
	// replica is which node answered, nil when the metadata primary did.
	replica *uint8
}

// openAnswer opens a positioned reply once, keeping its frontier beside it.
func openAnswer(reply []byte, replica *uint8) answered {
	var envelope wire.ManagedReplyEnvelope
	if err := wire.Unmarshal(reply, &envelope); err != nil {
		return answered{reply: reply, replica: replica}
	}
	frontier := envelope.Frontier
	return answered{reply: envelope.Reply, frontier: &frontier, replica: replica}
}

// nodeRouter picks the node of every managed request.
//
// It keeps to one node, chosen by a key of its own so two clients on one
// cluster do not pick the same one, and moves to the next after a failed
// link.
type nodeRouter struct {
	callerKey uint64
	mutex     sync.Mutex
	// roster is the ascending replica ids, empty until the first request
	// reads them.
	roster []uint8
	// moved counts the failed links so far, each one moving the caller to the
	// next node.
	moved uint64
}

// newNodeRouter opens a router under a fresh caller key.
//
// The key is the random half of a fresh operation id. Two clients on one
// cluster have to differ here or they would pick the same node and the
// spreading would not spread.
func newNodeRouter() *nodeRouter {
	id := NewOperationID()
	var key uint64
	for _, octet := range id[8:] {
		key = key<<8 | uint64(octet)
	}
	return &nodeRouter{callerKey: key}
}

// send routes one managed request and returns the opened reply.
//
// A request to the caller's node moves on to the next node after a failed
// link, at most once per node, and reads the roster again once when a replica
// id is no longer in it. A request to a named replica is sent there only.
func (router *nodeRouter) send(
	ctx context.Context, client ClusterRequester, code uint32, payload []byte,
	target managedTarget,
) (answered, error) {
	switch target.kind {
	case targetPrimary:
		reply, err := client.SendBinaryRequest(ctx, code, payload)
		if err != nil {
			return answered{}, err
		}
		return openAnswer(reply, nil), nil
	case targetReplica:
		replica := target.replica
		reply, err := client.SendBinaryRequestToReplica(ctx, code, payload, replica)
		if err != nil {
			return answered{}, err
		}
		return openAnswer(reply, &replica), nil
	default:
		return router.walkCaller(ctx, client, code, payload)
	}
}

// walkCaller sends to the caller's node, moving on when a link fails or the
// node cannot reach its own backend.
func (router *nodeRouter) walkCaller(
	ctx context.Context, client ClusterRequester, code uint32, payload []byte,
) (answered, error) {
	rosterRead := false
	failedLinks := 0
	for {
		replica, nodes, err := router.callerNode(ctx, client)
		if err != nil {
			return answered{}, err
		}
		reply, err := sendToNode(ctx, client, code, payload, replica, nodes)
		switch {
		case err == nil:
			opened := openAnswer(reply, &replica)
			// A node that answered but could not reach its own backend is a
			// node to move off, exactly like a link that dropped: the next
			// one may reach it.
			if !backendUnavailable(opened.reply) {
				return opened, nil
			}
			router.failed(replica)
			failedLinks++
			if !walksOn(ctx, failedLinks, nodes) {
				return opened, nil
			}
		case errors.Is(err, ierror.InvalidIdentifier{}) && !rosterRead:
			// The id is no longer in the cluster, so the roster this client
			// holds is stale. It is re-read once rather than every attempt.
			rosterRead = true
			router.forgetRoster()
		case transientTransport(err):
			router.failed(replica)
			failedLinks++
			if !walksOn(ctx, failedLinks, nodes) {
				return answered{}, err
			}
		default:
			return answered{}, err
		}
	}
}

// sendToNode sends one request to the caller's node.
//
// A roster of one is a deployment with one node, so the request goes over the
// band as a plain call rather than naming the only replica there is. That is
// what a single-node Apache Iggy, and every deployment that predates
// clustering, understands.
func sendToNode(
	ctx context.Context, client ClusterRequester, code uint32, payload []byte,
	replica uint8, nodes int,
) ([]byte, error) {
	if nodes <= 1 {
		return client.SendBinaryRequest(ctx, code, payload)
	}
	return client.SendBinaryRequestToReplica(ctx, code, payload, replica)
}

// callerNode is the caller's node and the roster size, reading the roster
// when none is known.
func (router *nodeRouter) callerNode(
	ctx context.Context, client ClusterRequester,
) (uint8, int, error) {
	if replica, nodes, picked := router.pick(); picked {
		return replica, nodes, nil
	}
	metadata, err := client.GetClusterMetadata(ctx)
	if err != nil {
		return 0, 0, err
	}
	roster := make([]uint8, 0, len(metadata.Nodes))
	seen := map[uint8]struct{}{}
	for _, node := range metadata.Nodes {
		if _, repeated := seen[node.ReplicaID]; repeated {
			continue
		}
		seen[node.ReplicaID] = struct{}{}
		roster = append(roster, node.ReplicaID)
	}
	// Ascending, so two clients reading the same cluster index the same
	// roster and a key picks the same node on both.
	sortReplicas(roster)
	router.mutex.Lock()
	router.roster = roster
	router.mutex.Unlock()
	replica, nodes, picked := router.pick()
	if !picked {
		return 0, 0, ierror.InvalidIdentifier{}
	}
	return replica, nodes, nil
}

// failed records a failed link to replica.
//
// The caller moves on only when replica is still its node, so several
// concurrent failures against one node move it once rather than once each.
func (router *nodeRouter) failed(replica uint8) {
	router.mutex.Lock()
	defer router.mutex.Unlock()
	if picked, ok := pickReplica(router.roster, router.callerKey, router.moved); ok &&
		picked == replica {
		router.moved++
	}
}

// forgetRoster drops the roster so the next request reads it again.
func (router *nodeRouter) forgetRoster() {
	router.mutex.Lock()
	defer router.mutex.Unlock()
	router.roster = nil
}

func (router *nodeRouter) pick() (uint8, int, bool) {
	router.mutex.Lock()
	defer router.mutex.Unlock()
	replica, ok := pickReplica(router.roster, router.callerKey, router.moved)
	return replica, len(router.roster), ok
}

// pickReplica is the node a key reaches after moving off the failed ones.
func pickReplica(roster []uint8, callerKey, moved uint64) (uint8, bool) {
	if len(roster) == 0 {
		return 0, false
	}
	index := (callerKey + moved) % uint64(len(roster))
	return roster[index], true
}

// sortReplicas orders the roster ascending.
func sortReplicas(roster []uint8) {
	for outer := 1; outer < len(roster); outer++ {
		value := roster[outer]
		inner := outer - 1
		for inner >= 0 && roster[inner] > value {
			roster[inner+1] = roster[inner]
			inner--
		}
		roster[inner+1] = value
	}
}

// walksOn reports whether a request whose link failed on failedLinks nodes
// moves to the next one.
//
// Every node is tried at most once, and an expired deadline ends the walk so
// the caller's next attempt starts on the next node with a fresh one rather
// than spending the whole deadline here.
func walksOn(ctx context.Context, failedLinks, nodes int) bool {
	if failedLinks >= nodes {
		return false
	}
	deadline, set := ctx.Deadline()
	return !set || time.Now().Before(deadline)
}

// backendUnavailable reports a node that answered but could not reach its own
// managed backend.
func backendUnavailable(reply []byte) bool {
	var failure wire.CommandError
	if err := wire.Unmarshal(reply, &failure); err != nil {
		return false
	}
	return failure.Code.IsBackendUnavailable()
}

// isRouteStale reports a node that no longer leads the partition a request
// named, or cannot be reached, so its route is looked up again.
func isRouteStale(err error) bool {
	return errors.Is(err, ierror.NotPrimary{}) ||
		errors.Is(err, ierror.InvalidIdentifier{}) ||
		errors.Is(err, ierror.PartitionNotFound{}) ||
		transientTransport(err)
}

// partitionPrimary is the replica leading one partition, as any bound replica
// answers it from its applied metadata.
func partitionPrimary(
	ctx context.Context, client ClusterRequester,
	stream, topic iggcon.Identifier, partitionID uint32,
) (uint8, error) {
	streamID, err := stream.MarshalBinary()
	if err != nil {
		return 0, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := topic.MarshalBinary()
	if err != nil {
		return 0, &InvalidError{Reason: "topic name: " + err.Error()}
	}
	request := append(append([]byte{}, streamID...), topicID...)
	request = appendUint32LE(request, partitionID)
	reply, err := client.SendBinaryRequest(ctx, getPartitionRouteCode, request)
	if err != nil {
		return 0, err
	}
	var route iggcon.PartitionRoute
	if err := route.UnmarshalBinary(reply); err != nil {
		return 0, &CodecError{Reason: "decode the partition route", Cause: err}
	}
	return route.PrimaryReplica, nil
}

func appendUint32LE(buffer []byte, value uint32) []byte {
	return append(buffer, byte(value), byte(value>>8), byte(value>>16), byte(value>>24))
}
