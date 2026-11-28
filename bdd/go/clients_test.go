package bdd_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedClientsScenariosWhenRunThenShouldHold runs the live
// connection listing feature: a connection's metadata lives on the node that
// holds it, so listing every connection asks each node of the cluster.
func TestGivenTheSharedClientsScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "clients.feature", time.Minute, clientsStep)
}

// clientsFlow is the listing a scenario took and the user it narrowed to.
type clientsFlow struct {
	listed []wire.ClientMetadata
	user   *uint32
}

func clientsFlowOf(state *world) *clientsFlow {
	if existing, found := state.get("clients"); found {
		return existing.(*clientsFlow)
	}
	flow := &clientsFlow{}
	state.put("clients", flow)
	return flow
}

func clientsStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	flow := clientsFlowOf(state)
	text := step.Text

	switch {
	case text == "I list every connected client":
		listed, err := state.client.ClientMetadata().All(ctx)
		if err != nil {
			return err
		}
		flow.listed = listed
		return nil

	case text == "I list every connected client of that user":
		return listClientsOfUser(ctx, state, flow)

	case text == "the listing holds an authenticated connection":
		return listingHoldsAuthenticated(flow)

	case text == "every listed connection belongs to that user":
		return everyListedBelongsTo(flow)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// listClientsOfUser asks the deployment for the connections of the user the
// first listing held.
func listClientsOfUser(ctx context.Context, state *world, flow *clientsFlow) error {
	if flow.user == nil {
		return fmt.Errorf("no listed connection named a user")
	}
	listed, err := state.client.ClientMetadata().Principal(laser.PrincipalID(*flow.user)).All(ctx)
	if err != nil {
		return err
	}
	flow.listed = listed
	return nil
}

// listingHoldsAuthenticated asserts this client's own signed-in connection
// is among the listed ones, which is what proves the listing reached the
// node holding it.
func listingHoldsAuthenticated(flow *clientsFlow) error {
	for _, client := range flow.listed {
		if client.UserID != nil {
			flow.user = client.UserID
			return nil
		}
	}
	return fmt.Errorf("want an authenticated connection among %d listed", len(flow.listed))
}

func everyListedBelongsTo(flow *clientsFlow) error {
	if flow.user == nil {
		return fmt.Errorf("the scenario narrowed to no user")
	}
	if len(flow.listed) == 0 {
		return fmt.Errorf("want at least this connection listed for the user")
	}
	for _, client := range flow.listed {
		if client.UserID == nil || *client.UserID != *flow.user {
			return fmt.Errorf("want every listed connection to belong to user %d, got %+v",
				*flow.user, client.UserID)
		}
	}
	return nil
}
