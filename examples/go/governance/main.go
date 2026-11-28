// Role grants with deny-wins matching, the on-behalf-of intersection, the
// external edge step-up, and a budgeted session submission.
//
// The live role and binding calls need server-side authorization. The pure
// decision pieces run everywhere.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	// targetUserEnv binds the example roles to a specific user. By default they
	// are bound to a dedicated demo user the example creates on first run.
	// Never the caller: the role set includes deny-wins grants, and binding
	// those to the session user would poison every later run against the same server.
	targetUserEnv = "LASER_GOVERNANCE_USER_ID"
	demoUser      = "governance-demo"
	demoPassword  = "governance-demo-secret"
	edgeAudience  = "mcp.laserdata"
	ticket        = "support/tickets/acme"
)

func main() { examples.RunExample("governance", run) }

func run(ctx context.Context, client *laser.Laser) error {
	if err := client.Bootstrap(ctx, 1, laser.ExpireAfter(examples.SessionRetention)); err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}

	examples.Phase("Capability RBAC: roles bound to a server-stamped user")
	if examples.ManagedGate(client.Capabilities(), examples.FeatureAuthz, "governance", "capability RBAC") {
		user, err := targetUser(ctx, client)
		if err != nil {
			return err
		}
		if err := installRoles(ctx, client, user); err != nil {
			return err
		}
	}

	examples.Phase("Permission intersection: agent grants cannot exceed the user")
	demonstrateIntersection()

	examples.Phase("External edge: audience validation and step-up")
	demonstrateEdgeAuth()

	examples.Phase("Session governor: submit a budgeted session")
	if err := submitBudgetedSession(ctx, client); err != nil {
		return err
	}

	fmt.Println("\ngovernance: role grants, deny-wins matching, on-behalf-of intersection,\n" +
		"external-edge step-up, and budgeted session submission share one governance model.")
	return nil
}

// targetUser is the user named by the environment, or the dedicated demo user created on first run.
func targetUser(ctx context.Context, client *laser.Laser) (uint32, error) {
	if raw := strings.TrimSpace(os.Getenv(targetUserEnv)); raw != "" {
		if id, err := strconv.ParseUint(raw, 10, 32); err == nil {
			return uint32(id), nil
		}
	}
	user := client.User(demoUser)
	existing, err := user.Get(ctx)
	if err != nil {
		return 0, fmt.Errorf("read the demo user: %w", err)
	}
	if existing != nil {
		return existing.ID, nil
	}
	created, err := user.Create(ctx, demoPassword, laser.CreateUserOptions{Status: laser.UserActive})
	if err != nil {
		return 0, fmt.Errorf("create the demo user: %w", err)
	}
	return created.ID, nil
}

func installRoles(ctx context.Context, client *laser.Laser, user uint32) error {
	roles := exampleRoles()
	names := make([]string, 0, len(roles))
	for _, role := range roles {
		if err := client.DefineRole(ctx, role); err != nil {
			return fmt.Errorf("define role %s: %w", role.Name, err)
		}
		fmt.Printf("defined role: %s\n", role.Name)
		names = append(names, role.Name)
	}
	if _, err := client.BindRoles(ctx, user, names); err != nil {
		return fmt.Errorf("bind the roles: %w", err)
	}

	who, err := client.Whoami(ctx)
	if err != nil {
		return fmt.Errorf("whoami: %w", err)
	}
	fmt.Printf("caller roles: [%s], effective grants: %d\n", strings.Join(who.Roles, ", "), len(who.Grants))
	support, err := client.ListRoles(ctx, "support", "")
	if err != nil {
		return fmt.Errorf("list roles: %w", err)
	}
	listed := make([]string, 0, len(support))
	for _, role := range support {
		listed = append(listed, role.Name)
	}
	fmt.Printf("roles with prefix `support`: [%s]\n", strings.Join(listed, ", "))
	reader, err := client.GetRole(ctx, "support-reader")
	if err != nil {
		return fmt.Errorf("read role support-reader: %w", err)
	}
	if reader == nil {
		fmt.Println("support-reader role was not visible after define")
	}
	bindings, err := client.GetBindings(ctx, user)
	if err != nil {
		return fmt.Errorf("read the bindings: %w", err)
	}
	fmt.Printf("user %d is bound to: [%s] at binding revision %d\n",
		user, strings.Join(bindings.Roles, ", "), bindings.Revision)
	return nil
}

func demonstrateIntersection() {
	user := []wire.Grant{
		allow(wire.FeatureKv, wire.ActionRead, wire.PrefixResource("support/")),
		deny(wire.FeatureKv, wire.ActionDelete, wire.AllResources()),
	}
	agent := []wire.Grant{
		allow(wire.FeatureKv, wire.ActionRead, wire.PrefixResource("support/tickets/")),
		allow(wire.FeatureKv, wire.ActionWrite, wire.PrefixResource("support/tickets/")),
		allow(wire.FeatureKv, wire.ActionDelete, wire.PrefixResource("support/tickets/")),
	}
	for _, action := range []wire.Action{wire.ActionRead, wire.ActionWrite, wire.ActionDelete} {
		allowed := wire.DelegatedAllow(agent, user, wire.FeatureKv, action, ticket, true)
		fmt.Printf("delegated %s %s: %t\n", action, ticket, allowed)
	}
	direct := wire.GrantsAllow(user, wire.FeatureKv, wire.ActionDelete, ticket, true)
	fmt.Printf("direct user delete %s: %t\n", ticket, direct)
}

func demonstrateEdgeAuth() {
	readOnly := laser.EdgeClaims{Audience: []string{edgeAudience}, Scopes: []string{"tool:read"}}
	foreign := laser.EdgeClaims{Audience: []string{"other.server"}, Scopes: []string{"tool:write"}}

	read := laser.AuthorizeEdge(readOnly, edgeAudience, "tool:read")
	fmt.Printf("edge read authorized: %t\n", read == nil)

	challenge := "none"
	var denied *laser.EdgeDenial
	if errors.As(laser.AuthorizeEdge(readOnly, edgeAudience, "tool:write"), &denied) {
		challenge = denied.Challenge()
	}
	fmt.Printf("edge write step-up challenge: %s\n", challenge)

	var rejected *laser.EdgeDenial
	wrongAudience := errors.As(laser.AuthorizeEdge(foreign, edgeAudience, "tool:write"), &rejected) &&
		rejected.Kind == laser.DenialWrongAudience
	fmt.Printf("foreign audience rejected: %t\n", wrongAudience)
}

func submitBudgetedSession(ctx context.Context, client *laser.Laser) error {
	agents, err := examples.AgentIDs("governance-auditor", "governance")
	if err != nil {
		return err
	}
	tokens := uint64(4_000)
	submitted, err := client.Sessions().Submit(agents[0], []byte("audit this incident")).
		From(agents[1]).
		Budget(wire.Budget{Tokens: &tokens}).
		Send(ctx)
	if err != nil {
		return fmt.Errorf("submit the budgeted session: %w", err)
	}
	fmt.Printf("submitted budgeted session: %s\n", submitted.Session)
	return nil
}

func exampleRoles() []wire.Role {
	return []wire.Role{
		{Name: "support-reader", Grants: []wire.Grant{
			allow(wire.FeatureKv, wire.ActionRead, wire.PrefixResource("support/")),
		}},
		{Name: "projection-operator", Grants: []wire.Grant{
			allow(wire.FeatureProjection, wire.ActionAdmin, wire.PrefixResource("support_")),
		}},
		{Name: "agent-runner", Grants: []wire.Grant{
			allow(wire.FeatureSession, wire.ActionRead, wire.AllResources()),
			allow(wire.FeatureSession, wire.ActionWrite, wire.AllResources()),
		}},
		{Name: "safety-deny", Grants: []wire.Grant{
			deny(wire.FeatureKv, wire.ActionDelete, wire.AllResources()),
		}},
	}
}

func allow(feature wire.Feature, action wire.Action, resource wire.ResourcePattern) wire.Grant {
	return wire.Grant{Effect: wire.EffectAllow, Feature: feature, Action: action, Resource: resource}
}

func deny(feature wire.Feature, action wire.Action, resource wire.ResourcePattern) wire.Grant {
	return wire.Grant{Effect: wire.EffectDeny, Feature: feature, Action: action, Resource: resource}
}
