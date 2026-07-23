use laser_examples::{
    PARTITIONS, cloud_feature_ready, env_u64, init_tracing, laser, phase, stream_for,
};
use laser_sdk::edge_auth::{EdgeClaims, authorize_edge};
use laser_sdk::prelude::full::*;
use laser_sdk::rbac::{
    Action, Effect, Feature, Grant, ResourcePattern, Role, delegated_allow, grants_allow,
};
use laser_sdk::wire::agent_workflow::RunBudget;
use tracing::{info, warn};

// The live role and binding calls need fork-native `authz`. The pure decision
// pieces run everywhere and mirror the Python example.
//
//   cargo run --release --example governance
//
// Set LASER_GOVERNANCE_USER_ID to bind the example roles to a specific Iggy
// user. The default is 1, the local root user in a dev server.

const EXAMPLE: &str = "governance";
const TARGET_USER_ENV: &str = "LASER_GOVERNANCE_USER_ID";

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let stream = stream_for(EXAMPLE);
    let laser = laser(&stream, Capabilities::OPEN).await?;
    laser.bootstrap(PARTITIONS).await?;

    phase("Capability RBAC: roles bound to a server-stamped user");
    let capabilities = laser.capabilities().await;
    if cloud_feature_ready(capabilities.authz, "capability RBAC", EXAMPLE) {
        let target_user = env_u64(TARGET_USER_ENV, 1) as u32;
        install_roles(&laser, target_user).await?;
    }

    phase("Permission intersection: agent grants cannot exceed the user");
    demonstrate_intersection();

    phase("External edge: audience validation and step-up");
    demonstrate_edge_auth();

    phase("Run governor: submit a budgeted managed run when served");
    if capabilities.agent_workflow {
        submit_budgeted_run(&laser).await?;
    } else {
        warn!("agent_workflow is not advertised, so the live budgeted-run submit is skipped.");
    }

    info!(
        "governance: role grants, deny-wins matching, on-behalf-of intersection, \
         external-edge step-up, and budgeted run submission share one governance model."
    );
    Ok(())
}

async fn install_roles(laser: &Laser, target_user: u32) -> Result<(), LaserError> {
    for role in roles() {
        info!(role = %role.name, "defining role");
        laser.define_role(role).await?;
    }
    let bound = vec![
        "support-reader".to_owned(),
        "projection-operator".to_owned(),
        "agent-runner".to_owned(),
        "safety-deny".to_owned(),
    ];
    let target_principal = PrincipalId::new(target_user);
    laser.bind_roles(target_principal, bound.clone()).await?;

    let who = laser.whoami().await?;
    info!(
        "caller roles: [{}], effective grants: {}",
        who.roles.join(", "),
        who.grants.len()
    );

    let support_roles = laser.list_roles(Some("support")).await?;
    info!(
        "roles with prefix `support`: [{}]",
        support_roles
            .iter()
            .map(|role| role.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let bindings = laser.get_bindings(target_principal).await?;
    info!("user {target_user} is bound to: [{}]", bindings.join(", "));

    if laser.get_role("support-reader").await?.is_none() {
        warn!("support-reader role was not visible after define");
    }
    Ok(())
}

fn roles() -> Vec<Role> {
    vec![
        Role {
            name: "support-reader".to_owned(),
            grants: vec![allow(
                Feature::Kv,
                Action::Read,
                ResourcePattern::prefix("support/"),
            )],
        },
        Role {
            name: "projection-operator".to_owned(),
            grants: vec![allow(
                Feature::Projection,
                Action::Admin,
                ResourcePattern::prefix("support_"),
            )],
        },
        Role {
            name: "agent-runner".to_owned(),
            grants: vec![
                allow(Feature::Agent, Action::Read, ResourcePattern::all()),
                allow(Feature::Agent, Action::Write, ResourcePattern::all()),
            ],
        },
        Role {
            name: "safety-deny".to_owned(),
            grants: vec![deny(Feature::Kv, Action::Delete, ResourcePattern::all())],
        },
    ]
}

fn demonstrate_intersection() {
    let user = vec![
        allow(
            Feature::Kv,
            Action::Read,
            ResourcePattern::prefix("support/"),
        ),
        deny(Feature::Kv, Action::Delete, ResourcePattern::all()),
    ];
    let agent = vec![
        allow(
            Feature::Kv,
            Action::Read,
            ResourcePattern::prefix("support/tickets/"),
        ),
        allow(
            Feature::Kv,
            Action::Write,
            ResourcePattern::prefix("support/tickets/"),
        ),
        allow(
            Feature::Kv,
            Action::Delete,
            ResourcePattern::prefix("support/tickets/"),
        ),
    ];

    let read_ticket = delegated_allow(
        &agent,
        &user,
        Feature::Kv,
        Action::Read,
        Some("support/tickets/acme"),
    );
    let write_ticket = delegated_allow(
        &agent,
        &user,
        Feature::Kv,
        Action::Write,
        Some("support/tickets/acme"),
    );
    let delete_ticket = delegated_allow(
        &agent,
        &user,
        Feature::Kv,
        Action::Delete,
        Some("support/tickets/acme"),
    );

    info!("delegated read support/tickets/acme: {read_ticket}");
    info!("delegated write support/tickets/acme: {write_ticket}");
    info!("delegated delete support/tickets/acme: {delete_ticket}");
    info!(
        "direct user delete support/tickets/acme: {}",
        grants_allow(
            &user,
            Feature::Kv,
            Action::Delete,
            Some("support/tickets/acme")
        )
    );
}

fn demonstrate_edge_auth() {
    let ok = EdgeClaims {
        audience: vec!["mcp.laserdata".to_owned()],
        scopes: vec!["tool:read".to_owned()],
    };
    let missing_scope = EdgeClaims {
        audience: vec!["mcp.laserdata".to_owned()],
        scopes: vec!["tool:read".to_owned()],
    };
    let wrong_audience = EdgeClaims {
        audience: vec!["other.server".to_owned()],
        scopes: vec!["tool:write".to_owned()],
    };

    info!(
        "edge read authorized: {}",
        authorize_edge(&ok, "mcp.laserdata", "tool:read").is_ok()
    );
    match authorize_edge(&missing_scope, "mcp.laserdata", "tool:write") {
        Ok(()) => warn!("edge write authorized unexpectedly"),
        Err(denial) => info!("edge write step-up challenge: {:?}", denial.challenge()),
    }
    info!(
        "foreign audience rejected: {}",
        authorize_edge(&wrong_audience, "mcp.laserdata", "tool:write").is_err()
    );
}

async fn submit_budgeted_run(laser: &Laser) -> Result<(), LaserError> {
    let budget = RunBudget {
        max_events: Some(8),
        max_model_calls: Some(1),
        max_tool_calls: Some(2),
        max_patches: None,
        max_depth: None,
        max_wall_clock_micros: Some(30_000_000),
        max_cost_usd: None,
    };
    match laser
        .runs()
        .submit_budgeted(
            "governance-auditor",
            Some(b"audit this incident".to_vec()),
            budget,
        )
        .await
    {
        Ok(run) => info!("submitted budgeted run: {}", run.run_id),
        Err(error) if error.is_unsupported() => {
            warn!("budgeted run submit returned unsupported: {error}")
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

fn allow(feature: Feature, action: Action, resource: ResourcePattern) -> Grant {
    Grant {
        effect: Effect::Allow,
        feature,
        action,
        resource,
    }
}

fn deny(feature: Feature, action: Action, resource: ResourcePattern) -> Grant {
    Grant {
        effect: Effect::Deny,
        feature,
        action,
        resource,
    }
}
