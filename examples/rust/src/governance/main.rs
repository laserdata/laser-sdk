use laser_examples::{
    PARTITIONS, env_u64, fresh_run, init_tracing, laser, managed_feature_ready, phase, stream_for,
};
use laser_sdk::edge_auth::{EdgeClaims, authorize_edge};
use laser_sdk::iggy::prelude::{Identifier, UserClient, UserStatus};
use laser_sdk::prelude::full::*;
use laser_sdk::rbac::{
    Action, Effect, Feature, Grant, ResourcePattern, Role, delegated_allow, grants_allow,
};

// The live role and binding calls need server-side `authz`. The pure decision
// pieces run everywhere and mirror the Python example.
//
//   cargo run --release --example governance
//
// Set LASER_GOVERNANCE_USER_ID to bind the example roles to a specific Iggy
// user. By default the roles are bound to a dedicated `governance-demo` user
// the example creates on first run. Never the caller: the role set includes
// deny-wins grants, and binding those to the session user would poison every
// later run against the same server.

const EXAMPLE: &str = "governance";
const TARGET_USER_ENV: &str = "LASER_GOVERNANCE_USER_ID";
const DEMO_USER: &str = "governance-demo";

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let stream = stream_for(EXAMPLE);
    let laser = laser(&stream, Capabilities::OPEN).await?;
    fresh_run(&laser, &stream, async {
        laser
            .bootstrap(
                PARTITIONS,
                laser_sdk::agent::TopicRetention::expire_after(std::time::Duration::from_secs(
                    86_400,
                )),
            )
            .await?;

        phase("Capability RBAC: roles bound to a server-stamped user");
        let capabilities = laser.capabilities().await;
        if managed_feature_ready(capabilities.authz, "capability RBAC", EXAMPLE) {
            let target_user = match std::env::var(TARGET_USER_ENV) {
                Ok(_) => env_u64(TARGET_USER_ENV, 0) as u32,
                Err(_) => demo_user_id(&laser).await?,
            };
            install_roles(&laser, target_user).await?;
        }

        phase("Permission intersection: agent grants cannot exceed the user");
        demonstrate_intersection();

        phase("External edge: audience validation and step-up");
        demonstrate_edge_auth();

        phase("Session governor: submit a budgeted session");
        submit_budgeted_session(&laser).await?;

        println!(
            "\ngovernance: role grants, deny-wins matching, on-behalf-of intersection,\n\
             external-edge step-up, and budgeted session submission share one governance model."
        );
        Ok(())
    })
    .await
}

/// The dedicated demo user the roles are bound to, created on first run.
/// Keeps the deny-wins role set off the calling user, so repeated example
/// runs never restrict the session that drives them.
async fn demo_user_id(laser: &Laser) -> Result<u32, LaserError> {
    let client = laser.client();
    if let Some(user) = client.get_user(&Identifier::named(DEMO_USER)?).await? {
        return Ok(user.id);
    }
    let created = client
        .create_user(
            DEMO_USER,
            "governance-demo-secret",
            UserStatus::Active,
            None,
        )
        .await?;
    Ok(created.id)
}

async fn install_roles(laser: &Laser, target_user: u32) -> Result<(), LaserError> {
    for role in roles() {
        let name = role.name.clone();
        laser.define_role(role).await?;
        println!("defined role: {name}");
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
    println!(
        "caller roles: [{}], effective grants: {}",
        who.roles.join(", "),
        who.grants.len()
    );

    let support_roles = laser.list_roles(Some("support"), None).await?;
    println!(
        "roles with prefix `support`: [{}]",
        support_roles
            .iter()
            .map(|role| role.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    if laser.get_role("support-reader").await?.is_none() {
        println!("support-reader role was not visible after define");
    }

    let bindings = laser.get_bindings(target_principal).await?;
    println!("user {target_user} is bound to: [{}]", bindings.join(", "));
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

    println!("delegated read support/tickets/acme: {read_ticket}");
    println!("delegated write support/tickets/acme: {write_ticket}");
    println!("delegated delete support/tickets/acme: {delete_ticket}");
    println!(
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

    println!(
        "edge read authorized: {}",
        authorize_edge(&ok, "mcp.laserdata", "tool:read").is_ok()
    );
    match authorize_edge(&missing_scope, "mcp.laserdata", "tool:write") {
        Ok(()) => println!("edge write authorized unexpectedly"),
        Err(denial) => println!(
            "edge write step-up challenge: {}",
            denial.challenge().unwrap_or_default()
        ),
    }
    println!(
        "foreign audience rejected: {}",
        authorize_edge(&wrong_audience, "mcp.laserdata", "tool:write").is_err()
    );
}

async fn submit_budgeted_session(laser: &Laser) -> Result<(), LaserError> {
    let submitted = laser
        .sessions()
        .submit(
            "governance-auditor".parse::<laser_sdk::types::AgentId>()?,
            b"audit this incident".to_vec(),
        )
        .from("governance".parse::<laser_sdk::types::AgentId>()?)
        .budget(laser_sdk::wire::agent::Budget {
            tokens: Some(4_000),
            cost_micros: None,
        })
        .send()
        .await?;
    println!("submitted budgeted session: {}", submitted.session);
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
