// Authorization mutation receipts through the public client against Laser
// Streams: an applied mutation keeps its outcome under the account that sent
// it, so the exact repeat of a request whose reply was lost returns the
// original outcome without a second effect.

use crate::harness;
use crate::test_iggy::TestIggy;
use laser_sdk::iggy::prelude::{Identifier, UserClient, UserCreateOptions, UserStatus};
use laser_sdk::prelude::full::{Laser, LaserError};
use laser_sdk::rbac::{
    Action, AuthzError, AuthzSubject, Effect, Feature, Grant, ResourcePattern, Role,
};
use laser_sdk::types::PrincipalId;
use laser_sdk::wire::result::ResultCode;

const PASSWORD: &str = "receipt-secret";
const RECEIPT_CAPACITY: usize = 10_000;

/// A user whose bound role grants `authz:admin`, connected on its own.
struct Administrator {
    laser: Laser,
    principal: PrincipalId,
    username: String,
    role: String,
}

async fn administrator(root: &Laser, connection_string: &str) -> Administrator {
    let suffix = ulid::Ulid::generate().to_string().to_lowercase();
    let username = format!("receipt-{suffix}");
    let role = format!("receipt-admin-{suffix}");
    let created = root
        .client()
        .create_user(
            &username,
            PASSWORD,
            UserStatus::Active,
            None,
            &UserCreateOptions::default(),
        )
        .await
        .expect("the user is created");
    root.define_role(Role {
        name: role.clone(),
        grants: vec![Grant {
            effect: Effect::Allow,
            feature: Feature::Authz,
            action: Action::Admin,
            resource: ResourcePattern::default(),
        }],
    })
    .await
    .expect("the administration role is defined");
    let principal = PrincipalId::new(created.id);
    root.bind_roles(principal, vec![role.clone()])
        .await
        .expect("the user is bound to the role");
    Administrator {
        laser: connect_as(connection_string, &username).await,
        principal,
        username,
        role,
    }
}

async fn connect_as(connection_string: &str, username: &str) -> Laser {
    let connection_string =
        connection_string.replacen("iggy:iggy@", &format!("{username}:{PASSWORD}@"), 1);
    Laser::connect(&connection_string)
        .await
        .expect("the user connects")
}

async fn binding_revisions(root: &Laser, principal: PrincipalId) -> usize {
    root.authz_history(
        AuthzSubject::Binding {
            user_id: principal.get(),
        },
        None,
        1_000,
    )
    .await
    .expect("the binding history reads")
    .events
    .len()
}

fn authz_code(error: &LaserError) -> ResultCode {
    error.code()
}

async fn remove(root: &Laser, administrator: &Administrator) {
    let _ = root
        .client()
        .delete_user(&Identifier::named(&administrator.username).expect("username"))
        .await;
    let _ = root.delete_role(administrator.role.clone()).await;
}

#[tokio::test]
async fn given_an_administrator_that_revoked_itself_when_the_exact_request_is_repeated_then_should_return_the_original_success_without_a_second_revision()
 {
    let root = harness::connected_laser().await;
    let connection_string = harness::connection_string().await;
    let administrator = administrator(&root, &connection_string).await;
    let mutation_id = format!(
        "self-revoke-{}",
        ulid::Ulid::generate().to_string().to_lowercase()
    );

    administrator
        .laser
        .bind_roles_as(
            administrator.principal,
            Vec::new(),
            None,
            mutation_id.clone(),
        )
        .await
        .expect("an administrator may revoke its own administration");
    let revisions = binding_revisions(&root, administrator.principal).await;

    administrator
        .laser
        .bind_roles_as(
            administrator.principal,
            Vec::new(),
            None,
            mutation_id.clone(),
        )
        .await
        .expect("the exact repeat returns the original success");
    assert_eq!(
        binding_revisions(&root, administrator.principal).await,
        revisions,
        "the repeat has no second effect"
    );

    let fresh = administrator
        .laser
        .bind_roles_as(
            administrator.principal,
            Vec::new(),
            None,
            "fresh-after-revocation",
        )
        .await
        .expect_err("a new identity is authorized against the current state");
    assert!(
        matches!(fresh, LaserError::Authz(AuthzError::Unauthorized)),
        "the revoked administrator cannot start a new mutation: {fresh:?}"
    );

    let conflict = administrator
        .laser
        .bind_roles_as(
            administrator.principal,
            vec![administrator.role.clone()],
            None,
            mutation_id,
        )
        .await
        .expect_err("the same identity with other content is a conflict");
    assert!(
        matches!(conflict, LaserError::Authz(AuthzError::MutationConflict(_))),
        "{conflict:?}"
    );
    assert_eq!(authz_code(&conflict), ResultCode::Conflict);
    remove(&root, &administrator).await;
}

#[tokio::test]
async fn given_a_reused_native_user_id_when_the_old_receipt_identity_is_sent_then_should_not_return_the_old_receipt()
 {
    let server = TestIggy::start().await;
    let root = harness::connected_laser_on(&server).await;
    let connection_string = server.connection_string();
    let first = administrator(&root, &connection_string).await;
    let mutation_id = format!(
        "reincarnation-{}",
        ulid::Ulid::generate().to_string().to_lowercase()
    );
    first
        .laser
        .bind_roles_as(first.principal, Vec::new(), None, mutation_id.clone())
        .await
        .expect("the first incarnation revokes itself");
    first
        .laser
        .close()
        .await
        .expect("the first incarnation closes");
    root.client()
        .delete_user(&Identifier::named(&first.username).expect("username"))
        .await
        .expect("the first incarnation is deleted");

    let created = root
        .client()
        .create_user(
            &first.username,
            PASSWORD,
            UserStatus::Active,
            None,
            &UserCreateOptions::default(),
        )
        .await
        .expect("the user is created again");
    assert_eq!(
        created.id,
        first.principal.get(),
        "the native user id is reused by the new incarnation"
    );
    let second = connect_as(&connection_string, &first.username).await;

    let outcome = second
        .bind_roles_as(first.principal, Vec::new(), None, mutation_id)
        .await
        .expect_err("the new incarnation receives no old receipt");
    assert!(
        matches!(outcome, LaserError::Authz(AuthzError::Unauthorized)),
        "the request is authorized afresh, and the new incarnation administers nothing: {outcome:?}"
    );
    second.close().await.expect("the second incarnation closes");
    remove(&root, &first).await;
}

#[tokio::test]
async fn given_a_receipt_when_the_server_restarts_then_should_answer_the_exact_repeat_from_the_recovered_receipt()
 {
    let server = TestIggy::start().await;
    let root = harness::connected_laser_on(&server).await;
    let administrator = administrator(&root, &server.connection_string()).await;
    let mutation_id = format!(
        "restart-{}",
        ulid::Ulid::generate().to_string().to_lowercase()
    );
    administrator
        .laser
        .bind_roles_as(
            administrator.principal,
            Vec::new(),
            None,
            mutation_id.clone(),
        )
        .await
        .expect("the administrator revokes itself");
    let revisions = binding_revisions(&root, administrator.principal).await;
    administrator
        .laser
        .close()
        .await
        .expect("the administrator closes");
    root.close().await.expect("root closes");

    server.restart().await;

    let root = harness::connected_laser_on(&server).await;
    let recovered = connect_as(&server.connection_string(), &administrator.username).await;
    recovered
        .bind_roles_as(administrator.principal, Vec::new(), None, mutation_id)
        .await
        .expect("the recovered receipt answers the exact repeat");
    assert_eq!(
        binding_revisions(&root, administrator.principal).await,
        revisions,
        "recovery kept the receipt, so the repeat has no second effect"
    );
    let fresh = recovered
        .bind_roles_as(
            administrator.principal,
            Vec::new(),
            None,
            "fresh-after-restart",
        )
        .await
        .expect_err("a new identity is authorized against the recovered state");
    assert!(matches!(fresh, LaserError::Authz(AuthzError::Unauthorized)));
    recovered
        .close()
        .await
        .expect("the recovered client closes");
}

#[tokio::test]
async fn given_receipt_capacity_filled_by_churn_when_a_new_identity_arrives_then_should_refuse_it_as_resource_limit_and_keep_the_retained_receipts()
 {
    let server = TestIggy::start().await;
    let root = harness::connected_laser_on(&server).await;
    let target = root
        .client()
        .create_user(
            "receipt-target",
            PASSWORD,
            UserStatus::Active,
            None,
            &UserCreateOptions::default(),
        )
        .await
        .expect("the target user is created");
    let principal = PrincipalId::new(target.id);
    for index in 0..RECEIPT_CAPACITY {
        root.bind_roles_as(principal, Vec::new(), None, format!("churn-{index}"))
            .await
            .unwrap_or_else(|error| panic!("churn {index} applies: {error}"));
    }

    let refused = root
        .bind_roles_as(principal, Vec::new(), None, "churn-beyond-capacity")
        .await
        .expect_err("a new identity finds no receipt room inside the replay floor");
    assert!(
        matches!(refused, LaserError::Authz(AuthzError::ResourceLimit(_))),
        "{refused:?}"
    );
    assert_eq!(authz_code(&refused), ResultCode::ResourceLimit);

    let revisions = binding_revisions(&root, principal).await;
    root.bind_roles_as(principal, Vec::new(), None, "churn-0")
        .await
        .expect("a retained receipt still answers its exact repeat");
    assert_eq!(
        binding_revisions(&root, principal).await,
        revisions,
        "the retained receipt has no second effect"
    );
    root.close().await.expect("root closes");
}

#[tokio::test]
async fn given_a_read_binding_when_rebound_with_its_revision_then_should_apply_once_and_refuse_the_stale_revision()
 {
    let root = harness::connected_laser().await;
    let connection_string = harness::connection_string().await;
    let administrator = administrator(&root, &connection_string).await;

    let read = root
        .get_bindings(administrator.principal)
        .await
        .expect("the binding reads");
    assert_eq!(read.roles, vec![administrator.role.clone()]);
    assert!(read.revision > 0, "a bound user has a binding revision");

    let bound = root
        .bind_roles_expect_revision(
            administrator.principal,
            vec![administrator.role.clone()],
            read.revision,
        )
        .await
        .expect("the revision just read replaces the binding");
    assert_eq!(bound.roles, vec![administrator.role.clone()]);
    assert_eq!(
        bound.revision,
        read.revision + 1,
        "the bind answers its new revision"
    );
    let reread = root
        .get_bindings(administrator.principal)
        .await
        .expect("the binding reads again");
    assert_eq!(reread.revision, read.revision + 1);

    let stale = root
        .bind_roles_expect_revision(administrator.principal, Vec::new(), read.revision)
        .await
        .expect_err("the revision read before the rebind is stale");
    assert!(
        matches!(
            stale,
            LaserError::Authz(AuthzError::Conflict { current_revision }) if current_revision == reread.revision
        ),
        "{stale:?}"
    );
    remove(&root, &administrator).await;
}
