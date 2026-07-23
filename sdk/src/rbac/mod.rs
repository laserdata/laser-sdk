use crate::error::{LaserError, decode_managed_reply};
use crate::laser::Laser;
use crate::types::PrincipalId;
use laser_wire::framing::encode_named;
use serde::Serialize;

pub use laser_wire::authz::{
    Action, AuthzError, AuthzEvent, AuthzEventKind, AuthzHistoryReply, AuthzHistoryReq, AuthzReply,
    AuthzSubject, BindRolesReq, BindingsReply, DefineRoleReq, DeleteRoleReq, Effect, Feature,
    GetBindingsReq, GetRoleReq, Grant, ListRolesReply, ListRolesReq, ResourceKind, ResourcePattern,
    Role, RoleBinding, WhoamiReply, WhoamiReq, delegated_allow, grants_allow, validate_role_name,
};
pub use laser_wire::codes::{
    AGDX_AUTHZ_BIND_ROLES_CODE, AGDX_AUTHZ_DEFINE_ROLE_CODE, AGDX_AUTHZ_DELETE_ROLE_CODE,
    AGDX_AUTHZ_GET_BINDINGS_CODE, AGDX_AUTHZ_GET_ROLE_CODE, AGDX_AUTHZ_HISTORY_CODE,
    AGDX_AUTHZ_LIST_ROLES_CODE, AGDX_AUTHZ_WHOAMI_CODE, AUTHZ_OP_VERSION,
};
pub use laser_wire::limits::MAX_ROLE_NAME_BYTES;

impl Laser {
    /// The caller's own effective capabilities (bound roles and their flattened
    /// grants). Always answered to the authenticated caller.
    pub async fn whoami(&self) -> Result<WhoamiReply, LaserError> {
        match self
            .execute_authz(
                AGDX_AUTHZ_WHOAMI_CODE,
                &WhoamiReq {
                    v: AUTHZ_OP_VERSION,
                },
            )
            .await?
        {
            AuthzReply::Whoami(reply) => Ok(reply),
            other => Err(unexpected("whoami", other)),
        }
    }

    /// List defined roles, optionally filtered by name prefix.
    pub async fn list_roles(&self, name_prefix: Option<&str>) -> Result<Vec<Role>, LaserError> {
        let request = ListRolesReq {
            v: AUTHZ_OP_VERSION,
            name_prefix: name_prefix.map(str::to_owned),
            search: None,
        };
        match self
            .execute_authz(AGDX_AUTHZ_LIST_ROLES_CODE, &request)
            .await?
        {
            AuthzReply::Roles(reply) => Ok(reply.roles),
            other => Err(unexpected("list_roles", other)),
        }
    }

    /// One role by name, or `None`. The name must pass [`validate_role_name`].
    pub async fn get_role(&self, name: impl Into<String>) -> Result<Option<Role>, LaserError> {
        let name = name.into();
        validate_role_name(&name)?;
        let request = GetRoleReq {
            v: AUTHZ_OP_VERSION,
            name,
        };
        match self
            .execute_authz(AGDX_AUTHZ_GET_ROLE_CODE, &request)
            .await?
        {
            AuthzReply::Role(role) => Ok(role),
            other => Err(unexpected("get_role", other)),
        }
    }

    /// One user's bound role names.
    pub async fn get_bindings(&self, principal: PrincipalId) -> Result<Vec<String>, LaserError> {
        let request = GetBindingsReq {
            v: AUTHZ_OP_VERSION,
            user_id: principal.get(),
        };
        match self
            .execute_authz(AGDX_AUTHZ_GET_BINDINGS_CODE, &request)
            .await?
        {
            AuthzReply::Bindings(reply) => Ok(reply.roles),
            other => Err(unexpected("get_bindings", other)),
        }
    }

    /// Define or replace a role (upsert). Requires `authz:admin`. The role
    /// name must pass [`validate_role_name`].
    pub async fn define_role(&self, role: Role) -> Result<(), LaserError> {
        validate_role_name(&role.name)?;
        let request = DefineRoleReq {
            v: AUTHZ_OP_VERSION,
            role,
        };
        self.execute_authz_ok(AGDX_AUTHZ_DEFINE_ROLE_CODE, &request)
            .await
    }

    /// Delete a role by name. Requires `authz:admin`. The name must pass
    /// [`validate_role_name`].
    pub async fn delete_role(&self, name: impl Into<String>) -> Result<(), LaserError> {
        let name = name.into();
        validate_role_name(&name)?;
        let request = DeleteRoleReq {
            v: AUTHZ_OP_VERSION,
            name,
        };
        self.execute_authz_ok(AGDX_AUTHZ_DELETE_ROLE_CODE, &request)
            .await
    }

    /// Bind a user's whole role set (replace). Requires `authz:admin`. Every
    /// name must pass [`validate_role_name`].
    pub async fn bind_roles(
        &self,
        principal: PrincipalId,
        roles: Vec<String>,
    ) -> Result<(), LaserError> {
        self.bind_roles_inner(principal, roles, None).await
    }

    /// Bind a user's whole role set if the current binding revision matches.
    /// Requires `authz:admin`.
    pub async fn bind_roles_expect_revision(
        &self,
        principal: PrincipalId,
        roles: Vec<String>,
        expect_revision: u64,
    ) -> Result<(), LaserError> {
        self.bind_roles_inner(principal, roles, Some(expect_revision))
            .await
    }

    async fn bind_roles_inner(
        &self,
        principal: PrincipalId,
        roles: Vec<String>,
        expect_revision: Option<u64>,
    ) -> Result<(), LaserError> {
        for role in &roles {
            validate_role_name(role)?;
        }
        let request = BindRolesReq {
            v: AUTHZ_OP_VERSION,
            user_id: principal.get(),
            roles,
            expect_revision,
        };
        self.execute_authz_ok(AGDX_AUTHZ_BIND_ROLES_CODE, &request)
            .await
    }

    /// Read a page of the authorization change history. Requires `authz:read`.
    pub async fn authz_history(
        &self,
        subject: AuthzSubject,
        after_revision: Option<u64>,
        limit: u32,
    ) -> Result<AuthzHistoryReply, LaserError> {
        let request = AuthzHistoryReq {
            v: AUTHZ_OP_VERSION,
            subject,
            after_revision,
            limit,
        };
        match self
            .execute_authz(AGDX_AUTHZ_HISTORY_CODE, &request)
            .await?
        {
            AuthzReply::History(reply) => Ok(reply),
            other => Err(unexpected("authz_history", other)),
        }
    }

    // Send one authorization command and decode its reply. Gated on the `authz`
    // capability (set by the connect-time probe). Without it, `Unsupported`.
    async fn execute_authz(
        &self,
        code: u32,
        request: &impl Serialize,
    ) -> Result<AuthzReply, LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.authz {
            return Err(LaserError::unsupported(
                "rbac",
                "the authorization surface is not served by this deployment",
            ));
        }
        let payload = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        let payload = self.send_raw_with_response(code, payload).await?;
        decode_managed_reply::<AuthzReply>(&payload)
    }

    // The mutating verbs share the `Ok`-or-`Err` shape.
    async fn execute_authz_ok(
        &self,
        code: u32,
        request: &impl Serialize,
    ) -> Result<(), LaserError> {
        match self.execute_authz(code, request).await? {
            AuthzReply::Ok => Ok(()),
            AuthzReply::Err(error) => Err(error.into()),
            other => Err(unexpected("authz write", other)),
        }
    }
}

fn unexpected(verb: &str, reply: AuthzReply) -> LaserError {
    match reply {
        AuthzReply::Err(error) => error.into(),
        _ => LaserError::Protocol(format!("{verb}: unexpected reply variant")),
    }
}
