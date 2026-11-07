// Authorization mutation receipts through the public client against Laser
// Streams: an applied mutation keeps its outcome under the account that sent
// it, so the exact repeat of a request whose reply was lost returns the
// original outcome without a second effect.
import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test, type TestContext } from "node:test"
import { UserStatus } from "apache-iggy"
import { AuthzExecutionError, InvalidError } from "../../src/client/errors.js"
import { code } from "../../src/client/error-classify.js"
import { Laser } from "../../src/client/laser.js"
import { TestIggy } from "../support/test-iggy.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const PASSWORD = "receipt-secret"
const RECEIPT_CAPACITY = 10_000

interface Administrator {
  readonly laser: Laser
  readonly userId: number
  readonly username: string
  readonly role: string
}

async function requireAuthz(context: TestContext, laser: Laser): Promise<boolean> {
  if ((await laser.capabilities()).authz) return true
  context.skip("authorization receipts need Laser Streams")
  return false
}

function connectAs(connectionString: string, username: string): Promise<Laser> {
  return Laser.connect(connectionString.replace(/^[^@]*@/u, `${username}:${PASSWORD}@`))
}

// A user whose bound role grants `authz:admin`, connected on its own.
async function administrator(root: Laser, connectionString: string): Promise<Administrator> {
  const suffix = randomUUID().slice(0, 12)
  const username = `receipt-${suffix}`
  const role = `receipt-admin-${suffix}`
  const created = await root.client.user.create({
    username,
    password: PASSWORD,
    status: UserStatus.Active
  })
  await root.defineRole({
    name: role,
    grants: [
      {
        effect: "allow",
        feature: "authz",
        action: "admin",
        resource: { kind: "all", value: "" }
      }
    ]
  })
  await root.bindRoles(created.id, [role])
  return { laser: await connectAs(connectionString, username), userId: created.id, username, role }
}

async function bindingRevisions(root: Laser, userId: number): Promise<number> {
  return (await root.authzHistory({ kind: "binding", userId }, 1_000)).events.length
}

async function remove(root: Laser, administrator: Administrator): Promise<void> {
  await root.client.user.delete({ userId: administrator.username }).catch(() => false)
  await root.deleteRole(administrator.role).catch(() => false)
}

function resultName(error: unknown): string | undefined {
  const result = code(error)
  return result.kind === "known" ? result.name : undefined
}

function unauthorized(error: unknown): boolean {
  return error instanceof AuthzExecutionError && resultName(error) === "Forbidden"
}

void test("given_an_administrator_that_revoked_itself_when_the_exact_request_is_repeated_then_should_return_the_original_success_without_a_second_revision", async (context) => {
  const root = await Laser.connect(CONNECTION_STRING)
  try {
    if (!(await requireAuthz(context, root))) return
    const admin = await administrator(root, CONNECTION_STRING)
    try {
      const mutationId = `self-revoke-${randomUUID()}`
      await admin.laser.bindRolesAs(admin.userId, [], mutationId)
      const revisions = await bindingRevisions(root, admin.userId)

      await admin.laser.bindRolesAs(admin.userId, [], mutationId)
      assert.equal(await bindingRevisions(root, admin.userId), revisions, "no second effect")

      await assert.rejects(
        admin.laser.bindRolesAs(admin.userId, [], "fresh-after-revocation"),
        unauthorized,
        "a new identity is authorized against the current state"
      )
      await assert.rejects(
        admin.laser.bindRolesAs(admin.userId, [admin.role], mutationId),
        (error: unknown) =>
          error instanceof AuthzExecutionError && resultName(error) === "Conflict",
        "the same identity with other content is a conflict"
      )
    } finally {
      await admin.laser.close()
      await remove(root, admin)
    }
  } finally {
    await root.close()
  }
})

void test("given_a_reused_native_user_id_when_the_old_receipt_identity_is_sent_then_should_not_return_the_old_receipt", async (context) => {
  const root = await Laser.connect(CONNECTION_STRING)
  try {
    if (!(await requireAuthz(context, root))) return
    const first = await administrator(root, CONNECTION_STRING)
    const mutationId = `reincarnation-${randomUUID()}`
    await first.laser.bindRolesAs(first.userId, [], mutationId)
    await first.laser.close()
    await root.client.user.delete({ userId: first.username })
    const created = await root.client.user.create({
      username: first.username,
      password: PASSWORD,
      status: UserStatus.Active
    })
    assert.equal(created.id, first.userId, "the native user id is reused by the new incarnation")
    const second = await connectAs(CONNECTION_STRING, first.username)
    try {
      await assert.rejects(
        second.bindRolesAs(first.userId, [], mutationId),
        unauthorized,
        "the new incarnation receives no old receipt"
      )
    } finally {
      await second.close()
      await remove(root, first)
    }
  } finally {
    await root.close()
  }
})

void test("given_an_invalid_mutation_id_when_used_then_should_reject_before_sending", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    await assert.rejects(laser.deleteRoleAs("receipt-invalid", "not an id"), InvalidError)
    await assert.rejects(laser.bindRolesAs(0, [], ""), InvalidError)
  } finally {
    await laser.close()
  }
})

void test("given_a_receipt_when_the_server_restarts_then_should_answer_the_exact_repeat_from_the_recovered_receipt", async (context) => {
  const server = await TestIggy.start()
  try {
    let root = await Laser.connect(server.endpoint)
    if (!(await requireAuthz(context, root))) {
      await root.close()
      return
    }
    const admin = await administrator(root, server.endpoint)
    const mutationId = `restart-${randomUUID()}`
    await admin.laser.bindRolesAs(admin.userId, [], mutationId)
    const revisions = await bindingRevisions(root, admin.userId)
    await admin.laser.close()
    await root.close()

    await server.restart()

    root = await Laser.connect(server.endpoint)
    const recovered = await connectAs(server.endpoint, admin.username)
    try {
      await recovered.bindRolesAs(admin.userId, [], mutationId)
      assert.equal(
        await bindingRevisions(root, admin.userId),
        revisions,
        "recovery kept the receipt"
      )
      await assert.rejects(
        recovered.bindRolesAs(admin.userId, [], "fresh-after-restart"),
        unauthorized
      )
    } finally {
      await recovered.close()
      await root.close()
    }
  } finally {
    await server.close()
  }
})

void test("given_receipt_capacity_filled_by_churn_when_a_new_identity_arrives_then_should_refuse_it_as_resource_limit_and_keep_the_retained_receipts", async (context) => {
  const server = await TestIggy.start()
  try {
    const root = await Laser.connect(server.endpoint)
    try {
      if (!(await requireAuthz(context, root))) return
      const target = await root.client.user.create({
        username: "receipt-target",
        password: PASSWORD,
        status: UserStatus.Active
      })
      for (let index = 0; index < RECEIPT_CAPACITY; index += 1) {
        await root.bindRolesAs(target.id, [], `churn-${String(index)}`)
      }
      await assert.rejects(
        root.bindRolesAs(target.id, [], "churn-beyond-capacity"),
        (error: unknown) =>
          error instanceof AuthzExecutionError && resultName(error) === "ResourceLimit"
      )
      const revisions = await bindingRevisions(root, target.id)
      await root.bindRolesAs(target.id, [], "churn-0")
      assert.equal(await bindingRevisions(root, target.id), revisions, "a retained receipt answers")
    } finally {
      await root.close()
    }
  } finally {
    await server.close()
  }
})
