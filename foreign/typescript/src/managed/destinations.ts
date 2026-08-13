import type { Capabilities } from "../client/capabilities.js"
import { QueryExecutionError } from "../client/errors.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import { mintUlidValue } from "../runtime/ulid.js"
import { CHECKPOINT_OP_VERSION } from "../wire/codes.js"
import {
  type CheckpointMutationResult,
  type CheckpointReadConsistency,
  type CheckpointRequestEnvelope,
  type DestinationCheckpointPage,
  type DestinationCheckpointView,
  type DestinationListFilter,
  type PublicCheckpointMutation,
  type QueryRoutePage
} from "../wire/checkpoint.js"
import {
  CheckpointCommand,
  DestinationGetCommand,
  DestinationListCommand,
  QueryRouteListCommand
} from "../wire/commands.js"
import type {
  DestinationDesiredState,
  MaterializationDestination,
  QueryRoute
} from "../wire/destination.js"
import { CheckpointRequestId, type DestinationId, type QueryRouteId } from "../wire/ids.js"

export class Destinations {
  constructor(
    private readonly transport: ManagedTransport,
    private readonly capabilities: () => Promise<Capabilities>
  ) {}

  async mutate(
    expectedGlobalStateRevision: bigint,
    mutation: PublicCheckpointMutation
  ): Promise<CheckpointMutationResult> {
    const request: CheckpointRequestEnvelope = {
      v: CHECKPOINT_OP_VERSION,
      requestId: CheckpointRequestId.fromU128(mintUlidValue()),
      expectedGlobalStateRevision,
      mutation
    }
    const reply = await executeManaged(
      this.transport,
      await this.capabilities(),
      CheckpointCommand,
      request
    )
    if (reply.kind === "ok") return reply.result
    throw new QueryExecutionError(`checkpoint mutation failed: ${reply.error.kind}`, reply.error)
  }

  register(
    expectedGlobalStateRevision: bigint,
    destination: MaterializationDestination
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, { kind: "register_destination", destination })
  }

  setDesiredState(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    expectedDefinitionRevision: bigint,
    desiredState: DestinationDesiredState
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "set_desired_state",
      destinationId,
      destinationGeneration,
      expectedDefinitionRevision,
      desiredState
    })
  }

  registerQueryRoute(
    expectedGlobalStateRevision: bigint,
    route: QueryRoute
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, { kind: "register_query_route", route })
  }

  removeQueryRoute(
    expectedGlobalStateRevision: bigint,
    routeId: QueryRouteId,
    routeGeneration: bigint,
    expectedDefinitionRevision: bigint
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "remove_query_route",
      routeId,
      routeGeneration,
      expectedDefinitionRevision
    })
  }

  async get(
    destinationId: DestinationId,
    consistency: CheckpointReadConsistency = "potentially_stale"
  ): Promise<DestinationCheckpointView | undefined> {
    const reply = await executeManaged(
      this.transport,
      await this.capabilities(),
      DestinationGetCommand,
      { v: CHECKPOINT_OP_VERSION, destinationId, consistency }
    )
    if (reply.kind === "destination") return reply.destination
    if (reply.kind === "err") {
      throw new QueryExecutionError(`destination read failed: ${reply.error.kind}`, reply.error)
    }
    throw new QueryExecutionError("destination read returned an unexpected reply", {})
  }

  async list(
    filter: DestinationListFilter = {},
    after?: DestinationId,
    limit = 100,
    consistency: CheckpointReadConsistency = "potentially_stale"
  ): Promise<DestinationCheckpointPage> {
    const reply = await executeManaged(
      this.transport,
      await this.capabilities(),
      DestinationListCommand,
      {
        v: CHECKPOINT_OP_VERSION,
        filter,
        ...(after === undefined ? {} : { after }),
        limit,
        consistency
      }
    )
    if (reply.kind === "destinations") return reply.page
    if (reply.kind === "err") {
      throw new QueryExecutionError(`destination list failed: ${reply.error.kind}`, reply.error)
    }
    throw new QueryExecutionError("destination list returned an unexpected reply", {})
  }

  async queryRoutes(
    nameContains?: string,
    after?: QueryRouteId,
    limit = 100,
    consistency: CheckpointReadConsistency = "potentially_stale"
  ): Promise<QueryRoutePage> {
    const reply = await executeManaged(
      this.transport,
      await this.capabilities(),
      QueryRouteListCommand,
      {
        v: CHECKPOINT_OP_VERSION,
        ...(nameContains === undefined ? {} : { nameContains }),
        ...(after === undefined ? {} : { after }),
        limit,
        consistency
      }
    )
    if (reply.kind === "query_routes") return reply.page
    if (reply.kind === "err") {
      throw new QueryExecutionError(`query route list failed: ${reply.error.kind}`, reply.error)
    }
    throw new QueryExecutionError("query route list returned an unexpected reply", {})
  }
}
