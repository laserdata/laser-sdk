import type { Capabilities } from "../client/capabilities.js"
import { QueryExecutionError } from "../client/errors.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import { mintUlidValue } from "../runtime/ulid.js"
import { CHECKPOINT_OP_VERSION } from "../wire/codes.js"
import {
  type CheckpointMutationResult,
  type CheckpointReadConsistency,
  type CheckpointRequestEnvelope,
  type CompletedAttempt,
  type DestinationBlock,
  type DestinationBlockCode,
  type DestinationCheckpointPage,
  type DestinationCheckpointView,
  type DestinationListFilter,
  type PreparedAttempt,
  type PublicCheckpointMutation,
  type QueryRoutePage,
  type RepairRecord,
  type RetentionGap,
  type SupervisorActorAssertion
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
import {
  type CheckpointOwnerId,
  CheckpointRequestId,
  type DestinationId,
  type QueryRouteId
} from "../wire/ids.js"

export class Destinations {
  constructor(
    private readonly transport: ManagedTransport,
    private readonly capabilities: () => Promise<Capabilities>
  ) {}

  async mutate(
    expectedGlobalStateRevision: bigint,
    mutation: PublicCheckpointMutation,
    supervisorAssertion?: SupervisorActorAssertion
  ): Promise<CheckpointMutationResult> {
    const request: CheckpointRequestEnvelope = {
      v: CHECKPOINT_OP_VERSION,
      requestId:
        supervisorAssertion?.claims.requestId ?? CheckpointRequestId.fromU128(mintUlidValue()),
      expectedGlobalStateRevision,
      mutation,
      ...(supervisorAssertion === undefined ? {} : { supervisorAssertion })
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

  bindTable(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    expectedDefinitionRevision: bigint,
    tableUuid: Uint8Array
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "bind_table",
      destinationId,
      destinationGeneration,
      expectedDefinitionRevision,
      tableUuid
    })
  }

  addPartition(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    expectedCheckpointRevision: bigint,
    partitionId: number
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "add_partition",
      destinationId,
      destinationGeneration,
      expectedCheckpointRevision,
      partitionId
    })
  }

  observePartitionLifecycle(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    expectedCheckpointRevision: bigint,
    partitionId: number
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "observe_partition_lifecycle",
      destinationId,
      destinationGeneration,
      expectedCheckpointRevision,
      partitionId
    })
  }

  acquireLease(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    owner: CheckpointOwnerId,
    expectedLeaseSequence: bigint,
    leaseDurationMicros: bigint
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "acquire_lease",
      destinationId,
      destinationGeneration,
      owner,
      expectedLeaseSequence,
      leaseDurationMicros
    })
  }

  renewLease(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    owner: CheckpointOwnerId,
    epoch: bigint,
    expectedLeaseSequence: bigint,
    leaseDurationMicros: bigint
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "renew_lease",
      destinationId,
      destinationGeneration,
      owner,
      epoch,
      expectedLeaseSequence,
      leaseDurationMicros
    })
  }

  takeOverLease(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    owner: CheckpointOwnerId,
    expectedLeaseSequence: bigint,
    leaseDurationMicros: bigint
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "takeover_lease",
      destinationId,
      destinationGeneration,
      owner,
      expectedLeaseSequence,
      leaseDurationMicros
    })
  }

  prepare(
    expectedGlobalStateRevision: bigint,
    expectedCheckpointRevision: bigint,
    attempt: PreparedAttempt
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "prepare",
      expectedCheckpointRevision,
      attempt
    })
  }

  complete(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    owner: CheckpointOwnerId,
    epoch: bigint,
    expectedCheckpointRevision: bigint,
    completion: CompletedAttempt
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "complete",
      destinationId,
      destinationGeneration,
      owner,
      epoch,
      expectedCheckpointRevision,
      completion
    })
  }

  recordBlock(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    expectedCheckpointRevision: bigint,
    block: DestinationBlock
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "record_block",
      destinationId,
      destinationGeneration,
      expectedCheckpointRevision,
      block
    })
  }

  clearBlock(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    expectedCheckpointRevision: bigint,
    expectedCode: DestinationBlockCode
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "clear_block",
      destinationId,
      destinationGeneration,
      expectedCheckpointRevision,
      expectedCode
    })
  }

  recordRetentionGap(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    expectedCheckpointRevision: bigint,
    gap: RetentionGap
  ): Promise<CheckpointMutationResult> {
    return this.mutate(expectedGlobalStateRevision, {
      kind: "record_retention_gap",
      destinationId,
      destinationGeneration,
      expectedCheckpointRevision,
      gap
    })
  }

  acceptRetentionGap(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    expectedCheckpointRevision: bigint,
    nextOffset: bigint,
    supervisorAssertion: SupervisorActorAssertion
  ): Promise<CheckpointMutationResult> {
    return this.mutate(
      expectedGlobalStateRevision,
      {
        kind: "accept_retention_gap",
        destinationId,
        destinationGeneration,
        expectedCheckpointRevision,
        nextOffset
      },
      supervisorAssertion
    )
  }

  supersedeGeneration(
    expectedGlobalStateRevision: bigint,
    expectedDefinitionRevision: bigint,
    replacement: MaterializationDestination,
    supervisorAssertion: SupervisorActorAssertion
  ): Promise<CheckpointMutationResult> {
    return this.mutate(
      expectedGlobalStateRevision,
      { kind: "supersede_generation", expectedDefinitionRevision, replacement },
      supervisorAssertion
    )
  }

  recordRepair(
    expectedGlobalStateRevision: bigint,
    destinationId: DestinationId,
    destinationGeneration: bigint,
    expectedCheckpointRevision: bigint,
    repair: RepairRecord,
    supervisorAssertion: SupervisorActorAssertion
  ): Promise<CheckpointMutationResult> {
    return this.mutate(
      expectedGlobalStateRevision,
      {
        kind: "record_repair",
        destinationId,
        destinationGeneration,
        expectedCheckpointRevision,
        repair
      },
      supervisorAssertion
    )
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
    consistency: CheckpointReadConsistency
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
    consistency: CheckpointReadConsistency,
    filter: DestinationListFilter = {},
    after?: DestinationId,
    limit = 100
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
    consistency: CheckpointReadConsistency,
    nameContains?: string,
    after?: QueryRouteId,
    limit = 100
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
