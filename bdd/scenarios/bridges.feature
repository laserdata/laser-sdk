@bridges
Feature: External protocols preserve AGDX lifecycle
  Bridge adapters translate external protocol operations onto the durable log
  without losing lifecycle, streaming, state, or loop-prevention semantics.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions
    And a new conversation

  Scenario: A repeated bridge hop is rejected
    When bridge "mcp-gateway" enters after hops "edge-gateway,a2a-gateway"
    Then the bridge hops are "edge-gateway,a2a-gateway,mcp-gateway"
    When bridge "a2a-gateway" enters the same route
    Then the bridge route is rejected as a loop

  Scenario: A cancelled A2A task replays its terminal state
    When I submit and cancel an A2A task
    Then the replayed A2A task state is "Canceled"

  Scenario: An AG-UI state delta reconstructs the latest state
    When I publish an AG-UI count snapshot of 1 and replace it with 2
    Then the reconstructed AG-UI count is 2

  Scenario: A chunked chat renders a complete AG-UI stream
    When I stream chat chunks "hello " and "world"
    Then AG-UI renders the chat lifecycle in order
