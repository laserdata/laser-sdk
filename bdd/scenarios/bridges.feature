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

  Scenario: A bound A2A task answers only the caller that submitted it
    When caller "alice" submits a bound A2A task
    And caller "bob" tries to cancel the bound A2A task
    And an anonymous caller tries to cancel the bound A2A task
    And caller "alice" tries to cancel the bound A2A task
    Then the bound A2A cancel outcomes are "bob:refused,anonymous:refused,alice:Canceled"

  Scenario: An AG-UI state delta reconstructs the latest state
    When I publish an AG-UI count snapshot of 1 and replace it with 2
    Then the reconstructed AG-UI count is 2

  Scenario: A chunked chat renders a complete AG-UI stream
    When I stream chat chunks "hello " and "world"
    Then AG-UI renders the chat lifecycle in order

  Scenario: A long chunked chat renders from its opening chunk
    When I stream 60 chat chunks
    Then AG-UI renders one run with 60 text contents from its opening chunk

  Scenario: Addressed bridge calls and input requests reach only their addressee
    Given responder "alpha" answers every command with its own name
    And responder "beta" answers A2A tasks and input requests with its own name
    When I submit an A2A task to "beta"
    And I call the MCP tool "search" on "alpha"
    And I request input from "beta"
    Then the A2A task completes with "beta"
    And the MCP tool result is "alpha"
    And the input decision is "beta"
    And responder "alpha" answered exactly "tool"
    And responder "beta" answered exactly "task", "input"

  Scenario: A finished A2A task refuses a cancel and an unknown task is not found
    Given responders "alpha" and "beta" answer every command with their own name
    When I submit an A2A task to "beta"
    Then the A2A task completes with "beta"
    And canceling the A2A task is refused
    And an A2A task that was never submitted is not found
