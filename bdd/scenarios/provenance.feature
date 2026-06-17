@provenance
Feature: Provenance and causality on the wire
  Every agent message carries provenance: who sent it, the conversation it
  belongs to, its causal parent, and an idempotency key. All of it survives a
  round trip through the log unchanged.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 4 partitions
    And a new conversation

  Scenario: Provenance survives a round trip
    When I send an agent command "hello" with agent "planner" and idempotency key "k1"
    And I assemble the conversation
    Then the assembled message payload is "hello"
    And the assembled message agent is "planner"
    And the assembled message idempotency key is "k1"
    And the assembled message belongs to the conversation
