@agent
Feature: The AGDX agent envelope on the log
  The Agent Data Exchange Protocol rides the durable log. Messages sharing a
  conversation thread together and replay in order from offset 0, so a consumer
  reconstructs the exact exchange that happened.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 4 partitions
    And a new conversation

  Scenario: Messages thread under one conversation and replay in order
    When I send agent commands "one", "two", "three"
    And I assemble the conversation
    Then the assembled payloads are "one", "two", "three" in order

  Scenario: A different conversation is isolated
    When I send an agent command "first" with agent "planner" and idempotency key "a"
    And I start another conversation
    And I send an agent command "second" with agent "planner" and idempotency key "b"
    And I assemble the conversation
    Then the assembled message payload is "second"

  Scenario: A typed AGDX command rides the log
    When I publish an AGDX command "ping" via the typed producer
    And I assemble the conversation
    Then the AGDX command body is "ping"

  Scenario: A must-understand marker is rejected by a receiver lacking the feature
    When I build an agent event requiring feature bits the receiver lacks
    Then the receiver rejects it as not understood

  Scenario: An event with no must-understand marker is understood
    When I build a plain agent event
    Then the receiver understands it
