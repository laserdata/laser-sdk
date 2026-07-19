@governance
Feature: Action governance at the effect boundary
  A pre-effect policy hook decides before the SDK publishes. Enforce mode
  applies the verdict before the effect, observe mode records what enforcement
  would have done and lets the action through, and every non-allow decision
  leaves tamper-evident evidence on the audit topic.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 4 partitions
    And a new conversation

  Scenario: A blocked command never reaches the log
    Given the laser is governed by a policy that blocks "wire-funds" in "enforce" mode
    When I send a governed agent command "wire-funds"
    Then the send is rejected by policy
    And the audit topic records a "block" decision with outcome "blocked"

  Scenario: A blocked business publish never reaches the log
    Given the laser is governed by a policy that blocks "wire-funds" in "enforce" mode
    When I publish a governed business record "wire-funds"
    Then the send is rejected by policy
    And the audit topic records a "block" decision with outcome "blocked"

  Scenario: Observe mode records the decision and lets the send through
    Given the laser is governed by a policy that blocks "wire-funds" in "observe" mode
    When I send a governed agent command "wire-funds"
    And I assemble the conversation
    Then the assembled message payload is "wire-funds"
    And the audit topic records a "block" decision with outcome "effected"
