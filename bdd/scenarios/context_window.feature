@session
Feature: The context read window
  A context read is bounded. Each partition read covers the newest 10000
  records of the partition, then keeps the records of the conversation. A turn
  that sits behind more traffic than that is outside the read in every SDK.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  Scenario: A turn behind more than one window of traffic is outside the context
    When I open the session "agent-42"
    And I append an "instruction" turn "before the traffic" to the session
    And I publish 10000 unrelated records to topic "agent.commands"
    And I append an "instruction" turn "after the traffic" to the session
    Then the session context is only "instruction:after the traffic"
