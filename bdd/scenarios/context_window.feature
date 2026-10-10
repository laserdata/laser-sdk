@session
Feature: The context read window
  A context read is bounded. Each partition read covers the newest 10000
  records of the partition, then keeps the records of the conversation. A
  record that sits behind more traffic than that is outside the read in every
  SDK.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  Scenario: A record behind more than one window of traffic is outside the context
    When agent "planner" starts the session "agent-42"
    And the session records the message "before the traffic"
    And I publish 10000 unrelated records to topic "agent.sessions"
    And the session records the message "after the traffic"
    Then the session context holds only the message "after the traffic"
