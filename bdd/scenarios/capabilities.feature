@capabilities
Feature: Capability negotiation and the unsupported boundary
  At connect the SDK negotiates which surfaces are available. On open Apache
  Iggy the managed surface is absent, and every managed call returns a clean
  Unsupported error rather than a fallback or a partial result.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  Scenario: Open Apache Iggy advertises no managed capabilities
    When I read the negotiated capabilities
    Then managed query is unavailable
    And managed key-value is unavailable
    And forks are unavailable
    And the coordination features are unavailable

  Scenario: A managed query returns Unsupported on open Apache Iggy
    When I run a query against topic "events"
    Then the call fails as unsupported
    And the unified result code is unsupported

  Scenario: Compare-and-swap returns Unsupported on open Apache Iggy
    When I compare-and-swap key "lock" in namespace "coordination" expecting it absent
    Then the call fails as unsupported
    And the unified result code is unsupported

  Scenario: A read-your-writes query returns Unsupported on open Apache Iggy
    When I run a read-your-writes query against topic "events"
    Then the call fails as unsupported
    And the unified result code is unsupported
