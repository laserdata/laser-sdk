@capabilities
Feature: Capability negotiation and the unsupported boundary
  At connect the SDK negotiates which surfaces are available. On open Apache
  Iggy the managed surface is absent, and every managed call returns a clean
  Unsupported error rather than a fallback or a partial result. A call the
  client can refuse on its own fails before any round trip.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  @no_plane
  Scenario: Open Apache Iggy advertises no managed capabilities
    When I read the negotiated capabilities
    Then managed query is unavailable
    And managed key-value is unavailable
    And forks are unavailable
    And the coordination features are unavailable

  @no_plane
  Scenario: A managed query returns Unsupported on open Apache Iggy
    When I run a query against topic "events"
    Then the call fails as unsupported
    And the unified result code is unsupported

  @no_plane
  Scenario: Compare-and-swap returns Unsupported on open Apache Iggy
    When I compare-and-swap key "lock" in namespace "coordination" expecting it absent
    Then the call fails as unsupported
    And the unified result code is unsupported

  @no_plane
  Scenario: A read-your-writes query returns Unsupported on open Apache Iggy
    When I run a read-your-writes query against topic "events"
    Then the call fails as unsupported
    And the unified result code is unsupported

  Scenario: A key-value set that carries a precondition must use commit
    When I send a set of key "service:auth" in namespace "config" expecting version 1 without commit
    Then the call fails as invalid
    And the unified result code is invalid argument
