@capabilities
Feature: Capability negotiation and the unsupported boundary
  At connect the SDK negotiates which surfaces are available. On open Apache
  Iggy the managed surface is absent, and every managed call returns a clean
  Unsupported error rather than a fallback or a partial result. A call the
  client can refuse on its own fails before any round trip.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  @unmanaged
  Scenario: Open Apache Iggy advertises no managed capabilities
    When I read the negotiated capabilities
    Then managed query is unavailable
    And managed key-value is unavailable
    And forks are unavailable
    And the coordination features are unavailable

  @unmanaged
  Scenario: A managed query returns Unsupported on open Apache Iggy
    When I run a query against topic "events"
    Then the call fails as unsupported
    And the unified result code is unsupported

  @unmanaged
  Scenario: Standard streaming still works after an unsupported managed query
    When I run a query against topic "events"
    Then the call fails as unsupported
    And the unified result code is unsupported
    When I publish a JSON event to topic "events"
    Then the publish succeeds

  @unmanaged
  Scenario: Compare-and-swap returns Unsupported on open Apache Iggy
    When I compare-and-swap key "lock" in namespace "coordination" expecting it absent
    Then the call fails as unsupported
    And the unified result code is unsupported

  @unmanaged
  Scenario: A read-your-writes query returns Unsupported on open Apache Iggy
    When I run a read-your-writes query against topic "events"
    Then the call fails as unsupported
    And the unified result code is unsupported

  @unmanaged
  Scenario Outline: Every managed surface returns Unsupported on open Apache Iggy
    When I call the managed <surface> surface
    Then the call fails as unsupported
    And the unified result code is unsupported

    Examples:
      | surface                 |
      | projection registration |
      | materialization         |
      | schema registration     |
      | graph upsert            |
      | fork creation           |
      | change feed             |
      | lease acquisition       |
      | session read            |
      | batch                   |
