@capabilities @injected
Feature: The read-consistency pre-gate under injected capabilities
  A read-your-writes query against a query surface that advertises only the
  weakest consistency must fail as unsupported, before any round-trip. Exercising
  it needs injecting a capability set the live server does not report, so it runs
  only where an SDK exposes capability injection (the Rust runner, not the Python
  client).

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  Scenario: A managed-query connection refuses an unadvertised consistency level
    Given a managed-query connection that does not advertise read-your-writes
    When I run a read-your-writes query against topic "events"
    Then the call fails as unsupported
    And the unified result code is unsupported
