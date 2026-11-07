@capabilities @injected
Feature: The read-consistency pre-gate under injected capabilities
  A read-your-writes query against a query surface that advertises only the
  weakest consistency must fail as unsupported, before any round-trip. Exercising
  it injects a capability set that the live server does not report, then verifies
  the same local pre-gate in every SDK runner.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  Scenario: A managed-query connection refuses an unadvertised consistency level
    Given a managed-query connection that does not advertise read-your-writes
    When I run a read-your-writes query against topic "events"
    Then the call fails as unsupported
    And the unified result code is unsupported

  Scenario Outline: A prepared mutation preserves a managed command error
    When a prepared release receives managed result code <code>
    Then the command error keeps code <code> and retryability <retryable>

    Examples:
      | code | retryable |
      | 5    | false     |
      | 18   | true      |
      | 10   | false     |
      | 2    | false     |
      | 7    | false     |
      | 42   | false     |

  Scenario Outline: A prepared mutation reports an unresolved attempt as ambiguous
    When a prepared release receives the unresolved result code <code>
    Then the release fails as an ambiguous mutation

    Examples:
      | code |
      | 12   |
      | 19   |
      | 20   |
