@runs
Feature: The managed run registry and the unsupported boundary
  The run registry is a managed read model over the log: submit records
  intent, transitions fold from the status records a registered run stamps,
  cancel records an intent flag. On open Apache Iggy the registry is not
  advertised and every run verb returns a clean Unsupported error.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  Scenario: The run registry is not advertised on open Apache Iggy
    Then the run registry is unavailable

  Scenario: Submitting a run returns Unsupported on open Apache Iggy
    When I submit a run to agent "diagnoser"
    Then the call fails as unsupported
    And the unified result code is unsupported

  Scenario: Reading a run's status returns Unsupported on open Apache Iggy
    When I read the status of run "run-7"
    Then the call fails as unsupported
    And the unified result code is unsupported

  Scenario: Cancelling a run returns Unsupported on open Apache Iggy
    When I cancel run "run-7"
    Then the call fails as unsupported
    And the unified result code is unsupported

  Scenario: Listing runs returns Unsupported on open Apache Iggy
    When I list runs
    Then the call fails as unsupported
    And the unified result code is unsupported
