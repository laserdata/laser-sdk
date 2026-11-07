@kv @managed
Feature: Fenced leases on the managed key-value store
  A lease names one holder of a coordination key. Renewal keeps its token and
  release frees it. A fenced write from a holder whose lease ended is refused,
  and a takeover read at the new grant waits for every write the earlier
  holder made. These run against the managed store, so every SDK proves the
  ownership rules on the real fold.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped on a connection-string client

  Scenario: A holder renews and releases its lease
    When "worker-a" acquires the lease "job" for 5000 ms
    And "worker-a" renews the lease "job"
    Then the renewal of "worker-a" keeps its lease token
    When "worker-a" releases the lease "job"
    Then the release reports the lease was held

  Scenario: The next holder reads the earlier holder's write and fences the stale holder out
    When "worker-a" acquires the lease "job" for 1000 ms
    And "worker-a" writes "first" to "result" fenced by its lease
    And "worker-b" acquires the lease "job" once it is free
    Then a read of "result" at the lease grant of "worker-b" returns "first"
    When "worker-a" writes "late" to "result" fenced by its lease
    Then the fenced write fails as lease lost
    When "worker-b" writes "second" to "result" fenced by its lease
    Then reading "result" returns "second"
