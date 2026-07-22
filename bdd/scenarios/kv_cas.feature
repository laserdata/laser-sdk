@kv
Feature: Key-value compare-and-swap semantics
  Compare-and-swap over the key-value store, served here by the reference
  in-memory engine (no Iggy, no transport). It pins the race semantics every
  backend and every SDK must reproduce: create-if-absent, match-a-version,
  conflict reporting, version monotonicity, and expiry read as absence. This is
  the cross-SDK CAS contract.

  Scenario: Create-if-absent commits version one on an empty key
    Given an empty KV store
    When I create "lock" with "held" if absent
    Then the swap commits version 1

  Scenario: Create-if-absent conflicts on a present key
    Given an empty KV store
    And key "lock" holds "held"
    When I create "lock" with "stolen" if absent
    Then the swap conflicts with current version 1

  Scenario: A matching version commits and bumps the version
    Given an empty KV store
    And key "counter" holds "1"
    When I swap "counter" to "2" expecting version 1
    Then the swap commits version 2

  Scenario: A stale version token conflicts with the live version
    Given an empty KV store
    And key "counter" holds "1"
    When I swap "counter" to "2" expecting version 1
    Then the swap commits version 2
    When I swap "counter" to "3" expecting version 1
    Then the swap conflicts with current version 2

  Scenario: An expired key counts as absent for create-if-absent
    Given an empty KV store
    And key "session" holds "a" expiring at 1010
    When I create "session" with "b" if absent at 1020
    Then the swap commits version 1

  Scenario: Matching a version on an expired key conflicts with no current version
    Given an empty KV store
    And key "session" holds "a" expiring at 1010
    When I swap "session" to "b" expecting version 1 at 1020
    Then the swap conflicts because the key is absent
