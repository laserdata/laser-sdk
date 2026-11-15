@streaming
Feature: Scratch streams
  A scratch stream gives one program run its own uniquely named stream and
  removes it at the end, so repeated runs never collide and never leave data
  behind. The handle it returns uses the scratch stream as its default stream.

  Background:
    Given a running data platform
    And a fresh stream

  Scenario: A scratch stream exists under a unique name until it is deleted
    When I open a scratch stream with the prefix "bench"
    Then the scratch stream is named "bench-" and 8 lowercase hex characters
    And the scratch stream exists
    And the scratch handle's default stream is the scratch stream
    When I delete the scratch stream
    Then the scratch stream no longer exists
    And deleting the scratch stream again succeeds

  Scenario: Two scratch streams with one prefix get different names
    When I open two scratch streams with the prefix "bench"
    Then the two scratch streams have different names
