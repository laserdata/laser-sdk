@consistency @plane
Feature: Managed reads keep up with the client's own writes
  Every managed write reply carries the position its fold applied. The client
  keeps the highest position per surface and sends it with each later read of
  that surface, so the node that answers has applied at least the client's own
  writes, and a read never goes back behind an earlier one.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  Scenario: A key-value write gives the client a position its reads carry
    When I write "v1" to the managed key "profile" in namespace "consistency"
    Then the client has seen a key-value position
    And the managed key "profile" in namespace "consistency" reads "v1"

  Scenario: A later write moves the client's position forward
    When I write "v1" to the managed key "profile" in namespace "consistency"
    And I note the client's key-value position
    And I write "v2" to the managed key "profile" in namespace "consistency"
    Then the client's key-value position moved past the noted one
    And the managed key "profile" in namespace "consistency" reads "v2"
