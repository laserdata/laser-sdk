@graph
Feature: Knowledge graph traversal semantics
  The knowledge graph served here by the reference engine (no Iggy, no
  transport). It pins the contract every backend and every SDK must reproduce:
  content-addressed node convergence (the same entity from two observations is one
  node) and bounded directional traversal. This is the cross-SDK graph contract.

  Scenario: The same entity from two observations converges on one node
    Given an empty graph
    When I observe "Alice" works_at "Acme"
    And I observe "Bob" works_at "Acme"
    Then the graph holds 3 nodes

  Scenario: A two-hop traversal reaches the far node
    Given an empty graph
    When I observe "Alice" works_at "Acme"
    And I observe "Acme" located_in "Berlin"
    Then traversing from "Alice" out "works_at" then "located_in" reaches "Berlin"

  Scenario: A traversal respects the edge type
    Given an empty graph
    When I observe "Alice" works_at "Acme"
    And I observe "Alice" lives_in "Berlin"
    Then traversing from "Alice" out "works_at" reaches "Acme"
    And traversing from "Alice" out "works_at" does not reach "Berlin"

  Scenario: An incoming traversal walks edges backward
    Given an empty graph
    When I observe "Alice" works_at "Acme"
    Then traversing from "Acme" incoming "works_at" reaches "Alice"
