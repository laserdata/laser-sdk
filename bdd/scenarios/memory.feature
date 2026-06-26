@memory
Feature: Agentic memory recall semantics
  Agent memory served here by the reference engine (no Iggy, no transport). It
  pins the recall contract every backend and every SDK must reproduce: content-
  addressed dedup, recency order, feedback re-ranking, and forget. This is the
  cross-SDK memory contract.

  Scenario: Remembering the same fact twice with dedup stores one item
    Given an empty memory store
    When I remember "the budget is 5000" with dedup
    And I remember "the budget is 5000" with dedup
    Then the memory holds 1 item

  Scenario: Remembering the same fact without dedup stores two items
    Given an empty memory store
    When I remember "the budget is 5000"
    And I remember "the budget is 5000"
    Then the memory holds 2 items

  Scenario: Recent recall returns the most recent first
    Given an empty memory store
    When I remember "first"
    And I remember "second"
    Then recalling 2 items returns "second" then "first"

  Scenario: Positive feedback promotes an item in recall
    Given an empty memory store
    When I remember "cat"
    And I remember "dog"
    And I give "cat" a feedback weight of 5
    Then recalling 2 items returns "cat" then "dog"

  Scenario: Forgetting an item removes it from recall
    Given an empty memory store
    When I remember "keep"
    And I remember "drop"
    And I forget "drop"
    Then the memory holds 1 item
    And recalling 2 items returns "keep"
