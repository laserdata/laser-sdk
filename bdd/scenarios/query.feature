@query
Feature: Query semantics over a materialized index
  The query DSL run against a materialized read model, served here by the
  reference in-memory engine (no Iggy, no transport). It mirrors LaserData
  Cloud, so every SDK and the managed backend must return the same rows for the
  same query. This is the cross-SDK query-semantics contract.

  Background:
    Given a query index "api_calls" seeded with sample api-call rows

  Scenario: A comparison filter returns only matching rows
    When I query "api_calls" for latency_ms greater than 500
    Then the query returns 2 rows
    And every returned row has latency_ms greater than 500

  Scenario: Ordering is numeric, not lexical
    When I query "api_calls" ordered by latency_ms descending
    Then the returned latency_ms values are "900, 550, 30, 10" in order

  Scenario: A limit caps the page while the total counts every match
    When I query "api_calls" with limit 2
    Then the query returns 2 rows
    And the page total is 4

  Scenario: A count aggregate counts each group
    When I count "api_calls" grouped by status
    Then group "200" has count 3
    And group "500" has count 1
