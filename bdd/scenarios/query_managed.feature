@query @managed
Feature: Queries over a materialized topic at volume
  Two thousand JSON records published through the SDK, materialized by the
  managed backend, and read back through each SDK's own query builder.
  Filters, numeric ordering, totals, grouped counts and an offset walk must
  agree with what was published, at a size that spans many pages. The
  reference-engine query feature pins the same semantics without a server.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions
    And 2000 api-call records published and materialized into "api_calls"

  Scenario: A comparison filter counts every match
    When the materialized "api_calls" is queried for latency_ms greater than 1499 with its total
    Then the materialized total is 500
    And every materialized row has latency_ms greater than 1499

  Scenario: Ordering is numeric across the whole index
    When the materialized "api_calls" is queried ordered by latency_ms descending with limit 3
    Then the materialized latency_ms values are "1999, 1998, 1997" in order

  Scenario: A limit caps the page while the total counts every row
    When the materialized "api_calls" is queried with limit 25 and its total
    Then the materialized page holds 25 rows
    And the materialized total is 2000

  Scenario: A grouped count counts each status
    When the materialized "api_calls" is counted grouped by status
    Then the materialized group "200" has count 1500
    And the materialized group "500" has count 500

  Scenario: An offset walk returns every row once
    When the materialized "api_calls" is walked in pages of 250 ordered by latency_ms
    Then the walk returns every latency_ms from 0 to 1999 once

  Scenario: A whole read binds every row to its fields over one frontier
    When the materialized "api_calls" is read whole in pages of 500 ordered by latency_ms
    Then the walk returns every latency_ms from 0 to 1999 once
    And the whole read reports its frontier unmoved
