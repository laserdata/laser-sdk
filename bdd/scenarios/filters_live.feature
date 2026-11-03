@filters
Feature: Consumer filters on a streaming server
  The streaming server applies the filter, so a reader receives only the
  records it selects, with their original offsets. The saved-filter catalog
  needs a managed backend.

  Background:
    Given a running data platform
    And a fresh fleet change feed

  @managed
  Scenario: A group policy returns only safe-mode records
    When the anomaly desk reads the feed with the "safe mode" filter
    Then it receives the "mode update", "telemetry", and "decommission" records

  @unmanaged
  Scenario: A server without a managed backend refuses the catalog
    When the anomaly desk lists its saved filters
    Then the catalog is refused as unsupported

  @managed
  Scenario: Saved group policies preserve retries and revision state
    When the anomaly desk manages a saved policy by numeric group id
    Then it receives the "mode update", "telemetry", and "decommission" records

  Scenario: A normal unbound group consumer receives every original record
    When the anomaly desk reads every record through an unbound consumer group
    Then it receives every original feed record

  Scenario: An advanced unbound group reader follows the advertised capability
    When the anomaly desk reads every record through an unbound group reader
    Then the advanced reader follows the group-aware read capability

  @managed
  Scenario: A bounded sparse scan continues through an empty selection
    When the anomaly desk scans a hundred non-matches before its first match
    Then its first match follows an empty scan of one hundred records

  @managed
  Scenario: A normal group consumer preserves the source batch limit
    When the anomaly desk consumes a hundred non-matches before its first match
    Then its first match follows an empty scan of one hundred records

  @managed
  Scenario: A released policy cannot acknowledge an old page
    When the anomaly desk rejects a page after releasing its policy
    Then it receives every original feed record
