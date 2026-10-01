@filters
Feature: Consumer filters on a streaming server
  The streaming server applies the filter, so a reader receives only the
  records it selects, with their original offsets. The saved-filter catalog
  needs a managed plane.

  Background:
    Given a running data platform
    And a fresh fleet change feed

  Scenario: An inline filtered read returns only safe-mode records
    When the anomaly desk reads the feed with the "safe mode" filter
    Then it receives the "mode update", "telemetry", and "decommission" records

  @no_plane
  Scenario: A server without a managed plane refuses the catalog
    When the anomaly desk lists its saved filters
    Then the catalog is refused as unsupported

  @plane
  Scenario: Saved group policies preserve retries and revision state
    When the anomaly desk manages a saved policy by numeric group id
    Then it receives the "mode update", "telemetry", and "decommission" records
