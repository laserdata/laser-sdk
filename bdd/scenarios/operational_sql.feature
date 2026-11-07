@materialization @operational_sql @managed
Feature: Authorized operational SQL
  SQL reads materialized indexes through the same managed query authority as structured queries.

  Scenario: Read an index and refuse private relations
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions
    When I materialize whole records twice under one operation identity
    And I publish two arbitrary records to the materialized topic
    Then materialization reaches the captured source head
    And operational SQL counts the two materialized records
    And operational SQL refuses an internal relation and an unserved dialect

  @remote_sql
  Scenario: Materialize onto a declared SQL backend and read it in its dialect
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions
    And a declared SQL backend
    When I materialize whole records on the declared SQL backend
    And I publish two arbitrary records to the remote materialized topic
    Then materialization reaches the captured source head
    And the remote materialized query returns both original bodies exactly once
    And operational SQL in the backend dialect counts the two remote records
    And operational SQL refuses internal and catalog relations of the declared backend
