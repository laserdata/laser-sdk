@materialization
Feature: Materialization readiness and complete records
  Materialization preserves record bodies and reports progress against a captured source frontier.

  Scenario: Catch-up is measured against the captured head
    Given a realized binding with captured head 5 and applied offset 1
    When the binding advances to offset 5 while its source head grows to 100
    Then the binding has reached the captured head

  Scenario: Unknown history cannot prove completeness
    Given a realized binding with captured head 5 and applied offset 1
    When the binding has no known first offset
    Then the binding cannot claim complete history

  Scenario: History that expired before the binding read it is not complete
    Given a realized binding with captured head 5 and applied offset 5
    When another partition lost its records before the binding read them
    Then the binding cannot claim complete history

  Scenario: Only a permanent failure class fails the binding
    Given a pending binding whose table creation failed with class "schema"
    Then the binding has failed for good
    When its failure class becomes "transient"
    Then the binding has not failed for good

  @managed
  Scenario: Repeated setup preserves every arbitrary record once
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions
    When I materialize whole records twice under one operation identity
    And I publish two arbitrary records to the materialized topic
    Then materialization reaches the captured source head
    And the materialized query returns both original bodies exactly once

  @managed
  Scenario: Keyed state replaces and deletes one key
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions
    When I materialize keyed records
    And I publish keyed value "first"
    And I publish keyed value "latest"
    Then the keyed view contains only "latest"
    When I delete the materialized key
    Then the keyed view is empty
    When I publish keyed value "recreated"
    Then the keyed view contains only "recreated"

  @managed
  Scenario: Keyed state without a delete header never deletes a key
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions
    When I materialize keyed records without a delete header
    And I publish keyed value "first"
    And I publish keyed value "kept" with a "delete" header
    Then the keyed view contains only "kept"
