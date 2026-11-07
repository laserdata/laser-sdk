@schema_registry @managed
Feature: Writer schema registries per stream
  Each stream keeps its own writer schema registry, keyed by stream and id. Two streams number their schemas independently, and an id read in one stream never names another stream's schema.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  Scenario: Two streams number their writer schemas independently
    When the Avro writer schema "Order" is registered in the stream
    And the Avro writer schema "Payment" is registered in a second stream
    Then both registrations answer schema id 1
    And each stream reads schema 1 as its own writer schema
    And each stream lists only its own writer schema
