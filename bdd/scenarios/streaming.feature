@streaming
Feature: Streaming core
  Typed publish onto a durable log, against open Apache Iggy. The log is the
  source of truth, and every other surface is a read model on it.

  Background:
    Given a running data platform
    And a fresh stream

  Scenario: Bootstrap a stream and publish a typed event
    When I bootstrap the stream with 4 partitions
    Then the stream is ready
    When I publish a JSON event to topic "events"
    Then the publish succeeds

  Scenario: Publish many events in one batch
    When I bootstrap the stream with 2 partitions
    And I publish a batch of 10 JSON events to topic "events"
    Then all 10 events are published

  Scenario: Publish to several topics on one connection
    When I bootstrap the stream with 2 partitions
    And I publish a JSON event to topic "orders"
    And I publish a JSON event to topic "shipments"
    Then the publish succeeds
