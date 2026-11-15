@streaming
Feature: Stream administration
  Every client inspects and removes streams, topics, partitions and consumer
  groups through the SDK alone, against open Apache Iggy. The get and exists
  calls read a missing stream or topic as absent. Listing the topics,
  partitions or consumer groups of a missing stream or topic fails as not
  found.

  Background:
    Given a running data platform
    And a fresh stream

  Scenario: Create, list, inspect and delete a unique stream
    When I create the unique stream with topic "metrics" of 2 partitions
    Then the stream listing includes the unique stream
    And the unique stream exists with 1 topic
    And the unique stream lists topic "metrics" with 2 partitions
    When I delete the unique stream
    Then the unique stream no longer exists
    And the stream listing omits the unique stream

  Scenario: Add partitions to a topic and delete it
    When I create the unique stream with topic "hosts" of 1 partitions
    And I add 2 partitions to topic "hosts"
    Then topic "hosts" has 3 partitions
    When I delete topic "hosts"
    Then topic "hosts" no longer exists
    And a second delete of topic "hosts" reports it missing
    When I delete the unique stream
    Then the unique stream no longer exists

  Scenario: List and delete a consumer group
    When I create the unique stream with topic "jobs" of 1 partitions
    And I create consumer group "workers" on topic "jobs"
    Then topic "jobs" lists consumer group "workers"
    When I delete consumer group "workers" on topic "jobs"
    Then topic "jobs" lists no consumer groups
    When I delete the unique stream
    Then the unique stream no longer exists
