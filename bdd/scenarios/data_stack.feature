@data_stack
Feature: Destination and bounded query lifecycle
  Destination state changes use compare-and-set revisions, retention gaps block
  progress until an explicit decision, and bounded query executions expose page
  cursors and terminal cancellation state.

  Background:
    Given a fresh data stack contract model

  Scenario: A destination becomes enabled through an accepted operation
    When I register destination "orders-lakehouse" at global revision 0
    Then the operation is accepted with a stable identity
    And destination "orders-lakehouse" is disabled at definition revision 1
    When I enable destination "orders-lakehouse" at global revision 1 and definition revision 1
    Then destination "orders-lakehouse" is running at definition revision 2

  Scenario: A stale destination mutation reports the observed revision
    When I register destination "orders-lakehouse" at global revision 0
    And I enable destination "orders-lakehouse" at global revision 0 and definition revision 1
    Then the destination mutation conflicts with global revision 1

  Scenario: A retention gap blocks progress until explicitly accepted
    When I register destination "orders-lakehouse" at global revision 0
    And I enable destination "orders-lakehouse" at global revision 1 and definition revision 1
    And I record a retention gap from required offset 10 to retained offset 20
    Then destination "orders-lakehouse" is blocked by the retention gap at checkpoint revision 2
    When I accept the retention gap at next offset 20 and checkpoint revision 2
    Then destination "orders-lakehouse" is running at next offset 20

  Scenario: Query paging stops after cancellation
    Given a query execution with rows "one, two, three"
    When I read a query page with limit 2
    Then the query page contains "one, two" and a continuation cursor
    When I cancel the query execution
    Then the query execution status is cancelled
    And the continuation page is rejected as cancelled
