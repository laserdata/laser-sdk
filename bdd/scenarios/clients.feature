@clients @managed
Feature: Live connections are listed from every node
  A connection's advertised metadata lives on the node that holds it. Listing
  every connection asks each node of the cluster for its pages, so the whole
  roster is one listing without a shared metadata table.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions

  Scenario: The listing holds this client's authenticated connection
    When I list every connected client
    Then the listing holds an authenticated connection
    When I list every connected client of that user
    Then every listed connection belongs to that user
