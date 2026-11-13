@agent
Feature: Warm duplicate suppression across a restart
  An agent started with warm dedup refills its duplicate window from the
  records its group already committed. A command copy dropped past its
  deadline never ran, so a resend under its key runs after a restart. A
  copy that ran still suppresses a resend under its key, unless the policy
  leaves out every copy whose deadline has passed.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 1 partitions
    And a new conversation

  Scenario Outline: A restarted warm-dedup worker refills its window by its policy
    Given a warm-dedup worker "warm-worker" with policy "<policy>"
    When the worker is sent a command under key "ran" due in 2 seconds
    And the worker is sent a command under key "expired" already past its deadline
    And the worker is sent a command under key "plain" without a deadline
    Then the worker handled the keys "ran, plain"
    When the worker stops and the deadline of key "ran" passes
    And resends arrive under the keys "expired, ran, plain, expired" and then "fresh"
    And the worker restarts
    Then the restarted worker handled the keys "<handled>"

    Examples:
      | policy       | handled             |
      | dead_letters | expired, fresh      |
      | unexpired    | expired, ran, fresh |
      | all          | fresh               |
