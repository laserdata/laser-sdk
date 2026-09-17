@session
Feature: Agent sessions over the log
  A session is one conversation seen as typed turns. Each turn kind rides
  exactly one conversation-level agent topic, so a session-recorded turn is an
  ordinary agent message and reads back with its kind in every SDK.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 4 partitions

  Scenario: Typed turns read back as one context with their kinds
    When I open the session "agent-42"
    And I append an "instruction" turn "summarize the ticket" to the session
    And I append a "tool.result" turn "3 comments found" to the session
    And I append a "model.response" turn "it is a login bug" to the session
    Then the session context is "instruction:summarize the ticket", "tool.result:3 comments found", "model.response:it is a login bug" in order

  Scenario: Opening a session by the same id reaches the same conversation
    When I open the session "agent-42"
    Then opening the session "agent-42" again reaches the same conversation
    And opening the session "agent-43" reaches a different conversation

  Scenario: A checkpoint splits the history into before and after
    When I open the session "agent-42"
    And I append an "instruction" turn "first" to the session
    And I take a session checkpoint after 1 turn
    And I append a "tool.result" turn "second" to the session
    Then the turns since the checkpoint are "second"
    And the turns at the checkpoint are "first"
