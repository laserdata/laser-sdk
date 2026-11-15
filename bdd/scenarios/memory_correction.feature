@memory_log
Feature: Memory correction and recall on log memory
  Log memory keeps every fact remembered for a user. The first write creates
  the memory topic, a correction supersedes the old fact in one call for the
  user in every conversation, a semantic recall ranks facts by word overlap and
  drops none, and a keyword recall keeps only the facts that share a word. On a
  managed deployment a default recall right after a correction reads it.
  Conversation labels name a fresh conversation each, and every fact here is
  remembered in memory "prefs" for the user "ana".

  Background:
    Given a running data platform
    And a fresh stream

  Scenario: The first write creates the memory topic
    When the user states "the user prefers green tea"
    Then the memory topic exists on the stream

  Scenario: Supersede replaces a fact for the user in every conversation
    When the user states "the user is allergic to peanuts" in conversation "first"
    And the user corrects it to "the user is allergic to shellfish" in conversation "second"
    Then a folded recall for the user returns "the user is allergic to shellfish" only

  Scenario: Semantic recall ranks facts by word overlap and drops none
    When the user states "the user is allergic to peanuts"
    And the user states "green tea every morning"
    And the user states "the user lives in Lisbon"
    Then a folded semantic recall for "which city does the user live in" returns "the user lives in Lisbon", "the user is allergic to peanuts", "green tea every morning"

  Scenario: Keyword recall keeps only the facts that share a word
    When the user states "the user is allergic to peanuts"
    And the user states "the user lives in Lisbon"
    Then a folded keyword recall for "Lisbon" returns "the user lives in Lisbon" only

  @managed
  Scenario: A default recall right after a correction reads the correction
    When the user states "the user is allergic to peanuts" in conversation "first"
    And the user corrects it to "the user is allergic to shellfish" in conversation "second"
    Then a recall for the user returns "the user is allergic to shellfish" only
