@agent @workflow
Feature: Durable workflows over agent steps
  A workflow runs dependency-ordered steps, each one a command to an agent, and
  journals every completion. It verifies replies, compensates the completed
  steps in reverse order when a later step fails, and routes a step to a named
  agent or to an agent that advertises a skill. The outcome names the run's end
  status and the steps it completed, in order. A step body is the step label,
  an answering agent replies "<body> done by <agent>", and a compensation body
  is "undo <label>".

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with the defaults
    And agent "worker" answers workflow steps on the session topic

  Scenario: A multi-step run completes its steps in dependency order
    When the workflow "pipeline" runs "fetch", then "parse" after "fetch", then "store" after "parse" on "worker"
    Then the workflow completed the steps "fetch", "parse", "store" in order
    And the workflow output of step "store" is "store done by worker"

  Scenario: A verifier that rejects the reply fails the step as unverified
    When the workflow "audit" runs the step "check" on "worker" with a verifier that rejects every reply
    Then the workflow fails at step "check" with reason "unverified"

  Scenario: A failed step compensates the completed steps in reverse order
    Given agent "breaker" fails every workflow step on the session topic
    When the workflow "billing" runs "reserve" and "charge" on "worker" and then "ship" on "breaker", each with a compensation
    Then the workflow fails at step "ship" with reason "failed"
    And agent "worker" received the compensations "undo charge" then "undo reserve"

  Scenario: A step routed by skill reaches the agent that advertises it
    Given agent "classifier" advertising the skill "classify" answers workflow steps on the session topic
    When the workflow "routing" runs the step "label" on an agent with the skill "classify"
    Then the workflow completed the steps "label" in order
    And the workflow output of step "label" is "label done by classifier"

  Scenario: A step whose target never answers fails naming the target and the deadline
    When the workflow "silent" runs the step "wait" on "nobody" with a deadline of 500 milliseconds
    Then the workflow fails at step "wait" with reason "timed_out"
    And the failure names the target "nobody" and a deadline of 500 milliseconds
