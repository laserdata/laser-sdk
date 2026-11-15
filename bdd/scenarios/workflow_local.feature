@agent @workflow
Feature: Workflows of local steps
  A workflow step can run its body in the calling process instead of sending
  it to an agent. Its completion is journaled like a routed step, so a resumed
  run never runs a recorded step again, and a failure compensates the
  completed steps in reverse order. The first run on a stream without agent
  topics creates them, so a workflow needs no agent at all. A local step body
  returns "<label> done in process", and a compensation body records
  "undo <label>".

  Background:
    Given a running data platform
    And a fresh stream

  Scenario: Three local steps run in order on a stream without agent topics
    When the workflow "jobs" runs the local steps "generate", "process", "finish"
    Then the workflow completed the steps "generate", "process", "finish" in order
    And the workflow output text of step "finish" is "finish done in process"

  Scenario: A resumed run never runs a recorded local step again
    When the workflow "jobs" runs the local steps "generate", "process", "finish" and step "finish" fails once
    And the workflow "jobs" resumes the same run of local steps
    Then the local step bodies ran "generate", "process", "finish", "finish"
    And the workflow completed the steps "generate", "process", "finish" in order

  Scenario: A failed local step compensates the completed local steps in reverse order
    When the workflow "billing" runs the local steps "reserve" and "charge" with compensations and then "ship" which fails
    Then the workflow fails with the local step error "ship failed"
    And the local compensations ran "undo charge" then "undo reserve"
