@agent @workflow
Feature: Workflows of plain functions
  A workflow can be a function that calls its steps in ordinary code. Each step
  call is journaled under the run, so a resumed run returns the recorded value
  without running the step again. A step function returns "<label> done", and
  the workflow function returns the value of its last step.

  Background:
    Given a running data platform
    And a fresh stream

  Scenario: Three steps run in order and the outcome holds the workflow's value
    When the workflow function "telemetry" calls the steps "ingest", "analyze", "report"
    Then the workflow completed the steps "ingest", "analyze", "report" in order
    And the workflow function returned "report done"

  Scenario: A resumed run does not run a recorded step again
    When the workflow function "telemetry" calls the steps "ingest", "analyze", "report" and step "report" fails once
    And the workflow function "telemetry" resumes the same run
    Then the local step bodies ran "ingest", "analyze", "report", "report"
    And the workflow completed the steps "ingest", "analyze", "report" in order
    And the workflow function returned "report done"

  Scenario: A step that fails fails the run and keeps what completed
    When the workflow function "telemetry" calls the steps "ingest", "analyze", "report" and step "analyze" fails once
    Then the workflow fails with the local step error "analyze failed"
    When the workflow function "telemetry" resumes the same run
    Then the local step bodies ran "ingest", "analyze", "analyze", "report"
    And the workflow completed the steps "ingest", "analyze", "report" in order

  Scenario: A step called twice is journaled under its own label
    When the workflow function "telemetry" calls the step "sample" twice and then "report"
    Then the workflow completed the steps "sample", "sample#2", "report" in order
    And the local step bodies ran "sample", "sample", "report"
