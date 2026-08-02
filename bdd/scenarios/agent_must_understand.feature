@agent @wire
Feature: The AGDX must-understand marker
  The must-understand marker is a wire-contract concern: a receiver lacking a
  required feature bit must reject the message. It is exercised by building and
  round-tripping an envelope in process, so it has no transport step and every
  SDK runner can verify the same receiver rule.

  Scenario: A must-understand marker is rejected by a receiver lacking the feature
    When I build an agent event requiring feature bits the receiver lacks
    Then the receiver rejects it as not understood

  Scenario: An event with no must-understand marker is understood
    When I build a plain agent event
    Then the receiver understands it
