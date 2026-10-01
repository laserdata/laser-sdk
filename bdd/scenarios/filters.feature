@filters
Feature: Consumer filter evaluation
  The evaluation rules the streaming server and every SDK share, run locally
  with no server. A satellite fleet streams the change feed of its mission-ops
  database, and the anomaly desk only wants satellites entering safe mode.
  Comparison truth is three-valued: a missing field or a type mismatch is
  unknown, and an unknown result selects nothing.

  Scenario Outline: The strict filter needs evidence that the mode changed
    Given the "safe mode" filter
    When it evaluates the "<record>" record
    Then the record is <verdict>

    Examples:
      | record                      | verdict  |
      | mode update                 | selected |
      | mode update without changed | rejected |
      | battery update              | rejected |
      | decommission                | selected |
      | telemetry                   | selected |
      | telemetry with null mode    | rejected |
      | battery telemetry           | rejected |
      | ground station update       | rejected |

  Scenario Outline: Presence distinguishes a null value from a missing field
    Given the "mode present" filter
    When it evaluates the "<record>" record
    Then the record is <verdict>

    Examples:
      | record                   | verdict  |
      | telemetry                | selected |
      | telemetry with null mode | selected |
      | battery telemetry        | rejected |

  Scenario Outline: The values-only filter selects every update of a satellite in safe mode
    Given the "safe mode values" filter
    When it evaluates the "<record>" record
    Then the record is <verdict>

    Examples:
      | record                      | verdict  |
      | mode update                 | selected |
      | mode update without changed | selected |
      | battery update              | selected |
      | decommission                | rejected |

  Scenario: A missing field is unknown, and negation keeps it unknown
    Given the "mode is not safe" filter
    When it evaluates the "decommission" record
    Then the record is rejected

  Scenario Outline: Large integers compare exactly
    Given the "catalog number 9007199254740993" filter
    When it evaluates the "<record>" record
    Then the record is <verdict>

    Examples:
      | record               | verdict  |
      | catalogued satellite | selected |
      | neighbor satellite   | rejected |

  Scenario Outline: Timestamps compare as instants only after explicit coercion
    Given the "contact after noon UTC" filter
    When it evaluates the "<record>" record
    Then the record is <verdict>

    Examples:
      | record               | verdict  |
      | afternoon contact    | selected |
      | morning contact      | rejected |
      | contact without zone | rejected |

  Scenario Outline: A header filter decides without decoding the payload
    Given the "critical priority" filter
    When it evaluates the "opaque" record with header "priority" set to "<priority>"
    Then the record is <verdict>

    Examples:
      | priority | verdict  |
      | critical | selected |
      | routine  | rejected |

  Scenario: A payload that is not JSON is a fault
    Given the "safe mode" filter
    When it evaluates the "malformed" record
    Then the record is a fault

  Scenario: The digest identifies exactly the filter's semantics
    Given the "safe mode" filter
    Then its digest survives a round trip through its wire form
    And a different fault policy gives a different digest
