@bindings
Feature: Projection bindings and rejected records
  Invalid records hold their partition by default. A binding can send them to the dead-letter topic instead. The default policy is omitted from the wire form.

  Scenario: A binding holds rejected records by default
    Given a binding from stream "telemetry" topic "readings"
    When I build the binding
    Then the binding's wire form omits invalid_records

  Scenario: A binding opts into skipping rejected records
    Given a binding from stream "telemetry" topic "readings"
    When the binding lets the backend skip rejected records
    And I build the binding
    Then the binding's wire form carries invalid_records dead_letter

  Scenario: A binding keeps references for oversized records
    Given a binding from stream "telemetry" topic "readings"
    When the binding keeps references for oversized records
    And I build the binding
    Then the binding's wire form carries oversize reference
