@bindings
Feature: Projection bindings and rejected records
  A record that a destination or the extraction refuses holds its partition
  until an ordered skip names it. A binding can let the backend skip such a
  record on its own. The opt-in rides the binding's wire form, and a binding
  without it omits the field.

  Scenario: A binding holds rejected records by default
    Given a binding from stream "telemetry" topic "readings"
    When I build the binding
    Then the binding's wire form omits skip_rejected

  Scenario: A binding opts into skipping rejected records
    Given a binding from stream "telemetry" topic "readings"
    When the binding lets the backend skip rejected records
    And I build the binding
    Then the binding's wire form carries skip_rejected true
