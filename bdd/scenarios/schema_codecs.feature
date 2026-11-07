@schema_codecs
Feature: Writer schema payload integrity
  A writer schema accepts one complete payload and rejects malformed bytes.

  Scenario Outline: Decode exactly one writer payload
    Given a "<codec>" writer schema
    When I decode schema payload "<payload>"
    Then the schema payload is <outcome>

    Examples:
      | codec       | payload           | outcome  |
      | avro_long   | [14]              | accepted |
      | avro_long   | [14,0]            | rejected |
      | json_string | [34,111,107,34]   | accepted |
      | json_string | [34,255,34]       | rejected |

  Scenario: Every client decodes the shared writer schema corpus into one model
    When I decode every case of the shared writer schema corpus
    Then every corpus case matches its expected value
