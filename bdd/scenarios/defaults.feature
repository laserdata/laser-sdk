@agent
Feature: Defaults and next-step errors
  A first program needs the fewest calls: bootstrap takes no arguments, and
  every error such a program meets names the call that fixes it, in each
  client's own spelling.

  Background:
    Given a running data platform

  Scenario: Bootstrap with no arguments creates the agent topics with the defaults
    Given a fresh stream
    When I bootstrap the stream with the defaults
    Then topic "agent.sessions" has 1 partition and expires records after 86400 seconds

  Scenario: A publish without a default stream names the call that sets one
    Given a client without a default stream
    When I publish "hello" to topic "orders" of the default stream
    Then the error names the call that sets a default stream

  Scenario: A workflow step to an agent without an inbox names both fixes
    Given a fresh stream bootstrapped with the defaults
    When the workflow "lost" runs the step "ping" on "ghost" through the advertised inbox route
    Then the error names the agent capabilities and the fixed inbox route
