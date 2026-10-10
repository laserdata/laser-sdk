@session
Feature: Agent sessions over the log
  A session is one conversation on the agent.sessions lane. Its lifecycle,
  model and tool calls, and state are typed records on that lane, so every
  SDK writes and reads them the same way on open Apache Iggy.

  Background:
    Given a running data platform
    And a fresh stream bootstrapped with 4 partitions

  Scenario: A session writes its start and its end on the lane
    When agent "planner" starts the session "incident"
    And the session ends
    Then the session lane shows "session.started", "session.completed"

  Scenario: A second end repeats the latched state and a different verb is refused
    When agent "planner" starts the session "incident"
    And the session ends
    And the session ends
    And I cancel the session
    Then the call fails as invalid
    And the session lane shows "session.started", "session.completed", "session.completed"

  Scenario: Work that fails ends the session as failed
    When agent "planner" runs the session "incident" with work that fails with "provider exploded"
    Then the session lane shows "session.started", "session.failed"
    And the session failure message is "provider exploded"

  Scenario: A canceled session ends as canceled
    When agent "planner" starts the session "incident"
    And I cancel the session
    Then the session lane shows "session.started", "session.canceled"

  Scenario: The same label reaches the same session
    Then the session "agent-42" has the same id when created twice
    And the sessions "agent-42" and "agent-43" have different ids

  Scenario: A checkpoint splits the history into before and after
    When agent "planner" starts the session "incident"
    And the session records the message "first"
    And I take a session checkpoint after 2 records
    And the session records the message "second"
    Then the records since the checkpoint are "second"
    And the last record at the checkpoint is "first"

  Scenario: A model call and a tool call are recorded without their secrets
    When agent "planner" starts the session "incident"
    And the session records a model call to "gpt-test" whose request carries an api key
    And the session records a call of tool "lookup" whose arguments carry a token
    Then the session lane shows "session.started", "model.request", "model.response", "tool.call", "tool.result"
    And no recorded call carries a secret value

  Scenario: State deltas fold into the document and the end writes a snapshot
    When agent "planner" starts the session "incident"
    And the session state sets "step" to 1
    And the session state sets "done" to false
    And the session ends
    Then the folded session state has "step" 1 and "done" false at revision 2
    And the session lane shows 3 "state.updated" records

  Scenario: A submitted session is picked up and completed by its agent
    Given agent "worker" ends every session it handles
    When "client" submits a session to agent "worker"
    Then the submitted session starts as "session.submitted" and reaches "session.completed"

  Scenario: Operator control rides the control topic
    When operator "operator" asks to cancel the session "incident"
    Then the control topic holds a "session_cancel" request for the session

  Scenario: A declared partition layout routes commands to the addressee and replies to the requester
    Given the agents use a declared partition layout with "planner" on 0 and "worker" on 2
    When "planner" sends a command to "worker" in a new session and "worker" replies
    Then the command landed on partition 2 and the reply on partition 0

  Scenario: A declared topic layout routes commands and replies to the agents' own topics
    Given the agents use a declared topic layout with "planner" on "planner.inbox" and "worker" on "worker.inbox"
    When "planner" sends a command to "worker" in a new session and "worker" replies
    Then the topic "worker.inbox" holds the command and the topic "planner.inbox" holds the reply
    And the session lane holds neither

  Scenario: Remembered items carry the session agent as producer
    When agent "planner" starts the session "incident"
    And the session remembers "the deploy key rotates on fridays" through its linked memory
    Then the remembered item names producer "sdk:planner"

  Scenario: A terminal verb after a refused publish re-sends the latched record and a different verb is refused
    Given session publishes pass through a policy that can refuse them
    When agent "planner" starts the session "incident"
    And the policy refuses the next session status publish
    And I end the session
    Then the send is rejected by policy
    When I cancel the session
    Then the call fails as invalid
    When the session ends
    And the session ends
    Then the session lane shows "session.started", "session.completed", "session.completed"
    And every terminal record on the lane carries one record id

  Scenario: Concurrent terminal calls from clones of one session write one latched state
    When agent "planner" starts the session "incident"
    And 4 clones of the session end it while 4 other clones cancel it at once
    Then the calls of one verb all succeed and the calls of the other all fail as invalid
    And the lane holds 4 terminal records of the winning verb under one record id

  Scenario: Two agents on one partition never handle each other's records
    Given a fresh stream bootstrapped for sessions in the single-partition layout
    And agents "alpha" and "beta" each record the commands they handle
    When "client" sends "a1" to "alpha", "b1" to "beta", "a2" to "alpha", "b2" to "beta" in separate sessions
    Then every command landed on the same partition
    And agent "alpha" handled exactly "a1", "a2"
    And agent "beta" handled exactly "b1", "b2"

  Scenario: A control request written on the session lane is never applied
    Given agent "worker" ends every session it handles
    When agent "planner" starts the session "incident"
    And "intruder" writes the control requests "session_pause", "session_cancel" for agent "worker" on the session lane
    And "planner" sends work to agent "worker" in the session
    Then the session lane shows "session.started", "unauthorized_control", "unauthorized_control", "agent.message", "session.completed"
    And agent "worker" sees no pending pause or cancel for the session

  Scenario: The context manifest lists the source address of every fragment
    When agent "planner" starts the session "incident"
    And the session records the message "first"
    And the session records the message "second"
    And the session records a model call to "gpt-test" with the context assembled from its 3 records
    Then the session lane shows "session.started", "agent.message", "agent.message", "model.request", "context.assembled", "model.response"
    And the context manifest lists the address of every assembled fragment

  Scenario: A model call recorded without an assembled context writes no manifest
    When agent "planner" starts the session "incident"
    And the session records a model call to "gpt-test" without an assembled context
    And the session ends
    Then the session lane shows "session.started", "model.request", "model.response", "session.completed"

  Scenario: A session lens never starts a heartbeat
    Given sessions publish heartbeats every 100 milliseconds
    And agent "worker" ends every session it handles
    When agent "planner" opens the session "incident"
    And the session records the message "hello"
    And "planner" sends work to agent "worker" in the session
    Then the session lane shows "agent.message", "agent.message", "session.completed"
    And no heartbeat is published within 1 second

  Scenario: Heartbeats stop when the last lease drops
    Given sessions publish heartbeats every 100 milliseconds
    When agent "planner" starts the sessions "first" and "second"
    Then a heartbeat lists the sessions "first", "second"
    When the lease on the session "first" is released
    Then a heartbeat lists the sessions "second"
    When the lease on the session "second" is released
    Then the heartbeats stop

  Scenario: Heartbeat batches never mix sessions of two streams
    Given sessions publish heartbeats every 100 milliseconds
    And a second stream bootstrapped for sessions on the same connection
    When agent "planner" starts the session "here" on this stream and the session "there" on the second stream
    Then the heartbeats of each stream list only that stream's session

  Scenario: A refused state snapshot remains pending for the end retry
    Given session publishes pass through a policy that can refuse them
    When agent "planner" starts the session "checkpoint"
    And the session state sets "step" to 1
    And the policy refuses the next state snapshot publish
    And I write a state snapshot
    Then the send is rejected by policy
    When the session ends
    Then the session lane shows "session.started", "state.updated", "state.updated", "session.completed"
