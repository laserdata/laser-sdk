@sessions @plane
Feature: Managed sessions on a deployment that indexes them
  A deployment that serves sessions folds every registered stream into a
  session index. Reads list and inspect sessions from that index without
  scanning the log, and every SDK reads the same rows.

  Background:
    Given a running data platform

  Scenario Outline: Every layout yields the same session row and counts
    Given a managed session stream in the <layout> layout
    When agent "planner" runs the session "layouts" with one command answered by "worker"
    Then the indexed session "layouts" is completed by "planner" with 4 events
    And the stream counts 1 completed session

    Examples:
      | layout              |
      | shared              |
      | per-agent topic     |
      | declared partitions |
      | single partition    |

  Scenario: A2A and MCP children roll up under their root
    Given a managed session stream in the shared layout
    And agent "worker" answers every command it receives
    When agent "planner" starts the session "rollup"
    And the session submits an A2A task as a child
    And the session calls the MCP tool "lookup" as a child
    Then the session tree of "rollup" holds 2 children
    And every child names "rollup" as its parent and root

  Scenario: A fan-out branch rolls up under its root
    Given a managed session stream in the shared layout
    And agent "worker" answers every command it receives
    When agent "planner" starts the session "fan-out"
    And the session fans out one branch to "worker"
    Then the session tree of "fan-out" holds 1 child
    And every child names "fan-out" as its parent and root

  Scenario: A resumed workflow run opens no phantom child
    Given a managed session stream in the shared layout
    And agent "worker" answers every command it receives
    When the workflow "triage" runs its one step on "worker"
    And the workflow "triage" resumes the same run
    Then the session tree of the workflow run holds 1 child

  Scenario: Untargeted low-altitude work reaches a filter-bound agent
    Given a managed session stream in the single partition layout
    And agent "worker" counts the work it handles
    And agent "worker" has a bound addressee filter
    When agent "planner" starts the session "broadcast"
    And agent "planner" sends untargeted low-altitude work with send_agent and request
    Then agent "worker" handles both untargeted records for the session "broadcast"

  Scenario: A recalled memory item links to the session
    Given a managed session stream in the shared layout
    When agent "planner" starts the session "recall"
    And the session remembers "the deploy key rotates on fridays" through its linked memory
    And the session records that it recalled the remembered item
    Then the session links the remembered item as written and recalled

  Scenario: State history keeps old and new digests
    Given a managed session stream in the shared layout
    When agent "planner" starts the session "state"
    Then the indexed session "state" is active
    When the session state sets "step" to 1
    And the session state sets "note" to "draft"
    And the session state is replaced by {"step": 2}
    And the session writes a state snapshot
    Then the indexed state of the session is {"step": 2}
    And the indexed state history chains its digests over 3 deltas and the snapshot

  Scenario: A state write right after the session starts is accepted
    Given a managed session stream in the shared layout
    When agent "planner" starts the session "eager"
    And the session state sets "step" to 1
    Then the indexed state of the session is {"step": 1}

  Scenario: A graph node re-observed by a second session links to both
    Given a managed session stream in the shared layout
    When agent "planner" starts the session "first-look"
    Then the indexed session "first-look" is active
    When the session links "service:auth" to "service:db" in graph "infra"
    And agent "planner" starts the session "second-look"
    And the session links "service:auth" to "service:cache" in graph "infra"
    Then the sessions "first-look" and "second-look" both link the graph node "service:auth"

  Scenario: A key-value write inside a session appears under the session lens
    Given a managed session stream in the shared layout
    When agent "planner" starts the session "desk"
    Then the indexed session "desk" is active
    When the session sets key "ticket" to "open" in namespace "desk"
    Then the session links key "ticket" in namespace "desk"
    And the key-value lens of the session in namespace "desk" lists "ticket"

  Scenario: A linked write right after bootstrap keeps its link
    Given a managed session stream in the shared layout
    When agent "planner" starts the session "early"
    And the session sets key "ticket" to "open" in namespace "desk"
    Then the session links key "ticket" in namespace "desk"

  Scenario: A session reference naming another stream is refused at the edge
    Given a managed session stream in the shared layout
    And a second managed session stream
    When agent "planner" starts the session "victim" on the second stream
    And I set key "loot" in namespace "desk" linked to the second stream's session
    Then the linked write is refused
    And the second stream's session links no key-value item

  Scenario: Heartbeats keep a quiet session live and never appear in its events
    Given a managed session stream whose agents heartbeat every 1 second
    When agent "planner" starts the session "quiet" with an idle timeout of 3 seconds
    And the session stays quiet for 5 seconds
    Then the indexed session is live with a recent heartbeat
    And no indexed event of the session is a heartbeat
    When the session lease is released
    Then the indexed session becomes idle

  Scenario: The change feed holds one row per fold batch and only its stream's sessions
    Given a managed session stream in the shared layout
    And a second managed session stream
    When agent "planner" starts the session "feed"
    And the session streams 6 chunks in one batch
    And agent "planner" starts the session "elsewhere" on the second stream
    Then the index counts 7 records for the session
    And the change feed names the session in at most 2 rows
    And the change feed never names the second stream's session

  Scenario: Every read answers a finished session from the index
    Given a managed session stream in the shared layout
    When agent "planner" starts the session "reads"
    Then the indexed session "reads" is active
    When a watch follows the stream's session changes
    And the session records a call of tool "lookup" whose arguments carry a token
    And the session state sets "step" to 1
    And the session ends
    Then listing the stream finds "reads" as completed
    And the indexed events of the session are "session.started", "tool.call", "tool.result", "state.updated", "state.updated", "session.completed"
    And the indexed state of the session is {"step": 1}
    And the session sources show folded offsets within their heads
    And the session has no links
    And the watch reports the session

  Scenario: The runtime ends a session over its budget instead of handling its work
    Given a managed session stream in the single partition layout
    And agent "worker" counts the work it handles
    When agent "planner" starts the session "spend" with a budget of 100 tokens
    And the session records a model call that used 120 tokens
    Then the indexed session "spend" is over its budget
    When agent "planner" sends the session 2 work records for "worker"
    And agent "planner" starts the session "within"
    And agent "planner" sends the session 1 work record for "worker"
    Then agent "worker" handles only the work of the session "within"
    And the session "spend" ends failed once with reason "budget"

  @lane_identity
  Scenario Outline: Retained handles refuse changed lane identity before writes
    Given a managed session stream in the <layout> layout
    When agent "planner" starts the session "identity"
    And I retain the session and state handles for lane identity checks
    And the session source changes its <change>
    Then retained session handles refuse lifecycle and state writes as stale
    And a fresh handle refuses the stale lane registration
    When the session source is explicitly removed and registered again
    Then a fresh session can write lifecycle and state on the recovered lane
    And retained session handles still refuse the recovered lane

    Examples:
      | layout              | change           |
      | shared              | partition count  |
      | shared              | topic generation |
      | per-agent topic     | partition count  |
      | per-agent topic     | topic generation |
      | declared partitions | partition count  |
      | declared partitions | topic generation |
      | single partition    | partition count  |
      | single partition    | topic generation |

  @lane_identity
  Scenario Outline: Retained handles refuse a recreated stream before writes
    Given a managed session stream in the <layout> layout
    When agent "planner" starts the session "identity"
    And I retain the session and state handles for lane identity checks
    And the session source changes its stream generation
    Then retained session handles refuse lifecycle and state writes as stale
    When the session source is explicitly removed and registered again
    Then a fresh session can write lifecycle and state on the recovered lane
    And retained session handles still refuse the recovered lane

    Examples:
      | layout              |
      | shared              |
      | per-agent topic     |
      | declared partitions |
      | single partition    |

  @lane_identity
  Scenario Outline: Exact record reads refuse a recreated source with reused addresses
    Given a managed session stream in the single partition layout
    When agent "planner" starts the session "identity"
    And I retain a generation-bearing reference to its first record
    And the session source changes its <change>
    And the session source is explicitly removed and registered again
    And a replacement session records its start at the retained source address
    Then reading the retained source reference returns no replacement record

    Examples:
      | change            |
      | topic generation  |
      | stream generation |

  @lane_identity
  Scenario: A lease from a deleted stream never heartbeats into its replacement
    Given a managed session stream whose agents heartbeat every 1 second
    When agent "planner" starts the session "old-lease"
    And the session source changes its stream generation
    Then the old lease publishes no heartbeat into the recreated stream
    When the session source is explicitly removed and registered again
    Then a fresh session can write lifecycle and state on the recovered lane

  @lane_identity
  Scenario: A native lane writer needs no aggregate session read grant
    Given a managed session stream in the per-agent topic layout
    When a native lane writer without session read grants writes lifecycle and state
    Then aggregate session reads remain refused for that writer
    And listing the stream finds "writer" as completed
