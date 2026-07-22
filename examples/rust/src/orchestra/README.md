# orchestra - one orchestrator over a pool of long-running capability agents

The orchestration example. One orchestrator coordinates a pool of capability agents (classify, diagnose, remediate, and a deliberately slow one) entirely over the log, never a direct call. It is **interactive and paced**: it stops at each phase and waits for Enter, so you can open the LaserData console's Orchestration view (`/orchestration`) and watch every transition happen live. A matched Python peer lives at [`examples/python/orchestra.py`](../../../python/orchestra.py), 1:1 with this one.

The agents connect once at the start, each on its own connection, and stay up for the whole run, so the console shows a live, populated fabric the entire time. The run, phase by phase:

1. **Discovery** - six agents connect and advertise a capability card and a live inbox. The orchestrator resolves them from the fused registry (cards folded from the registry topic), so it never hard-codes who can do what.
2. **Contract** - a directed task to one capable agent with a deadline (`Laser::contract(Router::to_capable("classify"))`). The orchestrator learns the reply, or that it never came. Acknowledgment-on-pickup tells consumed from expired.
3. **Fan-out** - a panel scattered to every capable agent (`Router::all_capable("diagnose")`). One diagnose agent advertises itself `Unavailable`, so capability resolution leaves it out with no orchestrator change, and the panel reaches two of the three.
4. **Workflow** - a journalled, dependency-ordered run (`Laser::workflow`): `triage`, then a `diagnose` panel under a `verify_with` check, then `remediate`. A `Budget` caps the dispatches and wall clock, and each step builds its task from the prior steps' outputs. The journal shows the completed steps in the console's Workflow panel.
5. **Quarantine** - an operator quarantines a misbehaving agent (`Laser::quarantine`), a registry fact every fused registry folds, and the next panel routes around it.
6. **Recovery** - the operator reinstates it (`Laser::unquarantine`), and the panel is whole again.
7. **Expiry + recovery** - a tight-deadline task to the slow agent times out, and the orchestrator recovers by re-dispatching the task to a healthy agent.

Routing uses a fixed inbox topic (`InboxRoute::Fixed`) so the example runs against a stock local Apache Iggy: each branch is target-filtered to its agent on the shared commands topic. A managed deployment advertises per-agent inboxes and uses the default `InboxRoute::Advertised`, with no example change.

```
cargo run --release --example orchestra
```

Then open the LaserData console's Orchestration view and pick this example's stream (the console seeds a best-effort guess, or you choose it). Press Enter to walk the phases while watching presence, the registry, contracts, and the workflow journal update live. Everything here is open-server features (the log plus the SDK's client-side coordination), so it runs against a stock Apache Iggy with no managed backend. Presence advertisement is the one fork-served piece: it populates the console's Presence panel against the LaserData fork, and is a harmless no-op against stock Iggy (the registry, contracts, and workflow panels work on both).
