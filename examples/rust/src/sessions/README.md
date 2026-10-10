# sessions: inspect an incident across agent sessions

This example records an incident root, diagnosis and remediation children, and a separate maintenance root inside one Iggy stream. Several agent identities contribute to the incident. The application collects child results explicitly, and each session keeps its own state.

Run the sessions example with the standard example command for this language. It prints the recorded event and managed resource link counts for each session.

Agent work runs as application functions. The example records a mock model response and calls no model service. The agent and orchestra examples demonstrate long-running consumers. On open Apache Iggy, the native records and state are available. Managed KV, graph, links and index views require a supporting deployment.
