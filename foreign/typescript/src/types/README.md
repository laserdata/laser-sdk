# Type conventions

The TypeScript SDK keeps wire-sized values exact and prevents IDs from becoming interchangeable through structural typing.

- `number` carries validated unsigned values up to 32 bits and bounded counts
- `bigint` carries offsets, revisions, fences, timestamps, expiry, and token use
- `Uint8Array` carries public payloads and binary wire fields
- `MessageId` is a partition ID plus an exact offset
- `AgentId`, `ConversationId`, and the other IDs validate at construction and remain distinct types
- optional fields use absence or `undefined` unless the wire distinguishes null

Use the public parsers and constructors instead of assertions. Parsing failures are structured `InvalidError` values. Incoming JSON remains `unknown` until a codec validates it.

The Apache Iggy adapter is the only Node `Buffer` conversion boundary. Domain and wire APIs keep `Uint8Array`, including schema and fixture codecs.
