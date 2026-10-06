import assert from "node:assert/strict"
import { test } from "node:test"

import { ContentType, Record, recordHeaders } from "../../src/index.js"
import { Record as FullRecord } from "../../src/full.js"
import type { LaserTransport, MessageWithHeaders } from "../../src/iggy/apache-iggy.js"
import { Topic } from "../../src/stream/topic.js"
import { LOGICAL_SCHEMA_FINGERPRINT } from "../../src/wire/headers.js"

void test("given_root_and_full_record_metadata_when_a_fingerprint_is_set_then_should_publish_its_exact_owned_bytes", async () => {
  assert.equal(Record, FullRecord)
  const fingerprint = new Uint8Array(32).fill(7)
  const metadata = new Record().contentType(ContentType.Arrow).logicalSchemaFingerprint(fingerprint)
  fingerprint.fill(8)
  assert.deepEqual(recordHeaders(metadata).get(LOGICAL_SCHEMA_FINGERPRINT), {
    kind: "raw",
    value: new Uint8Array(32).fill(7)
  })
  const sent: MessageWithHeaders[] = []
  const transport = {
    sendMessagesWithHeaders: (
      _stream: string,
      _topic: string,
      records: readonly MessageWithHeaders[]
    ) => {
      sent.push(...records)
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  const payload = new Uint8Array([1, 2, 3])
  await new Topic(transport, "readings", "ipc").publishBatch().addRecord(payload, metadata).send()
  const first = sent[0]
  assert.ok(first !== undefined)
  assert.deepEqual(first.payload, payload)
  assert.deepEqual(first.headers.get(LOGICAL_SCHEMA_FINGERPRINT), {
    kind: "raw",
    value: new Uint8Array(32).fill(7)
  })
})
