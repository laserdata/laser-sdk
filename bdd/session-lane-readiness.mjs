import { AgentTopic, isUnavailable } from "../foreign/typescript/dist/index.js";
import {
  abortable,
  delay,
} from "../foreign/typescript/dist/client/call-options.js";
import { INTERNAL_TRANSPORT } from "../foreign/typescript/dist/client/internals.js";

// The bound the managed steps give every index read, which a loaded host needs.
const CONVERGE_MS = 30_000;
// The first probe's time. A probe that ran out doubles the next one's.
const PROBE_MS = 1_000;

export async function waitForPartitions(
  reader,
  stream,
  timeoutMs = CONVERGE_MS,
) {
  const controller = new AbortController();
  const signal = controller.signal;
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const topics = await untilReady(
      async () => {
        const listed = await reader.client.topic.list({ streamId: stream });
        return listed.some(
          (topic) =>
            topic.name === AgentTopic.Sessions && topic.partitionsCount > 0,
        )
          ? listed
          : undefined;
      },
      signal,
      stream,
    );
    const transport = reader[INTERNAL_TRANSPORT]();
    await Promise.all(
      topics.flatMap((topic) =>
        Array.from({ length: topic.partitionsCount }, (_, partitionId) =>
          untilReady(
            (attempt) =>
              transport.pollMessages(
                stream,
                topic.name,
                { kind: "single", partitionId, name: "laser-checkpoint" },
                { kind: "last" },
                1,
                false,
                undefined,
                attempt,
              ),
            signal,
            `${topic.name}/${partitionId}`,
          ),
        ),
      ),
    );
  } finally {
    clearTimeout(timer);
    controller.abort();
  }
}

async function untilReady(probe, signal, source) {
  const operation = `session partition readiness for ${source}`;
  let bound = PROBE_MS;
  for (;;) {
    signal.throwIfAborted();
    const attempt = new AbortController();
    const within = AbortSignal.any([signal, attempt.signal]);
    const timer = setTimeout(() => attempt.abort(), bound);
    try {
      const answer = await abortable(probe(within), within, operation);
      if (answer !== undefined) return answer;
    } catch (error) {
      if (signal.aborted || !(attempt.signal.aborted || isUnavailable(error)))
        throw error;
      if (attempt.signal.aborted) bound *= 2;
    } finally {
      clearTimeout(timer);
    }
    await delay(100, signal, operation);
  }
}
