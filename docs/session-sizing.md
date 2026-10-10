# Session sizing

This page reports what a local managed stack measured for the session index: how much a role scans on the shared session lane, how filtered pages behave under concurrent readers, how fast the plane folds session records, how state and its history grow, and how the change feed behaves when several folds of one stream commit at once. Every number below was measured on the setup described here. Treat them as one host's results, not as a supported deployment size.

## Setup

| Item | Value |
| --- | --- |
| Host | AMD Ryzen 9 7950X (16 cores, 32 threads), 61 GiB RAM, Kingston KC3000 2 TB NVMe under disk encryption, Linux 7.1.5 |
| Streaming server | The LaserData Iggy fork, `0.9.3-ld`, release build, default configuration (filtered reads admit `max_concurrent = 16` pages per shard) |
| Plane | Plane `0.22.0`, release build, session store on the embedded engine or on the external SQL store |
| External SQL store | Server version 16.14 in Docker on the same host, default settings, as `scripts/run-managed-bdd.sh --postgres` starts it |
| Client | Rust Laser SDK `0.7.0`, release build of `bench/examples/session_sizing.rs` |
| Date | 2026-10-09 |

Everything ran on one host, so there is no network between the client, the server, the plane, and the external store. The host was not dedicated to the runs. The stack was started with `scripts/run-managed-bdd.sh stack`, or `scripts/run-managed-bdd.sh --postgres stack` for the external store, which wire the server and the plane exactly as the plane's end-to-end tests do. Each measurement creates its own stream:

```sh
cd bench
cargo run --release --example session_sizing -- <host:port> scan <roles> <records>
cargo run --release --example session_sizing -- <host:port> admission <readers> <records>
cargo run --release --example session_sizing -- <host:port> fold <partitions> <sessions> <records-per-session>
cargo run --release --example session_sizing -- <host:port> state <deltas> <value-bytes> <distinct|same>
```

## Scan amplification per role

In the shared layout every role reads `agent.sessions` through its own consumer group bound to the addressee filter (`agdx.to` is the role or `*`). The workload was 24,000 commands on a 4-partition lane across 64 sessions, addressed round robin to the roles, each record 390 bytes. Each role read from the first record until it had every record addressed to it, and the server reported how many records it examined.

| Roles | Matched per role | Examined per role | Examined per matched record | Read time per role |
| --- | --- | --- | --- | --- |
| 2 | 12,000 | 24,000 | 2.0 | 47 to 52 ms |
| 4 | 6,000 | 24,000 | 4.0 | 40 to 46 ms |
| 8 | 3,000 | 24,000 | 8.0 | 31 to 44 ms |

Every role examines the whole lane, so the server scans the lane once per role. With records spread evenly across R roles, each delivered record costs R examined records, and the server examines R times the lane's write rate. The per-agent topic layout removes this cost, because each role reads only its own topic.

## Page admission under concurrent readers

The workload was 96,000 commands on a 4-partition lane, addressed round robin to 8 roles. N readers, each in its own group bound to the addressee filter of role `i mod 8`, read concurrently from the first record. A page round is one reader call that returned a page with matches.

| Readers | Records examined in total | Wall time | Slowest reader | Examined per second, all readers | Page round p50 | Page round p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 96,000 | 181 ms | 119 ms | 0.53 million | 6.9 ms | 26.5 ms |
| 4 | 383,998 | 256 ms | 152 ms | 1.50 million | 8.3 ms | 27.9 ms |
| 8 | 767,990 | 411 ms | 229 ms | 1.86 million | 9.6 ms | 42.2 ms |
| 16 | 1,535,984 | 760 ms | 437 ms | 2.02 million | 16.3 ms | 75.8 ms |
| 32 | 3,071,996 | 1,357 ms | 715 ms | 2.26 million | 25.3 ms | 119.8 ms |

Wall time includes creating each reader's group. No reader saw an error at any count. The reader retries a round the server did not admit, so admission pressure shows up as round latency rather than as failures. From 1 to 32 readers the median round slowed by a factor of 3.7 and the 99th percentile by 4.5, while the total examined rate grew by a factor of 4.3 and flattened past 8 readers.

## Fold rate

Each session had 10 records on `agent.sessions`: its start, 8 notes of about 210 bytes, and its end. All records were published before the stream was registered as a session source, so the timed part is the fold alone. The clock starts when the registration is published and stops when the session list counts every session as completed. The first part of that time is the plane noticing the registration, which its supervisor checks every 2 seconds, so the table also gives the rate from the first completed session to the last.

| Store | Partitions | Records | Registration to done | Records per second, end to end | Records per second, first to last completed |
| --- | --- | --- | --- | --- | --- |
| Embedded | 1 | 50,000 | 2,562 ms, 2,380 ms | 19,511, 21,007 | 43,824, 39,599 |
| Embedded | 4 | 50,000 | 2,791 ms, 2,782 ms | 17,911, 17,970 | 39,798, 43,468 |
| Embedded | 8 | 50,000 | 4,639 ms, 4,726 ms | 10,776, 10,580 | 39,097, 39,791 |
| Embedded | 1 | 200,000 | 7,559 ms | 26,455 | 30,582 |
| Embedded | 8 | 200,000 | 9,396 ms | 21,285 | 30,169 |
| External SQL | 1 | 50,000 | 3,487 ms, 3,290 ms | 14,336, 15,197 | 43,807, 34,323 |
| External SQL | 4 | 50,000 | 4,603 ms, 2,890 ms | 10,862, 17,300 | 43,782, 29,983 |
| External SQL | 8 | 50,000 | 2,567 ms, 2,372 ms | 19,472, 21,077 | 43,968, 36,993 |
| External SQL | 1 | 200,000 | 10,328 ms | 19,364 | 26,060 |
| External SQL | 8 | 200,000 | 7,663 ms | 26,097 | 34,927 |

The 50,000-record runs ran twice and the table lists both. The 200,000-record runs ran once. On the embedded engine the rate did not change with the partition count: every fold batch goes through the engine's one writer, so more partitions add folds but no write capacity. On the external store, 8 partitions folded 200,000 records 34 percent faster than 1 partition, far from 8 times. The folds of one stream all update shared rows in their transactions, such as the stream's change sequence. Publishing the records from 64 concurrent publishers took 3.5 seconds in the one 50,000-record run that recorded it and 12.5 to 14.5 seconds for 200,000.

## Change feed under concurrent folds

The fold runs above also read back each stream's whole change feed. Every run, on both stores and at every partition count, produced a gap-free sequence. A fold commits one change row per batch for its stream: 50,000 records produced 25 rows on 1 partition and 30 or 31 on 8, and 200,000 records produced 100 rows on 1 partition and 103 or 104 on 8. A session whose records span two batches is named in both rows, so 20,000 sessions were named 25,568 times on 1 partition and about 20,700 times on 8. Folds of one stream allocate the next sequence from one row in the same transaction as their data, so concurrent folds of a stream commit one at a time at that row, which is how the sequence stays gap-free.

## State and history growth

One session applied 1,000 state deltas with a 64-byte value each, either to 1,000 distinct keys or to one key. The SDK also wrote a snapshot after every 64 deltas, so the store held 1,015 history rows.

| Store | Bytes per history row | Bytes per history row with indexes |
| --- | --- | --- |
| Embedded | 164 (column data) | about 310 (table plus its two indexes) |
| External SQL | 224 (row data) | 404 to 484 (relation size growth) |

A history row holds the revision, the operation id, the outcome, the source position, and the old and new document digests, never the value, so it is the same size for distinct keys and for one key. The current document is one row per session: after 1,000 distinct keys it was 80,781 bytes of JSON, after 1,000 writes to one key it was 78 bytes. The index served the state 106 to 114 ms after the last delta was sent. A state read returned at most 1,000 history rows even when asked for more.

## What the numbers mean for a deployment

- A shared lane read by R filtered roles costs the server R scans of the lane. Pick per-agent topics when the roles are many or the lane is busy.
- Filtered page rounds stayed under 30 ms at the median up to 32 concurrent readers on 4 partitions, and the tail grew about four times. A stream with many concurrent readers per shard pays in latency first.
- One plane folded about 26,000 to 44,000 session records per second on this host on either store. A deployment whose agents write session records faster than its planes fold them builds a backlog, and adding partitions to one stream does not raise the rate on the embedded engine.
- Plan about 0.3 KB on the embedded engine and about 0.5 KB on the external store per state delta for history, plus one current document per session, and the retention policy decides how long the history stays.

[Agents, groups, and layouts](building-agents.md#agents-groups-and-layouts) turns these results into layout and partition guidance.
