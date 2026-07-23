import assert from "node:assert/strict"
import { readdir } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { FIXTURE_MANIFEST } from "./support/fixture-manifest.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

void test("given_the_rust_fixture_corpus_when_compared_to_the_manifest_then_should_have_no_added_or_removed_files", async () => {
  const onDisk = new Set(await readdir(FIXTURES_DIR))
  const manifested = new Set(Object.keys(FIXTURE_MANIFEST))

  const added = [...onDisk].filter((name) => !manifested.has(name)).sort()
  const removed = [...manifested].filter((name) => !onDisk.has(name)).sort()

  assert.deepEqual(
    added,
    [],
    `wire/fixtures gained files not classified in fixture-manifest.ts: ${added.join(", ")}`
  )
  assert.deepEqual(
    removed,
    [],
    `fixture-manifest.ts names files no longer in wire/fixtures: ${removed.join(", ")}`
  )
})

void test("given_a_fixture_marked_covered_when_listed_then_should_actually_exist_on_disk", async () => {
  const onDisk = new Set(await readdir(FIXTURES_DIR))
  const covered = Object.keys(FIXTURE_MANIFEST)

  assert.ok(covered.length > 0, "expected at least one covered fixture")
  for (const name of covered) {
    assert.ok(
      onDisk.has(name),
      `manifest marks ${name} covered but it is missing from wire/fixtures`
    )
  }
})
