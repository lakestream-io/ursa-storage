# Stream-level base schema version

## Problem

When a stream has historical data written with older schema versions, a new
table may be created from a recent record and omit fields that exist only in
older records. Later compaction can then fail when those older records are
decoded.

## Design

An optional `baseSchemaVersion` policy property establishes the minimum schema
history used to initialize or evolve a materialized table.

- If unset, materialization keeps the existing behavior.
- If set and the table does not exist, schema evolution starts from that
  version and applies every version through the current record's version.
- If the table exists, only versions newer than the table's recorded schema
  version are applied.
- A configured version greater than the current record's version is rejected.
- A missing registry version is a materialization error; the implementation
  must not silently skip history.

The writer resolves the policy once and carries the value in
`EntryEncoderContext`. Kafka entry decoding then passes the same context to
`SchemaEvolutionManager`, which owns the version-range calculation.

## Compatibility

The property is additive and optional. It changes only table schema bootstrap;
it does not reinterpret stored record bytes or alter log offsets.

## Tests

- No configured floor preserves current behavior.
- A new table applies every version from the floor through the current record.
- An existing table resumes after its last applied version.
- Missing and out-of-range versions fail deterministically.
- Namespace defaults and stream overrides resolve to the expected value.
