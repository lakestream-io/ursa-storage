# LIP-161: Table Materialization Framework

- *Author(s)*: Sijie Guo (and Claude Code automation, T1-T15)
- *Proposal time*: 2026-05-21
- *Implemented*: YES
- *Released*: NO
- *Repository*: https://github.com/lakestream-io/ursa-storage
- *Discussion Link*:

## TL;DR

Previously, Ursa could materialize a stream into a Delta or Iceberg lakehouse
table via a hard-wired compaction code path. This proposal refactors that path
into a **generic stream-to-table materialization framework** with a pluggable
`TableMaterializer` SPI, so new backends like **ClickHouse**, JDBC warehouses,
or search engines can be added without re-orchestrating WAL reads or rewriting
the schema-evolution pipeline. The framework is configured through a new typed
`TableMaterializationPolicy` exposed in `lakestream-api` and attached at
namespace or stream scope, giving users a one-click "materialize this stream
to that table" experience.

## Background Knowledge

**Ursa storage** is a lakehouse-native stream storage library: data is written
to a cloud-storage WAL, then physically compacted into columnar (Parquet)
Compacted Objects so that reads can be fast. See the
[Ursa Storage Developer Guide](../developer/WAL-Cloud-Storage-Developer-Guide.md).

**Today's compaction framework** does two distinct things:

1. **Internal compaction** — WAL → Compacted Object (Parquet) on object
   storage. This is what `CompactedObjectReader` reads back via
   `UnifiedStreamReader`. This is the stream's own data; not going away.
2. **External materialization** — the same WAL records → an external
   Delta/Iceberg table. This is what users actually mean by "Tableflow-style"
   behaviour: turn my topic into a queryable table.

Before this LIP, the two concerns were tangled inside `ursa-storage-compact`
(orchestration) and `ursa-storage-lakehouse` (writers). Adding a non-lakehouse
sink required re-implementing schema services, schema-evolution, encoder
pipelines, scheduling, and failure handling — none of which are
lakehouse-specific in any meaningful way.

**lakestream-api** is the protocol-neutral API surface for Ursa streams:
`StreamCatalog`, `Stream`, `Namespace`, `Log`, `LogCursor`, `StreamReader`,
`StreamWriter`. New code should use lakestream-api, not the core `StorageApi`.
Before this LIP, lakestream-api had no concept of "where this stream is
materialised to".

**Confluent Tableflow** is the reference user-experience for stream-to-table —
enable on a topic, get an Iceberg/Delta table. We borrowed several design
decisions (multi-format target, retention dual-bound, `SUSPEND`/`SKIP`/`LOG`
error modes) and explicitly rejected others (mandatory Schema Registry, no
partition-in-policy, no backfill, no pause).

## Motivation

1. **Customers ask for non-lakehouse sinks** — ClickHouse for analytics,
   JDBC warehouses, search indexes. Re-implementing the orchestration / schema
   / evolution stack per sink is expensive and error-prone.
2. **The existing compaction code path leaked lakehouse types** into the
   orchestrator (`CompactionWorker` did `instanceof LakehouseCompactionServiceImpl`;
   `CompactionScheduler` imported a half-dozen `io.lakestream.ursa.lakehouse.*`
   types) — any new sink would have had to leak its own types the same way.
3. **No first-class user surface for "stream → table"** — configuration lived
   in operator-side `lakehouse.properties`, not on the stream itself. Users
   could not introspect from the lakestream API "is this stream materialized
   to a table, and where?"
4. **Schema conversion + evolution** were already abstract in v2
   (`SchemaService`, `SchemaEvolutionManager`, `TableSchemaService<V, R>`) but
   trapped in `io.lakestream.ursa.lakehouse.v2.serde` with no public
   contract; we lost the leverage by not exposing them as a framework.

## Goals

### In Scope

- Define a typed `TableMaterializationPolicy` in `lakestream-api`, attachable
  at `Namespace` (active — materializes every stream in the namespace) and
  `Stream` (override + opt-out via `enabled = false`) scope.
- Lift the schema/encoder pipeline (`SchemaService`, `SchemaEvolutionManager`,
  `EntryEncoder`, `TableSchemaService`) into a new `ursa-storage-materialization`
  module with no lakehouse coupling.
- Define a pluggable SPI: `TableMaterializer`, `TableMaterializerFactory`,
  `MaterializationService` loaded via `ServiceLoader`.
- Refactor the existing Delta + Iceberg writers into a `LakehouseTableMaterializer`
  adapter implementing the new SPI. Zero behaviour change for existing
  deployments.
- Ship a `ClickHouseTableMaterializer` as proof of extensibility: batched
  INSERT into a ClickHouse `ReplacingMergeTree` with `ALTER TABLE` schema
  evolution and per-backend evolution policy gating.
- Keep `compactionServiceClass` working as a deprecated alias for one
  release; introduce `materializationServiceClass` as the new key.
- Document the **categorization** of settings: `TableCatalog` (connection,
  type, tuning) is registered once at the cluster; `TableMaterializationPolicy`
  is the same record applied at two layers (active namespace + override
  stream) and groups settings into Identity / Framework / Schema (evolution +
  primaryKey + baseSchemaVersion) / Table / `connectionOverrides`; cluster
  deployment knobs (credentials, perf, threads) stay in operator config.
  Users always know unambiguously where each setting belongs.

### Out of Scope

- Push/continuous (streaming) materialization driver. Pull/batch only for v1.
- One-stream-to-many-tables fan-out. One stream → one table mapping inherited
  from today's compaction model.
- New source-format support. Kafka source decoding stays in its integration boundary.
- Cross-sink transactions. Each `TableMaterializer.commit()` is independent.
- Migrating legacy v1 lakehouse code paths (root `io.lakestream.ursa.lakehouse`
  packages). Touch only `v2/`.
- A user-visible REST / Admin API for managing the policy. Phase 1 exposes the
  API only at the `StreamCatalog` SDK level; integration-specific admin surfaces
  are follow-up work.

## High Level Design

```
┌────────────────────────────────────────────────────────────────────┐
│                       CompactionScheduler                          │
│   (leader election, WAL scan, task assignment — unchanged)         │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ tasks
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│                       CompactionWorker                             │
│   for each WAL task:                                               │
│     1. read WAL entries (existing)                                 │
│     2. write Compacted Object (existing — internal compaction)     │
│     3. resolve effective materialization policy                    │
│        (deep-merge namespace then stream); if catalogRef set       │
│        and enabled != false: call                                  │
│        MaterializationService.materialize(task, effectivePolicy)   │
└──────────────────────────────┬─────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│         MaterializationService (extracted from today's             │
│         LakehouseCompactionServiceImpl)                            │
│   - resolves TableMaterializerFactory by policy.catalogRef         │
│   - drives the EntryEncoder → TableMaterializer pipeline           │
│   - applies SchemaEvolutionManager + per-sink EvolutionPolicy      │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ SPI: TableMaterializerFactory
              ┌────────────────┼──────────────────┐
              ▼                ▼                  ▼
   LakehouseTableMaterializer  ClickHouseTable…   <future sinks>
   (Delta + Iceberg writers,   (MergeTree
    refactored from existing)   INSERT/MERGE)
```

The `MaterializationService` is **part of** the existing `CompactionService`,
not a replacement: internal WAL→CO compaction still owns orchestration, and
delegates only the external-table phase to the pluggable layer.

## Detailed Design

### Design & Implementation Details

#### Two records — `TableCatalog` (cluster) and `TableMaterializationPolicy` (namespace + stream)

The configuration surface splits into two first-class concepts. The same
`TableMaterializationPolicy` record applies at two layers: **namespace
(active — materializes every stream)** and **stream (override + opt-out)**.

```
TableCatalog                              (registered at cluster, name-addressable)
  name / type / connection / properties
                                  ▲
                                  │ referenced by name
TableMaterializationPolicy on Namespace   (ACTIVE — drives materialization
  catalogRef + tableNaming required;       for every stream in the namespace)
  framework / evolution / primaryKey /
  baseSchemaVersion / table defaults
                                  │ deep-merged; stream wins per field
                                  ▼
TableMaterializationPolicy on Stream      (OVERRIDE — partial)
  any field from the same record + optional
  enabled = false to opt this stream out;
  optional tableIdentifier to shadow the
  derived name; connectionOverrides
```

A stream is materialized **iff** the resolved policy has `catalogRef` set
(from either layer) **and** `enabled` is not explicitly `false` at stream
level. Source schema declaration stays on the existing `Stream.schema()`
(`SchemaConfig`) — the policy does not duplicate it.

| Concept | Owner | Lifecycle | Carries |
|---|---|---|---|
| `TableCatalog` | Cluster ops | Hybrid: config bootstrap + runtime `registerTableCatalog` API | Name, `TableCatalogType`, connection settings (incl. `catalog-impl` for Iceberg sub-flavours), type-level tuning |
| `TableMaterializationPolicy` (namespace) | Platform / namespace owner | Set via `setNamespaceMaterialization` | Active policy with `catalogRef` + `tableNaming` template + framework/evolution/table defaults |
| `TableMaterializationPolicy` (stream) | Stream owner | Set via `setStreamMaterialization` or `createStream(..., materialization)` | Per-stream overrides; optional `enabled = false` opt-out; optional explicit `tableIdentifier`; `connectionOverrides` |
| *(Cluster Conf — NOT in any of these)* | Operator | Deployment config | Storage credentials, perf limits, executor sizing |

#### Pluggable SPI

A new module `ursa-storage-materialization` hosts:

- **`TableMaterializer<Record>`** — `write(record, ctx)`,
  `commit() -> CommitResult`, `close()`,
  `supportedEvolutions() -> EvolutionPolicy`.
- **`TableMaterializerFactory`** — `catalogType() -> TableCatalogType`,
  `create(policy, resolvedCatalog, stream, runtime)`,
  `schemaService(policy, resolvedCatalog, stream)`. Loaded via `ServiceLoader`.
- **`MaterializationService`** — orchestration entry point. Slim down today's
  `CompactionService` interface (rename methods, drop WAL-specific helpers
  from the interface).
- **`MaterializationRuntime`** — injected bag of `SchemaService`,
  `SchemaEvolutionManager`, metrics, executors, failure handler.

The framework's dispatch flow: resolve `TableCatalog` from `policy.catalogRef`
→ look up `TableMaterializerFactory` whose `catalogType() == catalog.type()` →
factory builds a `TableMaterializer` from the resolved `TableCatalog` +
policy.

Generic serde (`SchemaService`, `SchemaEvolutionManager`, `EntryEncoder`,
`EntryEncoderContext`, `EntrySerdeFactory`, plus Kafka source decoding)
moved out of `io.lakestream.ursa.lakehouse.v2.serde` and into
`io.lakestream.ursa.materialization.serde`. Target-format encoders
(Delta/Iceberg) stay in `ursa-storage-lakehouse`.

#### Lakehouse refactor (zero behaviour change)

- New `LakehouseIcebergTableMaterializerFactory`,
  `LakehouseDeltaTableMaterializerFactory`,
  `LakehouseDeltaUcTableMaterializerFactory` registered for
  `TableCatalogType.ICEBERG`, `DELTA`, `DELTA_UC` respectively.
- New `LakehouseTableMaterializer` wraps existing `AbstractLakehouseWriter` +
  Delta/Iceberg writer subclasses; reads its connection settings from the
  resolved `TableCatalog` rather than from `LakehouseConfiguration`.
- On `CompactionScheduler` startup, the existing `iceberg.catalog.<n>.*` /
  `delta.catalog.<n>.*` / `unityCatalog*` operator-config keys are translated
  into `TableCatalog` records and upserted into the new Oxia keyspace
  `tablecatalogs/`. No operator-side disk migration.
- `LakehouseCompactionServiceImpl` split: the internal WAL→CO compaction half
  stayed (renamed for clarity); the external-write half became
  `LakehouseMaterializationService` implementing the new SPI.
- The `CompactionWorker` `instanceof LakehouseCompactionServiceImpl` hack and
  the `S3Exception | AzureException` catch were replaced by sink-neutral
  invalidation through `MaterializationService.invalidate(...)`.
- The `CompactionScheduler` lakehouse-specific wiring (`PublishCompactTaskRunner`,
  `CompactedTaskRunner`, `AsyncCompactedDataCleaner`, `CompactedDataCleanupHandler`)
  was pushed behind the `CompactionStorageBindings` interface; the default impl
  `LakehouseCompactionStorageBindings` is loaded reflectively, so
  `ursa-storage-compact/src/main` no longer imports any
  `io.lakestream.ursa.lakehouse.*` symbol. A grep gate in CI enforces this.

#### ClickHouse sink (proof of extensibility)

New module `ursa-storage-clickhouse` added `TableCatalogType.CLICKHOUSE`:

- `ClickHouseTableMaterializerFactory` — `catalogType() == TableCatalogType.CLICKHOUSE`.
- `ClickHouseTableMaterializer` — buffers rows, commit issues a single batched
  `INSERT INTO ... VALUES` per task. Idempotency relies on `ReplacingMergeTree`
  + `(primaryKey, ingested_at)` order key. Connection settings come from the
  resolved `TableCatalog.connection` (DSN, auth) plus optional
  `connectionOverrides` on the policy.
- `ClickHouseTableSchemaService` — translates source schemas (Avro / JSON /
  Protobuf) → ClickHouse column types using the existing Avro intermediate
  (see `AvroToClickHouseSchema`). `evolveTableSchema(...)` issues
  `ALTER TABLE ... ADD COLUMN`; rejects drops / narrowing per
  `EvolutionPolicy.forClickHouse()`.

### Public-facing Changes

#### Public API (Java — `lakestream-api`)

New types under `io.lakestream.api.materialization`:

- **Top-level**: `TableCatalog`, `TableMaterializationPolicy`,
  `MaterializationState`
- **Identity / refs**: `TableCatalogType` (enum), `TableIdentifier`,
  `TableNaming`
- **Policy sub-records**: `FrameworkConf`, `TableConf`, `EvolutionPolicy`,
  `ResolvedMaterialization`
- **Inside `FrameworkConf`**: `WriteMode`, `StartPosition`, `ErrorHandling`,
  `CommitConfig`
- **Inside `TableConf`**: `TableMode`, `PartitionSpec`, `PartitionTransform`,
  `SortColumn`, `RetentionConfig`, `Compression`

Source schema declaration stays on the existing `Stream.schema()`
(`SchemaConfig`); the policy does not duplicate it. Generic serde types
(`SchemaService`, `SchemaEvolutionManager`, `EntryEncoder`, etc.) keep their
existing names — they were moved out of the lakehouse module but **not
renamed** to maximise reuse and minimise churn in downstream callers.

`Namespace` record gained `Optional<TableMaterializationPolicy> materialization()`
— the active namespace policy.
`Stream` interface gained `Optional<TableMaterializationPolicy> materialization()`
— the override policy — plus `Optional<ResolvedMaterialization> effectiveMaterialization()`
returning the deep-merged effective policy with the `TableCatalog` already
resolved (empty if the stream is not materialized).

`StreamCatalog` gained:

```java
// TableCatalog CRUD (cluster-scoped, admin-only)
CompletableFuture<Void>               registerTableCatalog(TableCatalog catalog);
CompletableFuture<Boolean>            unregisterTableCatalog(String name);
CompletableFuture<TableCatalog>       getTableCatalog(String name);
CompletableFuture<List<TableCatalog>> listTableCatalogs();

// Namespace policy (active)
CompletableFuture<Void> setNamespaceMaterialization(String namespace,
                                                    TableMaterializationPolicy policy);
CompletableFuture<Void> clearNamespaceMaterialization(String namespace);

// Stream policy (override)
CompletableFuture<Void> setStreamMaterialization(StreamIdentifier id,
                                                  TableMaterializationPolicy policy);
CompletableFuture<Void> clearStreamMaterialization(StreamIdentifier id);

// One-click create-with-policy
CompletableFuture<Stream> createStream(StreamIdentifier id, StreamConfig config,
                                       Partitioning partitioning, SchemaConfig schema,
                                       Map<String, String> properties,
                                       Optional<TableMaterializationPolicy> materialization);
```

The previous `createStream(...)` overload (without materialization) stays for
source compatibility.

The unused `TableMetadata` record was **deleted**.

#### Configuration

- New key: `materializationServiceClass` — class name of the orchestration
  service. Default:
  `io.lakestream.ursa.lakehouse.compact.LakehouseMaterializationService`.
- New key: `compactionStorageBindingsClass` — class name of the
  `CompactionStorageBindings` impl. Default:
  `io.lakestream.ursa.lakehouse.compact.LakehouseCompactionStorageBindings`.
- Deprecated alias: `compactionServiceClass` — honoured for one release with
  a `WARN` log on use. When only `compactionServiceClass` is set, the
  scheduler reuses its value as `materializationServiceClass`.
- Existing `iceberg.catalog.<n>.<k>`, `delta.catalog.<n>.<k>`,
  `unityCatalog*` keys keep their current semantics. On startup, the
  scheduler translates each into a `TableCatalog` record (type-inferred from
  the prefix) and upserts into the new `tablecatalogs/` Oxia keyspace. New
  ClickHouse / future-backend bindings use a parallel prefix
  (`clickhouse.catalog.<name>.*`, `bigquery.catalog.<name>.*`, …) and follow
  the same translation rule.
- Per-stream topic properties used today for materialization (`identifierFields`,
  `partitionKey`, `upsertMode`, `iceberg.table.cdc-field`, etc.) become
  **read-on-upgrade** for one release: if a stream is loaded with these on
  its property map and no `TableMaterializationPolicy`, the framework
  synthesises a policy from them (pointing at the default backend) and logs a
  deprecation message.

#### Metrics

All metric names live under the new `ursa.materialization.*` namespace.
Common attributes: `catalog` (the registered `TableCatalog.name`),
`catalog_type` (`ICEBERG`/`DELTA`/…), `stream` (the `StreamIdentifier`).

- `ursa.materialization.records.written` — counter; attributes: `catalog`,
  `catalog_type`, `stream`. Unit: records.
- `ursa.materialization.commit.duration` — histogram; attributes: `catalog`,
  `catalog_type`, `stream`. Unit: nanoseconds.
- `ursa.materialization.commit.retries` — counter; attributes: `catalog`,
  `catalog_type`, `stream`, `outcome` (`success`/`exhausted`). Unit: retries.
- `ursa.materialization.schema.evolution.applied` — counter; attributes:
  `catalog`, `catalog_type`, `stream`, `operation` (`addColumn`/`widenType`/…).
  Unit: evolutions.
- `ursa.materialization.schema.evolution.rejected` — counter; attributes:
  `catalog`, `catalog_type`, `stream`, `reason`. Unit: evolutions.
- `ursa.materialization.errors.dlq` — counter; attributes: `catalog`,
  `catalog_type`, `stream`. Unit: records.
- `ursa.materialization.state` — gauge; attributes: `catalog`, `catalog_type`,
  `stream`. Values: 0=PENDING, 1=RUNNING, 2=DEGRADED, 3=SUSPENDED, 4=PAUSED.

#### CLI

No new CLI surface in this LIP. Policy management goes through the
`StreamCatalog` Java SDK in v1. Integration-specific CLI commands are follow-up work.

## Monitoring

- Watch `ursa.materialization.state` per stream — a stream stuck in
  `DEGRADED` or `SUSPENDED` indicates a hard failure (schema-incompatible,
  commit failures exhausted, sink unavailable).
- Alert when `ursa.materialization.commit.retries{outcome="exhausted"}`
  increases — repeated commit failures signal a catalog or sink-side problem.
- Alert when `ursa.materialization.schema.evolution.rejected` is non-zero
  for a stream — producers are sending schemas the sink's `EvolutionPolicy`
  refuses.
- Alert when `ursa.materialization.errors.dlq` exceeds a per-stream baseline
  — dead-letter pressure.

## Security Considerations

- `TableCatalog` records carry **cluster-level credential material** (catalog
  tokens, DSN credentials, service account refs). Credentials should be
  stored as **references** to a secret store (`secret://…`), not inline
  strings. Auditors can confirm no plaintext credentials leak into Oxia by
  grepping the persisted `tablecatalogs/` keyspace.
- `TableCatalog` CRUD (`registerTableCatalog` / `unregisterTableCatalog`) is
  **admin-only** — same RBAC level as cluster operator config edits. Stream
  owners cannot register backends; they only reference them by name.
- `setStreamMaterialization` / `setNamespaceMaterialization` are subject to
  the same RBAC checks as the existing `setStreamProperties` /
  `setNamespaceProperties` — namespace owner can set the active namespace
  policy; stream owner can set/override or opt out at stream level only.
- DLQ topics named in `framework.errorHandling.dlqTopic` are subject to
  existing topic-create RBAC: the framework must not auto-create a DLQ topic
  the caller cannot themselves create.
- The pluggable SPI loads sinks via `ServiceLoader` — operators must vet
  ClickHouse / future sink dependencies for transitive vulnerabilities and
  pin selected versions through Ursa-owned dependency management.
  `clickhouse-jdbc:0.6.5:all` is currently pinned explicitly because no
  focused upstream BOM covers it.

## Backward & Forward Compatibility

### Revert

To revert from a deployment running this LIP back to the prior version:

1. Stop the compaction tier (`CompactionMain`).
2. Roll deployment images back to the pre-LIP build.
3. The catalog-stored materialization policies remain in Oxia but are ignored
   by the rolled-back code; lakehouse-only compaction resumes via the
   existing `compactionServiceClass` path.
4. Streams created with the new `createStream(..., materialization)` overload
   continue to function: the synthesised topic-properties shim ensures the
   old code can still discover their lakehouse target.

No data is lost; no rewrites required.

### Upgrade

- No data-plane upgrade steps required. Cluster catalog bindings
  (`iceberg.catalog.*`, `delta.catalog.*`, `unityCatalog*`) keep working
  unchanged.
- On first start after upgrade, the compaction tier scans existing streams;
  any stream whose **topic properties** carry materialization-relevant keys
  (`identifierFields`, `partitionKey`, `upsertMode`, `iceberg.table.cdc-field`,
  …) and no `materialization` policy is logged at `WARN` and a synthetic
  policy is composed in-memory for the run. Operators are encouraged to
  migrate to the typed policy via `setStreamMaterialization(...)` within one
  release.

## How will this be made available?

### Fully-managed product: Hosted / BYOC Cloud

- The Tableflow-style "enable" surface (UI button / Cloud API / Terraform
  resource) is wired to `StreamCatalog.setStreamMaterialization(...)` under
  the hood.
- Cloud-managed catalog bindings (Iceberg REST, Glue, Unity Catalog, Polaris)
  are configured at the **environment** level by Cloud Ops; users pick a
  binding by name in the policy.
- Documentation in `lakestream-docs` adds a new section "Materialize a stream to a
  table" with a per-backend page (Iceberg, Delta, ClickHouse). Each page
  documents the recognised keys on the `TableCatalog.connection` /
  `TableCatalog.properties` maps and which keys are sensibly overrideable
  per-stream via `policy.connectionOverrides`.
- Runbook: troubleshooting `DEGRADED`/`SUSPENDED` materialization state —
  covers schema-incompatibility, commit-exhausted, sink-unavailable; mapping
  each to the relevant metric and the operator action.

### Self-managed product: Platform / Private Cloud

- The same `StreamCatalog` Java API is available; platform documentation
  explains how to register table catalogs, set the active namespace policy,
  and attach per-stream override policies. See
  [docs/user/table-materialization.md](../user/table-materialization.md) for
  the user-facing quickstart.
- Operators configure cluster `TableCatalog` definitions in
  `lakehouse.properties` exactly as today (`iceberg.catalog.<name>.*`,
  `delta.catalog.<name>.*`) — bootstrap translates each prefix into a typed
  `TableCatalog`. For ClickHouse, a new prefix `clickhouse.catalog.<name>.*`
  carries DSN, auth, and optional default database.
- Platform docs link to the migration guide showing how to convert today's
  topic-property-based materialization into the typed
  `TableMaterializationPolicy` via `setStreamMaterialization(...)`.

## Alternatives

1. **Build a new framework alongside the existing compaction code.** Rejected
   per discussion: the user explicitly wanted a refactor in-place, not a
   parallel implementation. Keeping two code paths doubles operational
   surface and creates a migration cliff.
2. **Keep the existing `CompactionService` interface as-is and just add
   ClickHouse alongside Delta/Iceberg.** Rejected: the interface today baked
   in lakehouse-specific exception types, the `LakehouseCompactionServiceImpl`
   `instanceof` check in the worker, and a storage-specific `initialize(...)`
   signature. Adding ClickHouse without refactoring would have forced
   ClickHouse to inherit lakehouse coupling.
3. **Two separate record types — a lean `NamespaceMaterializationDefaults`
   and a full per-stream `TableMaterializationPolicy`.** Considered and
   **rejected**: the user wanted namespace policy to **actively materialize**
   every stream (not just supply defaults if a stream opts in), so the
   namespace record had to carry the full active policy. One record applied
   at both layers with deep-merge override semantics is therefore the right
   shape: namespace settings are active and stream settings override.
4. **Multi-format `backends: List<TableCatalogType>` inside one policy
   (Tableflow style).** Considered. Rejected: a single registered
   `TableCatalog` already captures one connection + one type; multi-format =
   multiple `TableCatalog`s + multiple policies. Cleaner, no list-merge
   semantics on the single most important field, and aligns with the user's
   "one stream → one table" rule.
5. **`TableCatalog` as inline blob on every policy (no registration).**
   Considered. Rejected: every stream policy would have to redeclare
   connection settings, credentials would multiply across Oxia keys, and
   rotating a catalog token would need an update to every stream. The
   named-binding pattern with a separate `TableCatalog` keyspace is
   operationally far better.
6. **Stream-to-stream as the first new sink, not ClickHouse.** Considered for
   "easier proof of extensibility" but ClickHouse was the customer-driven
   first target. Stream-to-stream can come in a follow-up LIP if demand
   surfaces.
7. **Materialization-per-stream cardinality = N (multi-table fan-out).**
   Considered. Deferred: the existing 1:1 model covers known use cases; the
   policy record can be extended to a `List<TableMaterializationPolicy>`
   later without breaking changes.

## General Notes

- This LIP defines the framework; per-backend LIPs (Iceberg-specific knobs,
  Delta-specific knobs, ClickHouse table engines + idempotency semantics) can
  follow when those backends' surface expands.
- The naming `TableMaterializationPolicy` is chosen over `TableflowPolicy` /
  `TableSinkPolicy` to align with the user-facing phrase "materialize a
  stream to a table" and to avoid naming collision with Confluent Tableflow.
- The new module `ursa-storage-materialization` is intentionally small:
  SPI + serde + framework only. `TableCatalog` implementations live in their
  own modules (`ursa-storage-lakehouse`, new `ursa-storage-clickhouse`, …) so
  dependency footprints stay isolated.

## Implementation Status

All 14 implementation tasks landed across commits `929397ca5..5e8dc1e12` on
branch `ursa-storage-table-materialization-framework`:

| Task | Commit | Summary |
|------|--------|---------|
| T1 | `929397ca5` | `lakestream-api` materialization types |
| T2 | `a76ed1400` | Materialization policy on `Namespace`, `Stream`, `StreamCatalog` |
| T3 | `fec7ac082` | `TableMaterializationPolicy.resolve` + `TableNaming` interpolation |
| T4 | `b6d74e207` | New `ursa-storage-materialization` module + generic serde moved |
| T5 | `5364909ea` | Public SPI for stream-to-table sinks |
| T6 | `b3899c15d` | `IndexedStreamCatalog` persistence for materialization policy |
| T7 | `cfdeb88e7` | `TableCatalogBootstrap` translates legacy config to `TableCatalog` |
| T8 | `9c64ef4bb` | `LakehouseTableMaterializer` adapter + factories |
| T9 | `236cd648d` | Split `LakehouseCompactionServiceImpl` + add `CompactionStorageBindings` |
| T10 | `9c168edcd` | Wire orchestrator through `MaterializationService` SPI |
| T11 | `38ff983c7` | New `ursa-storage-clickhouse` module + materializer |
| T12 | `be0c6c51b` | `ClickHouseTableSchemaService` + Avro → ClickHouse translation |
| T13 | `2b073afd7` | End-to-end materialization tests via Testcontainers |
| T14 | `5e8dc1e12` | Cross-cutting policy resolution + evolution gating + metrics contract |

Two follow-ups are tracked separately:

1. **CAS-protect `IndexedStreamCatalog` read-modify-write paths** on
   namespace/stream metadata edits to eliminate a latent
   last-writer-wins race when multiple admin clients mutate policies
   concurrently.
2. **Reconnect `LakehouseTableMaterializerFactory.schemaService`** and
   populate `CommitResult` once `IWriteResult` accessors are added to the
   lakehouse writers — currently `schemaService(...)` returns `null` for
   lakehouse factories and `CommitResult` carries empty
   `lastOffset`/`rowCount` after a successful Delta/Iceberg commit. Neither
   gap impacts the orchestrator's correctness, but they leave headroom
   on the metrics surface and prevent the framework from skipping
   schema-evolution probes when the writer already knows the resolved
   schema.
