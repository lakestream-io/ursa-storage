# Materialize a Stream to a Table

Ursa Storage can materialize a stream into an external table for analytics
consumption. The framework supports Apache Iceberg, Delta Lake, Delta on
Unity Catalog, and ClickHouse out of the box.

## Concept Quick Reference

- **TableCatalog** — a registered, named table store (e.g.,
  `iceberg-glue-prod`, `clickhouse-analytics`). Set up once at cluster scope.
- **TableMaterializationPolicy** — the per-stream or per-namespace policy
  describing where + how to materialize.
- Namespace policy is **active** — it materializes every stream in the
  namespace.
- Stream policy is the **override** — set only the fields that differ from
  the namespace default. Set `enabled = false` to opt out.

The exact records live in
`io.lakestream.api.materialization`; every functional field of
`TableMaterializationPolicy` is an `Optional<T>` so the same record can be
used as a full policy (at the namespace) or as a sparse override (at the
stream).

## Quickstart (via the StreamCatalog API)

```java
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.materialization.Compression;
import io.lakestream.api.materialization.EvolutionPolicy;
import io.lakestream.api.materialization.PartitionSpec;
import io.lakestream.api.materialization.PartitionTransform;
import io.lakestream.api.materialization.TableCatalog;
import io.lakestream.api.materialization.TableCatalogType;
import io.lakestream.api.materialization.TableConf;
import io.lakestream.api.materialization.TableMaterializationPolicy;
import io.lakestream.api.materialization.TableNaming;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// 1. Ops registers a TableCatalog (typically at startup via operator config).
TableCatalog ch = new TableCatalog(
        "clickhouse-prod",
        TableCatalogType.CLICKHOUSE,
        Map.of("dsn", "clickhouse://host:9000/", "user", "ursa"),
        Map.of());
streamCatalog.registerTableCatalog(ch).join();

// 2. Platform team attaches a namespace policy. Every functional field is an
//    Optional, so set only what the namespace actually owns.
TableMaterializationPolicy namespacePolicy = new TableMaterializationPolicy(
        Optional.of("clickhouse-prod"),                        // catalogRef
        Optional.of(new TableNaming(Optional.empty(),
                "${stream.name}")),                            // tableNaming
        Optional.empty(),                                      // tableIdentifier
        Optional.empty(),                                      // enabled
        Optional.empty(),                                      // framework
        Optional.of(EvolutionPolicy.forClickHouse()),          // evolution
        Optional.empty(),                                      // primaryKey
        Optional.empty(),                                      // baseSchemaVersion
        Optional.empty(),                                      // table
        Map.of());                                             // connectionOverrides
streamCatalog.setNamespaceMaterialization("analytics", namespacePolicy).join();

// 3. New streams in the "analytics" namespace materialize automatically.
//    Stream owners can override with a stream-level policy:
TableMaterializationPolicy override = new TableMaterializationPolicy(
        Optional.empty(),                                      // catalogRef inherits
        Optional.empty(),                                      // tableNaming
        Optional.empty(),                                      // tableIdentifier
        Optional.empty(),                                      // enabled
        Optional.empty(),                                      // framework
        Optional.empty(),                                      // evolution
        Optional.of(List.of("tenant_id", "event_id")),         // primaryKey
        Optional.empty(),                                      // baseSchemaVersion
        Optional.of(new TableConf(
                Optional.empty(),
                Optional.of(List.of(new PartitionSpec(
                        "event_date",
                        PartitionTransform.DAY,
                        Optional.empty()))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Compression.ZSTD))),               // table
        Map.of());                                             // connectionOverrides
streamCatalog.setStreamMaterialization(
        new StreamIdentifier("analytics", "events"), override).join();

// 4. Or opt one stream out of the namespace's active policy:
TableMaterializationPolicy optOut = new TableMaterializationPolicy(
        Optional.empty(),                                      // catalogRef
        Optional.empty(),                                      // tableNaming
        Optional.empty(),                                      // tableIdentifier
        Optional.of(false),                                    // enabled = false
        Optional.empty(),                                      // framework
        Optional.empty(),                                      // evolution
        Optional.empty(),                                      // primaryKey
        Optional.empty(),                                      // baseSchemaVersion
        Optional.empty(),                                      // table
        Map.of());                                             // connectionOverrides
streamCatalog.setStreamMaterialization(
        new StreamIdentifier("analytics", "internal_audit"), optOut).join();
```

> Note: `TableMaterializationPolicy` is a plain Java record — there is no
> builder or `toBuilder()` / wither API. Construct via the canonical
> constructor and use `TableMaterializationPolicy.empty()` as the starting
> point if you only need a couple of fields populated. Stream-level
> overrides only need the fields that differ from the namespace policy;
> every other field uses `Optional.empty()` to inherit.

## Configuration Keys

Operator-side keys read on `CompactionScheduler` startup:

| Key | Default | Notes |
|-----|---------|-------|
| `materializationServiceClass` | `io.lakestream.ursa.lakehouse.compact.LakehouseMaterializationService` | Active `MaterializationService` SPI impl. |
| `compactionStorageBindingsClass` | `io.lakestream.ursa.lakehouse.compact.LakehouseCompactionStorageBindings` | Wires the publish / commit / cleanup runners. |
| `compactionServiceClass` | _(deprecated alias)_ | Honoured for one release. The scheduler logs a WARN when set without `materializationServiceClass`. |
| `blackTopicOfCompact` | _(none)_ | Comma-separated logical stream names. A `-partition-N` suffix is normalized to the logical stream, so per-partition exclusion is not supported. |
| `commitTimeoutInSeconds` | `1800` | Upper bound for a lakehouse commit round and for draining an in-flight commit during leader demotion. Exceeding it fail-stops the compaction process so an unfenced old commit cannot overlap a successor. |
| `iceberg.catalog.<name>.*` / `delta.catalog.<name>.*` / `unityCatalog*` | _(none)_ | Per-catalog connection settings. Translated into `TableCatalog` records on startup by `TableCatalogBootstrap`. |
| `clickhouse.catalog.<name>.dsn` / `…user` / `…password-ref` | _(none)_ | ClickHouse catalog connection bootstrap. |

See [ursa-storage-compact/CLAUDE.md](../../ursa-storage-compact/CLAUDE.md#configuration-keys-operator-surface)
for the full table.

## Version 1.0 Upgrade Notes

The 1.0 catalog-based task publisher discovers streams from Lakestream namespace indexes and stores
publication cursors under each canonical `namespace/stream-partition-N` name. Stop every old
publisher before starting the 1.0 publisher; rolling old and new publishers against the same
compaction metadata is not supported.

Pre-1.0 named cursors could contain a non-negative offset with a cumulative byte size of zero. The
1.0 publisher never treats zero as a valid size for a published offset:

- If a durable prepared task proves the missing cumulative size, the publisher repairs the cursor
  atomically while holding its publication lease.
- If the value cannot be proved, publication for that partition is quarantined and the lease is
  released. Use `update-publish-task-offset` to record the verified cumulative bytes through the
  published offset. Do not estimate this value: a wrong baseline can replay or skip data.
- A durable prepared task whose range, identity, or byte totals cannot be reconciled with the
  cursor follows the same lease-release and quarantine path instead of retrying on every scan.
- `--offset=-1` must use `--cumulative-size=0`; every non-negative offset must use a positive
  cumulative size.

The compaction integration also has source-incompatible API and configuration changes:

- `CompactTaskManager.updatePublishedOffset(String, long, long, long)` now requires the cumulative
  size argument.
- Third-party `CompactTaskManager` implementations must implement
  `releasePublicationLeaseAsync(PublicationLease)`. It must return immediately and use the
  implementation's native asynchronous metadata-store API; performing blocking remote I/O before
  returning prevents lease-release timeouts from supervising that request.
- `CompactionStorageBindings.createTopicManager()` was removed. The publisher discovers streams
  through `StreamCatalog`.
- `LakehouseCompactionStorageBindings.Dependencies` now accepts a `StreamCatalog`, a dedicated
  scheduled `publicationControlExecutor`, and a separate `publicationWorkerExecutor`; it no longer
  accepts `TopicProvider`, `TopicManager`, or `AsyncOxiaClient`. Custom reflective bindings must
  update their `Dependencies` constructor to the new 13-argument signature.
- The built-in publication worker uses daemon threads. A publication that exceeds its deadline is
  fenced and remains identity-tracked until its callable returns, but an interrupt-ignoring
  callable cannot hold process shutdown open after bounded dependency cleanup completes.
- A lakehouse commit that is already executing cannot be safely cancelled. Leader demotion retains
  the Oxia leader record while waiting up to `commitTimeoutInSeconds` for such work to return. If it
  does not drain, the service logs the failure, increments
  `ursa.storage.compact.commit.drain.timeout.count`, and fail-stops the process; process termination
  prevents the old callable from overlapping the successor before the ephemeral record is released.
- `update-publish-task-offset` now requires `--cumulative-size`.
- `refreshLocalTopicInternalInSeconds` was removed because it was not used. There is no replacement.

If an older deployment used stream-ID-only metadata rather than the canonical named cursor, use a
fresh catalog/compaction metadata namespace or perform an explicit offline migration. Reusing an old
materialized table with a fresh cursor can replay the stream from offset zero, so reset or migrate
the table and cursor together.

## Supported Sinks

| Backend | Type Constant | Evolution Policy |
|---------|---------------|------------------|
| Iceberg | `TableCatalogType.ICEBERG` | `EvolutionPolicy.forIceberg()` — addColumn, addNullableColumn, widenType |
| Delta Lake | `TableCatalogType.DELTA` | `EvolutionPolicy.forDelta()` — same as Iceberg |
| Delta on Unity Catalog | `TableCatalogType.DELTA_UC` | `EvolutionPolicy.forDelta()` |
| ClickHouse | `TableCatalogType.CLICKHOUSE` | `EvolutionPolicy.forClickHouse()` — addColumn, addNullableColumn only |

Adding a new sink requires implementing the
`io.lakestream.ursa.materialization.TableMaterializerFactory` SPI and
registering it under
`META-INF/services/io.lakestream.ursa.materialization.TableMaterializerFactory`.
See `ursa-storage-clickhouse` for a worked example.

## Troubleshooting

- **`MaterializationException(MESSAGE_SCHEMA_INCOMPATIBLE)`** — schema
  evolution request was outside the sink's allowed policy. Check
  `EvolutionPolicy` and the stream's source schema (`Stream.schema()`).
- **`MaterializationException` with `LAKEHOUSE_*` codes** — sink-side commit
  failure. Check the underlying catalog (Iceberg/Delta/Unity) status.
- **No materialization happening** — verify
  `stream.effectiveMaterialization().isPresent()`. Common causes:
  `catalogRef` doesn't resolve to a registered `TableCatalog`; namespace
  policy missing `tableNaming` template; stream policy set
  `enabled = false`.

Useful metrics for active debugging (all under the `ursa.materialization.*`
namespace):

- `ursa.materialization.state` (gauge) — per-stream materialization state.
  `0=PENDING`, `1=RUNNING`, `2=DEGRADED`, `3=SUSPENDED`, `4=PAUSED`.
- `ursa.materialization.records.written` (counter) — should increase
  whenever the stream has fresh WAL data.
- `ursa.materialization.schema.evolution.rejected` (counter) — non-zero
  means the producer is sending a schema the sink refuses.
- `ursa.materialization.commit.retries{outcome="exhausted"}` (counter) —
  non-zero means commits are failing past the configured retry limit.

## See Also

- [LIP-161: Table Materialization Framework](../lip/LIP-161-Table-Materialization-Framework.md)
- [Compaction Orchestration Flow](../../ursa-storage-compact/CLAUDE.md#orchestration-flow-t10)
- [ClickHouse sink module README](../../ursa-storage-clickhouse/CLAUDE.md)
- [Lakehouse materializer adapter notes](../../ursa-storage-lakehouse/CLAUDE.md)
