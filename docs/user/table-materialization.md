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

## Table Naming Templates

`TableNaming.tableNameTemplate` is interpolated once per stream when the policy
is resolved. Three variables are supported, all case-sensitive:

| Variable | Resolves to |
|----------|-------------|
| `${stream.namespace}` | The stream's namespace. |
| `${stream.name}` | The stream's name within its namespace. |
| `${stream.property.<key>}` | The value of the stream property `<key>`, taken from `StreamMetadata.properties()`. |

`${stream.property.<key>}` lets one namespace policy route streams to tables
named after something the stream itself carries — a tenant, a region, a dataset
name a producer set as a stream property:

```java
new TableNaming(Optional.of("analytics"), "${stream.property.dataset}_events")
```

Resolution fails with `IllegalArgumentException` when the template names a
variable that does not exist, and equally when it names a stream property that
is unset or blank on that stream: an unresolvable name is never silently
replaced with a default or an empty string. Templates are interpolated per
stream, so a policy valid for one stream can still fail for its neighbour.

Only `TableNaming.toTableIdentifier(StreamIdentifier, Map<String, String>)`
resolves property variables. The single-argument
`toTableIdentifier(StreamIdentifier)` has no properties to consult and rejects
any template that uses one. `tableNamespacePrefix` is used literally and is
never interpolated.

`${stream.property.<key>}` therefore only works where the stream's own
properties are at hand, which today means catalog-side resolution:
`StreamCatalog.resolveMaterialization` loads the stream and passes
`StreamMetadata.properties()` into the template. The compaction worker's
backwards-compatible fallback — `TableCatalogBootstrap.resolveFromProperties`,
reached only when catalog-side resolution returns empty and the deployment
drives materialization from compaction task properties instead — has no stream
properties to pass, so it resolves the policy without them and a template using
`${stream.property.<key>}` fails there with `IllegalArgumentException`. A
deployment that names tables after stream properties must drive materialization
from a catalog policy.

## Configuration Keys

Operator-side keys read on `CompactionScheduler` startup:

| Key | Default | Notes |
|-----|---------|-------|
| `materializationServiceClass` | `io.lakestream.ursa.lakehouse.compact.LakehouseMaterializationService` | Active `MaterializationService` SPI impl. |
| `compactionStorageBindingsClass` | `io.lakestream.ursa.lakehouse.compact.LakehouseCompactionStorageBindings` | Wires the publish / commit / cleanup runners. |
| `compactionServiceClass` | _(deprecated alias)_ | Honoured for one release. The scheduler logs a WARN when set without `materializationServiceClass`. |
| `iceberg.catalog.<name>.*` / `delta.catalog.<name>.*` / `unityCatalog*` | _(none)_ | Per-catalog connection settings. Translated into `TableCatalog` records on startup by `TableCatalogBootstrap`. |
| `clickhouse.catalog.<name>.dsn` / `…user` / `…password-ref` | _(none)_ | ClickHouse catalog connection bootstrap. |

See [ursa-storage-compact/CLAUDE.md](../../ursa-storage-compact/CLAUDE.md#configuration-keys-operator-surface)
for the full table.

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
  `EvolutionPolicy` and the stream's source schema (`StreamMetadata.schema()`).
- **`MaterializationException` with `LAKEHOUSE_*` codes** — sink-side commit
  failure. Check the underlying catalog (Iceberg/Delta/Unity) status.
- **No materialization happening** — verify
  `streamCatalog.resolveMaterialization(streamId).join().isPresent()`. Common causes:
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
