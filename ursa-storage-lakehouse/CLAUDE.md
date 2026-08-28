# ursa-storage-lakehouse

Lakehouse integration module. 304 Java files in main, 151 in test. Largest module.

## Architecture: v2 vs v1

- **`io.lakestream.ursa.lakehouse.v2.*`** — Current code path. All new work goes here.
- **`io.lakestream.ursa.lakehouse.*`** (root packages) — Legacy v1. Do not add new code.

## Package Layout

### V2 (current)
```
v2/delta/          — Delta Lake table format integration
v2/iceberg/        — Apache Iceberg table format integration
v2/io/             — I/O layer
  └── parquet/     — Parquet file reading/writing
v2/serde/          — Sink-specific serde wiring (Delta/Iceberg/Parquet only)
  ├── delta/       — Kafka → Delta record encoders
  ├── iceberg/     — Iceberg record encoders + ProtobufNativeToIcebergConverter
  ├── kafka/parquet/   — Kafka → Parquet encoders/decoders
  └── LakehouseSerdeRegistry — registers the above with the generic
                               EntrySerdeFactory in ursa-storage-materialization
```

**Generic serde framework (SchemaService, SchemaEvolutionManager, EntryEncoder, EntryEncoderContext,
EntrySerdeFactory, Kafka source-format decoders, plus the JSON/Avro converters and
LakehouseEntryMetadata) lives in `ursa-storage-materialization`** under
`io.lakestream.ursa.materialization.serde`. The renames during the relocation were:
- `LakehouseEntry<T>` → `MaterializationRecord<T>`
- `LakehouseTableSchemaService<V,R>` → `TableSchemaService<V,R>`

## LIP-161 Materializer Classes (T8 + T9)

This module also provides the `TableMaterializer` implementations and the
orchestrator bindings for Iceberg / Delta / Delta-UC:

| Class | Purpose |
|-------|---------|
| `v2.LakehouseTableMaterializer` | `TableMaterializer<GenericEntry>` adapter wrapping `AbstractLakehouseWriter` + Delta/Iceberg writer subclasses. `write(record, ctx)` delegates to the existing per-format writer; `commit()` calls `close()` and converts `List<IWriteResult>` to `CommitResult`. |
| `v2.LakehouseIcebergTableMaterializerFactory` | `catalogType() == TableCatalogType.ICEBERG` |
| `v2.LakehouseDeltaTableMaterializerFactory` | `catalogType() == TableCatalogType.DELTA` |
| `v2.LakehouseDeltaUcTableMaterializerFactory` | `catalogType() == TableCatalogType.DELTA_UC` |
| `compact.LakehouseMaterializationService` | External-write half of the legacy `LakehouseCompactionServiceImpl`, refactored to implement the `MaterializationService` SPI. Includes `invalidate(StreamIdentifier)` which absorbs today's `invalidateCompactWorker(...)`. |
| `compact.LakehouseCompactionServiceImpl` | Internal WAL→CO compaction half — the lakehouse-specific compactor wiring. Loaded as `compactionServiceClass` legacy default. |
| `compact.LakehouseCompactionStorageBindings` | Default `CompactionStorageBindings` impl loaded reflectively from `ursa-storage-compact`; supplies `PublishCompactTaskRunner`, `CompactedTaskRunner`, `AsyncCompactedDataCleaner`, `CompactedDataCleanupHandler`. |

## SPI Registration

```
src/main/resources/META-INF/services/io.lakestream.ursa.materialization.TableMaterializerFactory
  → io.lakestream.ursa.lakehouse.v2.LakehouseIcebergTableMaterializerFactory
  → io.lakestream.ursa.lakehouse.v2.LakehouseDeltaTableMaterializerFactory
  → io.lakestream.ursa.lakehouse.v2.LakehouseDeltaUcTableMaterializerFactory
```

Iceberg sub-flavours (Glue / REST / Hadoop / Polaris / Unity) are routed
through `TableCatalog.connection["catalog-impl"]` — one factory handles all
Iceberg catalogs.

## T9 Split — Internal vs External Compaction

Before T9, `LakehouseCompactionServiceImpl` carried both halves:
- WAL → Compacted Object (Parquet on object storage) — the stream's own
  data, kept here under `compact.LakehouseCompactionServiceImpl`.
- WAL → external Delta/Iceberg table — extracted to
  `compact.LakehouseMaterializationService` so the orchestrator can
  dispatch through the new `MaterializationService` SPI without importing
  any lakehouse types.

The `CompactionWorker` `instanceof LakehouseCompactionServiceImpl` hack
and the `S3Exception | AzureException` reach-around were replaced with
the sink-neutral `MaterializationService.invalidate(streamId)` call.

### V1 Legacy (161 files — do not extend)
```
catalog/           — Table catalog management
cleaner/           — Compacted data cleanup
compact/           — Compaction logic
delta/             — Delta Lake v1
iceberg/           — Iceberg v1 (includes GCP BigQuery metastore)
parquet/           — Parquet utilities
schema/            — Schema management
utils/             — Utilities (includes lock subpackage)
writer/            — Table writers
```

## Table Format Isolation

**Critical rule**: Iceberg and Delta packages must not cross-reference.
- `*.delta.*` packages must not import from `*.iceberg.*`
- `*.iceberg.*` packages must not import from `*.delta.*`
- Shared logic goes in common packages (`serde/utils/`, `io/`)

## Vendor Code

Patched upstream sources — modify with extreme care:
- `org.apache.iceberg.avro` (3 files) — Iceberg Avro patches
- `io.delta.kernel` (2 files) — Delta Kernel API patches
- `io.delta.kernel.defaults` — Delta Kernel defaults

## Code Generation

Protobuf (standard): `protobuf-maven-plugin`
- `src/main/proto/serde_data.proto` — serialization data structures
- `src/main/proto/lakehouse_entry_metadata.proto` — source entry metadata
- 32 test proto files for integration testing

Regenerate: `mvn generate-sources -pl ursa-storage-lakehouse`

Avro: `avro-maven-plugin` for test schema generation.

## Testing

```bash
# Run lakehouse tests
mvn -B -ntp test -pl ursa-storage-lakehouse -Dgroups=lakehouse
```

All test classes use `@Tag("lakehouse")`.

## Pitfalls

- New code goes in `v2/` — never in root packages
- Don't cross-reference Iceberg/Delta packages
- Vendor code: prefer upstream fixes over local patches
- Schema conversion has many edge cases — test with Avro, JSON, and Protobuf schemas
- Check null handling in schema evolution scenarios
