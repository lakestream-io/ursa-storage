# ursa-storage-clickhouse

ClickHouse sink for the table materialization framework. Implements the
`TableMaterializer` SPI defined in `ursa-storage-materialization` and is
discovered by the orchestrator via `ServiceLoader`.

## Key Classes

| Class | Purpose |
|-------|---------|
| `ClickHouseTableMaterializerFactory` | `TableCatalogType.CLICKHOUSE` factory, registered via `META-INF/services` |
| `ClickHouseTableMaterializer` | Batched INSERT into `ReplacingMergeTree` or `MergeTree` per `WriteMode` |
| `ClickHouseTableSchemaService` | Avro → ClickHouse type translation + `ALTER TABLE` evolution |
| `AvroToClickHouseSchema` | Pure type-translation helper |
| `ClickHouseConnectionFactory` | JDBC connection construction from `TableCatalog.connection` + `policy.connectionOverrides` |
| `ClickHouseTableEngine` | Maps a policy's `WriteMode` + primary key onto a concrete table engine (`MergeTree` / `ReplacingMergeTree`) |
| `ClickHouseColumn` / `ClickHouseSchema` | Lightweight POJOs representing the resolved column set |

## SPI Registration

```
src/main/resources/META-INF/services/io.lakestream.ursa.materialization.TableMaterializerFactory
  → io.lakestream.ursa.clickhouse.ClickHouseTableMaterializerFactory
```

`LakehouseMaterializationService` discovers the factory via `ServiceLoader`
when a stream resolves to a `TableCatalog` of type `CLICKHOUSE`. No
configuration is required beyond registering the `TableCatalog` itself.

## Running Tests

```bash
# Unit tests (default — fast, no Docker)
mvn -B -ntp test -pl ursa-storage-clickhouse

# Integration tests (requires Docker)
mvn -B -ntp test -pl ursa-storage-clickhouse -Dgroups=clickhouse -DexcludeGroups=
```

The integration tests use Testcontainers and require Docker. The
`clickhouse` tag is excluded by default via the module pom's
`<excludeGroups>clickhouse</excludeGroups>` so unit tests stay fast.

## Pitfalls

- `clickhouse-jdbc:0.6.5:all` is pinned in this module
  (`<clickhouse-jdbc.version>0.6.5</clickhouse-jdbc.version>` in the module
  pom) because the parent dependency management does not cover it.
- Evolution policy is intentionally strict (`EvolutionPolicy.forClickHouse()`):
  only column-add (nullable and non-nullable) is allowed. Drop / narrow /
  rename / reorder are rejected with
  `MaterializationException(MESSAGE_SCHEMA_INCOMPATIBLE)`.
- Idempotency relies on `ReplacingMergeTree` `ORDER BY` over the primary
  key — the user must declare a primary key in the policy
  (`TableMaterializationPolicy.primaryKey()`) for `WriteMode.UPSERT`. With
  `WriteMode.APPEND` the engine falls back to a plain `MergeTree` with an
  ingestion timestamp.
- The integration tests pull the ClickHouse server image lazily; first run
  is slow.
- `clickhouse-http-client` is a runtime-only dependency consumed by the
  `:all` shaded driver via reflection — do not remove it from the pom even
  if no source code references it.
