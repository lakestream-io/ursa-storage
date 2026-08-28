# Ursa Storage module map

Use this map to choose the narrowest module for a change. The API, common,
core, and lakestream layers are broker-protocol neutral.

## Dependency direction

```text
lakestream-api --------+-----------------------> materialization ----> clickhouse
                       |                                |
ursa-storage-common ---+--> core --> lakestream         +-----------> lakehouse
                                      |                                |
                                      +-------------------------------+--> compact

lakehouse-kafka-reader --> lakestream-api + lakehouse reader contracts
kafka-runtime ---------> core + lakestream + lakehouse-kafka-reader
containers ------------> test infrastructure
tools -----------------> standalone benchmarks and utilities
```

## Module responsibilities

| Module | Owns | Must not own |
|--------|------|--------------|
| `lakestream-api` | Public value types and stream/log/catalog interfaces | Runtime implementations, broker clients, object-store SDKs |
| `ursa-storage-common` | Shared utilities and exceptions | Protocol record formats |
| `ursa-storage-core` | WAL, file storage, indexes, cache, storage-engine SPI | Broker metadata and client APIs |
| `ursa-storage-lakestream` | Catalog, layouts, logs, cursors, stream readers/writers | Broker protocol adapters |
| `ursa-storage-materialization` | Materializer SPI, schema evolution, neutral records, Kafka codecs | Table-format-specific commits |
| `ursa-storage-lakehouse` | Iceberg and Delta writers/readers and catalogs | Broker control-plane integration |
| `ursa-storage-lakehouse-kafka-reader` | Kafka compacted-object decoding | Generic catalog or log behavior |
| `ursa-storage-kafka-runtime` | Kafka ingestion/read runtime assembly and provider registration | Public API definitions or dependencies from core/Lakestream back into this leaf |
| `ursa-storage-clickhouse` | ClickHouse sink and schema mapping | Orchestrator behavior |
| `ursa-storage-compact` | Task scheduling, state transitions, commit/cleanup orchestration | Sink implementation details |
| `ursa-storage-containers` | Reusable integration-test containers | Production behavior |
| `ursa-storage-test` | End-to-end scenarios | Public API definitions |
| `ursa-storage-tools` | Performance and diagnostic commands | Core runtime dependencies |

## Main data flows

### Append and stream read

```text
integration adapter
  -> StreamCatalog / StreamWriter
  -> StreamLayout
  -> Log
  -> LogStorage
  -> WAL cache and object storage

StreamReader
  -> UnifiedStreamReader
  -> raw WAL reader or CompactedObjectReader
  -> integration adapter
```

### Materialization

```text
CompactionScheduler
  -> MaterializationService
  -> TableMaterializerFactory (ServiceLoader)
  -> lakehouse or ClickHouse materializer
  -> external table/catalog
```

Kafka record parsing happens in the materialization or isolated Kafka-reader
module. After decoding, the framework passes protocol-neutral records through
schema evolution and sink-specific encoding.

## Change routing

- Add or change a public stream concept in `lakestream-api` first.
- Put object-storage, WAL, cache, and index behavior in `ursa-storage-core`.
- Put lifecycle and routing implementations in `ursa-storage-lakestream`.
- Put reusable table sink contracts in `ursa-storage-materialization`.
- Put Iceberg/Delta behavior in `ursa-storage-lakehouse`.
- Put Kafka wire decoding in the Kafka codec or Kafka reader module.
- Keep the orchestrator dependent on SPIs rather than concrete sinks.

## Ownership checks

Entries can carry reference-counted buffers. Every producer/consumer boundary
must document whether ownership is transferred, retained, or copied. Tests that
read entries should close them after assertions unless the API explicitly
returns an unowned copy.

## Useful commands

```bash
mvn -B -ntp test -pl <module>
mvn -B -ntp checkstyle:check -pl <module>
mvn -B -ntp spotbugs:check -pl <module>
mvn -B -ntp clean install -DskipTests
```
