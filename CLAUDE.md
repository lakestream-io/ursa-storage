# Ursa Storage

Lakehouse-native stream storage built on object storage. The project targets Java 17
and is built as a Maven multi-module reactor.

## Build commands

```bash
# Fast compile/package feedback
mvn -B -ntp clean install -DskipTests

# Test one module
mvn -B -ntp test -pl ursa-storage-core

# Repository quality gates
mvn -B -ntp license:check
mvn -B -ntp checkstyle:check
mvn -B -ntp clean install -DskipTests
mvn -B -ntp spotbugs:check
```

## Architecture

The core API and implementation are protocol neutral. Broker integrations translate
their native record representation at the repository boundary; they must not leak
protocol types into `lakestream-api`, `ursa-storage-common`, `ursa-storage-core`, or
`ursa-storage-lakestream`.

| Layer | Module | Responsibility |
|-------|--------|----------------|
| Public API | `lakestream-api` | Stream metadata, log, cursor, catalog, and materialization contracts |
| Shared code | `ursa-storage-common` | Utilities, configuration helpers, and common exceptions |
| Storage engine | `ursa-storage-core` | WAL and object-storage implementation; internal `StorageApi` |
| API implementation | `ursa-storage-lakestream` | Catalogs, layouts, logs, cursors, readers, and writers |
| Materialization SPI | `ursa-storage-materialization` | Sink SPI, schema handling, and Kafka record decoding |
| Lakehouse sink | `ursa-storage-lakehouse` | Iceberg and Delta materialization |
| Kafka compacted reader | `ursa-storage-lakehouse-kafka-reader` | Isolated Kafka-format compacted-object reader |
| ClickHouse sink | `ursa-storage-clickhouse` | ClickHouse materialization implementation |
| Orchestrator | `ursa-storage-compact` | WAL-to-compacted-object scheduling and sink dispatch |
| Test support | `ursa-storage-containers`, `ursa-storage-test` | Containers and end-to-end coverage |
| Tools | `ursa-storage-tools` | Benchmarks and operational utilities |

New integrations should use `lakestream-api`; `StorageApi` remains an internal engine
contract. Kafka-specific codecs and readers are intentionally isolated in integration
modules rather than placed in the protocol-neutral layers.

## Core domain model

```text
StreamCatalog
  -> StreamMetadata
      -> StreamLayout
          -> LogId
  -> openLog -> Log
      -> LogCursor
      -> LogStorage
  -> openReader / openWriter

StreamWriter / StreamReader route stream operations through the selected layout.
UnifiedStreamReader routes reads between raw WAL data and compacted objects.
```

Payload ownership is explicit: callers must release or close reference-counted entry
buffers at the ownership boundary documented by the API.

## Materialization

`ursa-storage-materialization` defines the `TableMaterializer` SPI. Implementations are
loaded with `ServiceLoader`, allowing lakehouse and ClickHouse sinks to remain separate.
Kafka entries are decoded directly from native Kafka `MemoryRecords` before schema evolution and
table encoding. Protocol-neutral modules must not depend on a broker client or broker
metadata model.

The compaction orchestrator uses these implementation-class properties:

| Property | Purpose |
|----------|---------|
| `materializationServiceClass` | Selects stream-to-table dispatch |
| `compactionStorageBindingsClass` | Selects publish, commit, and cleanup bindings |
| `compactionServiceClass` | Deprecated compatibility alias |

## Code conventions

- Every source file needs the repository license header.
- Every main Java package needs `package-info.java`.
- Do not use wildcard imports.
- Static imports precede regular imports and both groups are alphabetized.
- Use SLF4J rather than standard-output logging.
- Test classes end in `Test`, not `Tests`.
- Keep lines at or below 120 characters.
- Manage dependency versions through the repository BOMs.
- Fix real SpotBugs findings rather than adding broad exclusions.
- Confluent Community License artifacts (`kafka-json-schema-*`, `kafka-protobuf-*`) are test scope
  only; the enforcer rule fails the build otherwise. JSON Schema / Protobuf decoding uses
  `ursa-storage-materialization`'s `serde.kafka.schema` package instead.

## Documentation

- [Build locally](docs/developer/build.md)
- [Contributing](docs/developer/contribute.md)
- [Third-party license notes](docs/developer/third-party-licenses.md)
- [Concepts](docs/concepts.md)
- [Module map](docs/agent/module-map.md)
- [Table materialization](docs/user/table-materialization.md)
- [Materialization design](docs/lip/LIP-161-Table-Materialization-Framework.md)

## Local infrastructure

This repository does not maintain a Docker Compose stack. Integration tests use
Testcontainers to provision their dependencies. Start Docker and verify the daemon with:

```bash
docker info
```

The repository integration tests create protocol-facing clients only in their dedicated
integration modules; the storage runtime itself remains broker independent.
