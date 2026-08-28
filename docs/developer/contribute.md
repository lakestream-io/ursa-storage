# Contributing

## Project structure

```text
ursa-storage/
├── lakestream-api/                         # Public protocol-neutral API
├── ursa-storage-common/                    # Shared utilities
├── ursa-storage-core/                      # WAL and object-storage engine
├── ursa-storage-lakestream/                # Catalog, log, cursor, and stream implementation
├── ursa-storage-materialization/           # Materialization SPI and Kafka codecs
├── ursa-storage-lakehouse/                 # Iceberg and Delta integration
├── ursa-storage-lakehouse-kafka-reader/    # Isolated Kafka compacted reader
├── ursa-storage-kafka-runtime/             # Leaf Lakestream runtime for Kafka ingestion/read
├── ursa-storage-clickhouse/                # ClickHouse materializer
├── ursa-storage-compact/                   # Compaction orchestrator
├── ursa-storage-containers/                # Test infrastructure
├── ursa-storage-test/                      # End-to-end tests
└── ursa-storage-tools/                     # Performance and diagnostic tools
```

Keep public APIs and the core storage path independent of broker protocols.
Record-format adapters belong in an integration module; table-format behavior
belongs in its sink module.
The Kafka runtime is a leaf: core and Lakestream APIs must never depend on it or
on the Kafka reader module.

## Workflow

1. Create a focused branch.
2. Add tests for the behavior being changed.
3. Run the affected module tests and repository quality gates.
4. Update user and architecture documentation when contracts change.
5. Submit a pull request describing compatibility and ownership implications.

## Code style

- Add JavaDoc for public APIs.
- Include the repository license header.
- Avoid wildcard imports and standard-output logging.
- Name test classes with the `Test` suffix.
- Keep buffer ownership and cleanup explicit.
- Preserve serialized field numbers and identifiers during schema evolution.
