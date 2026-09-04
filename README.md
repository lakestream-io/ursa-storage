# Ursa Storage: A lakehouse-native streaming storage

**Stream as a storage primitive for the lakehouse paradigm**

Ursa Storage introduces log and stream as first-class storage primitives for the lakehouse paradigm, turning any object, block, or file store into stream storage. It augments existing lakehouse architectures by unifying real-time data streaming and efficient analytical access on the same data—without brokers or protocol coupling. The result is a simple, powerful, developer-friendly abstraction that adds data streaming capabilities to modern data platforms while remaining open, portable, and easy to adopt.

## Principles

The storage is designed with the following principles.

### Stream-Table Duality

The core concept in Ursa, where data simultaneously exists as both a stream (for real-time streaming) and a table (for analytical queries). This duality:
- Eliminates the need for separate streaming and batch storage systems
- Provides unified data access through either the Streaming API or Table API 
- Reduces data duplication and movement
- Enables seamless data processing for both streaming and batch workloads

### Diskless & Leaderless Architecture

By turning any object, block, or file store into shared log or stream storage, Ursa enables diskless event-streaming integrations without coupling the storage engine to a broker protocol. This provides:

- No single point of failure or bottleneck for partitions
- Reduced cross-AZ network traffic (up to 10x cost reduction)
- Lower operational complexity (no leader elections or rebalancing)
- Rebalance-free architecture for event brokers

### Zero-ETL Design

An architectural approach that eliminates the need for separate ETL processes between streaming data and lakehouse tables:
- Direct writes to lakehouse tables from streaming ingestion
- No intermediate staging areas or connectors
- A single copy of data serving both streaming and analytics
- Simplified data governance and lineage tracking

## Key Features

- **Stream-First Storage Primitive**: Unified abstraction for data streams and lakehouse tables
- **Multi-Cloud Support**: AWS S3, Google Cloud Storage, Azure Blob Storage
- **Lakehouse Integration**: Delta Lake and Apache Iceberg with multi-catalog support
- **High Performance**: Read/write caching, batching, and prefetching optimizations
- **Production-Ready**: Comprehensive metrics, observability, and distributed coordination

## Concepts & Architecture

- [Storage Concepts](docs/concepts.md)
- [Lakehouse Tables](docs/lakehouse-tables.md)
- [Feature Matrix](docs/feature-matrix.md)

## Modules

| Module | Description |
|--------|-------------|
| `lakestream-api` | Protocol-neutral stream, log, and catalog API |
| [ursa-storage-core](ursa-storage-core/README.md) | Core storage engine with multi storage backend support |
| [ursa-storage-common](ursa-storage-common/README.md) | Shared utilities, exceptions, and common interfaces |
| `ursa-storage-lakestream` | Lakestream API implementation and catalog |
| `ursa-storage-materialization` | Stream-to-table materialization SPI and Kafka codecs |
| [ursa-storage-lakehouse](ursa-storage-lakehouse/README.md) | Lakehouse integration with catalog support |
| [ursa-storage-lakehouse-kafka-reader](ursa-storage-lakehouse-kafka-reader/README.md) | Isolated Kafka compacted-data reader |
| [ursa-storage-kafka-runtime](ursa-storage-kafka-runtime/README.md) | Leaf runtime that wires Lakestream storage and Kafka compacted reads |
| `ursa-storage-clickhouse` | ClickHouse materialization sink |
| [ursa-storage-compact](ursa-storage-compact/README.md) | Distributed compaction service |
| [ursa-storage-containers](ursa-storage-containers/README.md) | Test infrastructure and testcontainers |
| [ursa-storage-test](ursa-storage-test/README.md) | Integration and end-to-end tests |
| [ursa-storage-tools](ursa-storage-tools/README.md) | Performance testing and benchmarking tools |

## Build & Run

See [Build & Run Locally](docs/developer/build.md) for complete build and local run instructions.

## Development

See [Contributing Guide](docs/developer/contribute.md) for information on how to contribute to the project.

## Support

- **Issues**: Report bugs and feature requests on GitHub Issues
- **Documentation**: See [docs/](docs/) for detailed guides
- **Community**: Join the Lakestream community

## License

Ursa Storage is licensed under the [Apache License, Version 2.0](LICENSE).

The binary distribution contains only Apache 2.0 and other permissively licensed third-party jars.
JSON Schema and Protobuf records are decoded without the Confluent Community License provider
artifacts; see [Third-party license notes](docs/developer/third-party-licenses.md) for details and
the build guard rails.
