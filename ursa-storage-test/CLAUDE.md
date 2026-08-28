# ursa-storage-test

Focused integration and contract tests for Kafka lakehouse ingestion.

## Test Coverage

- `KafkaLakehouseIngestionTest` — native Kafka `MemoryRecords` decoding into Iceberg and Delta rows.
- `KafkaBatchedRawParquetFramingTest` — raw `MemoryRecords` Parquet round trip.
- `KafkaS3BackendIntegrationTest` — S3-compatible backend integration (`@Tag("docker")`).
- `KafkaGcsBackendIntegrationTest` — GCS backend integration (`@Tag("docker")`).

## Running Tests

```bash
# Local framing and lakehouse tests
mvn -B -ntp test -pl ursa-storage-test

# Backend tests (requires Docker)
mvn -B -ntp test -pl ursa-storage-test -Dgroups=docker
```

Use native Kafka `MemoryRecords` as the complete Ursa entry payload in fixtures.
Do not add a storage envelope or replace these fixtures with a value-only frame.
