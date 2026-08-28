# ursa-storage-test

This module contains focused integration tests for Kafka lakehouse ingestion:

- Native Kafka `MemoryRecords`, schema-registry decoding, and Iceberg record conversion run in-process.
- Kafka-framed key/value bytes are round-tripped through the S3 backend with LocalStack.
- Kafka-framed key/value bytes are round-tripped through the GCS backend with the fake GCS server.

The backend tests use Testcontainers and are skipped automatically when Docker is unavailable. Broker-to-reader coverage lives beside the Kafka reader implementation, where it uses a standalone Apache Kafka broker.
