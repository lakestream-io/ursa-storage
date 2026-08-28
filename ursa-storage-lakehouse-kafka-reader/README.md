# Ursa Lakehouse Kafka Reader

This artifact contains the compacted-object reader used by the Ursa Kafka integration:

- artifact: `io.lakestream:ursa-storage-lakehouse-kafka-reader`
- factory: `io.lakestream.ursa.kafka.reader.KafkaLakehouseReaderFactory`
- supported format: V2 `KAFKA_BATCHED_RAW_PARQUET`
- supported storage backends: local files, S3/S3A, GCS, and Azure Data Lake Storage
  Gen2 through `AZUREDFS`/ABFS (`abfss://`)

`AZUREBLOB` and `AZURELOCAL` are rejected at configuration time. Those backend names require
the legacy WASB connector, which Hadoop 3.5 no longer provides. Azure deployments must use an
HNS-enabled storage account and configure `AZUREDFS`.

Kafka compaction writes this format with a `ManagedTableFileIndex` in the Lakestream
`EntryIndex`. The reader uses that index to select a Parquet file, seeks its companion
`.index` file by Kafka offset, and returns owned `LogEntry` buffers. Callers must close every
returned entry exactly once.

The artifact intentionally does not support the historical V1 generic lakehouse format.
An `EntryIndex` without `ManagedTableFileIndex`, or a Parquet file with a different serde type,
fails explicitly.

The test-only dependency on `ursa-storage-lakehouse` verifies that files produced by the
Kafka encoder and Parquet writer remain readable.
