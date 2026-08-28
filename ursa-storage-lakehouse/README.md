# Lakehouse Integration for Ursa Storage

This module enables compaction of streaming data from the WAL Objects into lakehouse tables (Delta Lake, Apache Iceberg). It provides table writers, catalog integrations, schema evolution, and data format conversions (Avro, Protobuf, JSON, Kafka) to make stream data accessible to analytical engines like Apache Spark, Trino, and Athena.