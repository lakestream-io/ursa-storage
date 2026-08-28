# Ursa Storage Concepts

This document provides definitions for key terms and concepts used in Ursa Storage, a lakehouse-native streaming storage. The detailed design can be found in the [Ursa paper](https://www.vldb.org/pvldb/vol18/p5184-guo.pdf).

## Core Concepts

### Event, Message, or Record

An event is an individual unit of data in a stream. This unit is also referred to as a **Record** or **Message**. Events are the fundamental data entities that move through event brokers and are ultimately persisted in Ursa storage.

### Entry, Records, or RecordBatch

An entry (also known as a batch of records or RecordBatch) is a collection of multiple records or messages grouped together for efficient processing and storage. In Ursa Storage, an entry serves as the unit of storage atomicity—every event within a batch is committed and stored together. Batching is typically performed on the client side before writing to the storage layer. Compression algorithms are often applied to these batches to further reduce storage requirements. This approach improves throughput and minimizes overhead within Ursa Storage.

### Log

A **Log** is a core abstraction that represents an ordered, append-only sequence of entries, each persistently recording events within a stream. The log acts as the authoritative source of truth, ensuring durability, immutability, and reproducibility. Each record is indexed with an atomically increasing sequence number called the **offset**.

### Offset

Each record is indexed with an atomically increasing sequence number, known as "offset".

## Log Format

The **Log Format**—referred to as **Stream Format** in the Ursa paper—describes the storage layout of a **Log**. Each **Log** is assigned a unique **Stream ID** at creation by the metadata service. Each log consists of physical data objects stored either in WAL (write-ahead log) storage or Lakehouse storage. These objects include:

### WAL Object (WO)

A WAL Object (WO) aggregates records from one or multiple logs in a row-based format, enabling efficient appends. WOs serve as the primary, immediately durable storage for newly arriving events. Each WO contains metadata for fast indexing and integrity, has a binary format optimized for low-latency writes and reads, and is stored in a configured object, block, or file backend. WAL Objects are written sequentially as data arrives and are retained for a configurable hot window to support low-latency consumption and recovery.

### Compacted Object (CO)

A Compacted Object (CO) is used to store data specific to an individual log. A CO is created by periodically merging (compacting) data from the same log across multiple WOs. The CO can be stored in either a row-based format (i.e., a WO specific to a log), a columnar format (such as a Parquet object), or both. COs are designed to support long-term retention, fast scans, and analytical workloads.

### Stream Offset Index

The **Stream Offset Index** is a multi-level index that maintains the mapping from logical offsets to their corresponding physical locations within WAL Objects and Compacted Objects. This index enables efficient offset- and time-based lookups, underpins core streaming semantics such as offset commits, cursor tracking, consumer recovery, and consumer resume. The index is updated incrementally as new data is appended and when old data is compacted. Integrations can add secondary indexes without changing the primary offset space.

### Table Metadata

When Compacted Objects (COs) in Ursa are stored as Parquet files, they can be committed to a lakehouse table without duplicating or re-copying data. This table is known as an "Internal Table" or "SBT" (Stream-Backed Table). This approach is how Ursa Storage achieves "zero-copy" stream-table duality.

## Schema

Each log (or stream) in Ursa Storage can be linked to a schema managed by an external schema registry. Ursa Storage uses this schema information to efficiently transform records from row-based serialization formats to columnar storage formats. Additionally, it utilizes schema compatibility modes to safely evolve the structure of the underlying lakehouse tables over time.

## Table Formats

Ursa Storage leverages the open table formats for organizing compacted data, primarily Apache Iceberg or Delta Lake. These formats provide:
- ACID transactions
- Schema evolution
- Time travel and versioning
- Partition pruning and file-level statistics
- Metadata management for efficient queries

Ursa Storage supports two table modes: **Internal Table** vs **External Table**. It can be configured at either cluster-level or per stream-level.

### Internal Table

Internal Table is the internal table storage representation of Compacted Objects of a given stream. It also known as SBT (Stream-Backed Table). Because these Compacted Objects are both indexed by Stream Offset Index and Table Metadata, Ursa manages the entire lifecycle of this table. This table is typically registered as an _external table_ in an external data catalog for data consumption. 

In this mode, stream-table duality is natively supported. All the data can be accessed via either Streaming API or Table API. Consumers can read data as either streams or tables. 

Internal Table is great for raw data. i.e. bronze layer in [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture).

#### External Table

External Table is an external table outside of Ursa's management, where both data and metadata is managed by external systems. It is also known as "SDT" (Stream-Delivered-to Table). All the data written to the external table is not indexed by Stream Offset Index anymore. So you can't stream read those data from the external table. If you want to stream data back, you still need to keep another copy of data indexed by the Stream Offset Index.

External Table provides the flexiblity to allow appending, updating, or inserting (upsert) to those tables. You can also rerouting and repartitioning the data for efficient reads. It is useful for seamless streaming ingestion to lakehouse tables.

External Table is great for curated data. i.e. silver and gold layers in [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture).

See [Lakehouse Tables](./lakehouse-tables.md) for more details.

## System Components

Ursa Storage is composed of the following key components:

### Stream Catalog Service

The Stream Catalog Service (SCS) is a centralized metadata service responsible for offset assignment, offset index management, log and stream metadata, and additional metadata such as transaction state management. By delegating these tasks to a dedicated service, Ursa Storage enables a leaderless architecture, eliminating the need for brokers to perform leader-based coordination. This design ensures consistent offset ordering for each topic-partition and simplifies failover scenarios.

### Stream Storage Service

The Stream Storage Service combines a write-ahead log (WAL) for newly ingested messages—stored in row-based formats—with a lakehouse storage layer for long-term, per-log data retention in cloud object stores such as Amazon S3.

#### Write-Ahead Log Storage

All data is initially written to a shared write-ahead log as row-based WAL objects. The configured backend can use cloud object storage (AWS S3, GCS, Azure Blob Storage), block storage, or a distributed file system.

#### Lakehouse Storage

After compaction, Compacted Objects are stored in the lakehouse storage layer, optimized for long-term retention and analytical workloads.

### Compaction Service

The Compaction Service is a background process that periodically merges smaller, multiplexed WAL Objects into larger, log-specific Compacted Objects. It provides the following functions:

- Converts row-oriented WAL data into columnar Parquet format
- Optimizes file sizes for improved query performance
- Registers compacted files with the table format catalog
- Maintains offset mappings to support seamless stream consumption
- Enforces retention and deletion policies
