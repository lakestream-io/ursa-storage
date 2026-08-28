# Ursa Glossary

An alphabetical reference of terms used in Ursa documentation and codebase.

## A-C

**CO (Compacted Object)**: An object specific to a log, created by merging data from multiple WAL Objects, designed for long-term retention and analytics.

**Compaction Service**: The background process that periodically merges WAL Objects into larger, log-specific Compacted Objects.

## D-E

**Entry**: A batch of multiple events or messages grouped together for efficient processing and atomic storage.

**Entry Format**: The format for serializing entries within WAL Objects, including metadata and offsets.

**Event**: An individual record or message representing a single unit of data in a stream.

**External Table**: A lakehouse table managed outside Ursa, where data is not indexed by the Stream Offset Index.

## F-L

**Internal Table**: A table managed by Ursa that represents Compacted Objects indexed for both streaming and analytical access.

**Leaderless Architecture**: Ursa's design that eliminates leader-based replication, reducing cross-AZ traffic and operational complexity.

## M-R

**Message**: Another name for an Event in a stream.

**Record**: Another name for an Event in a stream.

**RecordBatch**: Another name for an Entry, grouping multiple records together.

## S

**Schema**: A structural definition for stream data linked to an external schema registry.

**Stream Format**: The organizational format describing layout and storage of logs and streams.

**Stream Index**: See Stream Offset Index.

**Stream Offset Index**: A multi-level index mapping logical offsets to physical storage locations within WAL and Compacted Objects.

**Stream-Table Duality**: The concept where data can be accessed as both a stream and a table.

## T-W

**Table Format**: An open format like Apache Iceberg or Delta Lake for managing compacted data with transactions and schema evolution.

**WAL (Write-Ahead Log)**: The append-only log ensuring immediate durability of newly ingested events.

**WAL Object (WO)**: A row-based object aggregating records from one or more logs, used for efficient appends.

**WO**: See WAL Object.

**WO Format**: The binary storage format used for WAL Objects, optimized for low-latency operations.

## Z

**Zero-ETL Design**: A method that eliminates separate ETL processes by writing data directly from streams to tables.

---

*For detailed explanations and architectural context, see the [Concepts Guide](./concepts.md).*
