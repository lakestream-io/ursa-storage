# Lakehouse Tables: Internal Table vs External Table

Ursa Storage supports two modes of lakehouse table integration: **Internal Table** and **External Table**. Understanding these concepts is essential for leveraging stream-table duality and seamless analytics in modern data architectures.

## Internal Table

An **Internal Table** (sometimes called a Stream-Backed Table, or SBT) is a lakehouse table managed entirely by Ursa Storage. In this mode:

- **Compacted Objects (COs)** produced by Ursa's compaction process for each stream are *directly* tracked and indexed by Ursa.
- The table metadata—including schema, versions, and data files—is managed by Ursa and registered with the lakehouse catalog (e.g., Apache Iceberg or Delta Lake).
- All data in the Internal Table remains indexed by the **Stream Offset Index**, supporting both:
  - **Streaming reads:** via the Ursa Stream API, enabling consumers to resume reading from any offset, support replay, and catch-up.
  - **Analytical queries:** via standard Table APIs (Spark, Trino, DuckDB, etc.), making the same data queryable as a lakehouse table.
- There is no data duplication—the same physical data serves both streaming and analytics use cases.
- The lifecycle of data (retention, compaction, cleanup) is managed cohesively by Ursa.

**When to use Internal Tables?**  
- For raw data or bronze-layer storage in a medallion architecture.
- When you want both streaming semantics (offset tracking, replays) and analytical queries on the *same* physical data.
- For use cases that require zero-copy, unified access (true stream-table duality).

## External Table

An **External Table** (sometimes called a Stream-Delivered Table, or SDT) is a table managed outside of Ursa Storage. In this mode:

- Data flows from Ursa’s compaction process *into* an external lakehouse table, but Ursa **does not manage** its metadata or files after delivery.
- Compacted data is written (or appended) to a table in an external data catalog.
- Data in the External Table is **not indexed** by the Stream Offset Index—therefore:
  - Streaming reads (with offset semantics or consumer resume) are **not supported** on this data through Ursa.
  - Analytical access via Table APIs continues to work, managed by the external catalog.
- Ursa can perform upserts and certain optimizations (such as repartitioning) during delivery. You can also perform further table maintenance and optimizations (e.g., additional repartitioning, upserts, data enrichment) using external processing frameworks.
- The lifecycle (retention, deletion policies) is governed by the external lakehouse system, not Ursa.

**When to use External Tables?**  
- For curated data (silver/gold layers); after cleansing, transformation, and aggregation.
- When external teams or tools govern data lifecycle and access patterns.
- When you plan to ingest from Ursa into target tables and no longer need streaming semantics on that data.

## Summary Table

| Feature                | Internal Table (SBT)  | External Table (SDT)     |
|------------------------|:---------------------:|:------------------------:|
| Managed by Ursa        | Yes                   | No                       |
| Offset Indexing        | Yes                   | No                       |
| Streaming Reads        | Yes                   | No                       |
| Analytical Table API   | Yes                   | Yes                      |
| Data Copies            | single copy      | Two copies. One for streaming and the other for tabular access |
| Primary Use Cases      | Raw data, bronze tier | Curated data, silver/gold|
| Data Lifecycle Control | Ursa                  | External system          |

> **For more on Ursa concepts and architecture, see [Ursa Storage Concepts](./concepts.md).**
