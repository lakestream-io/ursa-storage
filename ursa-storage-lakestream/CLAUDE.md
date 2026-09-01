# ursa-storage-lakestream

Implements the Lakestream API on top of `ursa-storage-core`. This is the integration library for external consumers.

**All new integrations should depend on this module**, not on `ursa-storage-core` directly.

## Key Classes

| Class | Purpose |
|-------|---------|
| `IndexedStreamCatalog` | Oxia-backed `StreamCatalog` implementation — the primary catalog |
| `LogImpl` | `Log` implementation — wraps `LogStorage` + entry index cache + `UnifiedStreamReader` |
| `LeasedLog` | Catalog-opened `Log` handle that owns a durable write lease and drains appends on close |
| `LogCursorImpl` | `LogCursor` implementation — cursor state, mark-delete, individual acks |
| `StreamWriterImpl` | `StreamWriter` — routes writes via `StreamLayout` to owned `Log` handles |
| `StreamReaderImpl` | `StreamReader` — delegates to `UnifiedStreamReader` for transparent reads |
| `DefaultUnifiedStreamReader` | RAW/PARQUET transparent routing — reads from WAL or compacted objects |
| `IndividualAcksTracker` | Per-message acknowledgment tracking |
| `IndividualAcksTrackerSegment` | Bitmap segment for individual ack tracking |
| `CursorStateStore` | Persistence for cursor state (mark-delete, individual acks) |

## Layout Implementations

| Class | Purpose |
|-------|---------|
| `IndexedLayout` | Fixed partition count — maps partition index to `LogId` |
| `SingleLogLayout` | Single-log stream (non-partitioned) |
| `StreamLayoutFactory` | Creates layouts from stream metadata |
| `IndexedStreamPosition` | Position within an indexed stream |

## Compacted Object Reader

| Class | Package | Purpose |
|-------|---------|---------|
| `CompactedObjectReader` | `reader` | Interface for reading compacted (Parquet) data |
| `CompactedObjectReaderFactory` | `reader` | Factory for creating compacted readers |
| `NoopCompactedObjectReader` | `reader` | No-op implementation (no compaction available) |
| `NoopCompactedObjectReaderFactory` | `reader` | Factory returning no-op readers |

## Bootstrap

| Class | Purpose |
|-------|---------|
| `LakestreamBootstrap` | Wires up `IndexedStreamCatalog` with storage, Oxia, and compacted readers |
| `StreamCatalogService` | Service lifecycle management for `StreamCatalog` |

## Other

| Class | Purpose |
|-------|---------|
| `BinarySearch` | Binary search over entry headers |
| `MarkDeleteRecord` | Serialized mark-delete state |
| `UnifiedStreamReader` | Interface for transparent RAW/PARQUET reading |

## Package Layout

```
io.lakestream.ursa.lakestream.impl     — Core implementations (IndexedStreamCatalog, LogImpl, etc.)
io.lakestream.ursa.lakestream.reader   — CompactedObjectReader interfaces and no-op implementations
```

## Pitfalls

- **Layout resolution requires Oxia**: `IndexedStreamCatalog.getLayout()` reads partition metadata from Oxia — tests need Oxia (or mock)
- **Explicit data-plane ownership**: open logs, readers, and writers through `StreamCatalog` and close every returned handle
- **Cursor state persistence**: `LogCursorImpl` persists state to Oxia via `CursorStateStore` — understand the write-ahead pattern before modifying
- **IndividualAcksTracker**: uses bitmap segments for per-message ack tracking. Changes need careful concurrency review
- **CompactedObjectReader wiring**: `DefaultUnifiedStreamReader` needs a `CompactedObjectReaderFactory` — if compaction is not configured, use `NoopCompactedObjectReaderFactory`
