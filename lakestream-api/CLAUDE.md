# lakestream-api

Protocol-neutral stream storage API. Clean interfaces and records only — zero implementation, no external runtime dependencies.

**This is the preferred API for all new code.** Do not use `StorageApi` (core) directly.

Package: `io.lakestream.api`

## Type Catalog

### Level 2: Stream Catalog
| Type | Kind | Purpose |
|------|------|---------|
| `StreamCatalog` | interface | Namespace + stream CRUD, `TableCatalog` CRUD, namespace/stream materialization policy CRUD, layout resolution, reader/writer factory |
| `Stream` | interface | Opened stream handle: identifier, config, partitioning, schema, properties, materialization (override + effective), layout, writer, reader |
| `StreamIdentifier` | record | Logical stream identity: `(namespace, name)` — use `fullName()` for display |
| `Namespace` | record | Namespace metadata: name, properties, optional namespace-level materialization policy |

### Level 1: Log Storage
| Type | Kind | Purpose |
|------|------|---------|
| `LogStorage` | interface | Single-log operations: append, read, trim, delete — addressed by `LogId` |
| `LogId` | record | Type-safe log identity wrapping `long id` (maps to core's `streamId`) |

### Level 0+: Log and Cursor (per-log managed operations)
| Type | Kind | Purpose |
|------|------|---------|
| `Log` | interface | Per-log operations: append, read, fence, cursor management, retention, search |
| `LogCursor` | interface | Per-log cursor: sequential reads, mark-delete, individual acks, seek, persistence |

### Data Types
| Type | Kind | Purpose |
|------|------|---------|
| `LogEntry` | interface | Entry read from a log: offset, numberOfRecords, timestamp, size, payload (`ByteBuf`) |
| `LogEntryHeader` | record | Entry metadata without payload: offset, numberOfRecords, timestamp, size |
| `EntryHeader` | record | Lightweight header: offset, numberOfRecords |
| `LogOffset` | record | Offset descriptor: offset, numberOfRecords, timestamp |
| `EntryIndex` | interface | Full entry index: offset, numberOfRecords, timestamp, file position info |
| `LogEntryIndex` | record | Entry index with log ID context |

### Configuration
| Type | Kind | Purpose |
|------|------|---------|
| `StreamConfig` | record | Retention and cleanup policy (DELETE, COMPACT, COMPACT_DELETE) |
| `Partitioning` | record | Partition strategy + config map |
| `PartitioningStrategy` | enum | INDEXED, RANGE (future), SINGLE |
| `SchemaConfig` | record | Schema configuration for a stream |
| `RoutingKey` | record | Write routing: `ofIndex(int)` or `roundRobin()` |

### Identity and Position
| Type | Kind | Purpose |
|------|------|---------|
| `StreamPosition` | interface | Position within a stream (log + offset) |
| `Position` | record | Simple position record |
| `FileInfo` | record | File metadata |

### State and Lifecycle
| Type | Kind | Purpose |
|------|------|---------|
| `LogState` | record | Current state of a log |
| `LogStateManager` | interface | Manages log state transitions |
| `LifecycleState` | enum | Log lifecycle states |

### Layout and Catalog
| Type | Kind | Purpose |
|------|------|---------|
| `StreamLayout` | interface | Maps stream to logs: `logIds()`, `resolveForWrite(RoutingKey)` |
| `StreamWriter` | interface | Write to a stream via layout routing |
| `StreamReader` | interface | Read from a stream (transparent RAW/PARQUET routing) |
| `CatalogPaths` | interface | Oxia key path construction for catalog metadata |

### Exceptions (`io.lakestream.api.exception`)
| Type | Purpose |
|------|---------|
| `AlreadyExistsException` | Stream or namespace already exists |
| `NoSuchStreamException` | Stream not found |
| `NoSuchNamespaceException` | Namespace not found |
| `NamespaceNotEmptyException` | Cannot drop non-empty namespace |

## Key Design Rules

- **All async**: operations return `CompletableFuture`
- **Netty ByteBuf**: payloads use `io.netty.buffer.ByteBuf` to avoid conversion overhead with the internal storage engine
- **Identity separate from operations**: `LogId` identifies a log; `LogStorage`/`Log` operates on it. `StreamIdentifier` identifies a stream; `StreamCatalog` operates on it
- **No Partition type**: partitioning expressed through `StreamLayout` → `LogId` list
- **Minimal dependencies**: only the API's small, explicitly declared runtime surface

## Pitfalls

- `LogId.id()` maps directly to core's `long streamId` — they are the same value
- `Log` and `LogCursor` are Level 0+ APIs that evolved beyond the original two-level design; they provide per-log managed operations directly
- `StreamCatalog` API differs from the original design doc — check the actual interface, not `project-docs/lakestream-api-design.md`
