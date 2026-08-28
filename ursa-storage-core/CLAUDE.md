# ursa-storage-core

**Internal storage engine — new code should use the Lakestream API (`lakestream-api`) instead.**

Core storage engine. Implements `StorageApi`, `WalStorage`, `FileStorage` — the low-level
storage primitives. The `StorageApiLogStorage` adapter in this module bridges the engine
to the Lakestream `LogStorage` interface.

## Key Interfaces

| Interface | Package | Purpose |
|-----------|---------|---------|
| `StorageApi` | `io.lakestream.ursa.storage` | Top-level storage operations (append, read, trim, delete) |
| `WalStorage` | `io.lakestream.ursa.storage` | WAL read/write abstraction |
| `FileStorage` | `io.lakestream.ursa.storage` | Cloud file operations (S3, GCS, Azure, local) |
| `PersistCache` | `io.lakestream.ursa.storage.impl` | Write/read caching layer |

## Package Layout

```
io.lakestream.ursa.metrics          — OpenTelemetry metrics instrumentation
io.lakestream.ursa.storage          — Core interfaces and data models
io.lakestream.ursa.storage.impl     — Main implementations
  ├── compaction/                      — Compaction service and task providers
  ├── exception/                      — Custom exception types
  └── utils/                          — FIFOCache, RangeScan utilities
io.lakestream.ursa.utils.cache      — PrefetchCache, SerDesUtils
```

## Cloud Storage Backends

Four `FileStorage` implementations:
- `S3FileStorage` — AWS S3 (uses AWS SDK v2, s3-transfer-manager)
- `GCSFileStorage` — Google Cloud Storage
- `AzureFileStorage` — Azure Blob Storage
- `LocalFileStorage` — Local filesystem (testing/dev)

Changes to `FileStorage` affect all backends — test with at least S3 (via LocalStack).

## Code Generation

LightProto plugin generates code from `.proto` files in `src/main/proto/`:
- `storage_format.proto` — EntryIndex, EntryOffsets, compression/file types

Regenerate: `mvn generate-sources -pl ursa-storage-core`

## Testing

```bash
mvn -B -ntp test -pl ursa-storage-core
```

## Pitfalls

- `FileStorage` changes affect all backends — don't test only one
- `PersistCache` changes need concurrency review (`WriteCache` + `EntryCache` are accessed from multiple threads)
- `EntryIndex` uses `@Accessors(fluent = true)` — getters have no `get` prefix
- WAL format changes require proto regeneration
- **Don't call `StorageApi` directly from new code** — use `LogStorage`/`Log` from `lakestream-api`. The `StorageApiLogStorage` adapter in this module bridges `StorageApi` → `LogStorage`
