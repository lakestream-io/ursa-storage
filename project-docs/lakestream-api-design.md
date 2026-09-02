# Lakestream API design

## Purpose

Lakestream provides a protocol-neutral Java API for ordered logs, logical
streams, catalogs, and compacted-object reads. Integrations translate their
native records at the edge; the storage API does not expose broker client,
metadata, or position types.

## Design constraints

- Public API types are interfaces, records, enums, and documented exceptions.
- Stream identity is independent from any broker topic URI format.
- A logical stream can map to one or many logs through a `StreamLayout`.
- A log offset counts records. An entry may span a range of record offsets.
- Storage implementations may use reference-counted buffers internally, but
  ownership must be explicit at every public boundary.
- Compacted-object readers are extension points and do not belong to a broker
  adapter.

## Layering

```text
lakestream-api
  StreamCatalog, StreamMetadata, StreamLayout
  Log, LogCursor, LogStorage
  StreamReader, StreamWriter
  CompactedObjectReader, CompactedObjectReaderFactory

ursa-storage-lakestream
  IndexedStreamCatalog
  LogImpl, LogCursorImpl
  IndexedLayout, SingleLogLayout
  UnifiedStreamReader

ursa-storage-core
  WAL and file-storage implementation
  cache, indexing, object-store access
```

The API module depends only on its documented Netty {@code ByteBuf} payload
contract. It must remain free of Oxia, cloud SDK, broker, and table-format
dependencies. Implementations transfer or borrow buffers exactly as documented
without leaking any other runtime-specific classes.

## Domain model

### Log

A `Log` is an ordered append-only sequence identified by `LogId`. It owns
per-log lifecycle operations such as append, read, fence, retention, binary
search, and cursor creation.

### LogCursor

A `LogCursor` is named progress over a log. It tracks the read position,
acknowledged ranges, and durable mark-delete state. Cursor state is a storage
concept rather than a broker subscription type.

### Stream metadata and data-plane access

A logical stream is identified by `StreamIdentifier(namespace, name)`.
`StreamMetadata` is its immutable, resource-free catalog snapshot, and its
`StreamLayout` maps routing keys or partitions to one or more logs. Callers
open closeable data-plane resources explicitly through `StreamCatalog.openLog`,
`StreamCatalog.openReader`, or `StreamCatalog.openWriter`.

### StreamCatalog

`StreamCatalog` owns namespace and stream lifecycle, layouts, configuration,
and materialization policy. Metadata is persisted with Ursa-owned records and
Ursa-owned key paths so no external control-plane schema becomes part of the
storage contract.

### StreamReader and StreamWriter

These interfaces route operations through the selected layout. The unified
reader chooses raw or compacted storage based on availability and offsets while
preserving one monotonic record-offset space.

## Compacted-object integration

`CompactedObjectReaderFactory` is a generic extension point. A table-format
module supplies file and metadata access; a record-format module supplies any
wire-format decoder it requires. The Kafka implementation is isolated in
`ursa-storage-lakehouse-kafka-reader`, keeping the base lakehouse and API
modules reusable by other integrations.

## Catalog paths

The default catalog layout uses Ursa-owned paths:

```text
/streams/{namespace}/{stream}
/streams/{namespace}/{stream}-partition-{partition}
/admin/streams/{namespace}/{stream}
/admin/streams/_namespaces/{namespace}
/admin/streams/_tablecatalogs/{catalog}
/admin/streams/_tombstones/{namespace}/{stream}
```

The leading-underscore segments — `_namespaces`, `_tablecatalogs`,
`_tombstones` — sit where a namespace name would, so `CatalogPaths` reserves
them: a stream namespace must never be named after one, or its streams would
collide with those records. Tombstones live outside the per-namespace stream
prefix on purpose, so listing a namespace never reads completed deletions.

Exact serialization and transactional update rules are implementation details
of `ursa-storage-lakestream`. Readers must reject incompatible metadata
versions rather than guess another system's layout.

## Compatibility rules

- Additive fields require stable defaults and round-trip tests.
- Existing field numbers and serialized identifiers are never reused.
- API evolution must preserve offset and buffer-ownership semantics.
- Integration-specific compatibility belongs in the integration module.
- A new protocol adapter may depend on the public API, but the API cannot
  depend back on that adapter.

## Implementation status

The repository contains the public API, Oxia-backed catalog, metadata snapshots,
log and cursor implementations, layouts, stream readers/writers, and
compacted-object reader extension points. Kafka record decoding and Kafka
compacted reads live in their dedicated modules.
