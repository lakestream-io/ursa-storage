# Iceberg duplicate data-file repair tool (`table dedup-files`)

**Date:** 2026-06-05
**Status:** Approved (design)
**Module:** `ursa-storage-tools`

## Problem

Under a double-commit bug in compaction, the same parquet data file can be committed to
an Iceberg table twice, in two separate snapshots. Both snapshots' manifests remain live in
the current snapshot, so the same file path appears twice when the current snapshot is
scanned. Query engines then fail with:

```
Duplicate file path seen in the Iceberg metadata snapshot. ...
File path: 'compaction/bi/connectors/src_newsbreak_traffic__v1/data/00000-...-00001.parquet',
SnapshotId: '5995583720813093636'.
```

### Concrete instance (reference)

Metadata file `00190-426679db-633c-4f56-b2ea-8c95de172209.metadata.json`:

- 188 snapshots, format-version 2, current snapshot `5870197209620298703`, `total-delete-files = 0`.
- Exactly one duplicated `lakestream.tags` value
  `{"bi/connectors/src_newsbreak_traffic__v1-partition-0":"14018266:1885"}`
  appears in two consecutive snapshots:
  - seq **142**, snapshot `5995583720813093636`, op `overwrite`
  - seq **143**, snapshot `5585739062223581560`, op `overwrite`
- Both committed the same parquet file → duplicate file path in the current snapshot.

## Goals

1. **Detect** duplicate data-file references in a stream's Iceberg table.
2. **Fix** them by committing a new snapshot through the catalog that leaves each file
   referenced exactly once.
3. Default to a safe **dry-run** (report only); require an explicit flag to mutate the table.

## Non-goals

- Tables that contain delete files (position/equality deletes). The tool aborts on these.
- Removing immutable historical snapshots or their `lakestream.tags`. History stays as-is.
- Fixing the upstream double-commit bug in compaction (separate work).

## Command surface

New subcommand under the existing `table` group command (sibling of `expire-snapshots`,
`probe-table` in `io.lakestream.ursa.storage.admin.Table`):

```
ursa-admin table dedup-files \
    --stream bi/connectors/src_newsbreak_traffic__v1 \
    [--catalog-name <name>] \
    [--apply]
```

| Option | Required | Default | Notes |
|--------|----------|---------|-------|
| `--stream` | yes | — | Ursa stream identifier. Resolves the target table from the stream materialization policy. |
| `--catalog-name` | **yes** | — | Must be specified explicitly. The catalog name is **never** auto-resolved from compact-task properties: silently resolving the catalog on a destructive repair risks targeting the wrong catalog, so the operator must name it. |
| `--apply` | no | false (dry-run) | When absent, detect + report only. When present, commit the fix. |

Table loading mirrors `ExpireSnapshots` (`LakehouseConfiguration` + `IcebergTable.loadTable()`),
but catalog resolution does **not**: the required `--catalog-name` is used as-is and set on
`LakehouseConfiguration.CATALOG_NAME`. The command reads the config file via
`parent.getParent().getConfigFile()`.

## Detection (two signals)

### Primary / authoritative — duplicate file paths in the current snapshot

Iterate `table.newScan().useSnapshot(currentSnapshotId).planFiles()`, collect each
`FileScanTask.file()` (`DataFile`) grouped by `path()`. Any path appearing more than once is
live corruption. Keep exactly one `DataFile` object per duplicated path, preserving its full
metrics (record count, file size, partition data, column stats). This is the signal the fix
acts on and verifies against.

### Secondary / diagnostic — duplicate `lakestream.tags` across snapshots

Group all `table.snapshots()` by their `summary().get("lakestream.tags")` value; report any
value shared by ≥2 snapshots, listing their sequence numbers and snapshot ids. This explains
the root cause but is never mutated (history is immutable). Where possible, map each
duplicated current-snapshot file path back to the snapshots/tags that introduced it.

### Guard

If `total-delete-files > 0` (any position/equality delete files present), abort with a clear
message and non-zero exit. Repair targets append-only RAW/compaction tables; interacting with
deletes is out of scope.

## Fix mechanism

A single Iceberg **transaction** committed through the catalog (table stays authoritative;
reversible via snapshot rollback):

```java
Transaction txn = table.newTransaction();

DeleteFiles del = txn.newDelete();
duplicatedPaths.forEach(del::deleteFile);   // deleteFile(path) removes ALL entries for the path
del.commit();

AppendFiles app = txn.newAppend();
keptDataFiles.forEach(app::appendFile);      // re-add exactly one entry per path
app.commit();

txn.commitTransaction();
```

- `DeleteFiles.deleteFile` deletes by path, removing every manifest entry for that path
  (confirmed by existing `IcebergTable.delete(List<ParquetFileStat>)` usage).
- Re-appending the same `DataFile` re-references the same physical S3 object — no data is
  moved or physically deleted. Older snapshots (142/143) still reference the object, so it is
  not GC-eligible.
- Net result: each previously-duplicated path is referenced exactly once with original
  metrics preserved.

**Rationale for transaction over `RewriteFiles`:** both primitives (`newDelete`, `newAppend`)
are already proven in this codebase, and delete-by-path-then-append has unambiguous semantics
(it cannot accidentally re-add a still-existing file). The single-snapshot
`table.newRewrite().rewriteFiles(toDelete, toAdd)` alternative was considered but carries
riskier validation around re-adding the same path. Trade-off accepted: the fix produces two
snapshots (one delete, one append) instead of one.

## Verification after `--apply`

`table.refresh()`, re-run the current-snapshot file-path scan, and assert **zero** duplicate
paths remain. Print before/after duplicate counts and total live-file counts. Exit non-zero
and log loudly if any duplicate survives.

## Output

Dry-run and apply both print:

- Resolved table identifier and catalog.
- Duplicate file paths in the current snapshot, with per-path occurrence counts.
- Duplicate `lakestream.tags` groups (seq numbers + snapshot ids).
- Dry-run: "would remove N duplicate references across M paths" and a hint to pass `--apply`.
- Apply: before/after counts and the new snapshot ids created by the fix.

All output via `System.out`/`System.err` consistent with the existing admin commands
(`ExpireSnapshots`, `ProbeTable`), which are CLI entry points.

## Testing

Integration test mirroring `ExpireSnapshotsTest`:

1. Build a local catalog table (Hadoop/in-memory) with the table's partition spec.
2. Append the same `DataFile` twice via two separate snapshots to reproduce the duplicate.
3. Run detection: assert the duplicated path is found and the diagnostic tag grouping works.
4. Run the fix: assert exactly one reference per path remains and total record count is
   unchanged.
5. Re-run detection: assert zero duplicates (verification path).
6. Guard test: a table with delete files causes the tool to abort.

## Files

- New: `ursa-storage-tools/src/main/java/io/lakestream/ursa/storage/admin/DedupFiles.java`
- Edit: `ursa-storage-tools/src/main/java/io/lakestream/ursa/storage/admin/Table.java`
  (register `DedupFiles.class` as a subcommand).
- New: `ursa-storage-tools/src/test/java/io/lakestream/ursa/storage/admin/DedupFilesTest.java`

All new source files require the license header (`mvn license:format` if missing) and the
`admin` test package is exempt from the `JavadocPackage` rule.
