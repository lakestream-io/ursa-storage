# ursa-storage-compact

Compaction orchestration. 8 Java files in main, 12 in test.

Coordinates WAL → Compacted Object compaction across the cluster, plus
sink-neutral materialization dispatch via the T10 `MaterializationService` SPI.

## Key Classes

| Class | Purpose |
|-------|---------|
| `CompactionMain` | CLI entry point |
| `CompactionScheduler` | Lifecycle orchestration and coordination |
| `CompactionWorker` | Executes individual compaction tasks + dispatches materialization |
| `LeaderElectionService` | Distributed leader election via Oxia |
| `CompactLeader` | Leader election handler |

## Package Layout

```
io.lakestream.ursa.compact        — Main compaction (5 classes)
io.lakestream.ursa.compact.elect  — Leader election (2 classes)
```

## Orchestration Flow (T10)

```
CompactionScheduler.start()
  ├── TableCatalogBootstrap.bootstrap(streamCatalog, properties)
  │     (loaded reflectively from the integration module so this module
  │      stays free of integration-package imports)
  ├── initCompactRunner()
  │     spins up N CompactionWorker threads, each holding:
  │       - CompactionService (legacy WAL → CO path)
  │       - MaterializationService (new sink dispatch)
  │       - StreamCatalog (loadStream + effectiveMaterialization)
  └── startLeaderElectionService()
        when elected leader:
          - storageBindings.createPublishCompactTaskRunner().start()
          - storageBindings.createCompactedTaskRunner().start()
          - storageBindings.createAsyncCompactedDataCleaner().start()

CompactionWorker.run() loop, per task:
  1. CompactionService.compactStream(task)        (internal WAL → CO)
  2. Stream stream = streamCatalog.loadStream(id)
  3. if (stream.effectiveMaterialization().isPresent())
        materializationService.materialize(MaterializationTask)
  4. on MaterializationException with non-retryable code:
        materializationService.invalidate(streamId)
        (the outer ExceptionCode-based retry/quarantine routes the failure)
```

The orchestrator drives both halves through reflective loaders so the compact
module has zero direct integration-package (`io.lakestream.ursa.lakehouse.*`)
imports. The grep gate
`grep -RIn "io.lakestream.ursa.lakehouse" ursa-storage-compact/src/main`
must return empty.

## Configuration Keys (operator surface)

| Key | Default | Notes |
|-----|---------|-------|
| `compactionStorageBindingsClass` | `io.lakestream.ursa.lakehouse.compact.LakehouseCompactionStorageBindings` | Wires the publish/commit/cleaner runners. |
| `materializationServiceClass` | `io.lakestream.ursa.lakehouse.compact.LakehouseMaterializationService` | The active materialization service. |
| `compactionServiceClass` | (deprecated alias for the legacy combined service) | Still honoured: when set without `materializationServiceClass`, the scheduler logs a WARN and uses the alias's value. The historical default (`LakehouseCompactionServiceImpl`) is mapped to the new default. |

## Dependencies

This module integrates the heaviest dependency set in the project:
- **ursa-storage-core** — StorageApi, compaction task providers, the
  `CompactionStorageBindings` + `MaterializationService` SPIs
- **ursa-storage-lakestream** — `IndexedStreamCatalog` (resolution of
  `effectiveMaterialization()`)
- **ursa-storage-materialization** — SPI + serde, runtime
- **ursa-storage-lakehouse** (provided) — concrete bindings + materialization
  service, all loaded reflectively
- **Oxia** — Metadata coordination, leader election
- **Oxia** — Metadata coordination and leader election

Changes here affect the entire data pipeline.

## Testing

```bash
mvn -B -ntp test -pl ursa-storage-compact
```

Key tests:
- `CompactionWorkerTest` — blacklist filtering + per-code quarantine routing
- `CompactionWorkerMaterializationDispatchTest` — materialize() called when policy resolves
- `CompactionWorkerMaterializationFailureTest` — invalidate() called on non-retryable failure
- `CompactionSchedulerWiringTest` — config-key resolution incl. deprecated alias
- `KafkaE2ETest` — end-to-end compaction flow
- `LeaderElectionServiceTest` — distributed coordination

## Pitfalls

- Heaviest dependency set — changes cascade across modules
- Leader election logic is coordination-sensitive
- E2E tests require full Docker infrastructure
- Never add direct `io.lakestream.ursa.lakehouse.*` imports — the T10 grep
  gate fails the build. Use the bindings/SPI or reflective load instead.
