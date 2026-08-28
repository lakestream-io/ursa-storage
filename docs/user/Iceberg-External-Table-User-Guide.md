# Configure an external Iceberg table

Ursa can materialize a stream into an Iceberg table managed by an external
catalog. The storage layer receives these settings through a
`TableMaterializationPolicy`; the surrounding integration decides how users
create or update that policy.

## Required settings

1. Register an Iceberg catalog as a `TableCatalog`.
2. Attach an enabled materialization policy to a namespace or stream.
3. Select the catalog name and Iceberg table identifier.
4. Grant the compaction service access to the catalog and object store.

A stream-level policy overrides its namespace policy. Set `enabled = false` on
the stream policy to opt a stream out of namespace-wide materialization.

## Writer properties

Pass Iceberg writer properties with the `iceberg.write-props.` prefix in the
policy properties map. For example:

```properties
iceberg.write-props.write.parquet.compression-codec=zstd
```

Use `iceberg.table-props.` for table properties:

```properties
iceberg.table-props.history.expire.max-snapshot-age-ms=86400000
```

The accepted keys and values follow the Iceberg version used by the
`ursa-storage-lakehouse` module.

## Upsert and identifiers

Enable upsert mode only when the target table has stable identifier fields:

```properties
upsertModeEnabled=true
identifierFields=account_id,event_id
```

All identifier fields must be present in every decoded record. Changing the
identifier set after data has been committed requires an explicit migration.

## Partitioning

The `partitionKey` property accepts a JSON array of Iceberg partition
transforms. Example:

```json
[
  {"sourceColumn":"timestamp","transform":"hour","targetName":"ts_hour"},
  {"sourceColumn":"address","transform":"truncate[7]","targetName":"address_prefix"}
]
```

Validate transforms against the source schema before enabling the policy.
Partition evolution changes future writes and does not rewrite existing data.

## Operational checks

- Confirm the selected catalog resolves the expected table.
- Start with a test stream and verify schema, partitions, and snapshots.
- Monitor compaction failures and end-to-end materialization latency.
- Keep credentials in the deployment secret mechanism, not in policy text.
