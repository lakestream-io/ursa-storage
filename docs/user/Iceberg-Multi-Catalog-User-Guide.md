# Configure multiple Iceberg catalogs

Ursa can register multiple Iceberg catalogs and route each namespace or stream
to a selected catalog through its materialization policy.

## Register catalogs

Create one `TableCatalog` record for each catalog. Give every record a stable,
unique name and configure its backend-specific endpoint, warehouse, and
credential reference. Supported backends depend on the Iceberg catalog
implementations packaged with `ursa-storage-lakehouse`.

Credentials must be supplied by the runtime secret mechanism. Do not embed
secrets in catalog records, stream properties, or documentation examples.

## Select a catalog

Set `catalog.name` in the namespace materialization policy to establish a
default:

```properties
catalog.name=analytics-default
```

A stream policy can override the namespace default:

```properties
catalog.name=regulated-data
```

Resolution follows this order:

1. Enabled stream policy.
2. Enabled namespace policy.
3. No materialization when neither policy is enabled.

The referenced catalog must already exist. Ursa fails the materialization task
instead of silently choosing another catalog.

## Example layout

```text
catalog analytics-default -> REST catalog, general warehouse
catalog regulated-data    -> dedicated REST catalog and restricted warehouse

namespace commerce        -> analytics-default
stream commerce/payments  -> regulated-data
```

## Operations

- Grant the compaction runtime access to every selected catalog and warehouse.
- Validate one stream per catalog before enabling a namespace-wide policy.
- Monitor catalog resolution and commit failures separately.
- Treat catalog reassignment as a migration: existing snapshots stay in the
  original catalog unless an explicit data migration moves them.
