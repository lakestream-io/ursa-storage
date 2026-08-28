# Configure Variant type for Iceberg tables

Apache Iceberg Variant columns can store heterogeneous and semi-structured
values. Ursa recognizes `logicalType: "variant"` in supported Avro and JSON
schemas and maps the value to an Iceberg Variant column during Kafka
materialization.

## Prerequisites

- The Iceberg catalog and query engine must support Variant.
- The Ursa Variant feature flag must be enabled.
- The schema registry used by the Kafka integration must retain the logical
  type annotation.

Validate engine support against the official
[Apache Iceberg documentation](https://iceberg.apache.org/).

## Supported values

- Primitive values: string, integer, long, float, double, boolean, and bytes.
- Maps, lists, arrays, and sets.
- Nested records and whole records.
- `variant-metadata-fields` for frequently queried nested fields.

## Evolution rules

Supported changes:

- Add a new Variant field.
- Remove an existing Variant field.

Unsupported changes:

- Convert an existing non-Variant field to Variant.
- Convert a Variant field to another type.

Records that violate the table schema are routed through the configured
materialization failure policy.

## Avro

Declare the logical type directly in the Avro schema:

```json
{
  "name": "attributes",
  "type": {
    "type": "map",
    "values": "string",
    "logicalType": "variant",
    "variant-metadata-fields": "[\"region\", \"source\"]"
  }
}
```

For reflection-based serializers, an `@AvroSchema` annotation can carry the
same schema fragment. Verify the generated registry schema before producing
data; some serializers discard unknown logical-type properties.

## JSON

The JSON schema must preserve a `logicalType: variant` description for the
field. With reflection-based Java schema generation, this can be expressed as:

```java
@JsonPropertyDescription("logicalType: variant")
private Map<String, String> attributes;
```

Inspect the registered schema to confirm that the annotation survived schema
generation.

## Query performance

Use `variant-metadata-fields` only for nested fields that are filtered or
projected frequently. Adding many metadata fields increases write and metadata
cost. Test the resulting table with the exact Iceberg engine version used in
production.

## Operational rollout

1. Enable Variant on a test stream.
2. Produce representative Kafka records for every expected value shape.
3. Verify the Iceberg schema and query results.
4. Exercise allowed and rejected schema evolution.
5. Monitor materialization failures before expanding the policy.
