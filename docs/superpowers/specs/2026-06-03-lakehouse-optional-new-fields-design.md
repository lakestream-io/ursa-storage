# Make newly-added fields optional during Lakehouse schema evolution

**Date:** 2026-06-03
**Status:** Design — pending review
**Module:** `ursa-storage-lakehouse`

## Problem

When a Kafka topic uses a `Forward`/`ForwardTransitive` schema-compatibility
strategy, the registry permits a new schema version to **add required fields**. The
Lakehouse compaction path then tries to evolve the Iceberg/Delta table to the new
schema. Both table formats reject the addition of a *required* (non-nullable) field to
an existing table, the resulting `SchemaEvolutionException` is wrapped as a
`MessageSerDeException(MESSAGE_SCHEMA_INCOMPATIBLE)`, and the record is routed to the
**Dead Letter Table (DLT)** instead of being compacted.

This is semantically wrong: a brand-new field has no value in pre-existing rows, so the
only correct table representation is an **optional/nullable** column. We should evolve
the table by adding the new field as optional rather than dead-lettering the data.

### Confirmed failure path

1. Encoder calls `SchemaEvolutionManager.evolveSchema(...)`
   (`v2/serde/SchemaEvolutionManager.java`), which delegates to the format-specific
   `evolveSchemaWithVersion(...)`.
2. On `SchemaEvolutionException`, `SchemaEvolutionManager` wraps it as
   `MessageSerDeException(MESSAGE_SCHEMA_INCOMPATIBLE)` (lines 76/94/125).
3. `AbstractLakehouseWriter.onErrorWithCtx` (`v2/AbstractLakehouseWriter.java`, ~line
   153) catches `MessageSerDeException` and sends the record to the DLT via
   `failureMessageHandler.sendFailureMessage(...)`.

### Where each format rejects the new required field

**Iceberg** — `IcebergTable.updateTableSchemaIfNeeded` (`iceberg/IcebergTable.java`):
- Line 706: `validateSchema(currentSchema, reassignedSchema, configuration.checkIcebergNullability(), false)`.
- Iceberg's `TypeUtil.validateSchema` flags every new required field as *"required, but
  is missing"* — for both top-level fields (e.g. `audit_trail`) and fields nested in an
  existing struct (e.g. `details.processing_notes`) — and throws
  `SchemaEvolutionException`. As established in "Verification finding" below, this error
  fires **regardless of `checkIcebergNullability`** (the flag only gates a separate
  optional→required check), so it cannot be turned off by tuning that flag.
- Critically, this throws **before** `applySchemaChanges`/`applyStructChanges` (line
  715) runs. That method already downgrades new fields to optional via
  `UpdateSchema.addColumn(...)` (which adds optional columns) and recurses into existing
  structs (`handleTypeEvolution` → `applyStructChanges`, line 974) — but it never
  executes because validation fails first. The existing downgrade is effectively dead
  code in the default configuration.

**Delta** — `DeltaTable.evolveSchemaWithVersion` (`delta/DeltaTable.java`):
- Builds the evolved schema via
  `CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution(oldSchema, deltaSchema, ...)`
  and commits it. In that method
  (`delta/CustomColumnMapping.java`), the new-field branch (`index == -1`, lines
  34–38) preserves `field.isNullable()`, so a new required field stays non-nullable.
- Delta Kernel rejects the non-nullable column on commit → `KernelException` →
  `SchemaEvolutionException` (line 141).

## Goal

Evolve the table by adding newly-introduced fields as **optional/nullable** instead of
dead-lettering the record, for both Iceberg and Delta, gated by a configuration flag
that defaults to ON. Genuinely unsafe changes must still be handled safely.

## Decisions

- **Gating:** new config flag `make-new-fields-optional`, default **ON**. When OFF, the
  current strict behavior (new required field → DLT) is fully preserved.
- **Scope:** both Iceberg and Delta.
- **Keep `checkIcebergNullability` ON by default** — we do not relax it. Other
  nullability-incompatible changes remain governed by it unchanged.
- **Make-existing-field-required attempts:** behavior is **unchanged** from today (still
  DLT under `checkIcebergNullability=true`). This is *not* affected by this feature,
  because the transform below only touches genuinely new fields. (An earlier decision to
  "keep optional silently" was tied to a discarded line-706 relaxation approach and no
  longer applies.)

## Verification finding (why relaxing the nullability flag does NOT work)

The Iceberg `validateSchema` "required, but is missing" error — the exact error in the
reported exception — is **not gated by `checkNullability`**. In
`org.apache.iceberg.types.CheckCompatibility.field(...)` (iceberg-api 1.10.0, the version
in use; identical in 1.9.2):

```java
if (field == null) {
  if (readField.isRequired()) {
    return ImmutableList.of(readField.name() + " is required, but is missing");
  }
  return NO_ERRORS; // optional field reads as nulls
}
...
if (checkNullability && readField.isRequired() && field.isOptional()) {
  errors.add(readField.name() + " should be required, but is optional");
}
```

Both `writeCompatibilityErrors` (flag on) and `typeCompatibilityErrors` (flag off) run
this same visitor; only the *second* check is flag-gated. So a newly-added required field
is rejected regardless of the flag. This is corroborated by the existing
`IcebergTableSchemaIncompatibleTest.testAddRequiredField_DifferentConfigs`, which asserts
the add throws for **all** nullability/ordering combinations.

Therefore the schema handed to validation must already carry new fields as optional. A
blunter "skip the line-706 validation entirely when the flag is on" was also rejected: it
would stop catching genuine type incompatibilities (the `testIncompatibleTypeChange_*`
cases), silently dropping those changes instead of failing.

## Design

### Config

Add a cross-format property to `LakehouseConfiguration`, read from the shared top-level
`properties` map (same pattern as `getParquetCompressionType()`):

- Key: `make-new-fields-optional`
- Default: `true` (`MAKE_NEW_FIELDS_OPTIONAL_DEFAULT = true`)
- Accessor: `boolean makeNewFieldsOptionalOnEvolution()`

Both `IcebergTable` and `DeltaTable` already hold a `LakehouseConfiguration`
(`configuration` / `config`).

### Iceberg — optionalize new fields before validation

Add a recursive transform and apply it to the incoming schema *before* the line-706
validation in `IcebergTable.updateTableSchemaIfNeeded`:

```java
Schema reassignedSchema = TypeUtil.reassignOrRefreshIds(newSchema, currentSchema);
if (configuration.makeNewFieldsOptionalOnEvolution()) {
    reassignedSchema = makeNewFieldsOptional(currentSchema, reassignedSchema);
}
validateSchema(currentSchema, reassignedSchema, configuration.checkIcebergNullability(), false);
// ... applySchemaChanges / apply / line-724 validation all use reassignedSchema as today
```

`makeNewFieldsOptional(currentStruct, newStruct)` rebuilds `newStruct`, preserving field
IDs:
- A field whose name is **absent** from `currentStruct` → emitted as **optional** (its
  type/subtree left as-is). Iceberg's validator returns `NO_ERRORS` at a missing optional
  field and never descends into it, so we do not need to rewrite the subtree of a
  brand-new field (e.g. `audit_trail`'s required children are fine).
- A field present in both, where **both** are structs → recurse (catches a new field
  nested inside an existing struct, e.g. `details.processing_notes`).
- All other existing fields → emitted unchanged (existing nullability preserved).

Why this and not flag-gating line 706: see "Verification finding" above — the
"required, but is missing" error is not gated by `checkNullability`, so the schema must
already carry new fields as optional. With the transform, the new fields hit the
`isOptional → NO_ERRORS` branch at line 706; `applyStructChanges` then adds them via
`addColumn` (optional) exactly as before; and line 724 stays strict as the authoritative
gate on the committed schema. `checkIcebergNullability` and type-incompatibility checks
are untouched, so existing-field changes (e.g. optional→required) behave exactly as today.

The existing required→optional branch in `applyStructChanges` (lines 855–863) becomes a
no-op for new fields (they arrive optional) but is left in place to limit blast radius.

### Delta — force new fields nullable during evolution

In `CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution`, the new-field
branch (`index == -1`, lines 34–38) must force `nullable = true` when the flag is on,
before assigning column IDs/physical names.

- The method's existing recursion into structs (line 43) means a new field nested in an
  existing struct (`details.processing_notes`) reaches the same `index == -1` branch, so
  one change covers top-level and nested new fields.
- For a brand-new field whose type is itself a struct/array/map (e.g.
  `audit_trail`), force the top-level field nullable. **Verify during implementation**
  whether Delta Kernel also requires nested fields within a brand-new subtree to be
  nullable; if so, force descendants nullable as well (a recursive helper analogous to
  the existing `makeOptionalIfNeeded`/`transformAndAssignColumnIdAndPhysicalName`).
- The flag must be threaded into this static method (e.g. an added boolean parameter)
  and into `DeltaTable.evolveSchemaWithVersion`'s call site.

### What is intentionally unchanged

- Iceberg line-724 validation and `checkIcebergNullability` semantics.
- The DLT routing and `SchemaEvolutionManager` wrapping behavior.
- Delta soft-delete handling in `CustomColumnMapping` (lines 54–60).

## Out of scope (v1)

- List/map **element/value-level** nullability evolution. We optionalize new *fields*;
  we do not rewrite element nullability of existing collections.
- Changing registry-level compatibility semantics — this is purely the Lakehouse table
  evolution behavior.

## Testing

### Primary fixture: the reported exception schema

Reproduce the exact v1.0 → v1.1 evolution from the reported exception as an Iceberg
`Schema` (and the Delta `StructType` equivalent), exercising every shape that failed or
could fail:
- **`audit_trail`** — new **top-level required** `list<struct<action: required string,
  actor: optional string, timestamp: required timestamp>>`.
- **`details.processing_notes`** — new **required `list<string>` nested inside the
  existing optional `details` struct**.
- **`details.source_system`** — new **optional** field nested in the existing struct
  (must remain unaffected / still optional).
- New top-level optional fields: `version_info`, `retry_count`, `processed_at`, `region`.
- Unchanged existing fields, incl. existing `required` ones (`event_type`, `labels`,
  `attributes`, `numeric_attributes`, `extensions`, `metadata.*`).

Expected result with the flag ON: evolution **succeeds**, no DLT; `audit_trail`,
`details.processing_notes`, and all new optional fields land as **optional/nullable**;
all pre-existing fields keep their original nullability; old rows read back `null` for
the new columns.

### Iceberg (`@Tag("lakehouse")`)

New tests (likely a new `IcebergTableMakeNewFieldsOptionalTest` or additions to
`IcebergTableSchemaEvolutionTest`), built on the existing `InMemoryCatalog` +
`createConfiguration` pattern in `IcebergTableSchemaIncompatibleTest`:
- Full exception-schema fixture → evolves successfully; assert new fields optional,
  existing fields unchanged.
- New top-level required field → optional.
- New required field nested in an existing struct → optional.
- New required `list`/`map` field → optional.
- **Flag OFF** → new required field still throws (`SchemaEvolutionException`) — current
  behavior preserved.
- **Untouched protections** (must still throw with flag ON): incompatible type change
  (`string→int`), and making an **existing** optional field required under
  `checkIcebergNullability=true`.

**Existing tests to update** (their current expectation is exactly the behavior we are
intentionally changing, under the now-default-ON flag):
- `IcebergTableSchemaIncompatibleTest.testAddRequiredField_DifferentConfigs` — must
  become flag-aware: flag ON → succeeds (field optional); flag OFF → throws.
- `IcebergTableSchemaIncompatibleTest.testNestedRequiredField_DifferentConfigs` — same.

### Delta (`@Tag("lakehouse")`)

In `delta/DeltaTableTest` / `v2/delta/DeltaTableSchemaServiceTest`:
- Delta `StructType` equivalent of the exception-schema fixture → evolves successfully;
  new fields nullable, existing unchanged; old rows read `null`.
- New nested required field inside an existing struct → nullable.
- **Flag OFF** → new required field still routes to DLT / throws `SchemaEvolutionException`.

## Affected files

- `LakehouseConfiguration.java` — new `make-new-fields-optional` flag + accessor.
- `iceberg/IcebergTable.java` — add `makeNewFieldsOptional(currentSchema, incoming)` and
  apply it before the line-706 validation (flag-gated); no change to line 706/724 flags.
- `delta/CustomColumnMapping.java` — force new fields nullable (flag-gated).
- `delta/DeltaTable.java` — thread the flag into the `CustomColumnMapping` call.
- New + updated tests in `ursa-storage-lakehouse/src/test/...` (Iceberg + Delta), incl.
  the two existing `IcebergTableSchemaIncompatibleTest` cases noted above.
