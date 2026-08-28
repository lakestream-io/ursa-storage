# Make Newly-Added Fields Optional During Lakehouse Schema Evolution — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evolve Iceberg and Delta tables by adding newly-introduced topic-schema fields as optional/nullable instead of dead-lettering the record, gated by a config flag (default ON).

**Architecture:** A new `make-new-fields-optional` flag on `LakehouseConfiguration` (default true). For Iceberg, a recursive `makeNewFieldsOptional(current, incoming)` transform rewrites the incoming schema so fields absent from the current table become optional, applied before the existing `validateSchema` call (the "required, but is missing" error is *not* gated by `checkNullability`, so the schema itself must carry new fields as optional). For Delta, `CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution` forces new fields to `nullable=true`.

**Tech Stack:** Java 17, Maven, JUnit 5, Mockito, Apache Iceberg 1.10.0 (`org.apache.iceberg.types`), Delta Kernel (`io.delta.kernel.types`), Lombok.

**Spec:** `docs/superpowers/specs/2026-06-03-lakehouse-optional-new-fields-design.md`

**Build/verify commands (this module):**
- Compile: `mvn -B -ntp -q -pl ursa-storage-lakehouse -am install -DskipTests`
- Run a single test class: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dtest=ClassName -DfailIfNoTests=false`
- Run a single test method: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dtest='ClassName#methodName' -DfailIfNoTests=false`
- Lakehouse group: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dgroups=lakehouse`

> Note: tests in this module are tagged `@Tag("lakehouse")` and are excluded from the default unit run; pass `-Dgroups=lakehouse` or target them by `-Dtest=`. Always include `@Tag("lakehouse")` on new test classes.

---

## File Structure

**Production (modify):**
- `ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/LakehouseConfiguration.java` — add flag constants + accessor.
- `ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/iceberg/IcebergTable.java` — add `makeNewFieldsOptional`/`optionalizeNewFields` helpers; apply before line-706 validation.
- `ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/delta/CustomColumnMapping.java` — add `makeNewFieldsOptional` boolean param; force new fields nullable; thread through recursion.
- `ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/delta/DeltaTable.java:123` — pass `config.makeNewFieldsOptionalOnEvolution()`.

**Tests (create/modify):**
- Modify `.../test/.../lakehouse/LakehouseConfigurationTest.java` — accessor test.
- Create `.../test/.../lakehouse/iceberg/IcebergTableMakeNewFieldsOptionalTest.java` — Iceberg behavior incl. full exception-schema fixture.
- Modify `.../test/.../lakehouse/iceberg/IcebergTableSchemaIncompatibleTest.java` — disable the flag in its config builder so its strict-mode assertions stay valid.
- Create `.../test/.../lakehouse/delta/CustomColumnMappingTest.java` — Delta transform incl. full exception-schema fixture.

---

## Task 1: Config flag `make-new-fields-optional`

**Files:**
- Modify: `ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/LakehouseConfiguration.java` (constants near line 109; accessor near line 528)
- Test: `ursa-storage-lakehouse/src/test/java/io/lakestream/ursa/lakehouse/LakehouseConfigurationTest.java`

- [ ] **Step 1: Write the failing test**

Add to `LakehouseConfigurationTest` (the class already imports `java.util.Properties` and uses JUnit 5 `@Test`):

```java
    @Test
    public void testMakeNewFieldsOptionalDefaultsTrue() {
        LakehouseConfiguration config = new LakehouseConfiguration(new Properties());
        assertTrue(config.makeNewFieldsOptionalOnEvolution());
    }

    @Test
    public void testMakeNewFieldsOptionalCanBeDisabled() {
        Properties properties = new Properties();
        properties.setProperty(LakehouseConfiguration.MAKE_NEW_FIELDS_OPTIONAL, "false");
        LakehouseConfiguration config = new LakehouseConfiguration(properties);
        assertEquals(false, config.makeNewFieldsOptionalOnEvolution());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dtest='LakehouseConfigurationTest#testMakeNewFieldsOptionalDefaultsTrue+testMakeNewFieldsOptionalCanBeDisabled' -DfailIfNoTests=false`
Expected: FAIL — compilation error, `MAKE_NEW_FIELDS_OPTIONAL` / `makeNewFieldsOptionalOnEvolution()` do not exist.

- [ ] **Step 3: Add constants**

In `LakehouseConfiguration.java`, immediately after the `CHECK_NULLABILITY_DEFAULT` line (currently line 109):

```java
    //  Adds newly-introduced fields as optional/nullable during schema evolution
    //  instead of routing the record to the DLT (default: true)
    public static final String MAKE_NEW_FIELDS_OPTIONAL = "make-new-fields-optional";
    public static final boolean MAKE_NEW_FIELDS_OPTIONAL_DEFAULT = true;
```

- [ ] **Step 4: Add accessor**

In `LakehouseConfiguration.java`, after `getCompressType()` (currently ends line 528), add (mirrors the existing `properties.getOrDefault(...).toString()` pattern):

```java
    public boolean makeNewFieldsOptionalOnEvolution() {
        return Boolean.parseBoolean(
            properties.getOrDefault(MAKE_NEW_FIELDS_OPTIONAL,
                String.valueOf(MAKE_NEW_FIELDS_OPTIONAL_DEFAULT)).toString());
    }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dtest='LakehouseConfigurationTest#testMakeNewFieldsOptionalDefaultsTrue+testMakeNewFieldsOptionalCanBeDisabled' -DfailIfNoTests=false`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/LakehouseConfiguration.java \
        ursa-storage-lakehouse/src/test/java/io/lakestream/ursa/lakehouse/LakehouseConfigurationTest.java
git commit -m "feat(lakehouse): add make-new-fields-optional config flag (default on)"
```

---

## Task 2: Iceberg `makeNewFieldsOptional` transform + wiring

**Files:**
- Modify: `ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/iceberg/IcebergTable.java` (lines ~704-706; add two private static helpers)
- Test: `ursa-storage-lakehouse/src/test/java/io/lakestream/ursa/lakehouse/iceberg/IcebergTableMakeNewFieldsOptionalTest.java` (create)

- [ ] **Step 1: Write the failing test**

Create `IcebergTableMakeNewFieldsOptionalTest.java`. The config builder mirrors `IcebergTableSchemaIncompatibleTest.createConfiguration` but also controls the new flag. `TableOptions` and `IcebergTable` usage match that existing test.

```java
/**
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.LakehouseConfiguration.MAKE_NEW_FIELDS_OPTIONAL;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
public class IcebergTableMakeNewFieldsOptionalTest {

    private Catalog catalog;
    private TableIdentifier tableIdentifier;
    private IcebergTable icebergTable;

    @BeforeEach
    public void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test-catalog", new HashMap<>());
        Namespace namespace = Namespace.of("test_namespace");
        if (catalog instanceof SupportsNamespaces sn) {
            sn.createNamespace(namespace);
        }
        tableIdentifier = TableIdentifier.of(namespace, "test_optional_table");
    }

    @AfterEach
    public void tearDown() {
        if (icebergTable != null) {
            icebergTable.close();
        }
    }

    // Initial table: an existing optional struct "details" plus required scalar fields.
    private Schema initialSchema() {
        return new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "labels", Types.ListType.ofRequired(3, Types.StringType.get())),
            Types.NestedField.optional(4, "details", Types.StructType.of(
                Types.NestedField.optional(5, "category", Types.StringType.get())
            ))
        );
    }

    // Evolved table: adds a new top-level required list<struct> (audit_trail), a new REQUIRED
    // field nested inside the existing "details" struct (processing_notes), and a new optional field.
    private Schema evolvedSchema() {
        return new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "labels", Types.ListType.ofRequired(3, Types.StringType.get())),
            Types.NestedField.optional(4, "details", Types.StructType.of(
                Types.NestedField.optional(5, "category", Types.StringType.get()),
                Types.NestedField.required(20, "processing_notes",
                    Types.ListType.ofRequired(21, Types.StringType.get())),
                Types.NestedField.optional(22, "source_system", Types.StringType.get())
            )),
            Types.NestedField.required(30, "audit_trail", Types.ListType.ofRequired(31,
                Types.StructType.of(
                    Types.NestedField.required(32, "action", Types.StringType.get()),
                    Types.NestedField.required(33, "timestamp", Types.TimestampType.withZone())
                ))),
            Types.NestedField.optional(40, "version_info", Types.StringType.get())
        );
    }

    @Test
    public void testNewRequiredFieldsAddedAsOptional_flagOn() throws Exception {
        LakehouseConfiguration configuration = createConfiguration(true);
        TableOptions tableOptions = TableOptions.builder().schema(initialSchema()).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        icebergTable.updateTableSchemaIfNeeded(evolvedSchema());

        Schema updated = icebergTable.getTable().schema();
        // New top-level field present and optional.
        assertNotNull(updated.findField("audit_trail"));
        assertTrue(updated.findField("audit_trail").isOptional());
        // New nested field inside the existing "details" struct present and optional.
        assertNotNull(updated.findField("details.processing_notes"));
        assertTrue(updated.findField("details.processing_notes").isOptional());
        // New optional field still optional.
        assertTrue(updated.findField("version_info").isOptional());
        // Pre-existing required field unchanged.
        assertTrue(updated.findField("labels").isRequired());
    }

    @Test
    public void testNewRequiredFieldRejected_flagOff() {
        LakehouseConfiguration configuration = createConfiguration(false);
        TableOptions tableOptions = TableOptions.builder().schema(initialSchema()).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // With the flag off, adding a new required field is rejected (current behavior preserved).
        assertThrows(Exception.class, () -> icebergTable.updateTableSchemaIfNeeded(evolvedSchema()));
    }

    @Test
    public void testIncompatibleTypeChangeStillRejected_flagOn() {
        LakehouseConfiguration configuration = createConfiguration(true);
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "value", Types.StringType.get()));
        TableOptions tableOptions = TableOptions.builder().schema(initial).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // String -> Int on an EXISTING field is an incompatible type change. The transform only
        // touches NEW fields, so this must still be rejected even with the flag on.
        Schema incompatible = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "value", Types.IntegerType.get()));
        assertThrows(Exception.class, () -> icebergTable.updateTableSchemaIfNeeded(incompatible));
    }

    @Test
    public void testMakeExistingFieldRequiredStillRejected_flagOn() {
        LakehouseConfiguration configuration = createConfiguration(true);
        Schema initial = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
        TableOptions tableOptions = TableOptions.builder().schema(initial).build();
        icebergTable = new IcebergTable(catalog, tableIdentifier, tableOptions, configuration);
        icebergTable.create(tableOptions);

        // Tightening an EXISTING optional field to required is governed by checkIcebergNullability
        // (default true) and is unaffected by this feature, so it must still be rejected.
        Schema tightened = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
        assertThrows(Exception.class, () -> icebergTable.updateTableSchemaIfNeeded(tightened));
    }

    private LakehouseConfiguration createConfiguration(boolean makeNewFieldsOptional) {
        Properties properties = new Properties();
        properties.setProperty("cluster", "test-cluster");
        properties.setProperty(MAKE_NEW_FIELDS_OPTIONAL, String.valueOf(makeNewFieldsOptional));

        return new LakehouseConfiguration(properties) {
            @Override
            public Properties getProperties() {
                return properties;
            }

            @Override
            public String getIcebergCatalogType(Optional<String> catalogName) {
                return "inmemory";
            }

            @Override
            public IcebergCatalogBackendType getIcebergCatalogBackendType(Optional<String> catalogName) {
                return IcebergCatalogBackendType.TABULAR;
            }

            @Override
            public Map<String, String> getIcebergTableProperties() {
                return new HashMap<>();
            }

            @Override
            public Optional<String> getCatalogName() {
                return Optional.of("test-catalog");
            }

            @Override
            public Duration getCatalogMaxOpenTime() {
                return Duration.ofMinutes(5);
            }

            @Override
            public int getIcebergSnapshotExpirationInterval() {
                return 3600;
            }

            @Override
            public String getBucketPath() {
                return "s3://test-bucket/";
            }
        };
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dtest='IcebergTableMakeNewFieldsOptionalTest' -DfailIfNoTests=false`
Expected: FAIL — `testNewRequiredFieldsAddedAsOptional_flagOn` throws `SchemaEvolutionException` ("audit_trail is required, but is missing"), because the transform does not exist yet. (The three other tests — `testNewRequiredFieldRejected_flagOff`, `testIncompatibleTypeChangeStillRejected_flagOn`, `testMakeExistingFieldRequiredStillRejected_flagOn` — already pass since they assert rejection.)

- [ ] **Step 3: Add the transform helpers to `IcebergTable.java`**

Add these two private static methods to `IcebergTable` (place them just before `updateTableSchemaIfNeeded`, near line 668). Fully-qualified `org.apache.iceberg.types.*` names match the surrounding code style in this file.

```java
    /**
     * Returns a copy of {@code incoming} in which every field absent from {@code current} is
     * marked optional. Fields present in both keep their nullability; structs present in both are
     * recursed so a new field nested inside an existing struct is also made optional. Field IDs,
     * names and docs are preserved. See docs/superpowers/specs/2026-06-03-lakehouse-optional-new-fields-design.md.
     */
    static org.apache.iceberg.Schema makeNewFieldsOptional(
            org.apache.iceberg.Schema current, org.apache.iceberg.Schema incoming) {
        org.apache.iceberg.types.Types.StructType merged =
            optionalizeNewFields(current.asStruct(), incoming.asStruct());
        return new org.apache.iceberg.Schema(merged.fields(), incoming.identifierFieldIds());
    }

    private static org.apache.iceberg.types.Types.StructType optionalizeNewFields(
            org.apache.iceberg.types.Types.StructType current,
            org.apache.iceberg.types.Types.StructType incoming) {
        java.util.List<org.apache.iceberg.types.Types.NestedField> result = new java.util.ArrayList<>();
        for (org.apache.iceberg.types.Types.NestedField field : incoming.fields()) {
            org.apache.iceberg.types.Types.NestedField currentField = current.field(field.name());
            if (currentField == null) {
                // New field: force optional, keep its type/subtree as-is.
                if (field.isOptional()) {
                    result.add(field);
                } else {
                    log.info("Adding new field '{}' as optional for backward compatibility", field.name());
                    result.add(org.apache.iceberg.types.Types.NestedField.optional(
                        field.fieldId(), field.name(), field.type(), field.doc()));
                }
            } else if (field.type().isStructType() && currentField.type().isStructType()) {
                // Existing struct present in both: recurse to catch new nested fields.
                org.apache.iceberg.types.Types.StructType newStruct = optionalizeNewFields(
                    currentField.type().asStructType(), field.type().asStructType());
                result.add(org.apache.iceberg.types.Types.NestedField.of(
                    field.fieldId(), field.isOptional(), field.name(), newStruct, field.doc()));
            } else {
                // Existing non-struct field: leave unchanged.
                result.add(field);
            }
        }
        return org.apache.iceberg.types.Types.StructType.of(result);
    }
```

- [ ] **Step 4: Wire the transform into `updateTableSchemaIfNeeded`**

In `IcebergTable.java`, replace the existing line 704 (`Schema reassignedSchema = TypeUtil.reassignOrRefreshIds(newSchema, currentSchema);`) and keep line 706 unchanged. The result should read:

```java
        // Let Iceberg handle ALL validation (it's comprehensive!)
        Schema reassignedSchema = TypeUtil.reassignOrRefreshIds(newSchema, currentSchema);

        if (configuration.makeNewFieldsOptionalOnEvolution()) {
            // The "required, but is missing" check in TypeUtil.validateSchema is NOT gated by
            // checkNullability, so new required fields must be optional in the schema we validate.
            reassignedSchema = makeNewFieldsOptional(currentSchema, reassignedSchema);
        }

        validateSchema(currentSchema, reassignedSchema, configuration.checkIcebergNullability(), false);
```

(No change to lines 708-735: `applySchemaChanges` continues to use `newSchema`; `applyStructChanges` already adds new fields via `addColumn`, which creates optional columns, so the committed schema and the line-724 validation both see the new fields as optional.)

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dtest='IcebergTableMakeNewFieldsOptionalTest' -DfailIfNoTests=false`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/iceberg/IcebergTable.java \
        ursa-storage-lakehouse/src/test/java/io/lakestream/ursa/lakehouse/iceberg/IcebergTableMakeNewFieldsOptionalTest.java
git commit -m "feat(lakehouse): add new Iceberg fields as optional during schema evolution"
```

---

## Task 3: Keep `IcebergTableSchemaIncompatibleTest` strict-mode assertions valid

**Why:** Its `testAddRequiredField_DifferentConfigs` and `testNestedRequiredField_DifferentConfigs` assert that adding a required field throws. With the flag now defaulting ON, that would no longer throw. The file's purpose is incompatible/strict-mode scenarios, so we pin the flag OFF in its config builder.

**Files:**
- Modify: `ursa-storage-lakehouse/src/test/java/io/lakestream/ursa/lakehouse/iceberg/IcebergTableSchemaIncompatibleTest.java` (the `createConfiguration` anonymous subclass, currently lines 658-708)

- [ ] **Step 1: Add the flag override to the test's config builder**

Inside the anonymous `LakehouseConfiguration` returned by `createConfiguration` (it already overrides `checkIcebergNullability()` etc.), add:

```java
            @Override
            public boolean makeNewFieldsOptionalOnEvolution() {
                // This suite verifies strict-mode (DLT) behavior; the optional-downgrade
                // feature is covered by IcebergTableMakeNewFieldsOptionalTest.
                return false;
            }
```

- [ ] **Step 2: Run the affected tests to verify they still pass**

Run: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dtest='IcebergTableSchemaIncompatibleTest' -DfailIfNoTests=false`
Expected: PASS (all tests, including `testAddRequiredField_DifferentConfigs` and `testNestedRequiredField_DifferentConfigs`, now under flag=off).

- [ ] **Step 3: Commit**

```bash
git add ursa-storage-lakehouse/src/test/java/io/lakestream/ursa/lakehouse/iceberg/IcebergTableSchemaIncompatibleTest.java
git commit -m "test(lakehouse): pin make-new-fields-optional off in Iceberg incompatible suite"
```

---

## Task 4: Delta — force new fields nullable in `CustomColumnMapping`

**Files:**
- Modify: `ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/delta/CustomColumnMapping.java`
- Modify: `ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/delta/DeltaTable.java:123`
- Test: `ursa-storage-lakehouse/src/test/java/io/lakestream/ursa/lakehouse/delta/CustomColumnMappingTest.java` (create)

- [ ] **Step 1: Write the failing test**

Create `CustomColumnMappingTest.java`. This is a pure-function unit test (no Delta Kernel commit). Mirrors the user's exception schema shape: an existing optional `details` struct gains a new required `processing_notes`, and a new top-level required `audit_trail` is added.

```java
/**
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lakehouse")
class CustomColumnMappingTest {

    private StructType oldSchema() {
        return new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("details", new StructType()
                .add(new StructField("category", StringType.STRING, true)), true));
    }

    // Adds a new required nested field (details.processing_notes) and a new required
    // top-level field (audit_trail). Both should be forced nullable when the flag is on.
    private StructType newSchema() {
        return new StructType()
            .add(new StructField("id", LongType.LONG, false))
            .add(new StructField("details", new StructType()
                .add(new StructField("category", StringType.STRING, true))
                .add(new StructField("processing_notes",
                    new ArrayType(StringType.STRING, false), false)), true))
            .add(new StructField("audit_trail",
                new ArrayType(StringType.STRING, false), false));
    }

    @Test
    void newFieldsForcedNullableWhenFlagOn() {
        StructType result = CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution(
            oldSchema(), newSchema(), new AtomicInteger(2), false, true);

        assertTrue(result.get("audit_trail").isNullable());
        StructType details = (StructType) result.get("details").getDataType();
        assertTrue(details.get("processing_notes").isNullable());
        // Pre-existing non-nullable field stays non-nullable.
        assertFalse(result.get("id").isNullable());
    }

    @Test
    void newRequiredFieldStaysNonNullableWhenFlagOff() {
        StructType result = CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution(
            oldSchema(), newSchema(), new AtomicInteger(2), false, false);

        assertFalse(result.get("audit_trail").isNullable());
        StructType details = (StructType) result.get("details").getDataType();
        assertFalse(details.get("processing_notes").isNullable());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dtest='CustomColumnMappingTest' -DfailIfNoTests=false`
Expected: FAIL — compilation error: `assignColumnIdAndPhysicalNameForTableEvolution` has 4 params, not 5.

- [ ] **Step 3: Add the `makeNewFieldsOptional` parameter and force-nullable logic**

In `CustomColumnMapping.java`, change the method signature and both branches. Replace the current method (lines 30-62) with:

```java
    public static StructType assignColumnIdAndPhysicalNameForTableEvolution(
        StructType oldSchema, StructType newSchema, AtomicInteger maxColumnId, boolean softDeleteEnabled,
        boolean makeNewFieldsOptional) {
        StructType finalSchema = new StructType();
        for (StructField field : newSchema.fields()) {
            int index = oldSchema.indexOf(field.getName());
            if (index == -1) {
                StructField newField = makeNewFieldsOptional ? makeNullable(field) : field;
                finalSchema = finalSchema.add(transformAndAssignColumnIdAndPhysicalName(
                    assignColumnIdAndPhysicalNameToField(
                        newField, maxColumnId), maxColumnId));
            } else {
                StructField oldField = oldSchema.at(index);
                if (oldField.getDataType() instanceof StructType) {
                    StructType structType =
                        assignColumnIdAndPhysicalNameForTableEvolution((StructType) oldField.getDataType(),
                            (StructType) field.getDataType(), maxColumnId, softDeleteEnabled,
                            makeNewFieldsOptional);
                    finalSchema = finalSchema.add(
                        new StructField(field.getName(), structType, field.isNullable(), oldField.getMetadata()));
                } else {
                    finalSchema =
                        finalSchema.add(new StructField(field.getName(), field.getDataType(), field.isNullable(),
                            oldField.getMetadata()));
                }
            }
        }
        if (softDeleteEnabled) {
            for (StructField oldField : oldSchema.fields()) {
                if (newSchema.indexOf(oldField.getName()) == -1) {
                    finalSchema = finalSchema.add(makeOptionalIfNeeded(oldField));
                }
            }
        }
        return finalSchema;
    }

    private static StructField makeNullable(StructField field) {
        if (field.isNullable()) {
            return field;
        }
        return new StructField(field.getName(), field.getDataType(), true, field.getMetadata());
    }
```

(Note: `makeNullable` is identical in spirit to the existing `makeOptionalIfNeeded`; kept as a distinct, intention-revealing name for the new-field path. `makeOptionalIfNeeded` is retained for the soft-delete path.)

- [ ] **Step 4: Thread the flag from `DeltaTable.evolveSchemaWithVersion`**

In `DeltaTable.java`, update the call at line 123-124 to pass the flag:

```java
        StructType newSchema = CustomColumnMapping.assignColumnIdAndPhysicalNameForTableEvolution(oldSchema,
            deltaSchema, columId, softDeleteEnabled, config.makeNewFieldsOptionalOnEvolution());
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dtest='CustomColumnMappingTest' -DfailIfNoTests=false`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/delta/CustomColumnMapping.java \
        ursa-storage-lakehouse/src/main/java/io/lakestream/ursa/lakehouse/delta/DeltaTable.java \
        ursa-storage-lakehouse/src/test/java/io/lakestream/ursa/lakehouse/delta/CustomColumnMappingTest.java
git commit -m "feat(lakehouse): force new Delta fields nullable during schema evolution"
```

---

## Task 5: Full quality gate + lakehouse test group

**Files:** none (verification only)

- [ ] **Step 1: License + checkstyle**

Run: `mvn -B -ntp license:check -pl ursa-storage-lakehouse && mvn -B -ntp checkstyle:check -pl ursa-storage-lakehouse`
Expected: BUILD SUCCESS. If license headers are missing on new files, run `mvn -B -ntp license:format -pl ursa-storage-lakehouse` and re-run, then `git add -A && git commit -m "chore: license headers"`.

- [ ] **Step 2: Compile whole module graph**

Run: `mvn -B -ntp clean install -DskipTests -pl ursa-storage-lakehouse -am`
Expected: BUILD SUCCESS.

- [ ] **Step 3: SpotBugs**

Run: `mvn -B -ntp spotbugs:check -pl ursa-storage-lakehouse`
Expected: BUILD SUCCESS. Fix any real findings (do not add broad exclusions).

- [ ] **Step 4: Run the full lakehouse test group**

Run: `mvn -B -ntp -pl ursa-storage-lakehouse test -Dgroups=lakehouse`
Expected: BUILD SUCCESS, including the new `IcebergTableMakeNewFieldsOptionalTest`, `CustomColumnMappingTest`, updated `IcebergTableSchemaIncompatibleTest`, and `LakehouseConfigurationTest`.

- [ ] **Step 5: Commit any fixups**

```bash
git add -A
git commit -m "chore(lakehouse): pass quality gates for make-new-fields-optional" --allow-empty
```

---

## Notes / Verification points for the implementer

- **Delta nested nullability:** Task 4 forces the *immediate* new field nullable. For a brand-new struct/array field whose nested children are non-nullable (e.g. `audit_trail`'s `action`), this is expected to be accepted by Delta Kernel because the new column itself is nullable. If an integration test (`DeltaTableTest`/`v2/delta/*`) reveals Kernel still rejecting the commit, extend `makeNullable` to recurse into struct/array/map children (mirror `transformAndAssignColumnIdAndPhysicalName`). This is the single most likely place to need adjustment.
- **`reassignedSchema` reassignment (Task 2, Step 4):** the local is reassigned, not captured in a lambda before the reassignment, so there is no effectively-final issue. Confirm no compile error.
- **`getProperties()` override in test configs:** the accessor reads the internal `properties` field (populated by `super(properties)` via `putAll`), so setting `MAKE_NEW_FIELDS_OPTIONAL` on the `Properties` passed to the constructor is sufficient; no method override of `makeNewFieldsOptionalOnEvolution()` is needed in Task 2's builder.
</content>
</invoke>
