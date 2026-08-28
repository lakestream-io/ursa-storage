/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.iceberg.Utilities.buildPartitionSpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class BuildPartitionSpecTest {

    private Schema flatSchema;
    private Schema nestedSchema;

    @BeforeEach
    public void setup() {
        // Flat schema with various types
        flatSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get()),
                Types.NestedField.optional(3, "timestamp", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(4, "date", Types.DateType.get()),
                Types.NestedField.optional(5, "category", Types.StringType.get())
        );

        // Nested schema with struct types
        nestedSchema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "error", Types.StructType.of(
                        Types.NestedField.optional(10, "code", Types.IntegerType.get()),
                        Types.NestedField.optional(11, "receivedAt", Types.TimestampType.withoutZone())
                )),
                Types.NestedField.optional(3, "project", Types.StructType.of(
                        Types.NestedField.optional(20, "name", Types.StringType.get()),
                        Types.NestedField.optional(21, "id", Types.IntegerType.get())
                ))
        );
    }

    // ========== NULL/EMPTY CONFIG TESTS ==========

    @Test
    public void testNullPartitionConfig() {
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, null);
        assertTrue(spec.getPartitionSpec().isUnpartitioned(), "Should return unpartitioned spec for null config");
        assertTrue(spec.getExpressions().isEmpty(), "Should have no expressions");
    }

    @Test
    public void testEmptyPartitionConfig() {
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, List.of());
        assertTrue(spec.getPartitionSpec().isUnpartitioned(), "Should return unpartitioned spec for empty config");
        assertTrue(spec.getExpressions().isEmpty(), "Should have no expressions");
    }

    // ========== IDENTITY TRANSFORM TESTS ==========

    @Test
    public void testIdentityTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("category", "identity", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        assertEquals(1, spec.getExpressions().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "category");
    }

    @Test
    public void testIdentityTransformIgnoresTargetName() {
        // Note: Identity transform does NOT support custom target names in Iceberg API
        // The partition field will use the source column name, not the target name
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("category", "identity", "cat_partition")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        // Identity always uses source column name, not target name
        assertPartitionFieldExists(spec.getPartitionSpec(), "category");
        // Verify the custom name was NOT used
        assertFalse(spec.getPartitionSpec().fields().stream()
                        .anyMatch(f -> f.name().equals("cat_partition")),
                "Identity transform should not use custom target name");
    }

    @Test
    public void testNoTransformDefaultsToIdentity() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("category", null, null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "category");
    }

    @Test
    public void testEmptyTransformDefaultsToIdentity() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("category", "", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "category");
    }

    // ========== TEMPORAL TRANSFORM TESTS ==========

    @Test
    public void testYearTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "year", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        assertEquals(1, spec.getExpressions().size());
    }

    @Test
    public void testYearTransformWithTargetName() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "year", "ts_year")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "ts_year");
    }

    @Test
    public void testMonthTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "month", "ts_month")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "ts_month");
    }

    @Test
    public void testDayTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("date", "day", "date_day")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "date_day");
    }

    @Test
    public void testHourTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "hour", "ts_hour")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "ts_hour");
    }

    @Test
    public void testMultipleTemporalTransforms() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "year", "ts_year"),
                new IcebergPartitionConfig("date", "month", "date_month")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(2, spec.getPartitionSpec().fields().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "ts_year");
        assertPartitionFieldExists(spec.getPartitionSpec(), "date_month");
    }

    // ========== BUCKET TRANSFORM TESTS ==========

    @Test
    public void testBucketTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[10]", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        assertEquals(1, spec.getExpressions().size());
    }

    @Test
    public void testBucketTransformWithTargetName() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[16]", "id_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "id_bucket");
    }

    @Test
    public void testBucketTransformWithSpaces() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[ 8 ]", "id_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "id_bucket");
    }

    @Test
    public void testBucketTransformVariousSizes() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[4]", "bucket_4"),
                new IcebergPartitionConfig("data", "bucket[32]", "bucket_32")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(2, spec.getPartitionSpec().fields().size());
    }

    @Test
    public void testInvalidBucketTransformMissingBracket() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[10", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned(), "Should skip invalid transform");
    }

    @Test
    public void testInvalidBucketTransformNonNumeric() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[abc]", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned(), "Should skip invalid numeric parameter");
    }

    @Test
    public void testInvalidBucketTransformMissingParameter() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[]", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned());
    }

    // ========== TRUNCATE TRANSFORM TESTS ==========

    @Test
    public void testTruncateTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "truncate[4]", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
    }

    @Test
    public void testTruncateTransformWithTargetName() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "truncate[10]", "data_prefix")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "data_prefix");
    }

    @Test
    public void testTruncateTransformVariousWidths() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "truncate[2]", "prefix_2"),
                new IcebergPartitionConfig("category", "truncate[5]", "prefix_5")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(2, spec.getPartitionSpec().fields().size());
    }

    @Test
    public void testInvalidTruncateTransformMissingBracket() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "truncate[5", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned());
    }

    @Test
    public void testInvalidTruncateTransformNonNumeric() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "truncate[xyz]", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned());
    }

    // ========== VOID TRANSFORM TESTS ==========

    @Test
    public void testVoidTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "void", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
    }

    @Test
    public void testVoidTransformWithTargetName() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "void", "void_partition")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "void_partition");
    }

    @Test
    public void testVoidTransformMultipleFields() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "void", "void_1"),
                new IcebergPartitionConfig("category", "void", "void_2")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(2, spec.getPartitionSpec().fields().size());
    }

    // ========== CASE SENSITIVITY TESTS ==========

    @Test
    public void testTransformCaseInsensitive() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "YEAR", "year1"),
                new IcebergPartitionConfig("date", "Month", "month1"),
                new IcebergPartitionConfig("category", "IDENTITY", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(3, spec.getPartitionSpec().fields().size());
    }

    @Test
    public void testBucketTransformCaseInsensitive() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "BUCKET[10]", "bucket1")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "bucket1");
    }

    @Test
    public void testTruncateTransformCaseInsensitive() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "TRUNCATE[5]", "trunc1")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "trunc1");
    }

    // ========== INVALID FIELD TESTS ==========

    @Test
    public void testNonExistentSourceColumn() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("nonexistent", "identity", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned(), "Should skip non-existent column");
    }

    @Test
    public void testBlankSourceColumn() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("", "identity", null),
                new IcebergPartitionConfig(null, "year", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned());
    }

    @Test
    public void testUnsupportedTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "unknown_transform", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned());
    }

    @Test
    public void testPartiallyInvalidTransformSyntax() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket", null)  // Missing parameter
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned());
    }

    // ========== DUPLICATE COLUMN TESTS ==========

    @Test
    public void testDuplicateSourceColumn() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "identity", null),
                new IcebergPartitionConfig("id", "bucket[10]", "id_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size(),
                "Should only create one partition for duplicate source");
        // First config wins
        assertPartitionFieldExists(spec.getPartitionSpec(), "id");
    }

    @Test
    public void testDuplicateSourceColumnDifferentTransforms() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "year", "ts_year"),
                new IcebergPartitionConfig("timestamp", "month", "ts_month")  // Should be ignored
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "ts_year");
    }

    // ========== MULTIPLE PARTITIONS TESTS ==========

    @Test
    public void testMultiplePartitions() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("category", "identity", null),
                new IcebergPartitionConfig("timestamp", "year", "ts_year"),
                new IcebergPartitionConfig("id", "bucket[8]", "id_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(3, spec.getPartitionSpec().fields().size());
        assertEquals(3, spec.getExpressions().size());
    }

    @Test
    public void testMixedValidAndInvalidPartitions() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("category", "identity", null),
                new IcebergPartitionConfig("nonexistent", "year", "invalid"),
                new IcebergPartitionConfig("timestamp", "month", "ts_month")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(2, spec.getPartitionSpec().fields().size(), "Should skip invalid config");
        assertPartitionFieldExists(spec.getPartitionSpec(), "category");
        assertPartitionFieldExists(spec.getPartitionSpec(), "ts_month");
    }

    @Test
    public void testComplexMultiPartitionSpec() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("category", "identity", null),
                new IcebergPartitionConfig("timestamp", "year", "ts_year"),
                new IcebergPartitionConfig("date", "month", "date_month"),
                new IcebergPartitionConfig("id", "bucket[16]", "id_bucket"),
                new IcebergPartitionConfig("data", "truncate[5]", "data_prefix")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(5, spec.getPartitionSpec().fields().size());
        assertEquals(5, spec.getExpressions().size());
    }

    // ========== NESTED FIELD TESTS ==========

    @Test
    public void testNestedFieldAccess() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("error.receivedAt", "hour", "error_hour")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(nestedSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "error_hour");
    }

    @Test
    public void testNestedFieldIdentityTransform() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("project.name", "identity", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(nestedSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        // Identity uses the full nested path as field name
        assertPartitionFieldExists(spec.getPartitionSpec(), "project.name");
    }

    @Test
    public void testMixedFlatAndNestedFieldPartitioning() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "truncate[4]", "id_prefix"),
                new IcebergPartitionConfig("error.receivedAt", "hour", "error_hour"),
                new IcebergPartitionConfig("project.name", "identity", "project_name_partition")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(nestedSchema, configs);
        assertEquals(3, spec.getPartitionSpec().fields().size(),
                "Should create partitions for both flat and nested fields");
        assertEquals(3, spec.getExpressions().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "id_prefix");
        assertPartitionFieldExists(spec.getPartitionSpec(), "error_hour");
        // Identity transform uses source name, not target name
        assertPartitionFieldExists(spec.getPartitionSpec(), "project.name");
    }

    @Test
    public void testInvalidNestedFieldPath() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("error.nonexistent", "hour", "invalid")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(nestedSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned(), "Should skip invalid nested path");
    }

    @Test
    public void testNestedFieldWithDifferentTransforms() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("project.name", "truncate[5]", "proj_prefix"),
                new IcebergPartitionConfig("project.id", "bucket[4]", "proj_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(nestedSchema, configs);
        assertEquals(2, spec.getPartitionSpec().fields().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "proj_prefix");
        assertPartitionFieldExists(spec.getPartitionSpec(), "proj_bucket");
    }

    @Test
    public void testDeeplyNestedFieldAccess() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("error.receivedAt", "day", "error_day"),
                new IcebergPartitionConfig("project.name", "identity", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(nestedSchema, configs);
        assertEquals(2, spec.getPartitionSpec().fields().size());
    }

    @Test
    public void testInvalidNestedPathDepth() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("project.name.invalid", "identity", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(nestedSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned(),
                "Should skip nested path that goes too deep");
    }

    // ========== WHITESPACE HANDLING TESTS ==========

    @Test
    public void testTransformWithWhitespace() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "  bucket[10]  ", "id_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "id_bucket");
    }

    @Test
    public void testTransformWithWhitespaceInBrackets() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[ 10 ]", "id_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "id_bucket");
    }

    @Test
    public void testTransformWithMixedWhitespace() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "  truncate[  5  ]  ", "data_prefix")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertPartitionFieldExists(spec.getPartitionSpec(), "data_prefix");
    }

    // ========== EDGE CASES ==========

    @Test
    public void testAllInvalidConfigs() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("nonexistent1", "year", null),
                new IcebergPartitionConfig("nonexistent2", "bucket[10]", null),
                new IcebergPartitionConfig("", "identity", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertTrue(spec.getPartitionSpec().isUnpartitioned(),
                "Should return unpartitioned when all configs are invalid");
    }

    @Test
    public void testSingleValidAmongInvalid() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("nonexistent", "year", null),
                new IcebergPartitionConfig("category", "identity", null),
                new IcebergPartitionConfig("invalid_field", "bucket[10]", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(1, spec.getPartitionSpec().fields().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "category");
    }

    @Test
    public void testZeroBucketSize() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[0]", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        // This may fail or succeed depending on Iceberg validation
        // Just ensure it doesn't crash
        assertNotNull(spec);
    }

    @Test
    public void testNegativeTruncateWidth() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("data", "truncate[-5]", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        // Should handle gracefully
        assertNotNull(spec);
    }

    @Test
    public void testVeryLargeBucketSize() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[1000000]", "big_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        // Should work if Iceberg allows it
        assertNotNull(spec);
    }

    // ========== EXPRESSION VALIDATION TESTS ==========

    @Test
    public void testExpressionsMatchPartitionFields() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("category", "identity", null),
                new IcebergPartitionConfig("timestamp", "year", "ts_year"),
                new IcebergPartitionConfig("id", "bucket[8]", "id_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);

        assertEquals(spec.getPartitionSpec().fields().size(), spec.getExpressions().size(),
                "Number of expressions should match number of partition fields");
    }

    @Test
    public void testExpressionNamesForCustomTargets() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "year", "custom_year")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);

        assertEquals(1, spec.getExpressions().size());
        IcebergExpression expr = spec.getExpressions().get(0);
        assertEquals("custom_year", expr.targetName());
    }

    @Test
    public void testExpressionNamesForDefaultNames() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "year", null)
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);

        assertEquals(1, spec.getExpressions().size());
        IcebergExpression expr = spec.getExpressions().get(0);
        assertNull(expr.targetName(), "Should have null name when no target name specified");
    }

    // ========== INTEGRATION TESTS ==========

    @Test
    public void testRealWorldPartitioningScenario() {
        // Simulates a typical event data partitioning scheme
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "day", "event_date"),
                new IcebergPartitionConfig("timestamp", "hour", "event_hour"),  // Will be skipped (duplicate)
                new IcebergPartitionConfig("category", "identity", null),
                new IcebergPartitionConfig("id", "bucket[16]", "user_bucket")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);

        // First timestamp config wins, second is skipped as duplicate
        assertEquals(3, spec.getPartitionSpec().fields().size());
        assertPartitionFieldExists(spec.getPartitionSpec(), "event_date");
        assertPartitionFieldExists(spec.getPartitionSpec(), "category");
        assertPartitionFieldExists(spec.getPartitionSpec(), "user_bucket");
    }

    @Test
    public void testTimeSeriesPartitioning() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("timestamp", "year", "year"),
                new IcebergPartitionConfig("date", "month", "month")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(2, spec.getPartitionSpec().fields().size());
    }

    @Test
    public void testHighCardinalityPartitioning() {
        List<IcebergPartitionConfig> configs = List.of(
                new IcebergPartitionConfig("id", "bucket[256]", "id_bucket"),
                new IcebergPartitionConfig("category", "truncate[3]", "cat_prefix")
        );
        IcebergPartitionSpec spec = buildPartitionSpec(flatSchema, configs);
        assertEquals(2, spec.getPartitionSpec().fields().size());
    }

    // ========== HELPER METHODS ==========

    private void assertPartitionFieldExists(PartitionSpec spec, String fieldName) {
        boolean found = spec.fields().stream()
                .anyMatch(f -> f.name().equals(fieldName));
        assertTrue(found, "Partition field '" + fieldName + "' should exist in spec. "
                + "Available fields: " + spec.fields().stream()
                .map(f -> f.name())
                .collect(java.util.stream.Collectors.joining(", ")));
    }

    private void assertPartitionFieldDoesNotExist(PartitionSpec spec, String fieldName) {
        boolean found = spec.fields().stream()
                .anyMatch(f -> f.name().equals(fieldName));
        assertFalse(found, "Partition field '" + fieldName + "' should not exist in spec");
    }
}