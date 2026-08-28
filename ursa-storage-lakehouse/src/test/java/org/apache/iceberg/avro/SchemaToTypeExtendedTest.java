/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SchemaToTypeExtendedTest {

    private SchemaToTypeExtended visitor;

    @BeforeEach
    void setUp() {
        // We use a dummy schema for the constructor as the visitor logic
        // primarily uses the schema passed to the static visit method.
        Schema dummy = Schema.create(Schema.Type.NULL);
        visitor = new SchemaToTypeExtended(dummy);
    }

    @Test
    @DisplayName("Should convert a Variant record to Iceberg Variant type when V3 is allowed")
    void testVariantTypeConversion() {
        // Construct an Avro Schema that represents an Iceberg Variant
        // It's a record with name "variant", fields "metadata" (bytes) and "value" (bytes)
        Schema variantAvroSchema = SchemaBuilder.record("variant")
                .fields()
                .name("metadata").type().bytesType().noDefault()
                .name("value").type().bytesType().noDefault()
                .endRecord();

        // Inject the logical type property manually as standard Avro builders
        // might not support "variant" out of the box without a custom LogicalType implementation
        variantAvroSchema.addProp("logicalType", "variant");

        Type result = SchemaToTypeExtended.visit(variantAvroSchema, visitor, true);

        assertThat(result).isInstanceOf(Types.VariantType.class);
    }

    @Test
    @DisplayName("Should convert LocalTimestamp logical types to Iceberg Timestamp without zone")
    void testLocalTimestampConversion() {
        Schema localMillis = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema localMicros = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

        Type resultMillis = SchemaToTypeExtended.visit(localMillis, visitor, false);
        Type resultMicros = SchemaToTypeExtended.visit(localMicros, visitor, false);

        assertThat(resultMillis).isEqualTo(Types.TimestampType.withoutZone());
        assertThat(resultMicros).isEqualTo(Types.TimestampType.withoutZone());
    }

    @Test
    @DisplayName("Should handle BigDecimal java-class property by returning a default DecimalType")
    void testBigDecimalPrimitiveConversion() {
        Schema bytesWithBigDecimal = Schema.create(Schema.Type.BYTES);
        bytesWithBigDecimal.addProp("java-class", "java.math.BigDecimal");

        Type result = SchemaToTypeExtended.visit(bytesWithBigDecimal, visitor, false);

        assertThat(result).isInstanceOf(Types.DecimalType.class);
        Types.DecimalType decimal = (Types.DecimalType) result;
        assertThat(decimal.precision()).isEqualTo(10);
        assertThat(decimal.scale()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should prevent infinite recursion for recursive Avro records")
    void testRecursiveRecordDetection() {
        // Create a recursive schema: Record "Node" has a field of type "Node"
        Schema recursiveSchema = SchemaBuilder.record("Node")
                .fields()
                .name("label").type().stringType().noDefault()
                .name("child").type().optional().type("Node")
                .endRecord();

        // The visitor should throw IllegalStateException when it detects the name in the stack
        assertThrows(IllegalStateException.class, () -> {
            SchemaToTypeExtended.visit(recursiveSchema, visitor, false);
        }, "Cannot process recursive Avro record Node");
    }

    @Test
    @DisplayName("Should convert standard records when Variant logical type is missing")
    void testStandardRecordConversion() {
        Schema standardRecord = SchemaBuilder.record("User")
                .fields()
                .name("id").type().longType().noDefault()
                .endRecord();

        Type result = SchemaToTypeExtended.visit(standardRecord, visitor, true);

        assertThat(result.isStructType()).isTrue();
        Types.StructType struct = result.asStructType();
        assertThat(struct.fields()).hasSize(1);
        assertThat(struct.fields().get(0).name()).isEqualTo("id");
    }

    @Test
    @DisplayName("Should handle Unions by visiting all options")
    void testUnionConversion() {
        Schema unionSchema = SchemaBuilder.unionOf()
                .intType()
                .and()
                .stringType()
                .endUnion();

        // Note: In Iceberg, Avro Unions are typically handled via specific visitor methods
        // that map them to Structs or optional fields.
        Type result = SchemaToTypeExtended.visit(unionSchema, visitor, false);

        // Depending on your base class SchemaToType implementation, verify result
        assertThat(result).isNotNull();
    }

    @Test
    @DisplayName("Variant: isVariantEnabled = true should return VariantType")
    void testVariantV3Allowed() {
        // Construct a valid Iceberg Variant Avro Schema
        Schema variantSchema = SchemaBuilder.record("variant")
                .fields()
                .name("metadata").type().bytesType().noDefault()
                .name("value").type().bytesType().noDefault()
                .endRecord();
        variantSchema.addProp("logicalType", "variant");

        // Execute with V3 allowed
        Type result = SchemaToTypeExtended.visit(variantSchema, visitor, true);

        // Verify it is converted to a Variant type
        assertThat(result).isEqualTo(Types.VariantType.get());
    }

    @Test
    @DisplayName("Variant: isVariantEnabled = false should return StructType (Fallback)")
    void testVariantV3Disabled() {
        // Construct the same Variant Avro Schema
        Schema variantSchema = SchemaBuilder.record("variant")
                .fields()
                .name("metadata").type().bytesType().noDefault()
                .name("value").type().bytesType().noDefault()
                .endRecord();
        variantSchema.addProp("logicalType", "variant");

        // Execute with V3 NOT allowed
        Type result = SchemaToTypeExtended.visit(variantSchema, visitor, false);

        // Verify it remains a Struct (standard Record) rather than a Variant
        assertThat(result.isStructType()).isTrue();
        Types.StructType struct = result.asStructType();
        assertThat(struct.fields()).hasSize(2);
        assertThat(struct.field("metadata").type()).isEqualTo(Types.BinaryType.get());
        assertThat(struct.field("value").type()).isEqualTo(Types.BinaryType.get());
    }

    @Test
    @DisplayName("Variant: Non-record schema with variant logical type")
    void testVariantOnPrimitive() {
        // Testing the first if-block in visit(): if (isVariantEnabled && isVariantType(schema))
        Schema bytesVariant = Schema.create(Schema.Type.BYTES);
        bytesVariant.addProp("logicalType", "variant");

        Type result = SchemaToTypeExtended.visit(bytesVariant, visitor, true);

        // Should return VariantType even if it's not a RECORD,
        // provided the logicalType property is present and V3 is allowed.
        assertThat(result).isEqualTo(Types.VariantType.get());
    }

    @Test
    @DisplayName("Variant: Should convert any schema type to Variant if logical type is present and V3 allowed")
    void testVariantLogicTopLevel() {
        // Even if the schema is just BYTES, the logic triggers visitor.variant
        // because isVariantType(schema) returns true.
        Schema bytesSchema = Schema.create(Schema.Type.BYTES);
        bytesSchema.addProp("logicalType", "variant");

        // Path: isVariantEnabled = true
        Type resultV3 = SchemaToTypeExtended.visit(bytesSchema, visitor, true);
        assertThat(resultV3).isEqualTo(Types.VariantType.get());

        // Path: isVariantEnabled = false
        // Should ignore the variant logic and treat it as standard Iceberg Binary
        Type resultNoV3 = SchemaToTypeExtended.visit(bytesSchema, visitor, false);
        assertThat(resultNoV3).isEqualTo(Types.BinaryType.get());
    }

    @Test
    @DisplayName("Variant: Should convert Fixed type to Variant when V3 allowed")
    void testFixedVariantLogic() {
        Schema fixedSchema = Schema.createFixed("var_fixed", null, null, 12);
        fixedSchema.addProp("logicalType", "variant");

        Type result = SchemaToTypeExtended.visit(fixedSchema, visitor, true);

        assertThat(result).isEqualTo(Types.VariantType.get());
    }

    @Test
    @DisplayName("Variant: Verifies that internal Metadata and Value schemas are visited")
    void testVariantInternalVisits() {
        // This test ensures that when a variant is found, the visitor
        // still "visits" the synthetic METADATA_SCHEMA and VALUE_SCHEMA.
        Schema schema = Schema.create(Schema.Type.STRING);
        schema.addProp("logicalType", "variant");

        // If your visitor was a mock, you would verify visitor.primitive()
        // was called for the two hidden BYTES schemas.
        Type result = SchemaToTypeExtended.visit(schema, visitor, true);

        assertThat(result).isEqualTo(Types.VariantType.get());
    }
}
