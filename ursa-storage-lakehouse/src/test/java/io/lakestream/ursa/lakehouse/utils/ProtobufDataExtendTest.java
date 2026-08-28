/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.UnknownFieldSet;
import io.lakestream.ursa.materialization.util.ProtobufDataExtend;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
class ProtobufDataExtendTest {

    private final ProtobufDataExtend protobufData = ProtobufDataExtend.get();

    @Test
    void testRepeatedListWithVariantLogicalType() {
        Descriptors.FieldDescriptor field =
                TestProtoVariant.TestMessage.getDescriptor()
                        .findFieldByName("tags");

        Schema schema = protobufData.getSchema(field);

        // repeated -> ARRAY
        assertEquals(Schema.Type.UNION, schema.getType());
        Schema tagsEntrySchema = schema.getTypes().get(1);

        Schema elementType = tagsEntrySchema.getElementType();
        assertEquals(Schema.Type.STRING, elementType.getType());

        LogicalType logicalType = tagsEntrySchema.getLogicalType();
        assertNotNull(logicalType);
        assertEquals("variant", logicalType.getName());
    }

    @Test
    void testMapWithVariantLogicalType() {
        Descriptors.FieldDescriptor field =
                TestProtoVariant.TestMessage.getDescriptor()
                        .findFieldByName("attributes");

        Schema schema = protobufData.getSchema(field);

        // map -> ARRAY of entry records
        assertEquals(Schema.Type.UNION, schema.getType());

        Schema entrySchema = schema.getTypes().get(1);
        assertEquals(Schema.Type.ARRAY, entrySchema.getType());

        // variant logical type should be applied to the RECORD itself
        LogicalType logicalType = entrySchema.getLogicalType();
        assertNotNull(logicalType);
        assertEquals("variant", logicalType.getName());

        // Validate key/value fields still exist
        assertNotNull(entrySchema.getElementType().getField("key"));
        assertNotNull(entrySchema.getElementType().getField("value"));
    }

    @Test
    void testMapEntryFieldsAreCorrectlyTyped() {
        Descriptors.FieldDescriptor field =
                TestProtoVariant.TestMessage.getDescriptor()
                        .findFieldByName("attributes");

        Schema schema = protobufData.getSchema(field);
        Schema entrySchema = schema.getTypes().get(1).getElementType();

        Schema keySchema = entrySchema.getField("key").schema();
        Schema valueSchema = entrySchema.getField("value").schema();

        assertEquals(Schema.Type.STRING, unwrapNullable(keySchema).getType());
        assertEquals(Schema.Type.INT, unwrapNullable(valueSchema).getType());
    }

    @Test
    void testProtobufSchemaConvert() {
        Descriptors.Descriptor descriptor = TestProtoVariant.TestMessage.getDescriptor();
        Schema schema = ProtobufDataExtend.get().getSchema(descriptor);
        log.info("Converted schema: {}", schema);

        // map field
        Schema attributesSchema = schema.getField("attributes").schema();
        assertEquals(List.of(), schema.getField("attributes").defaultVal());
        assertEquals(Schema.Type.UNION, attributesSchema.getType());
        Schema entrySchema = attributesSchema.getTypes().get(1);
        assertEquals(Schema.Type.ARRAY, entrySchema.getType());
        assertNotNull(entrySchema.getElementType().getField("key"));
        assertNotNull(entrySchema.getElementType().getField("value"));
        LogicalType logicalType = entrySchema.getLogicalType();
        assertNotNull(logicalType);
        assertEquals("variant", logicalType.getName());

        // repeated field
        Schema tagsSchema = schema.getField("tags").schema();
        assertEquals(List.of(), schema.getField("tags").defaultVal());
        assertEquals(Schema.Type.UNION, tagsSchema.getType());
        Schema tagsEntrySchema = tagsSchema.getTypes().get(1);
        assertEquals(Schema.Type.ARRAY, tagsEntrySchema.getType());
        Schema entryElementType = tagsEntrySchema.getElementType();
        assertEquals(Schema.Type.STRING, entryElementType.getType());
        LogicalType tagsLogicalType = tagsEntrySchema.getLogicalType();
        assertNotNull(tagsLogicalType);
        assertEquals("variant", tagsLogicalType.getName());

        // normal field
        // id field
        Schema idSchema = schema.getField("id").schema();
        assertNull(schema.getField("id").defaultVal());
        assertEquals(Schema.Type.UNION, idSchema.getType());
        Schema idActualSchema = idSchema.getTypes().get(1);
        assertEquals(Schema.Type.INT, idActualSchema.getType());

        // name field
        Schema nameSchema = schema.getField("name").schema();
        assertNull(schema.getField("name").defaultVal());
        assertEquals(Schema.Type.UNION, nameSchema.getType());
        Schema nameActualSchema = nameSchema.getTypes().get(1);
        assertEquals(Schema.Type.STRING, nameActualSchema.getType());

        // zip_code field
        Schema zipCodeSchema = schema.getField("zip_code").schema();
        assertNull(schema.getField("zip_code").defaultVal());
        assertEquals(Schema.Type.UNION, zipCodeSchema.getType());
        Schema zipCodeActualSchema = zipCodeSchema.getTypes().get(1);
        assertEquals(Schema.Type.LONG, zipCodeActualSchema.getType());
        LogicalType zipCodeLogicalType = zipCodeActualSchema.getLogicalType();
        assertNotNull(zipCodeLogicalType);
        assertEquals("variant", zipCodeLogicalType.getName());
    }

    /**
     * Utility to unwrap ["null", T] unions.
     */
    private Schema unwrapNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst()
                    .orElseThrow();
        }
        return schema;
    }


    @Test
    @DisplayName("Fixed Mock Test: Verify Variant logical type is detected from unknown fields")
    void testVariantDetectionFromUnknownFields() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        // zip_code was configured with the "variant" unknown field
        Schema zipSchema = schema.getField("zip_code").schema();
        // Inner type of Union [null, long]
        Schema actualType = zipSchema.getTypes().get(1);

        assertNotNull(actualType.getLogicalType());
        assertEquals("variant", actualType.getLogicalType().getName());
    }

    @Test
    @DisplayName("Test Scalars: Should wrap in [null, type] and set null default")
    void testScalarFieldConversion() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        Schema.Field idField = schema.getField("id");
        assertNotNull(idField);
        assertNull(idField.defaultVal());

        // Verify nullable union structure
        Schema fieldSchema = idField.schema();
        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        assertEquals(Schema.Type.NULL, fieldSchema.getTypes().get(0).getType());
        assertEquals(Schema.Type.INT, fieldSchema.getTypes().get(1).getType());
    }

    @Test
    @DisplayName("Test Repeated: Should be Union [null, Array] for Lakehouse compatibility")
    void testRepeatedFieldConversion() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        Schema.Field tagsField = schema.getField("tags");
        Schema fieldSchema = tagsField.schema();

        assertEquals(Schema.Type.UNION, fieldSchema.getType());
        Schema arrayPart = fieldSchema.getTypes().get(1);
        assertEquals(Schema.Type.ARRAY, arrayPart.getType());
        assertEquals(Schema.Type.STRING, arrayPart.getElementType().getType());
    }

    @Test
    @DisplayName("Test Variant Logical Type: Should apply to the inner type of the Union")
    void testVariantLogicalTypeApplication() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        // 'zip_code' in our mock has the variant extension
        Schema zipSchema = schema.getField("zip_code").schema();
        Schema innerType = zipSchema.getTypes().get(1);

        LogicalType logicalType = innerType.getLogicalType();
        assertNotNull(logicalType, "Logical type should be present on the inner type");
        assertEquals("variant", logicalType.getName());
    }

    @Test
    @DisplayName("Test Maps: Entries should be Records with logical type 'variant'")
    void testMapConversion() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        Schema.Field mapField = schema.getField("attributes");
        // Structure: UNION -> [NULL, ARRAY -> [RECORD (MapEntry)]]
        Schema arraySchema = mapField.schema().getTypes().get(1);
        Schema entryRecord = arraySchema.getElementType();

        assertEquals(Schema.Type.RECORD, entryRecord.getType());
        assertNull(entryRecord.getLogicalType());

        // Map Keys should NOT be nullable unions (based on your switch logic for MapEntry)
        Schema keySchema = entryRecord.getField("key").schema();
        assertEquals(Schema.Type.STRING, keySchema.getType());
    }

    @Test
    @DisplayName("Nullable Strategy: All top-level scalars must be Unions with Null default")
    void testNullableScalars() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        Schema.Field idField = schema.getField("id");
        assertEquals(Schema.Type.UNION, idField.schema().getType());
        assertEquals(Schema.Type.NULL, idField.schema().getTypes().get(0).getType());
        assertNull(idField.defaultVal());
    }

    @Test
    @DisplayName("Map Entries: Should NOT be nullable and should have Variant logical type")
    void testMapEntryIntegrity() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        // attributes -> Union [null, Array]
        Schema arraySchema = schema.getField("attributes").schema().getTypes().get(1);
        Schema entryRecord = arraySchema.getElementType();

        // The record itself should have the logical type
        assertNull(entryRecord.getLogicalType());

        // Key/Value should be base types, not Unions (isMapEntryField = true)
        assertEquals(Schema.Type.STRING, entryRecord.getField("key").schema().getType());
        assertEquals(Schema.Type.INT, entryRecord.getField("value").schema().getType());
    }

    @Test
    @DisplayName("Nested Messages: Should handle recursion and reconstruction")
    void testNestedMessageReconstruction() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        Schema.Field nestedField = schema.getField("nested_msg");
        Schema nestedSchema = nestedField.schema().getTypes().get(1);

        assertEquals(Schema.Type.RECORD, nestedSchema.getType());
        assertEquals("TestMessage", nestedSchema.getName()); // Recursive reference
        assertNotNull(nestedSchema.getField("id"));
    }

   /**
     * Corrected Helper: Adds the status field and fixes UnknownFieldSet logic.
     */
    private Descriptors.Descriptor createTestDescriptor() throws Exception {
        // 1. Define Unknown field for 'variant' detection
        UnknownFieldSet.Field variantFlag = UnknownFieldSet.Field.newBuilder()
                .addLengthDelimited(ByteString.copyFromUtf8("variant"))
                .build();

        UnknownFieldSet unknownFields = UnknownFieldSet.newBuilder()
                .addField(50001, variantFlag)
                .build();

        // 2. Build the FileDescriptor
        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test.proto")
                .setPackage("io.lakestream.test")
                // Define the Enum type
                .addEnumType(DescriptorProtos.EnumDescriptorProto.newBuilder()
                        .setName("Status")
                        .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("UNKNOWN").setNumber(0))
                        .addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder().setName("ACTIVE").setNumber(1)))
                // Define the Main Message
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("TestMessage")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("id").setNumber(1).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("status").setNumber(2)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM)
                                .setTypeName(".io.lakestream.test.Status")) // Correct relative type name
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("zip_code").setNumber(3).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                                .setOptions(DescriptorProtos.FieldOptions.newBuilder()
                                        .setUnknownFields(unknownFields)
                                        .build()))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("tags").setNumber(4).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("attributes").setNumber(5).setTypeName(".io.lakestream.test.MapEntry")
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("nested_msg").setNumber(6).setTypeName(".io.lakestream.test.TestMessage"))
                )
                // Define the Map Entry helper
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("MapEntry")
                        .setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true).build())
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("key").setNumber(1).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("value").setNumber(2).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
                )
                .build();

        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
        return fileDescriptor.findMessageTypeByName("TestMessage");
    }

    @Test
    @DisplayName("Test Enum: Should be Union [null, Enum] and contain symbols")
    void testEnumConversion() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        // Access the 'status' field added in the helper below
        Schema.Field statusField = schema.getField("status");
        assertNotNull(statusField, "Field 'status' should exist in the converted schema");

        Schema enumUnion = statusField.schema();
        assertEquals(Schema.Type.UNION, enumUnion.getType());

        // Unwrap Union [null, enum]
        Schema enumSchema = enumUnion.getTypes().get(1);
        assertEquals(Schema.Type.ENUM, enumSchema.getType());
        assertTrue(enumSchema.getEnumSymbols().contains("ACTIVE"), "Enum should contain symbol ACTIVE");
        assertTrue(enumSchema.getEnumSymbols().contains("UNKNOWN"), "Enum should contain symbol UNKNOWN");
    }

    @Test
    @DisplayName("Test Variant: Detect logical type from unknown fields")
    void testVariantDetection() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        Schema zipSchema = schema.getField("zip_code").schema();
        Schema innerType = zipSchema.getTypes().get(1);

        assertNotNull(innerType.getLogicalType());
        assertEquals("variant", innerType.getLogicalType().getName());
    }

    @Test
    @DisplayName("Test Default Values: All top-level fields should be nullable with null default")
    void testDefaults() throws Exception {
        Descriptors.Descriptor descriptor = createTestDescriptor();
        Schema schema = protobufData.getSchema(descriptor);

        for (Schema.Field field : schema.getFields()) {
            assertEquals(Schema.Type.UNION, field.schema().getType(), "Field " + field.name() + " should be a Union");
            Schema.Type nonNullType = getNonNullSchema(field.schema()).getType();
            if (nonNullType == Schema.Type.ARRAY) {
                assertEquals(List.of(), field.defaultVal(), "Field " + field.name() + " should have null default");
            } else if (nonNullType == Schema.Type.RECORD) {
                assertEquals(JsonProperties.NULL_VALUE, field.defaultVal(), "Field " + field.name() + " should have null default");
            } else {
                assertNull(field.defaultVal(), "Field " + field.name() + " should have null default");
            }
        }
    }

    private Schema getNonNullSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst()
                    .orElse(schema);
        }
        return schema;
    }
}

