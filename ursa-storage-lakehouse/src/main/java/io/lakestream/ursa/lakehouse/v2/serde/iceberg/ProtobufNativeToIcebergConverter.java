/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2.serde.iceberg;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;

public class ProtobufNativeToIcebergConverter {

    public static GenericRecord convertProtobufToIcebergRecord(DynamicMessage protobufMessage,
                                                               Schema icebergSchema) {
        if (protobufMessage == null || icebergSchema == null) {
            throw new IllegalArgumentException("Protobuf message and Iceberg schema must not be null");
        }

        return convertProtobufToIcebergRecord(protobufMessage, icebergSchema, protobufMessage.getDescriptorForType());
    }

    public static GenericRecord convertProtobufToIcebergRecord(DynamicMessage protobufMessage,
                                                               Schema icebergSchema,
                                                               Descriptors.Descriptor descriptor) {

        GenericRecord icebergRecord = GenericRecord.create(icebergSchema);

        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            Object protobufValue = protobufMessage.getField(field);
            Types.NestedField icebergField = icebergSchema.findField(field.getName());

            if (icebergField != null) {
                Object icebergValue = convertProtobufValueToIcebergValue(protobufValue, field, icebergField.type());
                icebergRecord.setField(field.getName(), icebergValue);
            }
        }

        return icebergRecord;
    }

    public static Object convertProtobufValueToIcebergValue(Object protobufValue,
                                                            Descriptors.FieldDescriptor field,
                                                            Type icebergType) {
        switch (field.getType()) {
            case INT32:
            case SINT32:
            case FIXED32:
            case SFIXED32:
                if (field.isRepeated()) {
                    List<Integer> icebergList = new ArrayList<>();
                    for (Object value : (List<?>) protobufValue) {
                        icebergList.add(((Number) value).intValue());
                    }

                    if (icebergType.isVariantType()) {
                        return Variant.of(VariantMetadata.empty(), Variants.of(icebergList.toString()));
                    }

                    return icebergList;
                } else if (icebergType.isVariantType()) {
                    return Variant.of(VariantMetadata.empty(), Variants.of(((Number) protobufValue).intValue()));
                } else {
                    return ((Number) protobufValue).intValue();
                }
            case UINT32: // Mapping UINT32 to Long for Iceberg compatibility to avoid data overflow
            case INT64:
            case UINT64:
            case SINT64:
            case FIXED64:
            case SFIXED64:
                if (field.isRepeated()) {
                    List<Object> icebergList = new ArrayList<>();
                    for (Object value : (List<?>) protobufValue) {
                        if (icebergType instanceof Types.IntegerType) {
                            // Special handling for UINT32 mapped to Iceberg IntegerType
                            icebergList.add(((Number) value).intValue());
                        } else {
                            icebergList.add(((Number) value).longValue());
                        }
                    }

                    if (icebergType.isVariantType()) {
                        return Variant.of(VariantMetadata.empty(), Variants.of(icebergList.toString()));
                    }
                    return icebergList;
                } else if (icebergType.isVariantType()) {
                    return Variant.of(VariantMetadata.empty(), Variants.of(((Number) protobufValue).longValue()));
                } else if (icebergType instanceof Types.IntegerType) {
                    // Special handling for UINT32 mapped to Iceberg IntegerType
                    return ((Number) protobufValue).intValue();
                } else {
                    return ((Number) protobufValue).longValue();
                }

            case FLOAT:
                if (field.isRepeated()) {
                    List<Float> icebergList = new ArrayList<>();
                    for (Object value : (List<?>) protobufValue) {
                        icebergList.add(((Number) value).floatValue());
                    }
                    if (icebergType.isVariantType()) {
                        return Variant.of(VariantMetadata.empty(), Variants.of(icebergList.toString()));
                    }
                    return icebergList;
                } else if (icebergType.isVariantType()) {
                    return Variant.of(VariantMetadata.empty(), Variants.of(((Number) protobufValue).floatValue()));
                } else {
                    return ((Number) protobufValue).floatValue();
                }

            case DOUBLE:
                if (field.isRepeated()) {
                    List<Double> icebergList = new ArrayList<>();
                    for (Object value : (List<?>) protobufValue) {
                        icebergList.add(((Number) value).doubleValue());
                    }

                    if (icebergType.isVariantType()) {
                        return Variant.of(VariantMetadata.empty(), Variants.of(icebergList.toString()));
                    }
                    return icebergList;
                } else if (icebergType.isVariantType()) {
                    return Variant.of(VariantMetadata.empty(), Variants.of(((Number) protobufValue).doubleValue()));
                } else {
                    return ((Number) protobufValue).doubleValue();
                }

            case BOOL:
                if (field.isRepeated()) {
                    List<Boolean> icebergList = new ArrayList<>();
                    for (Object value : (List<?>) protobufValue) {
                        icebergList.add((Boolean) value);
                    }

                    if (icebergType.isVariantType()) {
                        return Variant.of(VariantMetadata.empty(), Variants.of(icebergList.toString()));
                    }

                    return icebergList;
                } else if (icebergType.isVariantType()) {
                    return Variant.of(VariantMetadata.empty(), Variants.of((Boolean) protobufValue));
                } else {
                    return protobufValue;
                }

            case STRING:
                if (field.isRepeated()) {
                    List<String> icebergList = new ArrayList<>();
                    for (Object value : (List<?>) protobufValue) {
                        icebergList.add((String) value);
                    }

                    if (icebergType.isVariantType()) {
                        return Variant.of(VariantMetadata.empty(), Variants.of(icebergList.toString()));
                    }
                    return icebergList;
                } else if (icebergType.isVariantType()) {
                    return Variant.of(VariantMetadata.empty(), Variants.of((String) protobufValue));
                } else {
                    return protobufValue;
                }

            case BYTES:
                if (field.isRepeated()) {
                    List<ByteBuffer> icebergList = new ArrayList<>();
                    for (Object value : (List<?>) protobufValue) {
                        icebergList.add(ByteBuffer.wrap(((ByteString) value).toByteArray()));
                    }

                    if (icebergType.isVariantType()) {
                        return Variant.of(VariantMetadata.empty(), Variants.of(icebergList.toString()));
                    }

                    return icebergList;
                } else if (icebergType.isVariantType()) {
                    return Variant.of(VariantMetadata.empty(),
                            Variants.of(ByteBuffer.wrap(((ByteString) protobufValue).toByteArray())));
                } else {
                    return ByteBuffer.wrap(((ByteString) protobufValue).toByteArray());
                }

            case MESSAGE:
                // --- Repeated Message Handling ---
                if (field.isRepeated()) {
                    List<?> protobufList = (List<?>) protobufValue;

                    if (icebergType.isVariantType()) {
                        // If the target is a VARIANT, serialize the whole list into a single, comprehensive value
                        // (e.g., JSON string) and put that into the Variant.

                        // NOTE: Serializing a list of DynamicMessages to a string (like JSON)
                        // is a common, though slow, way to represent complex data in a single Variant field.
                        StringBuilder combinedJson = new StringBuilder("[");
                        boolean first = true;
                        for (Object value : protobufList) {
                            if (!first) {
                                combinedJson.append(",");
                            }
                            // Using toString() on DynamicMessage usually yields a debugging format (not JSON).
                            // For a robust solution, you would need a JSON serializer for DynamicMessage here.
                            // For now, we use the Protobuf-provided debug string representation.
                            combinedJson.append(((DynamicMessage) value).toString());
                            first = false;
                        }
                        combinedJson.append("]");

                        return Variant.of(VariantMetadata.empty(), Variants.of(combinedJson.toString()));

                    } else {
                        // Standard conversion to an Iceberg List of Structs
                        // The outer type MUST be a ListType if it's not a Variant.
                        Types.ListType listType = (Types.ListType) icebergType; // SAFE CAST HERE
                        Types.StructType elementType = listType.elementType().asStructType();
                        Schema elementSchema = new Schema(elementType.fields());

                        List<GenericRecord> icebergList = new ArrayList<>();
                        for (Object value : protobufList) {
                            icebergList.add(convertProtobufToIcebergRecord((DynamicMessage) value,
                                    elementSchema, field.getMessageType()));
                        }
                        return icebergList;
                    }
                } else {  // --- Scalar Message Handling ---
                    DynamicMessage nestedProtobufMessage = (DynamicMessage) protobufValue;

                    if (icebergType.isVariantType()) {
                        // FIX: Serialize the single DynamicMessage to its wire format bytes
                        // and store the ByteBuffer in the Variant.
                        byte[] rawBytes = nestedProtobufMessage.toByteArray();
                        ByteBuffer serializedData = ByteBuffer.wrap(rawBytes);

                        return Variant.of(VariantMetadata.empty(), Variants.of(serializedData));

                    } else {
                        // Standard conversion to an Iceberg Struct
                        // The outer type MUST be a StructType if it's not a Variant.
                        Types.StructType structType = icebergType.asStructType(); // SAFE CAST HERE
                        Schema nestedSchema = new Schema(structType.fields());
                        return convertProtobufToIcebergRecord(nestedProtobufMessage,
                                nestedSchema, field.getMessageType());
                    }
                }
            case ENUM:
                if (field.isRepeated()) {
                    List<String> icebergList = new ArrayList<>();
                    for (Object value : (List<?>) protobufValue) {
                        icebergList.add(((Descriptors.EnumValueDescriptor) value).getName());
                    }

                    if (icebergType.isVariantType()) {
                        return Variant.of(VariantMetadata.empty(), Variants.of(icebergList.toString()));
                    }

                    return icebergList;
                } else if (icebergType.isVariantType()) {
                    return Variant.of(VariantMetadata.empty(),
                            Variants.of(((Descriptors.EnumValueDescriptor) protobufValue).getName()));
                } else {
                    return ((Descriptors.EnumValueDescriptor) protobufValue).getName();
                }

            default:
                throw new RuntimeException("Unsupported field type: " + field.getType());
        }
    }
}