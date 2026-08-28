/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;

public final class DeltaVariantUtils {


    public static final String VALUE = "value";
    public static final String METADATA = "metadata";
    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final StructType VARIANT_SCHEMA = new StructType(List.of(
            new StructField(VALUE, BinaryType.BINARY, false),
            new StructField(METADATA, BinaryType.BINARY, false)));

    private DeltaVariantUtils() {
    }

    public static StructType variantSchema() {
        return VARIANT_SCHEMA;
    }

    public static GenericRow fromJson(String json) {
        if (json == null) {
            return null;
        }
        try {
            Variant variant = VariantBuilder.parseJson(json, false);
            return fromSparkVariant(variant);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to encode Delta variant using Spark variant codec", e);
        }
    }

    public static GenericRow fromValue(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return fromJson(MAPPER.writeValueAsString(value));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to serialize value to Delta variant JSON", e);
        }
    }

    public static GenericRow fromJsonNode(JsonNode jsonNode) {
        return fromValue(jsonNode);
    }

    public static String deserializeToJsonString(byte[] metadataBytes, byte[] valueBytes) {
        Variant variant = new Variant(valueBytes, metadataBytes);
        return variant.toJson(UTC_ZONE_ID);
    }

    public static String deserializeString(byte[] metadataBytes, byte[] valueBytes) {
        JsonNode node;
        try {
            node = MAPPER.readTree(deserializeToJsonString(metadataBytes, valueBytes));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize variant string", e);
        }
        return node.isNull() ? null : node.textValue();
    }

    private static GenericRow fromSparkVariant(Variant sparkVariant) {
        var ordinalToValue = new HashMap<Integer, Object>();
        ordinalToValue.put(VARIANT_SCHEMA.indexOf(VALUE), sparkVariant.getValue());
        ordinalToValue.put(VARIANT_SCHEMA.indexOf(METADATA), sparkVariant.getMetadata());
        return new GenericRow(VARIANT_SCHEMA, ordinalToValue);
    }
}
