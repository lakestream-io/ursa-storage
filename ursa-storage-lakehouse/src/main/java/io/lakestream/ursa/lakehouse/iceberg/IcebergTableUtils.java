/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

public class IcebergTableUtils {

    /**
     * Checks if the given Iceberg schema contains any data types introduced in Spec V3.
     * V3 Types: Unknown, TimestampNano, Variant, Geometry, Geography.
     */
    public static boolean containsV3Types(Schema schema) {
        AtomicBoolean hasV3 = new AtomicBoolean(false);
        TypeUtil.find(schema, type -> {
            if (isV3Type(type)) {
                hasV3.set(true);
            }
            return false; // continue searching
        });

        return hasV3.get();
    }

    private static boolean isV3Type(Type type) {
        // Check for specific V3 classes or TypeIDs
        // Note: Exact class names depend on your specific Iceberg library version (1.6.0+ or 1.7.0+)
        // which officially supports these experimental or finalized V3 types.

        switch (type.typeId()) {
            case UNKNOWN:
            case TIMESTAMP_NANO:   // timestamp_ns
            case VARIANT:
            case GEOMETRY:
            case GEOGRAPHY:
                return true;
            default:
                return false;
        }
    }

    public static boolean isV2TableFormat(Map<String, String> tableProperties) {
        String formatVersionStr = tableProperties.get(FORMAT_VERSION);
        if (StringUtils.isBlank(formatVersionStr)) {
            return true; // default v2
        }
        try {
            return Integer.parseInt(formatVersionStr) < 3;
        } catch (NumberFormatException e) {
            return true;
        }
    }
}
