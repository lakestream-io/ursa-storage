/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.utils;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class IcebergSchemaToTypeUtil {

    public static Type primitive(Schema primitive) {
        // first check supported logical types
        LogicalType logical = primitive.getLogicalType();
        if (logical != null) {
            String name = logical.getName();
            if (logical instanceof LogicalTypes.Decimal) {
                return Types.DecimalType.of(
                        ((LogicalTypes.Decimal) logical).getPrecision(),
                        ((LogicalTypes.Decimal) logical).getScale());

            } else if (logical instanceof LogicalTypes.Date) {
                return Types.DateType.get();

            } else if (
                    logical instanceof LogicalTypes.TimeMillis
                            || logical instanceof LogicalTypes.TimeMicros) {
                return Types.TimeType.get();
            } else if (
                    logical instanceof LogicalTypes.TimestampMillis
                            || logical instanceof LogicalTypes.TimestampMicros) {
                if (AvroSchemaUtil.isTimestamptz(primitive)) {
                    return Types.TimestampType.withZone();
                } else {
                    return Types.TimestampType.withoutZone();
                }
            } else if (logical instanceof LogicalTypes.LocalTimestampMillis
                    || logical instanceof LogicalTypes.LocalTimestampMicros) {
                return Types.TimestampType.withoutZone();
            } else if (LogicalTypes.uuid().getName().equals(name)) {
                return Types.UUIDType.get();
            }
        }

        String javaClassStr = primitive.getProp("java-class");
        if (javaClassStr != null) {
            if (javaClassStr.contains("BigDecimal")) {
                return Types.DecimalType.of(10, 2);
            }
        }

        switch (primitive.getType()) {
            case BOOLEAN:
                return Types.BooleanType.get();
            case INT:
                return Types.IntegerType.get();
            case LONG:
                return Types.LongType.get();
            case FLOAT:
                return Types.FloatType.get();
            case DOUBLE:
                return Types.DoubleType.get();
            case STRING:
            case ENUM:
                return Types.StringType.get();
            case FIXED:
                return Types.FixedType.ofLength(primitive.getFixedSize());
            case BYTES:
                return Types.BinaryType.get();
            case NULL:
                return null;
        }

        throw new UnsupportedOperationException(
                "Unsupported primitive type: " + primitive);
    }
}
