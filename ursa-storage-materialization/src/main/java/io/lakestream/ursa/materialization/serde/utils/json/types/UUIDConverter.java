/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.utils.json.types;

import static io.lakestream.ursa.materialization.serde.utils.json.PathsPrinter.print;

import java.util.Deque;
import java.util.UUID;
import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class UUIDConverter implements AvroTypeConverter {

    public static final UUIDConverter INSTANCE = new UUIDConverter();

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        if (jsonValue instanceof String stringValue) {
            return UUID.fromString(stringValue);
        }
        throw new AvroTypeException("Field " + print(path) + " is expected to be type: java.lang.String");
    }

    @Override
    public boolean canManage(Schema schema, Object jsonvalue, Deque<String> path) {
        return Schema.Type.FIXED.equals(schema.getType())
               && AvroTypeConverter.isLogicalType(schema, LogicalTypes.uuid().getName());
    }
}
