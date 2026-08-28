/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.utils.json.types;

import static io.lakestream.ursa.materialization.serde.utils.json.PathsPrinter.print;

import java.util.Base64;
import java.util.Collection;
import java.util.Deque;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;

public class BytesArrayConverter implements AvroTypeConverter {
    public static final BytesArrayConverter INSTANCE = new BytesArrayConverter();

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        try {
            return Base64.getDecoder().decode(jsonValue.toString());
        } catch (IllegalArgumentException e) {
            throw new AvroTypeException("Field " + print(path) + " is expected to be a valid base64 string."
                                        + " current value is " + jsonValue, e);
        }
    }

    @Override
    public boolean canManage(Schema schema, Object jsonvalue, Deque<String> path) {
        return (schema.getType().equals(Schema.Type.STRING) && jsonvalue instanceof Collection);
    }
}
