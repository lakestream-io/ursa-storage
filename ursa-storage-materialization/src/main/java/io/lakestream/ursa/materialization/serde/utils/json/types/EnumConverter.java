/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import static io.lakestream.ursa.materialization.serde.utils.json.PathsPrinter.print;
import static java.util.stream.Collectors.joining;

import java.util.Deque;
import java.util.List;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class EnumConverter extends AvroTypeConverterWithStrictJavaTypeCheck<String> {
    public static final AvroTypeConverter INSTANCE = new EnumConverter();

    private EnumConverter() {
        super(String.class);
    }

    @Override
    public Object convertValue(Schema.Field field, Schema schema, String value, Deque<String> path, boolean silently) {
        List<String> symbols = schema.getEnumSymbols();
        if (symbols.contains(value)) {
            return new GenericData.EnumSymbol(schema, value);
        }
        throw enumException(path, symbols.stream().map(String::valueOf).collect(joining(", ")));
    }

    @Override
    public boolean canManage(Schema schema, Object jsonValue, Deque<String> path) {
        return schema.getType().equals(Schema.Type.ENUM);
    }

    private static AvroTypeException enumException(Deque<String> fieldPath, String expectedSymbols) {
        return new AvroTypeException("Field " + print(fieldPath)
                                     + " is expected to be of enum type and be one of " + expectedSymbols);
    }
}
