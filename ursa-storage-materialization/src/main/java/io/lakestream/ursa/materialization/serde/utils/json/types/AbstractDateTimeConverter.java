/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde.utils.json.types;

import static io.lakestream.ursa.materialization.serde.utils.json.PathsPrinter.print;

import java.time.format.DateTimeParseException;
import java.util.Deque;
import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;


public abstract class AbstractDateTimeConverter implements AvroTypeConverter {

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        if (jsonValue instanceof String) {
            String dateTimeString = (String) jsonValue;
            try {
                return convertDateTimeString(dateTimeString);
            } catch (DateTimeParseException exception) {
                if (silently) {
                    return new Incompatible(getValidJsonFormat());
                } else {
                    throw new AvroTypeException("Field " + print(path) + " should be a valid "
                                                + getValidStringFormat() + ".");
                }
            }
        } else if (jsonValue instanceof Number) {
            return convertNumber((Number) jsonValue);
        }

        if (silently) {
            return new Incompatible(getValidJsonFormat());
        } else {
            throw new AvroTypeException("Field " + print(path)
                                        + " is expected to be type: java.lang.String or java.lang.Number.");
        }
    }

    @Override
    public boolean canManage(Schema schema, Object jsonValue, Deque<String> path) {
        return getUnderlyingSchemaType().equals(schema.getType())
               && AvroTypeConverter.isLogicalType(schema, getLogicalType().getName());
    }

    protected abstract Object convertDateTimeString(String dateTimeString);

    protected abstract Object convertNumber(Number numberValue);

    protected abstract Schema.Type getUnderlyingSchemaType();

    protected abstract LogicalType getLogicalType();

    private String getValidJsonFormat() {
        return getValidStringFormat() + " string, " + getValidNumberFormat() + " number";
    }

    protected abstract String getValidStringFormat();

    protected abstract String getValidNumberFormat();

}
