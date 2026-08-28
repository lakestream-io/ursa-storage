/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.function.Function;
import org.apache.avro.Schema;


public class PrimitiveConverter<T> extends AvroTypeConverterWithStrictJavaTypeCheck<T> {
    public static final AvroTypeConverter BOOLEAN =
        new PrimitiveConverter<>(Schema.Type.BOOLEAN, Boolean.class, bool -> bool);
    public static final AvroTypeConverter STRING =
        new PrimitiveConverter<>(Schema.Type.STRING, String.class, string -> string);
    public static final AvroTypeConverter INT =
        new PrimitiveConverter<>(Schema.Type.INT, Number.class, Number::intValue);
    public static final AvroTypeConverter LONG =
        new PrimitiveConverter<>(Schema.Type.LONG, Number.class, Number::longValue);
    public static final AvroTypeConverter DOUBLE =
        new PrimitiveConverter<>(Schema.Type.DOUBLE, Number.class, Number::doubleValue);
    public static final AvroTypeConverter FLOAT =
        new PrimitiveConverter<>(Schema.Type.FLOAT, Number.class, Number::floatValue);
    public static final AvroTypeConverter BYTES =
        new PrimitiveConverter<>(Schema.Type.BYTES, String.class,
            value -> ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));

    private final Schema.Type avroType;
    private final Function<T, Object> mapper;

    protected PrimitiveConverter(Schema.Type avroType, Class<T> javaType, Function<T, Object> mapper) {
        super(javaType);
        this.avroType = avroType;
        this.mapper = mapper;
    }

    @Override
    public Object convertValue(Schema.Field field, Schema schema, T value, Deque<String> path, boolean silently) {
        return mapper.apply(value);
    }

    @Override
    public boolean canManage(Schema schema, Object jsonValue, Deque<String> path) {
        return schema.getType().equals(avroType);
    }

}
