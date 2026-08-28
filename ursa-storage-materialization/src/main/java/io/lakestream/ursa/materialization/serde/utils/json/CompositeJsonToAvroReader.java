/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json;

import io.lakestream.ursa.materialization.serde.utils.json.types.ArrayConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.AvroTypeConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.BytesArrayConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.BytesDecimalConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.EnumConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.IntDateConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.IntTimeMillisConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.LongTimeMicrosConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.LongTimestampMicrosConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.LongTimestampMillisConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.MapConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.NullConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.PrimitiveConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.RecordConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.UUIDConverter;
import io.lakestream.ursa.materialization.serde.utils.json.types.UnionConverter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class CompositeJsonToAvroReader implements JsonToAvroReader {
    private final List<AvroTypeConverter> converters;
    private final AvroTypeConverter mainRecordConverter;

    public CompositeJsonToAvroReader() {
        this(Collections.emptyList(), null);
    }

    /**
     * this constructor allows you to specify custom converters. It can be used to convert
     * custom java types, or avro schema with logical type.
     *
     * @param additionalConverters additional converters that implement {@link AvroTypeConverter}.
     *                             These converters will override default converters.
     */
    public CompositeJsonToAvroReader(List<AvroTypeConverter> additionalConverters) {
        this(additionalConverters, null);
    }

    /**
     * this constructor allows you to specify custom converters. It can be used to convert custom
     * java types, or avro schema with logical type.
     *
     * @param additionalConverters additional converters that implement {@link AvroTypeConverter}.
     *                             These converters will override default converters.
     */
    public CompositeJsonToAvroReader(AvroTypeConverter... additionalConverters) {
        this(Arrays.asList(additionalConverters));
    }

    /**
     * this constructor allows you to specify custom converters. It can be used to convert custom
     * java types, or avro schema with logical type.
     *
     * @param additionalConverters additional converters that implement {@link AvroTypeConverter}.
     *                            These converters will override default converters.
     * @param unknownFieldListener the listener to customize unknown field error management
     */
    public CompositeJsonToAvroReader(List<AvroTypeConverter> additionalConverters,
                                     UnknownFieldListener unknownFieldListener) {
        this.mainRecordConverter = createMainConverter(unknownFieldListener);
        this.converters = new ArrayList<>();
        this.converters.addAll(additionalConverters);
        this.converters.add(BytesDecimalConverter.INSTANCE);
        this.converters.add(BytesArrayConverter.INSTANCE);
        this.converters.add(IntDateConverter.INSTANCE);
        this.converters.add(IntTimeMillisConverter.INSTANCE);
        this.converters.add(LongTimeMicrosConverter.INSTANCE);
        this.converters.add(LongTimestampMillisConverter.INSTANCE);
        this.converters.add(LongTimestampMicrosConverter.INSTANCE);
        this.converters.add(UUIDConverter.INSTANCE);
        this.converters.add(PrimitiveConverter.BOOLEAN);
        this.converters.add(PrimitiveConverter.STRING);
        this.converters.add(PrimitiveConverter.INT);
        this.converters.add(PrimitiveConverter.LONG);
        this.converters.add(PrimitiveConverter.DOUBLE);
        this.converters.add(PrimitiveConverter.FLOAT);
        this.converters.add(PrimitiveConverter.BYTES);
        this.converters.add(EnumConverter.INSTANCE);
        this.converters.add(NullConverter.INSTANCE);
        this.converters.add(mainRecordConverter);
        this.converters.add(new ArrayConverter(this));
        this.converters.add(new MapConverter(this));
        this.converters.add(new UnionConverter(this));
    }

    protected AvroTypeConverter createMainConverter(UnknownFieldListener unknownFieldListener) {
        return new RecordConverter(this, unknownFieldListener);
    }

    @Override
    public GenericData.Record read(Map<String, Object> json, Schema schema) {
        return (GenericData.Record) this.mainRecordConverter
            .convert(null, schema, json, new ArrayDeque<>(), false);
    }

    @Override
    public Object read(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        boolean pushed = !field.name().equals(path.peekLast());
        if (pushed) {
            path.addLast(field.name());
        }

        AvroTypeConverter converter = this.converters.stream()
                .filter(c -> c.canManage(schema, jsonValue, path))
                .findFirst()
                .orElseThrow(() -> new AvroTypeException("Unsupported type: " + field.schema().getType()));
        Object result = converter.convert(field, schema, jsonValue, path, silently);

        if (pushed) {
            path.removeLast();
        }
        return result;
    }
}
