/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import static java.util.stream.Collectors.toList;

import io.lakestream.ursa.materialization.serde.utils.json.JsonToAvroReader;
import java.util.Collection;
import java.util.Deque;
import org.apache.avro.Schema;

public class ArrayConverter extends AvroTypeConverterWithStrictJavaTypeCheck<Collection> {
    private final JsonToAvroReader jsonToAvroReader;

    public ArrayConverter(JsonToAvroReader jsonToAvroReader) {
        super(Collection.class);
        this.jsonToAvroReader = jsonToAvroReader;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object convertValue(Schema.Field field, Schema schema, Collection value, Deque<String> path,
                               boolean silently) {
        return ((Collection<Object>) value).stream()
                .map(item -> this.jsonToAvroReader.read(field, schema.getElementType(), item, path, false))
                .collect(toList());
    }

    @Override
    public boolean canManage(Schema schema, Object jsonValue, Deque<String> path) {
        return schema.getType().equals(Schema.Type.ARRAY);
    }
}
