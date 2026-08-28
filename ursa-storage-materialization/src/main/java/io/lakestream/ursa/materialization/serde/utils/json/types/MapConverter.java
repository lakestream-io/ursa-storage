/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import io.lakestream.ursa.materialization.serde.utils.json.JsonToAvroReader;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;

public class MapConverter extends AvroTypeConverterWithStrictJavaTypeCheck<Map> {
    private final JsonToAvroReader recordRecord;

    public MapConverter(JsonToAvroReader recordRecord) {
        super(Map.class);
        this.recordRecord = recordRecord;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object convertValue(Schema.Field field, Schema schema, Map jsonValue, Deque<String> path, boolean silently) {
        Map<String, Object> result = new HashMap<>(jsonValue.size());
        ((Map<String, Object>) jsonValue).forEach((k, v) ->
            result.put(k, this.recordRecord.read(field, schema.getValueType(), v, path, false))
        );
        return result;
    }

    @Override
    public boolean canManage(Schema schema, Object jsonValue, Deque<String> path) {
        return schema.getType().equals(Schema.Type.MAP);
    }

}
