/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import java.util.Deque;
import org.apache.avro.Schema;

public class NullConverter implements AvroTypeConverter {
    public static final NullConverter INSTANCE = new NullConverter();

    private NullConverter() {

    }

    @Override
    public Object convert(Schema.Field field, Schema schema, Object jsonValue, Deque<String> path, boolean silently) {
        return jsonValue == null ? null : new Incompatible("NULL");
    }

    @Override
    public boolean canManage(Schema schema, Object jsonValue, Deque<String> path) {
        return schema.getType().equals(Schema.Type.NULL);
    }
}
