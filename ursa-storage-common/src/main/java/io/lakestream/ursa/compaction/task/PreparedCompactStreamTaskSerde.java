/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.task;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.lakestream.ursa.json.UrsaObjectMapperFactory;
import java.io.IOException;

public class PreparedCompactStreamTaskSerde {

    public static final PreparedCompactStreamTaskSerde INSTANCE = new PreparedCompactStreamTaskSerde();

    private final ObjectReader objectReader;
    private final ObjectWriter objectWriter;

    public PreparedCompactStreamTaskSerde() {
        JavaType typeRef = TypeFactory.defaultInstance().constructType(PreparedCompactStreamTask.class);
        this.objectReader = UrsaObjectMapperFactory.getMapper().reader().forType(typeRef);
        this.objectWriter = UrsaObjectMapperFactory.getMapper().writer().forType(typeRef);
    }

    public byte[] serialize(PreparedCompactStreamTask value) throws IOException {
        return objectWriter.writeValueAsBytes(value);
    }

    public PreparedCompactStreamTask deserialize(byte[] content) throws IOException {
        // Legacy catalog tools can create the metadata path with empty content.
        if (content.length == 0) {
            throw new IOException("The content is empty");
        }
        return objectReader.readValue(content);
    }

}
