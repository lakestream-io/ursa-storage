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

public class CompactOffsetSerde {

    public static final CompactOffsetSerde INSTANCE = new CompactOffsetSerde();

    private final ObjectReader objectReader;
    private final ObjectWriter objectWriter;

    public CompactOffsetSerde() {
        JavaType typeRef = TypeFactory.defaultInstance().constructType(CompactedOffset.class);
        this.objectReader = UrsaObjectMapperFactory.getMapper().reader().forType(typeRef);
        this.objectWriter = UrsaObjectMapperFactory.getMapper().writer().forType(typeRef);
    }

    public byte[] serialize(CompactedOffset value) throws IOException {
        return objectWriter.writeValueAsBytes(value);
    }

    public CompactedOffset deserialize(byte[] content) throws IOException {
        // Legacy catalog tools can create the metadata path with empty content.
        if (content.length == 0) {
            throw new IOException("The content is empty");
        }
        return objectReader.readValue(content);
    }
}
