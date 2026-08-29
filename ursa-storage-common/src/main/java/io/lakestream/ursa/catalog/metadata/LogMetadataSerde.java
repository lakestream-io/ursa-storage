/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.catalog.metadata;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.lakestream.ursa.json.UrsaObjectMapperFactory;
import java.io.IOException;

/** JSON codec for persisted log metadata. */
public final class LogMetadataSerde {

    private static final JavaType TYPE_REF = TypeFactory.defaultInstance().constructType(LogMetadata.class);

    public static final LogMetadataSerde INSTANCE = new LogMetadataSerde();

    private final ObjectReader objectReader = UrsaObjectMapperFactory.getMapper().reader().forType(TYPE_REF);
    private final ObjectWriter objectWriter = UrsaObjectMapperFactory.getMapper().writer().forType(TYPE_REF);

    private LogMetadataSerde() {
    }

    public byte[] serialize(String path, LogMetadata value) throws IOException {
        if (value == LogMetadata.EMPTY) {
            return new byte[0];
        }
        return objectWriter.writeValueAsBytes(value);
    }

    public LogMetadata deserialize(String path, byte[] content) throws IOException {
        if (content.length == 0) {
            return LogMetadata.EMPTY;
        }
        LogMetadata metadata = objectReader.readValue(content);
        metadata.validateRegistrationIdentity();
        return metadata;
    }
}
