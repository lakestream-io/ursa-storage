/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.catalog.metadata;

import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Persisted catalog metadata for a log. */
@EqualsAndHashCode
@Getter
@ToString
public class LogMetadata {

    /**
     * Sentinel for a legacy metadata node whose content is an empty byte array.
     *
     * <p>The empty representation remains supported so existing catalogs can be read and initialized
     * without rewriting their persisted metadata.
     */
    public static final LogMetadata EMPTY = new LogMetadata(-1L, null, OptionalLong.empty());

    private final long streamId;
    @Setter
    private volatile Map<String, String> properties;
    @Setter
    private volatile OptionalLong terminatedOffset;

    // Required for JSON deserialization.
    public LogMetadata() {
        this(-1L, null, OptionalLong.empty());
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset) {
        this.streamId = streamId;
        this.properties = properties == null ? new TreeMap<>() : properties;
        this.terminatedOffset = terminatedOffset;
    }

    public long streamId() {
        return streamId;
    }

    public Map<String, String> properties() {
        return properties;
    }

    public OptionalLong terminatedOffset() {
        return terminatedOffset;
    }
}
