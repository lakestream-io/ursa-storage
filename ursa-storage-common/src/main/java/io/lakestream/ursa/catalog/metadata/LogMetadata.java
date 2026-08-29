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
import org.jetbrains.annotations.Nullable;

/**
 * Persisted catalog metadata for a log.
 *
 * <p>The nullable registration fields provide one-way compatibility for reading metadata written
 * before lifecycle fencing was introduced. Mixed-version writers are not supported: an older
 * writer can discard these fields or overwrite a deletion tombstone during a read-modify-write.
 */
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
    @Nullable
    private final String registrationIncarnationId;
    @Nullable
    private final String registrationOwnerToken;
    @Nullable
    private final Long registrationOwnerGeneration;
    private final boolean deleted;

    // Required for JSON deserialization.
    public LogMetadata() {
        this(-1L, null, OptionalLong.empty());
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset) {
        this(streamId, properties, terminatedOffset, null, null, null, false);
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset,
                       @Nullable String registrationIncarnationId,
                       @Nullable String registrationOwnerToken) {
        this(streamId, properties, terminatedOffset,
            registrationIncarnationId, registrationOwnerToken, null, false);
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset,
                       @Nullable String registrationIncarnationId,
                       @Nullable String registrationOwnerToken,
                       boolean deleted) {
        this(streamId, properties, terminatedOffset, registrationIncarnationId,
            registrationOwnerToken, null, deleted);
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset,
                       @Nullable String registrationIncarnationId,
                       @Nullable String registrationOwnerToken,
                       @Nullable Long registrationOwnerGeneration,
                       boolean deleted) {
        this.streamId = streamId;
        this.properties = properties == null ? new TreeMap<>() : properties;
        this.terminatedOffset = terminatedOffset;
        this.registrationIncarnationId = registrationIncarnationId;
        this.registrationOwnerToken = registrationOwnerToken;
        this.registrationOwnerGeneration = registrationOwnerGeneration;
        this.deleted = deleted;
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

    @Nullable
    public String registrationIncarnationId() {
        return registrationIncarnationId;
    }

    @Nullable
    public String registrationOwnerToken() {
        return registrationOwnerToken;
    }

    @Nullable
    public Long registrationOwnerGeneration() {
        return registrationOwnerGeneration;
    }

    public boolean deleted() {
        return deleted;
    }
}
