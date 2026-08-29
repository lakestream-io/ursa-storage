/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.catalog.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Set<Long> retiredStreamIds;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Set<Long> purgeableRetiredStreamIds;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Set<RetiredStreamMapping> retiredStreamMappings;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Set<String> retiredMappingKeys;
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
        this(streamId, properties, terminatedOffset,
            registrationIncarnationId, registrationOwnerToken, null, deleted);
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset,
                       @Nullable String registrationIncarnationId,
                       @Nullable String registrationOwnerToken,
                       @Nullable Long registrationOwnerGeneration,
                       boolean deleted) {
        this(streamId, properties, terminatedOffset, registrationIncarnationId,
            registrationOwnerToken, registrationOwnerGeneration, deleted, Set.of());
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset,
                       @Nullable String registrationIncarnationId,
                       @Nullable String registrationOwnerToken,
                       @Nullable Long registrationOwnerGeneration,
                       boolean deleted,
                       Set<Long> retiredStreamIds) {
        this(streamId, properties, terminatedOffset, registrationIncarnationId,
            registrationOwnerToken, registrationOwnerGeneration, deleted,
            retiredStreamIds, Set.of(), Set.of());
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset,
                       @Nullable String registrationIncarnationId,
                       @Nullable String registrationOwnerToken,
                       @Nullable Long registrationOwnerGeneration,
                       boolean deleted,
                       Set<Long> retiredStreamIds,
                       Set<Long> purgeableRetiredStreamIds) {
        this(streamId, properties, terminatedOffset, registrationIncarnationId,
            registrationOwnerToken, registrationOwnerGeneration, deleted,
            retiredStreamIds, purgeableRetiredStreamIds, Set.of(), Set.of());
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset,
                       @Nullable String registrationIncarnationId,
                       @Nullable String registrationOwnerToken,
                       @Nullable Long registrationOwnerGeneration,
                       boolean deleted,
                       Set<Long> retiredStreamIds,
                       Set<Long> purgeableRetiredStreamIds,
                       Set<RetiredStreamMapping> retiredStreamMappings) {
        this(streamId, properties, terminatedOffset, registrationIncarnationId,
            registrationOwnerToken, registrationOwnerGeneration, deleted,
            retiredStreamIds, purgeableRetiredStreamIds, retiredStreamMappings,
            mappingKeys(retiredStreamMappings));
    }

    public LogMetadata(long streamId, Map<String, String> properties, OptionalLong terminatedOffset,
                       @Nullable String registrationIncarnationId,
                       @Nullable String registrationOwnerToken,
                       @Nullable Long registrationOwnerGeneration,
                       boolean deleted,
                       Set<Long> retiredStreamIds,
                       Set<Long> purgeableRetiredStreamIds,
                       Set<RetiredStreamMapping> retiredStreamMappings,
                       Set<String> retiredMappingKeys) {
        this.streamId = streamId;
        this.properties = properties == null ? new TreeMap<>() : properties;
        this.terminatedOffset = Objects.requireNonNull(terminatedOffset, "terminatedOffset");
        this.registrationIncarnationId = registrationIncarnationId;
        this.registrationOwnerToken = registrationOwnerToken;
        this.registrationOwnerGeneration = registrationOwnerGeneration;
        this.deleted = deleted;
        this.retiredStreamIds = normalizeRetiredStreamIds(retiredStreamIds);
        this.purgeableRetiredStreamIds = normalizeRetiredStreamIds(
            purgeableRetiredStreamIds);
        this.retiredStreamMappings = normalizeRetiredStreamMappings(
            retiredStreamMappings);
        this.retiredMappingKeys = normalizeRetiredMappingKeys(retiredMappingKeys);
        validateRegistrationIdentity();
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

    /**
     * Physical stream IDs whose durable retirement cleanup is still pending.
     *
     * <p>Storage mapping fences, rather than an ever-growing ID history, prevent reuse after this
     * cleanup journal is acknowledged and cleared.
     */
    public Set<Long> retiredStreamIds() {
        return retiredStreamIds;
    }

    /**
     * Retired IDs whose physical data is safe to delete.
     *
     * <p>This is a subset of {@link #retiredStreamIds()}. An ID retired by a non-purging drop is
     * fenced from reuse but deliberately excluded so its data remains intact.
     */
    public Set<Long> purgeableRetiredStreamIds() {
        return purgeableRetiredStreamIds;
    }

    /** Expected-ID mapping deletions retained for crash-safe cleanup replay. */
    public Set<RetiredStreamMapping> retiredStreamMappings() {
        return retiredStreamMappings;
    }

    /** Mapping keys retained while their durable fence cleanup remains replayable. */
    public Set<String> retiredMappingKeys() {
        return retiredMappingKeys;
    }

    void validateRegistrationIdentity() {
        retiredStreamIds = normalizeRetiredStreamIds(retiredStreamIds);
        purgeableRetiredStreamIds = normalizeRetiredStreamIds(
            purgeableRetiredStreamIds);
        retiredStreamMappings = normalizeRetiredStreamMappings(retiredStreamMappings);
        retiredMappingKeys = normalizeRetiredMappingKeys(retiredMappingKeys);
        if (!retiredStreamIds.containsAll(purgeableRetiredStreamIds)) {
            throw new IllegalArgumentException(
                "Purgeable retired stream IDs must also be retired");
        }
        for (RetiredStreamMapping mapping : retiredStreamMappings) {
            if (mapping.streamId() >= 0
                    && !retiredStreamIds.contains(mapping.streamId())) {
                throw new IllegalArgumentException(
                    "Retired stream mappings must reference retired stream IDs");
            }
            if (!retiredMappingKeys.contains(mapping.mappingKey())) {
                throw new IllegalArgumentException(
                    "Retired stream mapping keys must be retained for replay");
            }
        }
        boolean hasIncarnation = registrationIncarnationId != null;
        boolean hasOwner = registrationOwnerToken != null;
        boolean hasGeneration = registrationOwnerGeneration != null;
        if (hasIncarnation != hasOwner || hasOwner != hasGeneration) {
            throw new IllegalArgumentException(
                "Registration identity requires incarnation, owner, and generation together");
        }
        if (hasGeneration && registrationOwnerGeneration < 0) {
            throw new IllegalArgumentException("Registration owner generation must be non-negative");
        }
    }

    private static Set<Long> normalizeRetiredStreamIds(@Nullable Set<Long> retiredStreamIds) {
        if (retiredStreamIds == null || retiredStreamIds.isEmpty()) {
            return Set.of();
        }
        TreeSet<Long> normalized = new TreeSet<>();
        for (Long retiredStreamId : retiredStreamIds) {
            if (retiredStreamId == null || retiredStreamId < 0) {
                throw new IllegalArgumentException(
                    "Retired stream IDs must be non-null and non-negative");
            }
            normalized.add(retiredStreamId);
        }
        return Collections.unmodifiableSet(normalized);
    }

    private static Set<String> normalizeRetiredMappingKeys(
            @Nullable Set<String> retiredMappingKeys) {
        if (retiredMappingKeys == null || retiredMappingKeys.isEmpty()) {
            return Set.of();
        }
        TreeSet<String> normalized = new TreeSet<>();
        for (String mappingKey : retiredMappingKeys) {
            if (mappingKey == null || mappingKey.isBlank()) {
                throw new IllegalArgumentException(
                    "Retired stream mapping keys must be non-blank");
            }
            normalized.add(mappingKey);
        }
        return Collections.unmodifiableSet(normalized);
    }

    private static Set<String> mappingKeys(
            @Nullable Set<RetiredStreamMapping> retiredStreamMappings) {
        if (retiredStreamMappings == null || retiredStreamMappings.isEmpty()) {
            return Set.of();
        }
        TreeSet<String> mappingKeys = new TreeSet<>();
        for (RetiredStreamMapping mapping : retiredStreamMappings) {
            mappingKeys.add(Objects.requireNonNull(
                mapping, "retiredStreamMapping").mappingKey());
        }
        return mappingKeys;
    }

    private static Set<RetiredStreamMapping> normalizeRetiredStreamMappings(
            @Nullable Set<RetiredStreamMapping> retiredStreamMappings) {
        if (retiredStreamMappings == null || retiredStreamMappings.isEmpty()) {
            return Set.of();
        }
        TreeMap<Long, TreeMap<String, Boolean>> merged = new TreeMap<>();
        for (RetiredStreamMapping mapping : retiredStreamMappings) {
            RetiredStreamMapping checked = Objects.requireNonNull(
                mapping, "retiredStreamMapping");
            merged.computeIfAbsent(checked.streamId(), ignored -> new TreeMap<>())
                .merge(checked.mappingKey(), checked.purge(), Boolean::logicalOr);
        }
        TreeSet<RetiredStreamMapping> normalized = new TreeSet<>();
        merged.forEach((streamId, mappings) -> mappings.forEach((mappingKey, purge) ->
            normalized.add(new RetiredStreamMapping(streamId, mappingKey, purge))));
        return Collections.unmodifiableSet(normalized);
    }

    /** A fenced mapping cleanup that remains replayable after lifecycle reincarnation. */
    public record RetiredStreamMapping(long streamId, String mappingKey, boolean purge)
            implements Comparable<RetiredStreamMapping> {

        /** Reads the historical exact-ID journal representation, whose purge intent is separate. */
        public RetiredStreamMapping(long streamId, String mappingKey) {
            this(streamId, mappingKey, false);
        }

        public RetiredStreamMapping {
            if (streamId < -1) {
                throw new IllegalArgumentException(
                    "Retired stream mapping ID must be -1 or non-negative");
            }
            if (mappingKey == null || mappingKey.isBlank()) {
                throw new IllegalArgumentException(
                    "Retired stream mapping key must be non-blank");
            }
        }

        @Override
        public int compareTo(RetiredStreamMapping other) {
            int idComparison = Long.compare(streamId, other.streamId);
            if (idComparison != 0) {
                return idComparison;
            }
            int keyComparison = mappingKey.compareTo(other.mappingKey);
            if (keyComparison != 0) {
                return keyComparison;
            }
            return Boolean.compare(purge, other.purge);
        }
    }
}
