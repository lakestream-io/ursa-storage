/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl;

import static io.lakestream.ursa.storage.impl.StorageConfig.PROTOBUF_VERSION;
import static io.lakestream.ursa.storage.impl.StorageConfig.STRING_VERSION;

import io.lakestream.api.EntryHeader;
import io.lakestream.api.EntryIndex;
import io.lakestream.ursa.storage.Key;
import io.lakestream.ursa.storage.Value;
import io.oxia.client.api.GetResult;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageFormat {

    // stream id generator related constants
    public static final String STREAM_ID_GENERATOR_PATH = "/stream-id-generator";
    public static final byte[] STREAM_ID_GENERATOR_VALUE = new byte[0];

    // stream register path
    public static final String STREAM_REGISTER_PATH = "/stream-id";

    // Durable terminal fences and ephemeral opened-handle leases for numeric stream IDs.
    public static final String STREAM_WRITE_FENCE_PATH = "/stream-write-fences";
    public static final String STREAM_WRITE_LEASE_PATH = "/stream-write-leases";

    public static final String MARK_DELETED_OFFSET_PATH = "/mark-deleted-offsets";

    @Getter
    private final int indexSerializeFormatVersion;


    // string cache to avoid calculate the stream key multiple times
    // for each stream id, we need to convert it to a 20-digit string then use it to access the oxia
    private final Map<Long, String> keyStringCache = new ConcurrentHashMap<>();
    private final Map<Long, String> largestKeyStringCache = new ConcurrentHashMap<>();
    private final Map<Long, String> smallestKeyStringCache = new ConcurrentHashMap<>();
    public static final String FIRST_UNCOMPACTED_OFFSET = "/first-uncompacted-offset";

    public StorageFormat(StorageConfig config) {
        this.indexSerializeFormatVersion = config.getIndexSerializeFormatVersion();
    }

    public boolean isProtobufFormat() {
        return this.indexSerializeFormatVersion >= PROTOBUF_VERSION;
    }

    public int getNonCompactedIndexFormatVersion() {
        return STRING_VERSION;
    }

    public EntryHeader getEntryHeader(GetResult result) {
        Key key = Key.parse(result.key());
        Value value = Value.parse(indexSerializeFormatVersion, result.value());
        int numberOfMessages = (int) value.numberOfMessages();
        long offset = key.offset() - numberOfMessages;
        return new EntryHeader(offset, numberOfMessages, result.version().createdTimestamp(),
                (int) value.entrySize(), key.cumulativeSize());
    }

    public EntryIndex getEntryIndex(GetResult result) {
        Key key = Key.parse(result.key());
        Value value = Value.parse(indexSerializeFormatVersion, result.value());
        int numberOfMessages = (int) value.numberOfMessages();
        long offset = key.offset() - numberOfMessages;
        var header = new EntryHeader(offset, numberOfMessages, result.version().createdTimestamp(),
                (int) value.entrySize(), key.cumulativeSize());
        return new EntryIndex(header,
                value.position(),
                value.entryCount(),
                EntryIndex.IndexType.valueOf(value.indexType().name()),
                value.entryOffsets(), value.extraData());
    }

    public String getStreamIdKey(long streamId) {
        return keyStringCache.computeIfAbsent(streamId, k -> String.format("%020d", k));
    }

    public String getLargestStreamIdKey(long streamId) {
        return largestKeyStringCache.computeIfAbsent(streamId, k -> Key.largestKey(streamId).toString());
    }

    public String getSmallestStreamIdKey(long streamId) {
        return smallestKeyStringCache.computeIfAbsent(streamId, k -> Key.smallestKey(streamId).toString());
    }

    public void removeCachedKey(long streamId) {
        keyStringCache.remove(streamId);
        smallestKeyStringCache.remove(streamId);
        largestKeyStringCache.remove(streamId);
    }
}
