/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.compact;

import io.lakestream.ursa.exception.ExceptionWithCode;
import io.lakestream.ursa.lakehouse.utils.TopicName;
import io.lakestream.ursa.lakehouse.v2.LakehouseRecordWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DLTFailureMessageHandler implements FailureMessageHandler {

    private static final int MAX_DLT_MESSAGE_IDS_PER_REASON = 10;
    private static final int MAX_DISTINCT_DLT_FAILURE_REASONS = 100;

    private final LakehouseRecordWriter<FailureMessage> lakehouseWriter;
    private final AtomicInteger dltRecordCount = new AtomicInteger(0);
    private final Map<String, List<String>> dltFailureReasonWithMessageIds = new BoundedReasonMap();
    private String topic;
    private int partition = -1;

    private DLTFailureMessageHandler(LakehouseRecordWriter<FailureMessage> lakehouseWriter) {
        this.lakehouseWriter = lakehouseWriter;
    }

    public static DLTFailureMessageHandler of(LakehouseRecordWriter<FailureMessage> lakehouseWriter) {
        return new DLTFailureMessageHandler(lakehouseWriter);
    }

    @Override
    public CompletableFuture<Void> sendFailureMessage(FailureMessage failureMessage) {
        if (topic == null && failureMessage.getTopic() != null) {
            topic = failureMessage.getTopic();
            partition = TopicName.get(topic).getPartitionIndex();
        }
        String failureReason = failureMessage.getFailureReason() != null
            ? failureMessage.getFailureReason() : "Unknown error";
        dltRecordCount.incrementAndGet();
        String messageId = failureMessage.getMessageId() != null
            ? failureMessage.getMessageId() : "unknown";
        List<String> messageIds = dltFailureReasonWithMessageIds
            .computeIfAbsent(failureReason, k -> new CopyOnWriteArrayList<>());
        if (messageIds.size() < MAX_DLT_MESSAGE_IDS_PER_REASON) {
            messageIds.add(messageId);
        }
        try {
            lakehouseWriter.write(failureMessage);
        } catch (ExceptionWithCode e) {
            return CompletableFuture.failedFuture(e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
        int totalDltRecords = dltRecordCount.get();
        if (totalDltRecords > 0) {
            log.warn("Sent {} record(s) to DLT for topic: {}, partition: {}, failure reasons with messageIds: {}",
                    totalDltRecords, topic, partition, dltFailureReasonWithMessageIds);
        }
    }

    int getDltRecordCount() {
        return dltRecordCount.get();
    }

    Map<String, List<String>> getDltFailureReasonWithMessageIds() {
        return Collections.unmodifiableMap(dltFailureReasonWithMessageIds);
    }

    /**
     * Bounded and normalizing map for DLT failure reasons to prevent unbounded
     * growth and excessively large log lines.
     */
    static final class BoundedReasonMap extends ConcurrentHashMap<String, List<String>> {
        private static final String OTHER_REASON_KEY = "OTHER";
        private static final String UNKNOWN_REASON_KEY = "UNKNOWN";
        private static final int MAX_REASON_KEY_LENGTH = 128;
        @Override
        public List<String> put(String key, List<String> value) {
            String normalizedKey = normalizeKey(key);
            return super.put(normalizedKey, value);
        }
        @Override
        public List<String> putIfAbsent(String key, List<String> value) {
            String normalizedKey = normalizeKey(key);
            return super.putIfAbsent(normalizedKey, value);
        }
        @Override
        public void putAll(Map<? extends String, ? extends List<String>> m) {
            if (m == null || m.isEmpty()) {
                return;
            }
            for (Map.Entry<? extends String, ? extends List<String>> entry : m.entrySet()) {
                this.put(entry.getKey(), entry.getValue());
            }
        }
        @Override
        public List<String> computeIfAbsent(String key,
                                            Function<? super String, ? extends List<String>> mappingFunction) {
            String normalizedKey = normalizeKey(key);
            return super.computeIfAbsent(normalizedKey, mappingFunction);
        }
        /**
         * Normalize the raw reason key by handling null/empty values, truncating
         * to a fixed maximum length, and enforcing a maximum number of distinct
         * keys by directing additional new reasons into a shared "OTHER" bucket.
         */
        private String normalizeKey(String rawKey) {
            String key = (rawKey == null || rawKey.isEmpty()) ? UNKNOWN_REASON_KEY : rawKey;
            if (size() >= MAX_DISTINCT_DLT_FAILURE_REASONS && !super.containsKey(key)) {
                key = OTHER_REASON_KEY;
            }
            if (key.length() > MAX_REASON_KEY_LENGTH) {
                key = key.substring(0, MAX_REASON_KEY_LENGTH);
            }
            return key;
        }
    }
}
