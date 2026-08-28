/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks messages that are missing schema version/schema id and are treated as bytes schema.
 * Records up to {@code maxRecordedMessageIds} message IDs for diagnostic logging.
 */
public class MissingSchemaVersionTracker {

    private static final int DEFAULT_MAX_RECORDED_MESSAGE_IDS = 10;

    private final int maxRecordedMessageIds;
    private final List<String> messageIds = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger totalCount = new AtomicInteger(0);

    public MissingSchemaVersionTracker() {
        this(DEFAULT_MAX_RECORDED_MESSAGE_IDS);
    }

    public MissingSchemaVersionTracker(int maxRecordedMessageIds) {
        this.maxRecordedMessageIds = maxRecordedMessageIds;
    }

    public void record(String messageId) {
        int count = totalCount.incrementAndGet();
        if (count <= maxRecordedMessageIds) {
            messageIds.add(messageId);
        }
    }

    public int getTotalCount() {
        return totalCount.get();
    }

    public List<String> getRecordedMessageIds() {
        return Collections.unmodifiableList(messageIds);
    }
}
