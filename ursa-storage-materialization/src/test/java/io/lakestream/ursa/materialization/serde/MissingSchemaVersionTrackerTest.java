/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class MissingSchemaVersionTrackerTest {

    @Test
    void testRecordsUpToMaxMessageIds() {
        MissingSchemaVersionTracker tracker = new MissingSchemaVersionTracker(3);

        tracker.record("msg-0");
        tracker.record("msg-1");
        tracker.record("msg-2");
        tracker.record("msg-3");
        tracker.record("msg-4");

        assertEquals(5, tracker.getTotalCount());

        List<String> recorded = tracker.getRecordedMessageIds();
        assertEquals(3, recorded.size());
        assertEquals("msg-0", recorded.get(0));
        assertEquals("msg-1", recorded.get(1));
        assertEquals("msg-2", recorded.get(2));
    }

    @Test
    void testDefaultMaxIs10() {
        MissingSchemaVersionTracker tracker = new MissingSchemaVersionTracker();

        for (int i = 0; i < 15; i++) {
            tracker.record("msg-" + i);
        }

        assertEquals(15, tracker.getTotalCount());
        assertEquals(10, tracker.getRecordedMessageIds().size());
    }

    @Test
    void testEmptyTracker() {
        MissingSchemaVersionTracker tracker = new MissingSchemaVersionTracker();

        assertEquals(0, tracker.getTotalCount());
        assertEquals(0, tracker.getRecordedMessageIds().size());
    }
}
