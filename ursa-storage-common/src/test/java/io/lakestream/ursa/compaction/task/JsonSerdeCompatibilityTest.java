/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.compaction.task;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonSerdeCompatibilityTest {

    @Test
    void preservesCompactedOffsetJsonFormat() throws Exception {
        CompactedOffset offset = new CompactedOffset(1L, 2L, 3L);

        byte[] content = CompactOffsetSerde.INSTANCE.serialize(offset);

        assertEquals("{\"id\":1,\"offset\":2,\"cumulativeSize\":3}",
                new String(content, StandardCharsets.UTF_8));
        assertEquals(offset, CompactOffsetSerde.INSTANCE.deserialize(content));
    }

    @Test
    void preservesPreparedTaskJsonFormat() throws Exception {
        PreparedCompactStreamTask task = PreparedCompactStreamTask.builder()
                .streamId(1L)
                .startOffset(2L)
                .endOffset(3L)
                .totalSize(4L)
                .cumulativeSize(5L)
                .status(PreparedCompactStreamTask.PUSHED_TASK)
                .taskName("task")
                .topic("org/analytics/stream")
                .properties(Map.of("key", "value"))
                .build();

        byte[] content = PreparedCompactStreamTaskSerde.INSTANCE.serialize(task);

        assertEquals("{\"streamId\":1,\"startOffset\":2,\"endOffset\":3,\"totalSize\":4,"
                        + "\"cumulativeSize\":5,\"status\":1,\"taskName\":\"task\","
                        + "\"topic\":\"org/analytics/stream\",\"properties\":{\"key\":\"value\"}}",
                new String(content, StandardCharsets.UTF_8));
        assertEquals(task, PreparedCompactStreamTaskSerde.INSTANCE.deserialize(content));
    }
}
