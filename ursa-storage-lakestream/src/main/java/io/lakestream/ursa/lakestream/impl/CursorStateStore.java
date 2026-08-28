/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakestream.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxia.client.api.AsyncOxiaClient;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CursorStateStore {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static String key(long streamId, long cursorId) {
        return String.format("mark-delete-%d-%d", streamId, cursorId);
    }

    public CompletableFuture<Void> removeMarkDeletePosition(AsyncOxiaClient oxia,
                                                             long streamId,
                                                             @Nullable Long cursorId) {
        if (cursorId == null) {
            return CompletableFuture.completedFuture(null);
        }
        return oxia.delete(key(streamId, cursorId)).thenApply(b -> null);
    }

    public CompletableFuture<Void> writeMarkDeletePosition(AsyncOxiaClient oxia,
                                                            long streamId,
                                                            @Nullable Long cursorId,
                                                            long offset,
                                                            Map<String, Long> properties) {
        if (cursorId == null) {
            return CompletableFuture.completedFuture(null);
        }

        MarkDeleteRecord md = new MarkDeleteRecord(offset, properties);
        try {
            byte[] value = OBJECT_MAPPER.writeValueAsBytes(md);
            return oxia.put(key(streamId, cursorId), value).thenApply(v -> null);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    public CompletableFuture<MarkDeleteRecord> readMarkDeletePosition(AsyncOxiaClient oxia,
                                                                       long streamId,
                                                                       @Nullable Long cursorId) {
        if (cursorId == null) {
            return CompletableFuture.completedFuture(null);
        }
        return oxia.get(key(streamId, cursorId))
                .thenApply(gr -> {
                    if (gr == null) {
                        return new MarkDeleteRecord(-1, Collections.emptyMap());
                    } else {
                        try {
                            return OBJECT_MAPPER.readValue(gr.value(), MarkDeleteRecord.class);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }
}
