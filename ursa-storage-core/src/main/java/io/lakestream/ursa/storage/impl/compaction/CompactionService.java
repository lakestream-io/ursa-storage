/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import io.lakestream.ursa.compaction.CompactTaskManager;
import io.lakestream.ursa.compaction.metrics.CompactionMetrics;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.lakestream.ursa.storage.BaseStreamIDGenerator;
import io.lakestream.ursa.storage.FileStorage;
import io.lakestream.ursa.storage.StorageApi;
import io.lakestream.ursa.storage.impl.StorageConfig;
import io.netty.buffer.ByteBufAllocator;
import io.oxia.client.api.AsyncOxiaClient;

public interface CompactionService {

    /**
     * Compact the stream.
     *
     */
    @Deprecated
    void compactStream(String topic, long streamId) throws Exception;

    /**
     * Legacy internal WAL → Compacted Object compaction.
     *
     * @deprecated Superseded by the materialization SPI dispatch in
     *     {@code CompactionWorker.maybeMaterialize(...)}, which unifies the
     *     SBT (internal compaction) and SDT (external table) paths. This call
     *     is retained as the flag-controlled fallback ({@code materializationEnabled=false})
     *     so deployments can roll back; remove once the SPI path is the sole path.
     */
    @Deprecated
    void compactStream(CompactStreamTask compactStreamTask) throws Exception;

    void initialize(ByteBufAllocator allocator, FileStorage fileStorage, BaseStreamIDGenerator idGenerator,
                    StorageApi storageApi, CompactTaskManager compactTaskManager,
                    StorageConfig config, AsyncOxiaClient oxiaClient,
                    CompactionMetrics compactionMetrics, Object ctx);

    void maintenance();

    static String compactedFileName(long streamId, String index) {
        return String.format("%020d-%s", streamId, index);
    }

    static boolean isCompactedFile(String file) {
        return file.contains("-");
    }

    static long parseCompactedFileIndex(String compactedFile) {
        String[] split = compactedFile.split("-");
        return Long.parseLong(split[1]);
    }

    void close();
}
