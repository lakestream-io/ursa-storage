/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import io.lakestream.api.StreamMetadata;
import io.lakestream.api.materialization.ResolvedMaterialization;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A unit of materialization work handed to {@link MaterializationService#materialize}.
 *
 * <p>Carries the resolved sink and the source-stream offset range. The
 * {@link MaterializationService} reads the source entries for
 * {@code [startOffset, endOffset)} from {@code StorageApi}, decodes each entry
 * into records, and writes them to the sink. The orchestrator does not need to
 * read or carry entries.
 *
 * @param streamMetadata           immutable source stream metadata
 * @param resolvedMaterialization  the resolved sink (catalog + table + effective policy)
 * @param sourceTopic              the canonical partition log name to read (distinct from
 *                                 {@code stream.fullName()})
 * @param streamId                 the numeric stream id the entry reader reads from
 * @param startOffset              inclusive start offset of the range in the source stream
 * @param endOffset                exclusive end offset of the range in the source stream
 * @param sourceTask               the originating compaction task; a Lakehouse sink records its
 *                                 write results onto this task and persists it as {@code COMPACTED}
 *                                 so the group-commit runner can pick it up. May be {@code null}
 *                                 for sinks (e.g. ClickHouse) that commit inline and ignore it.
 */
public record MaterializationTask(
        StreamMetadata streamMetadata,
        ResolvedMaterialization resolvedMaterialization,
        String sourceTopic,
        long streamId,
        long startOffset,
        long endOffset,
        @Nullable CompactStreamTask sourceTask) {

    /** Canonical constructor: validates required fields ({@code sourceTask} is optional). */
    public MaterializationTask {
        Objects.requireNonNull(streamMetadata, "streamMetadata");
        Objects.requireNonNull(resolvedMaterialization, "resolvedMaterialization");
        Objects.requireNonNull(sourceTopic, "sourceTopic");
    }

    /** Back-compat constructor for callers (e.g. ClickHouse, tests) without a source task. */
    public MaterializationTask(StreamMetadata streamMetadata, ResolvedMaterialization resolvedMaterialization,
                               String sourceTopic, long streamId, long startOffset, long endOffset) {
        this(streamMetadata, resolvedMaterialization, sourceTopic, streamId, startOffset, endOffset, null);
    }
}
