/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.materialization;

import io.lakestream.api.materialization.EvolutionPolicy;

/**
 * Pluggable sink contract for stream-to-table materialization.
 *
 * <p>Implementations are built by a {@link TableMaterializerFactory} and
 * called by the orchestrator (T9 / T10) for each batch of records to write.
 * The {@code R} type is the sink-side record type — Delta-Kernel {@code Row},
 * Iceberg {@code GenericRecord}, ClickHouse {@code Map<String,Object>}, etc.
 * Callers above the factory layer see {@code TableMaterializer<?>} and never
 * have to know the concrete record type.
 *
 * <p>Lifecycle: write* → commit → write* → commit → ... → close.
 *
 * @param <R> the sink-side record type
 */
public interface TableMaterializer<R> extends AutoCloseable {

    /**
     * Materializes a single record. Implementations buffer until {@link #commit()}.
     *
     * <p>Ownership of {@code record} transfers to this materializer as soon as this method is
     * invoked, including when the method rejects the record or throws. If the record owns a
     * reference-counted resource, the implementation must release exactly that owned reference on
     * every success and failure path. The caller must neither access nor release the record after
     * invoking this method.
     *
     * @param record  the record to materialize (sink-specific type {@code R})
     * @param context per-record materialization context (offsets, schema
     *                version, source-format metadata)
     * @throws MaterializationException if the record is unwritable
     */
    void write(R record, MaterializationContext context);

    /**
     * Commits the buffered records to the sink. Idempotency depends on the
     * sink: on Iceberg / Delta this is a transactional snapshot; on ClickHouse
     * this may be a batched INSERT with engine-side dedup. Throws on commit
     * failure; the framework will retry up to
     * {@code FrameworkConf.commit.maxRetries} times.
     *
     * @return sink-specific commit metadata wrapped in a {@link CommitResult}
     * @throws MaterializationException if the commit cannot be completed
     */
    CommitResult commit();

    /** Closes the underlying writer and releases resources. */
    @Override
    void close();

    /**
     * Returns the schema-evolution operations this sink supports.
     *
     * <p>The framework gates incoming evolutions against this policy before
     * forwarding them. Rejected ops surface as {@link MaterializationException}
     * with
     * {@link io.lakestream.ursa.exception.ExceptionCode#MESSAGE_SCHEMA_INCOMPATIBLE}.
     */
    EvolutionPolicy supportedEvolutions();
}
